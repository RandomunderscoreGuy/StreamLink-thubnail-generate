import os
import subprocess
import urllib.request
import urllib.parse
import urllib.error
import json
import time
from datetime import datetime
import re
import boto3
from botocore.config import Config

# ==========================================
# 1. HELPER FUNCTIONS & LOGGING
# ==========================================
def log(message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


class ColdFileError(Exception):
    """Raised when a file is still warming/caching on debrid (retry after cooldown)."""
    pass


# ==========================================
# 2. CORE CONFIGURATION & HEADERS
# ==========================================
BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN")
API_BASE_URL = "https://streamlink.cloud/api/"
D1_DB_NAME = "streamlink-db"
BATCH_SIZE = 10

CLIP_TIMEOUT_SEC = 45
POSTER_TIMEOUT_SEC = 60
FILE_BUDGET_SEC = 180          # Hard cap per file so 1 bad 4K file never stalls runner
MAX_COLD_ATTEMPTS = 3          # Auto-cancel after 3 tries total
COLD_RETRY_DELAY_SEC = 600     # Wait 10 minutes before retrying a cold/warming file

if not BEARER_TOKEN:
    log("❌ API_BEARER_TOKEN missing from environment variables!")
    exit(1)

headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9"
}

MAX_RUNTIME_SEC = 5.75 * 3600
ENGINE_START_TIME = time.time()
ACTIVE_STORAGE = os.environ.get("ACTIVE_STORAGE", "scaleway").lower()

# In-memory cooldown queue for cold files (avoids full D1 table scans)
# Structure: { raw_hash: { "record": dict, "attempts": int, "next_try_at": float } }
cold_retry_queue = {}

# ==========================================
# 3. MULTI-CLOUD STORAGE ADAPTERS
# ==========================================
class BaseStorageAdapter:
    def upload(self, local_path, s3_key, content_type):
        raise NotImplementedError()


class ScalewayAdapter(BaseStorageAdapter):
    def __init__(self):
        self.bucket = "streamlink-assets"
        self.cdn_base = "https://media.streamlink.cloud"
        self.client = boto3.client(
            "s3", region_name="fr-par", endpoint_url="https://s3.fr-par.scw.cloud",
            aws_access_key_id=os.environ.get("SCW_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("SCW_SECRET_KEY"),
            config=Config(s3={"addressing_style": "virtual"})
        )

    def upload(self, local_path, s3_key, content_type):
        self.client.upload_file(local_path, self.bucket, s3_key, ExtraArgs={"ContentType": content_type, "ACL": "public-read"})
        return f"{self.cdn_base}/{s3_key}"


storage_engine = ScalewayAdapter()

# ==========================================
# 4. DATABASE UPDATE HELPER (Primary Key Lookup)
# ==========================================
def update_db_status(raw_hash, status, poster_url=None, duration=0, res="NULL", codec="NULL", audio=2, is_hdr=0):
    safe_hash = raw_hash.replace("'", "''")
    safe_status = status.replace("'", "''")

    if status.startswith("FAILED") or status == "SKIPPED_NON_VIDEO":
        sql = (
            f"UPDATE global_assets "
            f"SET preview_animation = '{safe_status}', "
            f"migrated = CASE WHEN migrated = 1 THEN 2 ELSE 0 END "
            f"WHERE hash = '{safe_hash}';"
        )
    else:
        poster_val = f"'{poster_url.replace(chr(39), chr(39)*2)}'" if poster_url else "thumbnail"
        screenshots_val = "NULL" if poster_url else "screenshots"
        res_val = f"'{res}'" if res != "NULL" else "NULL"
        codec_val = f"'{codec}'" if codec != "NULL" else "NULL"

        sql = (
            f"UPDATE global_assets SET thumbnail = {poster_val}, screenshots = {screenshots_val}, "
            f"preview_animation = '{safe_status}', duration = {int(duration)}, "
            f"resolution = {res_val}, codec = {codec_val}, "
            f"audio_channels = {int(audio)}, is_hdr = {int(is_hdr)}, "
            f"migrated = CASE WHEN migrated = 1 THEN 2 ELSE 0 END "
            f"WHERE hash = '{safe_hash}';"
        )

    subprocess.run(["npx", "wrangler", "d1", "execute", D1_DB_NAME, "--remote", "--command", sql], stdout=subprocess.DEVNULL)


def cleanup_temp_files():
    for fname in ["thumbnail.jpg", "preview.mp4"] + [f"temp_clip_{i}.mp4" for i in range(5)]:
        if os.path.exists(fname):
            try:
                os.remove(fname)
            except OSError:
                pass


def schedule_or_cancel_cold_file(file_record, raw_hash, reason, current_attempt):
    if current_attempt < MAX_COLD_ATTEMPTS:
        next_try = time.time() + COLD_RETRY_DELAY_SEC
        cold_retry_queue[raw_hash] = {
            "record": file_record,
            "attempts": current_attempt,
            "next_try_at": next_try
        }
        # Mark in D1 so WHERE preview_animation IS NULL moves on immediately
        update_db_status(raw_hash, f"FAILED_COLD_RETRY_{current_attempt}")
        log(f"   🧊 Cold/unready ({reason}). Queued attempt {current_attempt + 1}/{MAX_COLD_ATTEMPTS} in {COLD_RETRY_DELAY_SEC // 60} min.")
    else:
        cold_retry_queue.pop(raw_hash, None)
        update_db_status(raw_hash, "FAILED_COLD_MAX_RETRIES")
        log(f"   🛑 Auto-cancelled after {MAX_COLD_ATTEMPTS} attempts ({reason}). Marked FAILED_COLD_MAX_RETRIES.")


# ==========================================
# 5. SINGLE FILE PROCESSOR
# ==========================================
# VOD-safe network flags: NO -reconnect_at_eof, strict 15s socket timeout
FFMPEG_NET_FLAGS = [
    "-user_agent", headers["User-Agent"],
    "-rw_timeout", "15000000",
    "-reconnect", "1",
    "-reconnect_streamed", "1",
    "-reconnect_delay_max", "3"
]

FFMPEG_HTTP_SEEK = [
    *FFMPEG_NET_FLAGS,
    "-seekable", "1",
    "-noaccurate_seek",
    "-analyzeduration", "5000000",
    "-probesize", "5000000"
]


def process_file(file_record, attempt_num, completed_updates):
    file_start = time.time()
    name = file_record["name"]
    target_url = file_record["target_url"]
    file_size = file_record["size"]
    raw_hash = str(file_record.get("hash", ""))
    provider_file_id = file_record.get("provider_file_id") or ""

    log(f"🎬 Processing (Try {attempt_num}/{MAX_COLD_ATTEMPTS}): {name}")

    if "urn:btih:" not in raw_hash or "||" not in raw_hash:
        update_db_status(raw_hash, "FAILED_MALFORMED_HASH")
        return

    magnet_hash = raw_hash.split("urn:btih:")[1].split("||")[0].upper()
    unique_file_id = f"{magnet_hash}_{re.sub(r'[^a-zA-Z0-9]', '_', name)}"

    poster_key = f"thumbnails/{unique_file_id}_hq_poster.jpg"
    preview_key = f"thumbnails/{unique_file_id}_hq_preview.mp4"
    poster_file, preview_file = "thumbnail.jpg", "preview.mp4"

    cleanup_temp_files()

    try:
        # 1. Resolve Direct URL (1 single debrid API call per attempt)
        api_url = (
            f"{API_BASE_URL}?url={urllib.parse.quote(target_url)}"
            f"&action=play-direct&size={file_size}&fileName={urllib.parse.quote(name)}"
        )
        if provider_file_id:
            api_url += f"&fileId={urllib.parse.quote(str(provider_file_id))}"

        req = urllib.request.Request(api_url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            status_code = response.getcode()
            resp_data = json.loads(response.read().decode())

        if status_code == 202 or resp_data.get("status") == "retry":
            raise ColdFileError(resp_data.get("message", "Provider still caching (202)"))

        direct_url = resp_data.get("url")
        if not direct_url:
            raise ColdFileError("API response missing 'url' key")

        # 2. Metadata Extraction via ffprobe
        ffprobe_cmd = [
            "ffprobe", "-v", "fatal",
            *FFMPEG_NET_FLAGS,
            "-show_entries", "stream=codec_type,width,height,codec_name,color_transfer,channels:format=duration",
            "-of", "json", direct_url
        ]
        try:
            probe_result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, timeout=30)
        except subprocess.TimeoutExpired:
            raise ColdFileError("ffprobe timed out (cold CDN link)")

        probe_data = json.loads(probe_result.stdout) if probe_result.stdout else {}
        duration = float(probe_data.get('format', {}).get('duration', 0) or 0)
        v_stream = next((s for s in probe_data.get('streams', []) if s.get('codec_type') == 'video'), {})
        a_stream = next((s for s in probe_data.get('streams', []) if s.get('codec_type') == 'audio'), {})

        width = v_stream.get('width', 0)
        height = v_stream.get('height', 0)
        codec = v_stream.get('codec_name', 'unknown').upper()
        color = v_stream.get('color_transfer', '')
        audio_channels = a_stream.get('channels', 2)

        if (duration <= 0 and codec == 'UNKNOWN') or width == 0:
            raise ColdFileError("Stream unreadable or still warming on CDN")

        if width >= 7600 or height >= 4320:
            resolution = "8K"
        elif width >= 3800 or height >= 2160:
            resolution = "4K"
        elif width >= 1900 or height >= 1080:
            resolution = "1080p"
        elif width >= 1200 or height >= 720:
            resolution = "720p"
        else:
            resolution = "480p"

        is_hdr = 1 if color in ['smpte2084', 'arib-std-b67'] else 0
        hdr_badge = " HDR" if is_hdr else ""
        audio_badge = f" | Audio: {audio_channels}ch" if audio_channels else ""
        log(f"   📊 Found: {resolution}{hdr_badge} | {codec}{audio_badge} | {int(duration)}s")

        # 3. Short Videos (<120s) -> JPEG Poster Only
        if duration < 120:
            log("   ⏩ Short video. Generating HQ JPEG poster only...")
            seek_time = "00:00:00" if duration <= 1 else "00:00:01"
            subprocess.run([
                "ffmpeg", "-y", "-v", "fatal",
                *FFMPEG_HTTP_SEEK,
                "-ss", seek_time, "-i", direct_url,
                "-frames:v", "1", "-qscale:v", "2",
                "-vf", "scale=1280:-2:flags=lanczos", poster_file
            ], timeout=POSTER_TIMEOUT_SEC, check=True)

            cdn_poster = storage_engine.upload(f"./{poster_file}", poster_key, "image/jpeg")
            update_db_status(raw_hash, "SKIPPED_SHORT", cdn_poster, duration, resolution, codec, audio_channels, is_hdr)
            completed_updates.append({"hash": raw_hash, "targetUrl": target_url, "posterUrl": cdn_poster})
            return

        # 4. Long Videos -> Fast Keyframe-Snapped Clips & Poster
        t1, t2, t3, t4, t5 = [round(duration * p, 2) for p in [0.10, 0.30, 0.50, 0.70, 0.90]]
        log("   ⚙️ Generating HQ V2 assets...")

        clip_files = []
        animation_failed = False
        MAX_CLIP_RETRIES = 2

        for i, t in enumerate([t1, t2, t3, t4, t5]):
            if (time.time() - file_start) >= FILE_BUDGET_SEC:
                log(f"   ⏱️ Per-file budget ({FILE_BUDGET_SEC}s) reached. Falling back to poster only.")
                animation_failed = True
                break

            clip_name = f"temp_clip_{i}.mp4"
            success = False

            for retry in range(MAX_CLIP_RETRIES):
                try:
                    subprocess.run([
                        "ffmpeg", "-y", "-v", "fatal",
                        *FFMPEG_HTTP_SEEK,
                        "-ss", str(t), "-i", direct_url,
                        "-t", "1",
                        "-vf", "scale=640:-2:flags=lanczos,fps=10",
                        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "26",
                        "-pix_fmt", "yuv420p", "-an",
                        clip_name
                    ], timeout=CLIP_TIMEOUT_SEC, check=True)

                    if os.path.exists(clip_name) and os.path.getsize(clip_name) > 1000:
                        success = True
                        clip_files.append(clip_name)
                        break
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as clip_err:
                    log(f"   ⚠️ Clip {i+1}/5 failed ({type(clip_err).__name__}). Retry {retry+1}/{MAX_CLIP_RETRIES}...")
                    time.sleep(3)

            if not success:
                log(f"   ❌ Aborting animated clips at frame {i+1}/5.")
                animation_failed = True
                break

            time.sleep(1.5)

        log("   📸 Extracting 1280px HQ Poster...")
        try:
            subprocess.run([
                "ffmpeg", "-y", "-v", "fatal",
                *FFMPEG_HTTP_SEEK,
                "-ss", str(t1), "-i", direct_url,
                "-frames:v", "1", "-qscale:v", "2",
                "-vf", "scale=1280:-2:flags=lanczos", poster_file
            ], timeout=POSTER_TIMEOUT_SEC, check=True)
        except Exception as poster_err:
            log(f"   ⚠️ Raw poster extraction failed: {poster_err}")
            if len(clip_files) > 0 and os.path.exists(clip_files[0]):
                subprocess.run([
                    "ffmpeg", "-y", "-v", "fatal",
                    "-i", clip_files[0], "-frames:v", "1",
                    "-qscale:v", "2",
                    "-vf", "scale=1280:-2:flags=lanczos", poster_file
                ], timeout=30, check=True)

        if not os.path.exists(poster_file) or os.path.getsize(poster_file) == 0:
            raise ColdFileError("Poster extraction produced no output")

        cdn_poster = storage_engine.upload(f"./{poster_file}", poster_key, "image/jpeg")

        if not animation_failed and len(clip_files) == 5:
            log("   🧬 Stitching local files into Cinematic MP4 Preview...")
            subprocess.run([
                "ffmpeg", "-y", "-v", "fatal", "-err_detect", "ignore_err",
                "-i", clip_files[0], "-i", clip_files[1], "-i", clip_files[2], "-i", clip_files[3], "-i", clip_files[4],
                "-filter_complex", "[0:v][1:v][2:v][3:v][4:v]concat=n=5:v=1:a=0[v]",
                "-map", "[v]", "-c:v", "libx264", "-preset", "faster", "-crf", "24", "-pix_fmt", "yuv420p", "-an", preview_file
            ], timeout=POSTER_TIMEOUT_SEC, check=True)

            log("   ☁️ Uploading V2 Previews to Scaleway...")
            cdn_preview = storage_engine.upload(f"./{preview_file}", preview_key, "video/mp4")
            update_db_status(raw_hash, cdn_preview, cdn_poster, duration, resolution, codec, audio_channels, is_hdr)
            completed_updates.append({"hash": raw_hash, "targetUrl": target_url, "posterUrl": cdn_poster})
        else:
            log("   🛡️ Saved static poster fallback.")
            update_db_status(raw_hash, "POSTER_ONLY", cdn_poster, duration, resolution, codec, audio_channels, is_hdr)
            completed_updates.append({"hash": raw_hash, "targetUrl": target_url, "posterUrl": cdn_poster})

        log("   ✅ Upload & DB Update Complete!")

    except ColdFileError as e:
        schedule_or_cancel_cold_file(file_record, raw_hash, str(e), attempt_num)

    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', errors='ignore')
        # Transient CDN/rate-limit codes -> 10-min cooldown; permanent 4xx -> instant cancel
        if e.code in (429, 500, 502, 503, 504):
            schedule_or_cancel_cold_file(file_record, raw_hash, f"HTTP {e.code}", attempt_num)
        else:
            log(f"   🛑 Permanent HTTP {e.code}: {e.reason} | {error_body[:150]}")
            update_db_status(raw_hash, f"FAILED_HTTP_{e.code}")

    except (urllib.error.URLError, TimeoutError) as e:
        schedule_or_cancel_cold_file(file_record, raw_hash, f"Network timeout: {e}", attempt_num)

    except Exception as e:
        log(f"   ❌ ERROR: {e}")
        update_db_status(raw_hash, "FAILED")

    finally:
        cleanup_temp_files()


# ==========================================
# 6. CONTINUOUS ENGINE LOOP
# ==========================================
log("🚀 Starting Continuous 6-Hour Preview Engine (V2 HQ + Cold-File Cooldown)...")

while True:
    if (time.time() - ENGINE_START_TIME) >= MAX_RUNTIME_SEC:
        log(f"⏳ Safety time limit reached ({len(cold_retry_queue)} cold items skipped). Gracefully exiting.")
        exit(0)

    completed_updates = []

    # A. Process any due cold-file retries first (after their 10-min cooldown)
    now_ts = time.time()
    due_hashes = [h for h, entry in cold_retry_queue.items() if now_ts >= entry["next_try_at"]]
    for h in due_hashes:
        if (time.time() - ENGINE_START_TIME) >= MAX_RUNTIME_SEC:
            exit(0)
        entry = cold_retry_queue.pop(h)
        log(f"\n[COLD RETRY] 🔁 Waking up queued file after cooldown...")
        process_file(entry["record"], entry["attempts"] + 1, completed_updates)
        time.sleep(2)

    # B. Query D1 for brand-new pending files (uses partial index cleanly)
    log("🔍 Querying D1 database for pending files...")
    fetch_sql = f"""
        SELECT hash, target_url, name, size, provider_file_id
        FROM global_assets
        WHERE preview_animation IS NULL
        ORDER BY created_at DESC
        LIMIT {BATCH_SIZE};
    """

    result = subprocess.run(["npx", "wrangler", "d1", "execute", D1_DB_NAME, "--remote", "--command", fetch_sql, "--json"], capture_output=True, text=True)
    try:
        raw_output = result.stdout.strip()
        if result.returncode != 0 or not raw_output:
            log(f"⚠️ D1 Query returned error/empty: {result.stderr[:200]}")
            time.sleep(10)
            continue

        json_start = raw_output.find('[') if '[' in raw_output else raw_output.find('{')
        parsed_json = json.loads(raw_output[json_start:]) if json_start != -1 else []
        pending_files = parsed_json[0].get("results", []) if isinstance(parsed_json, list) else parsed_json.get("results", [])
    except Exception as e:
        log(f"⚠️ Failed to parse D1 response: {e}")
        time.sleep(10)
        continue

    if not pending_files:
        if cold_retry_queue:
            next_wake = min(e["next_try_at"] for e in cold_retry_queue.values())
            wait_sec = max(15, min(60, int(next_wake - time.time())))
            log(f"✨ D1 queue empty. {len(cold_retry_queue)} cold file(s) cooling down; sleeping {wait_sec}s...")
            time.sleep(wait_sec)
        else:
            log("✨ Queue is empty. Sleeping for 30 seconds...")
            time.sleep(30)
        continue

    log(f"📦 Found {len(pending_files)} video(s) in queue.")

    for idx, file_record in enumerate(pending_files):
        if (time.time() - ENGINE_START_TIME) >= MAX_RUNTIME_SEC:
            exit(0)
        log(f"\n[{idx + 1}/{len(pending_files)}]")
        process_file(file_record, 1, completed_updates)
        time.sleep(2)

    if completed_updates:
        log(f"   🔔 Pinging Bulk Webhook to route {len(completed_updates)} new posters...")
        try:
            webhook_url = f"{API_BASE_URL}drive/webhook/finalize"
            webhook_headers = {**headers, "Content-Type": "application/json"}
            payload = json.dumps({"updates": completed_updates}).encode('utf-8')
            req = urllib.request.Request(webhook_url, data=payload, headers=webhook_headers, method='POST')
            with urllib.request.urlopen(req, timeout=15) as response:
                log(f"   ✅ Webhook success: {response.read().decode()}")
        except Exception as e:
            log(f"   ⚠️ Webhook failed (non-fatal): {e}")

    time.sleep(2)
