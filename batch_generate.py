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
    print(f"[{now}] {message}")

# ==========================================
# 2. CORE CONFIGURATION & HEADERS
# ==========================================
BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN")
API_BASE_URL = "https://streamlink.cloud/api/"
D1_DB_NAME = "streamlink-db"
BATCH_SIZE = 10
TIMEOUT_SEC = 180 

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
# 4. DATABASE UPDATE HELPER
# ==========================================
def update_db_status(target_url, name, status, poster_url=None, duration=0, res="NULL", codec="NULL", audio=2, is_hdr=0):
    safe_name = name.replace("'", "''") 
    safe_target = target_url.replace("'", "''") 
    
    if status.startswith("FAILED") or status == "SKIPPED_NON_VIDEO":
        sql = f"UPDATE global_assets SET preview_animation = '{status}' WHERE target_url = '{safe_target}' AND name = '{safe_name}';"
    else:
        poster_val = f"'{poster_url}'" if poster_url else "thumbnail"
        res_val = f"'{res}'" if res != "NULL" else "NULL"
        codec_val = f"'{codec}'" if codec != "NULL" else "NULL"
        
        # 🚀 THE FIX: Single-line SQL string prevents the Wrangler CLI from breaking on hidden newlines!
        sql = f"UPDATE global_assets SET thumbnail = {poster_val}, preview_animation = '{status}', duration = {int(duration)}, resolution = {res_val}, codec = {codec_val}, audio_channels = {int(audio)}, is_hdr = {int(is_hdr)} WHERE target_url = '{safe_target}' AND name = '{safe_name}';"
        
    subprocess.run(["npx", "wrangler", "d1", "execute", D1_DB_NAME, "--remote", "--command", sql], stdout=subprocess.DEVNULL)

# ==========================================
# 5. CONTINUOUS ENGINE LOOP
# ==========================================
log("🚀 Starting Continuous 6-Hour Preview Engine (Optimized WebP Edition)...")

while True:
    if (time.time() - ENGINE_START_TIME) >= MAX_RUNTIME_SEC: 
        log("⏳ Safety time limit reached. Gracefully exiting.")
        exit(0)

    fetch_sql = f"SELECT hash, target_url, name, size FROM global_assets WHERE preview_animation IS NULL ORDER BY created_at DESC LIMIT {BATCH_SIZE};"
    result = subprocess.run(["npx", "wrangler", "d1", "execute", D1_DB_NAME, "--remote", "--command", fetch_sql, "--json"], capture_output=True, text=True)

    try:
        raw_output = result.stdout.strip()
        json_start = raw_output.find('[') if '[' in raw_output else raw_output.find('{')
        parsed_json = json.loads(raw_output[json_start:]) if json_start != -1 else []
        pending_files = parsed_json[0].get("results", []) if isinstance(parsed_json, list) else parsed_json.get("results", [])
    except Exception:
        time.sleep(10)
        continue

    if not pending_files:
        log("✨ Queue is empty. Sleeping for 30 seconds...")
        time.sleep(30)
        continue

    for idx, file_record in enumerate(pending_files):
        if (time.time() - ENGINE_START_TIME) >= MAX_RUNTIME_SEC: 
            exit(0)

        name = file_record["name"]
        target_url = file_record["target_url"]
        file_size = file_record["size"]

        # 🚀 THE BOUNCER: Instantly skip and tag non-video files SILENTLY
        valid_video_exts = ('.mp4', '.mkv', '.avi', '.mov', '.m4v', '.flv', '.webm', '.wmv')
        if not name.lower().endswith(valid_video_exts):
            update_db_status(target_url, name, "SKIPPED_NON_VIDEO")
            continue

        # 👇 ONLY log if it actually passes the bouncer and is a real video!
        log(f"\n[{idx + 1}/{len(pending_files)}] Processing: {name}")

        raw_hash = str(file_record.get("hash", ""))
        if "urn:btih:" not in raw_hash or "||" not in raw_hash:
            update_db_status(target_url, name, "FAILED_MALFORMED_HASH")
            continue
            
        magnet_hash = raw_hash.split("urn:btih:")[1].split("||")[0].upper()
        unique_file_id = f"{magnet_hash}_{re.sub(r'[^a-zA-Z0-9]', '_', name)}"
        
        poster_file, preview_file = "thumbnail.webp", "preview.webp"
        if os.path.exists(poster_file): os.remove(poster_file)
        if os.path.exists(preview_file): os.remove(preview_file)

        try:
            # 1. Resolve Direct URL
            api_url = f"{API_BASE_URL}?url={urllib.parse.quote(target_url)}&action=play-direct&size={file_size}&fileName={urllib.parse.quote(name)}"
            req = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as response:
                direct_url = json.loads(response.read().decode()).get("url")

            if not direct_url:
                raise Exception("API response missing 'url' key.")

            # 2. 🚀 ADVANCED METADATA EXTRACTION
            ffprobe_cmd = [
                "ffprobe", "-v", "error", 
                "-show_entries", "stream=codec_type,width,height,codec_name,color_transfer,channels:format=duration", 
                "-of", "json", direct_url
            ]
            probe_result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, timeout=30)
            probe_data = json.loads(probe_result.stdout)
            
            duration = float(probe_data.get('format', {}).get('duration', 0))
            v_stream = next((s for s in probe_data.get('streams', []) if s.get('codec_type') == 'video'), {})
            a_stream = next((s for s in probe_data.get('streams', []) if s.get('codec_type') == 'audio'), {})

            width = v_stream.get('width', 0)
            height = v_stream.get('height', 0)
            codec = v_stream.get('codec_name', 'unknown').upper()
            color = v_stream.get('color_transfer', '')
            audio_channels = a_stream.get('channels', 2)
            
            # Logic parsing (Supports Cinematic Aspect Ratios)
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

            poster_key = f"thumbnails/{unique_file_id}_poster.webp"
            preview_key = f"thumbnails/{unique_file_id}_preview.webp"

            # 3. Short Videos (<120s) -> Lightweight Poster Only (800px, Quality 65)
            if duration < 120:
                log("   ⏩ Short video. Generating optimized poster only...")
                subprocess.run([
                    "ffmpeg", "-y", "-v", "error", "-ss", "00:00:01", 
                    "-i", direct_url, "-frames:v", "1", 
                    "-c:v", "libwebp", "-q:v", "65", 
                    "-vf", "scale=800:-2:flags=lanczos", poster_file
                ], timeout=TIMEOUT_SEC, check=True)
                
                cdn_poster = storage_engine.upload(f"./{poster_file}", poster_key, "image/webp")
                update_db_status(target_url, name, "SKIPPED_SHORT", cdn_poster, duration, resolution, codec, audio_channels, is_hdr)
                continue

            # 4. Long Videos -> Lightweight Static Poster & Animated Preview
            t1, t2, t3, t4, t5 = [round(duration * p, 2) for p in [0.10, 0.30, 0.50, 0.70, 0.90]]
            log("   ⚙️ Generating optimized WebP assets...")
            
            # Optimized Static Poster (800px width, 65 quality ~30-50KB)
            subprocess.run([
                "ffmpeg", "-y", "-v", "error", "-ss", str(t1), 
                "-i", direct_url, "-frames:v", "1", 
                "-c:v", "libwebp", "-q:v", "65", 
                "-vf", "scale=800:-2:flags=lanczos", poster_file
            ], timeout=TIMEOUT_SEC, check=True)
            
            # Optimized Animated WebP (480px width, 8 fps, 50 quality ~80-160KB)
            subprocess.run([
                "ffmpeg", "-y", "-v", "fatal", "-err_detect", "ignore_err",
                "-ss", str(t1), "-t", "1", "-i", direct_url,
                "-ss", str(t2), "-t", "1", "-i", direct_url,
                "-ss", str(t3), "-t", "1", "-i", direct_url,
                "-ss", str(t4), "-t", "1", "-i", direct_url,
                "-ss", str(t5), "-t", "1", "-i", direct_url,
                "-filter_complex", "[0:v][1:v][2:v][3:v][4:v]concat=n=5:v=1:a=0,fps=8,scale=480:-2:flags=lanczos[v]",
                "-map", "[v]", "-c:v", "libwebp_anim", "-loop", "0", "-q:v", "50", "-an", preview_file
            ], timeout=TIMEOUT_SEC, check=True)

            log("   ☁️ Uploading optimized WebP files to Scaleway...")
            cdn_poster = storage_engine.upload(f"./{poster_file}", poster_key, "image/webp")
            cdn_preview = storage_engine.upload(f"./{preview_file}", preview_key, "image/webp")
            
            update_db_status(target_url, name, cdn_preview, cdn_poster, duration, resolution, codec, audio_channels, is_hdr)
            log("   ✅ Upload & DB Update Complete!")

        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8', errors='ignore')
            log(f"   ❌ HTTP ERROR {e.code}: {e.reason} | Body: {error_body[:200]}")
            update_db_status(target_url, name, f"FAILED_HTTP_{e.code}")
        except Exception as e:
            log(f"   ❌ ERROR: {e}")
            update_db_status(target_url, name, "FAILED")

    time.sleep(2)
