import os
import subprocess
import urllib.request
import urllib.parse
import json
import time
from datetime import datetime
import re

# ==========================================
# 1. CONFIGURATION & HELPERS
# ==========================================
BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN")
if not BEARER_TOKEN:
    print("❌ API_BEARER_TOKEN missing from environment variables! Please check your GitHub Secrets.")
    exit(1)

API_BASE_URL = "https://streamlink.cloud/api/"
BUCKET_NAME = "streamlink-assets"
D1_DB_NAME = "streamlink-db"
BATCH_SIZE = 10
TIMEOUT_SEC = 180 

# 🚀 5.5 Hours limit in seconds (19,800 seconds)
MAX_RUNTIME_SEC = 5.5 * 3600
ENGINE_START_TIME = time.time()

headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
}

def log(message):
    """Helper to print messages with a precise timestamp"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}")

def update_db_status(target_url, name, status, poster_url=None):
    poster_val = f"'{poster_url}'" if poster_url else "thumbnail"
    safe_name = name.replace("'", "''") 
    safe_target = target_url.replace("'", "''") 
    
    sql = f"UPDATE global_assets SET thumbnail = {poster_val}, preview_animation = '{status}' WHERE target_url = '{safe_target}' AND name = '{safe_name}';"
    subprocess.run(["npx", "wrangler", "d1", "execute", D1_DB_NAME, "--remote", "--command", sql], stdout=subprocess.DEVNULL)

# ==========================================
# 2. CONTINUOUS ENGINE LOOP
# ==========================================
log("🚀 Starting Continuous 6-Hour Preview Engine...")

while True:
    # 🛑 Time Guard: Check if we are near the 6-hour limit
    elapsed_time = time.time() - ENGINE_START_TIME
    if elapsed_time >= MAX_RUNTIME_SEC:
        log("⏳ Reached 5.5-hour safety limit. Gracefully shutting down to cycle runner IP.")
        exit(0)

    log(f"\n📥 Querying D1 for up to {BATCH_SIZE} pending videos...")
    fetch_sql = f"SELECT hash, target_url, name, size FROM global_assets WHERE preview_animation IS NULL AND (name LIKE '%.mp4' OR name LIKE '%.mkv' OR name LIKE '%.avi' OR name LIKE '%.webm') ORDER BY created_at DESC LIMIT {BATCH_SIZE};"
    
    result = subprocess.run(
        ["npx", "wrangler", "d1", "execute", D1_DB_NAME, "--remote", "--command", fetch_sql, "--json"], 
        capture_output=True, text=True
    )

    try:
        raw_output = result.stdout.strip()
        json_start = raw_output.find('[') if '[' in raw_output else raw_output.find('{')
        
        if json_start != -1:
            clean_json = raw_output[json_start:]
            parsed_json = json.loads(clean_json)
        else:
            raise Exception("No JSON found in Wrangler output.")

        if isinstance(parsed_json, list):
            if "error" in parsed_json[0]:
                raise Exception(f"D1 SQL Error: {parsed_json[0]['error']}")
            pending_files = parsed_json[0].get("results", [])
        else:
            if "error" in parsed_json:
                raise Exception(f"D1 SQL Error: {parsed_json['error']}")
            pending_files = parsed_json.get("results", [])

    except Exception as e:
        log(f"❌ Failed to parse D1 database output: {e}")
        log(f"⚠️ RAW WRANGLER OUTPUT:\n{result.stdout}")
        time.sleep(10)
        continue

    # 💤 If queue is empty, sleep for 30 seconds and check again
    if not pending_files:
        log("✨ Queue is empty. Sleeping for 30 seconds...")
        time.sleep(30)
        continue

    log(f"🚀 Found {len(pending_files)} videos to process in this pass.")

    # ==========================================
    # 3. BATCH PROCESSING LOOP
    # ==========================================
    for idx, file_record in enumerate(pending_files):
        # Time Guard Check inside batch loop
        if (time.time() - ENGINE_START_TIME) >= MAX_RUNTIME_SEC:
            log("⏳ Mid-batch time limit hit. Shutting down safely...")
            exit(0)

        name = file_record["name"]
        target_url = file_record["target_url"]
        file_size = file_record["size"]
        
        log(f"\n[{idx + 1}/{len(pending_files)}] Processing: {name}")

        # Safe Hash Parsing
        raw_hash = str(file_record.get("hash", ""))
        if "urn:btih:" not in raw_hash or "||" not in raw_hash:
            log(f"   ⚠️ WARNING: Skipping malformed hash for: {name}")
            update_db_status(target_url, name, "FAILED_MALFORMED_HASH")
            continue
            
        magnet_hash = raw_hash.split("urn:btih:")[1].split("||")[0].upper()
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', name)
        unique_file_id = f"{magnet_hash}_{safe_name}"

        poster_file = "thumbnail.jpg"
        preview_file = "preview.mp4"
        
        if os.path.exists(poster_file): os.remove(poster_file)
        if os.path.exists(preview_file): os.remove(preview_file)

        try:
            # A. Resolve Direct Stream
            encoded_url = urllib.parse.quote(target_url)
            encoded_name = urllib.parse.quote(name)
            api_url = f"{API_BASE_URL}?url={encoded_url}&action=play-direct&size={file_size}&fileName={encoded_name}"
            
            req = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())
            
            if "url" not in data:
                raise Exception("API did not return a stream URL.")
            direct_url = data["url"]

            # B. Probe Duration
            probe_cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", direct_url]
            probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30)
            
            raw_duration = probe_result.stdout.strip()
            if not raw_duration:
                 raise Exception(f"FFprobe could not read stream. Size: {file_size} bytes")
                 
            duration = float(raw_duration)
            log(f"   ⏱️ Duration: {duration}s")

            # C. Short Video Logic (<120s)
            cdn_poster = f"https://cdn.streamlink.cloud/thumbnails/{unique_file_id}_poster.jpg"
            
            if duration < 120:
                log("   ⏩ Video under 120s. Generating thumbnail only...")
                subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", "00:00:01", "-i", direct_url, "-frames:v", "1", "-f", "image2", "-vf", "scale=1280:-2:flags=lanczos", "-q:v", "2", poster_file], timeout=TIMEOUT_SEC, check=True)
                
                if not os.path.exists(poster_file) or os.path.getsize(poster_file) < 1024:
                    raise Exception("Thumbnail generation failed (empty file).")
                    
                subprocess.run(["npx", "wrangler", "r2", "object", "put", f"{BUCKET_NAME}/thumbnails/{unique_file_id}_poster.jpg", "--file", f"./{poster_file}", "--remote"], stdout=subprocess.DEVNULL)
                update_db_status(target_url, name, "SKIPPED_SHORT", cdn_poster)
                continue

            # D. Standard Generation
            t1, t2, t3, t4, t5 = [round(duration * p, 2) for p in [0.10, 0.30, 0.50, 0.70, 0.90]]
            
            log("   ⚙️ Generating poster & preview animation...")
            subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", str(t1), "-i", direct_url, "-frames:v", "1", "-f", "image2", "-vf", "scale=1280:-2:flags=lanczos", "-q:v", "2", poster_file], timeout=TIMEOUT_SEC, check=True)
            
            ffmpeg_preview = [
                "ffmpeg", "-y", "-v", "fatal", "-err_detect", "ignore_err",
                "-ss", str(t1), "-t", "1", "-i", direct_url,
                "-ss", str(t2), "-t", "1", "-i", direct_url,
                "-ss", str(t3), "-t", "1", "-i", direct_url,
                "-ss", str(t4), "-t", "1", "-i", direct_url,
                "-ss", str(t5), "-t", "1", "-i", direct_url,
                "-filter_complex", "[0:v][1:v][2:v][3:v][4:v]concat=n=5:v=1:a=0,fps=12,scale=640:-2:flags=lanczos[v]",
                "-map", "[v]", "-c:v", "libx264", "-preset", "fast", "-an", preview_file
            ]
            subprocess.run(ffmpeg_preview, timeout=TIMEOUT_SEC, check=True)

            if not os.path.exists(preview_file) or os.path.getsize(preview_file) < 5000:
                raise Exception("Preview generation failed or stream dropped.")

            # E. Upload & Update
            log("   ☁️ Pushing to R2 Edge...")
            cdn_preview = f"https://cdn.streamlink.cloud/thumbnails/{unique_file_id}_preview.mp4"
            subprocess.run(["npx", "wrangler", "r2", "object", "put", f"{BUCKET_NAME}/thumbnails/{unique_file_id}_poster.jpg", "--file", f"./{poster_file}", "--remote"], stdout=subprocess.DEVNULL)
            subprocess.run(["npx", "wrangler", "r2", "object", "put", f"{BUCKET_NAME}/thumbnails/{unique_file_id}_preview.mp4", "--file", f"./{preview_file}", "--remote"], stdout=subprocess.DEVNULL)
            
            update_db_status(target_url, name, cdn_preview, cdn_poster)
            log("   ✅ Success!")

        except subprocess.TimeoutExpired:
            log(f"   ⚠️ TIMEOUT: Stream hung for over {TIMEOUT_SEC} seconds.")
            update_db_status(target_url, name, "FAILED_TIMEOUT")
        except Exception as e:
            log(f"   ❌ ERROR: {e}")
            update_db_status(target_url, name, "FAILED")

    # Brief pause after completing a batch before asking D1 for the next 10
    time.sleep(2)
