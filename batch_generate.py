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
    """Helper to print messages with a precise timestamp"""
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

# Emulate standard browser headers to prevent Cloudflare WAF 403 blocks
headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9"
}

# Time limit guard to prevent GitHub Action runner timeouts
MAX_RUNTIME_SEC = 5.75 * 3600
ENGINE_START_TIME = time.time()

# 🚀 THE MASTER SWITCH
ACTIVE_STORAGE = os.environ.get("ACTIVE_STORAGE", "scaleway").lower()

# ==========================================
# 3. MULTI-CLOUD STORAGE ADAPTERS
# ==========================================
class BaseStorageAdapter:
    def upload(self, local_path, s3_key, content_type):
        raise NotImplementedError("Subclasses must implement the upload method.")

class ScalewayAdapter(BaseStorageAdapter):
    def __init__(self):
        self.bucket = "streamlink-assets"
        # 🟢 UPDATED: Points to your new isolated subdomain
        self.cdn_base = "https://media.streamlink.cloud" 
        
        self.client = boto3.client(
            "s3",
            region_name="fr-par",
            endpoint_url="https://s3.fr-par.scw.cloud",
            aws_access_key_id=os.environ.get("SCW_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("SCW_SECRET_KEY"),
            config=Config(s3={"addressing_style": "virtual"})
        )

    def upload(self, local_path, s3_key, content_type):
        self.client.upload_file(
            local_path, 
            self.bucket, 
            s3_key, 
            ExtraArgs={"ContentType": content_type, "ACL": "public-read"}
        )
        return f"{self.cdn_base}/{s3_key}"

class CloudflareR2Adapter(BaseStorageAdapter):
    def __init__(self):
        self.bucket = "streamlink-assets"
        self.cdn_base = "https://cdn.streamlink.cloud"
        account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
        
        self.client = boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ.get("R2_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("R2_SECRET_KEY")
        )

    def upload(self, local_path, s3_key, content_type):
        self.client.upload_file(
            local_path, 
            self.bucket, 
            s3_key, 
            ExtraArgs={"ContentType": content_type}
        )
        return f"{self.cdn_base}/{s3_key}"

# Initialize active storage provider
log(f"🌩️ Connecting to cloud provider: {ACTIVE_STORAGE.upper()}")
if ACTIVE_STORAGE == "scaleway":
    storage_engine = ScalewayAdapter()
elif ACTIVE_STORAGE == "r2":
    storage_engine = CloudflareR2Adapter()
else:
    log("❌ Invalid ACTIVE_STORAGE provider defined!")
    exit(1)

# ==========================================
# 4. DATABASE UPDATE HELPER
# ==========================================
def update_db_status(target_url, name, status, poster_url=None):
    poster_val = f"'{poster_url}'" if poster_url else "thumbnail"
    safe_name = name.replace("'", "''") 
    safe_target = target_url.replace("'", "''") 
    
    sql = f"UPDATE global_assets SET thumbnail = {poster_val}, preview_animation = '{status}' WHERE target_url = '{safe_target}' AND name = '{safe_name}';"
    subprocess.run(["npx", "wrangler", "d1", "execute", D1_DB_NAME, "--remote", "--command", sql], stdout=subprocess.DEVNULL)

# ==========================================
# 5. CONTINUOUS ENGINE LOOP
# ==========================================
log("🚀 Starting Continuous 6-Hour Preview Engine (WebP Edition)...")

while True:
    elapsed_time = time.time() - ENGINE_START_TIME
    if elapsed_time >= MAX_RUNTIME_SEC:
        log("⏳ Reached 5.5-hour safety limit. Gracefully shutting down.")
        exit(0)

    log(f"\n📥 Querying D1 for up to {BATCH_SIZE} pending videos...")
    fetch_sql = f"SELECT hash, target_url, name, size FROM global_assets WHERE preview_animation IS NULL AND (name LIKE '%.mp4' OR name LIKE '%.mkv' OR name LIKE '%.avi' OR name LIKE '%.webm') ORDER BY created_at DESC LIMIT {BATCH_SIZE};"
    
    result = subprocess.run(["npx", "wrangler", "d1", "execute", D1_DB_NAME, "--remote", "--command", fetch_sql, "--json"], capture_output=True, text=True)

    try:
        raw_output = result.stdout.strip()
        json_start = raw_output.find('[') if '[' in raw_output else raw_output.find('{')
        parsed_json = json.loads(raw_output[json_start:]) if json_start != -1 else []
        pending_files = parsed_json[0].get("results", []) if isinstance(parsed_json, list) else parsed_json.get("results", [])
    except Exception as e:
        log(f"❌ Failed to parse D1 output: {e}")
        time.sleep(10)
        continue

    if not pending_files:
        log("✨ Queue is empty. Sleeping for 30 seconds...")
        time.sleep(30)
        continue

    log(f"🚀 Found {len(pending_files)} videos to process in this pass.")

    for idx, file_record in enumerate(pending_files):
        if (time.time() - ENGINE_START_TIME) >= MAX_RUNTIME_SEC:
            log("⏳ Mid-batch time limit hit. Shutting down safely...")
            exit(0)

        name = file_record["name"]
        target_url = file_record["target_url"]
        file_size = file_record["size"]
        
        log(f"\n[{idx + 1}/{len(pending_files)}] Processing: {name}")

        raw_hash = str(file_record.get("hash", ""))
        if "urn:btih:" not in raw_hash or "||" not in raw_hash:
            update_db_status(target_url, name, "FAILED_MALFORMED_HASH")
            continue
            
        magnet_hash = raw_hash.split("urn:btih:")[1].split("||")[0].upper()
        unique_file_id = f"{magnet_hash}_{re.sub(r'[^a-zA-Z0-9]', '_', name)}"
        
        # 🟢 UPDATED: Using .webp formats locally
        poster_file, preview_file = "thumbnail.webp", "preview.webp"
        
        if os.path.exists(poster_file): os.remove(poster_file)
        if os.path.exists(preview_file): os.remove(preview_file)

        try:
            # 1. Resolve Direct URL
            api_url = f"{API_BASE_URL}?url={urllib.parse.quote(target_url)}&action=play-direct&size={file_size}&fileName={urllib.parse.quote(name)}"
            req = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())
                
            direct_url = data.get("url")
            if not direct_url:
                raise Exception("API response missing 'url' key.")

            # 2. Get Video Duration
            raw_duration = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", direct_url], capture_output=True, text=True, timeout=30).stdout.strip()
            duration = float(raw_duration)
            
            # 🟢 UPDATED: Saving to S3 with .webp extensions
            poster_key = f"thumbnails/{unique_file_id}_poster.webp"
            preview_key = f"thumbnails/{unique_file_id}_preview.webp"

            # 3. Short Videos (<120s) -> Thumbnail Only
            if duration < 120:
                log("   ⏩ Short video. Generating WebP thumbnail only...")
                subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", "00:00:01", "-i", direct_url, "-frames:v", "1", "-c:v", "libwebp", "-q:v", "80", "-vf", "scale=1280:-2:flags=lanczos", poster_file], timeout=TIMEOUT_SEC, check=True)
                
                cdn_poster = storage_engine.upload(f"./{poster_file}", poster_key, "image/webp")
                update_db_status(target_url, name, "SKIPPED_SHORT", cdn_poster)
                continue

            # 4. Long Videos -> Standard Generation (Static & Animated WebP)
            t1, t2, t3, t4, t5 = [round(duration * p, 2) for p in [0.10, 0.30, 0.50, 0.70, 0.90]]
            log("   ⚙️ Generating WebP poster & animated preview...")
            
            # 🟢 UPDATED: Static WebP Generator
            subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", str(t1), "-i", direct_url, "-frames:v", "1", "-c:v", "libwebp", "-q:v", "80", "-vf", "scale=1280:-2:flags=lanczos", poster_file], timeout=TIMEOUT_SEC, check=True)
            
            # 🟢 UPDATED: Animated WebP Generator
            subprocess.run([
                "ffmpeg", "-y", "-v", "fatal", "-err_detect", "ignore_err",
                "-ss", str(t1), "-t", "1", "-i", direct_url,
                "-ss", str(t2), "-t", "1", "-i", direct_url,
                "-ss", str(t3), "-t", "1", "-i", direct_url,
                "-ss", str(t4), "-t", "1", "-i", direct_url,
                "-ss", str(t5), "-t", "1", "-i", direct_url,
                "-filter_complex", "[0:v][1:v][2:v][3:v][4:v]concat=n=5:v=1:a=0,fps=12,scale=640:-2:flags=lanczos[v]",
                "-map", "[v]", "-c:v", "libwebp_anim", "-loop", "0", "-q:v", "70", "-an", preview_file
            ], timeout=TIMEOUT_SEC, check=True)

            # 5. Dynamic Upload
            log("   ☁️ Pushing WebP assets to Cloud Storage...")
            cdn_poster = storage_engine.upload(f"./{poster_file}", poster_key, "image/webp")
            cdn_preview = storage_engine.upload(f"./{preview_file}", preview_key, "image/webp")
            
            update_db_status(target_url, name, cdn_preview, cdn_poster)
            log("   ✅ Success!")

        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8', errors='ignore')
            log(f"   ❌ HTTP ERROR {e.code}: {e.reason} | Body: {error_body[:200]}")
            update_db_status(target_url, name, f"FAILED_HTTP_{e.code}")
        except Exception as e:
            log(f"   ❌ ERROR: {e}")
            update_db_status(target_url, name, "FAILED")

    time.sleep(2)
