import os, json, time, subprocess, requests, threading, re, urllib.parse, hashlib, traceback
import asyncio, aiohttp
from flask import Flask, render_template_string, request, jsonify
import psycopg2
from psycopg2.extras import execute_values

app = Flask(__name__)

# ---------------------------
#  HARDCODED SECRETS & DBs
# ---------------------------
NEON_DB_URL = "postgresql://neondb_owner:npg_Rh0xIbmdFe5u@ep-quiet-block-a12aatzr-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
SUPABASE_URL = "https://cnkbewgpguyojiebztbs.supabase.co/rest/v1/reels"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNua2Jld2dwZ3V5b2ppZWJ6dGJzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQyODU0NzUsImV4cCI6MjA4OTg2MTQ3NX0.ldS5knPaT1imexuRH9jSlTDB1mRSpoozFXlmhbDw2fU"

# ---------------------------
#  DIRECTORIES & CONFIG
# ---------------------------
BASE_DIR = os.getcwd()
VIDEO_DIR = os.path.join(BASE_DIR, "watermarked_videos")
PREVIEW_DIR = os.path.join(BASE_DIR, "previews")
SETTINGS_FILE = "settings.json"

for d in [VIDEO_DIR, PREVIEW_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)

# ---------------------------
#  NEON DB INITIALIZATION
# ---------------------------
def init_neon_db():
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS repost_queue (
                    id SERIAL PRIMARY KEY,
                    video_id TEXT UNIQUE NOT NULL,
                    size_limit INT DEFAULT 10,
                    status TEXT DEFAULT 'not started',
                    error TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            try:
                cur.execute("ALTER TABLE repost_queue ADD COLUMN IF NOT EXISTS size_limit INT DEFAULT 10;")
            except:
                conn.rollback()
            
            cur.execute("CREATE TABLE IF NOT EXISTS image_hashes (vid TEXT PRIMARY KEY, hash TEXT);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON repost_queue(status);")
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS uploaded_videos (
                    hash TEXT PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    original_source_id TEXT,
                    uploaded_at TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON uploaded_videos(hash);")
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Neon DB Init Error: {e}")
        traceback.print_exc()

init_neon_db()

# ---------------------------
#  SETTINGS MANAGER
# ---------------------------
DEFAULT_SETTINGS = {
    "my_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InRlbHVndXN0dWZmcyIsImlhdCI6MTc3NTMyMDc3OSwiZXhwIjoxNzc3OTEyNzc5fQ.48_8h8tDpZapGhFzMFgb9-DJSa9UZyArE2gvyJbk-1Y",
    "my_user": "telugustuffs",
    "main_domain": "love.viraly.wtf",
    "upload_domain": "loveupload.viraly.wtf",
    "blacklist": "promo, link in bio, part 2, pt 2, subscribe",
    "del_payload": "U2FsdGVkX1+0BWWOC9q0iGdVxXxQPvzazMUrmc4pvXw=",
    "full_cookie": "_ga=GA1.1.176737717.1775237049; _ga_CHGRECY8GV=GS2.1.s1775372645$o5$g1$t1775372777$j59$l0$h0; accessToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InRlbHVndXN0dWZmcyIsImlhdCI6MTc3NTMyMDc3OSwiZXhwIjoxNzc3OTEyNzc5fQ.48_8h8tDpZapGhFzMFgb9-DJSa9UZyArE2gvyJbk-1Y; oldUserId=U2FsdGVkX18zmdA%2Bj20qXbN7HwHHjkbBEzE5nIJVaWE%3D; anonUserId=U2FsdGVkX1%2B0BWWOC9q0iGdVxXxQPvzazMUrmc4pvXw%3D; allow18=%7B%22allow18%22%3Atrue%7D"
}

if not os.path.exists(SETTINGS_FILE):
    json.dump(DEFAULT_SETTINGS, open(SETTINGS_FILE, 'w'), indent=4)

def get_settings():
    with open(SETTINGS_FILE, 'r') as f:
        return json.load(f)

def get_headers():
    conf = get_settings()
    return {
        "Cookie": conf.get("full_cookie", f"accessToken={conf['my_token']}; allow18=%7B%22allow18%22%3Atrue%7D"),
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

# ---------------------------
#  LOGGER & STATUS
# ---------------------------
log_messages = []
current_status = {"reposter": "Idle", "scraper": "Idle", "queue_size": 0}

def emit_log(msg, category="SYS", color="#10b981", is_error=False):
    t = time.strftime("%H:%M:%S")
    full_msg = f"[{t}] [{category}] {msg}"
    print(full_msg)
    if is_error:
        print(traceback.format_exc())
    log_messages.append({
        "time": t,
        "category": category,
        "message": msg,
        "color": color,
        "is_error": is_error
    })
    if len(log_messages) > 200:
        log_messages.pop(0)

# ---------------------------
#  SUPABASE HELPERS
# ---------------------------
def insert_into_supabase(video_ids):
    if not video_ids:
        return
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    records = [{"id": vid, "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")} for vid in video_ids]
    try:
        r = requests.post(SUPABASE_URL, headers=headers, json=records, timeout=10)
        if r.status_code >= 400:
            emit_log(f"Supabase insert error: {r.text[:200]}", "SUPABASE", "#ef4444", is_error=True)
        else:
            emit_log(f"Archived {len(video_ids)} IDs to Supabase", "SUPABASE", "#10b981")
    except Exception as e:
        emit_log(f"Supabase connection error: {e}", "SUPABASE", "#ef4444", is_error=True)

# ---------------------------
#  QUEUE HELPERS
# ---------------------------
def filter_existing_ids(vid_list):
    if not vid_list:
        return []
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            format_strings = ','.join(['%s'] * len(vid_list))
            cur.execute(f"SELECT video_id FROM repost_queue WHERE video_id IN ({format_strings})", tuple(vid_list))
            existing = {row[0] for row in cur.fetchall()}
        conn.close()
        return [v for v in vid_list if v not in existing]
    except Exception as e:
        emit_log(f"filter_existing_ids error: {e}", "DB", "#ef4444", is_error=True)
        return vid_list

def add_to_neon_queue(video_id, size_limit=10):
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO repost_queue (video_id, size_limit, status, updated_at) VALUES (%s, %s, 'not started', NOW()) ON CONFLICT (video_id) DO NOTHING;",
                (video_id, size_limit)
            )
            inserted = cur.rowcount > 0
            conn.commit()
        conn.close()
        return inserted
    except Exception as e:
        emit_log(f"add_to_neon_queue error for {video_id}: {e}", "DB", "#ef4444", is_error=True)
        return False

def get_next_job():
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE repost_queue SET status = 'doing', updated_at = NOW()
                WHERE id = (
                    SELECT id FROM repost_queue
                    WHERE status = 'not started' OR (status = 'doing' AND updated_at < NOW() - INTERVAL '10 minutes')
                    ORDER BY updated_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
                ) RETURNING id, video_id, size_limit;
            """)
            job = cur.fetchone()
            conn.commit()
        conn.close()
        if job:
            return {"id": job[0], "video_id": job[1], "size_limit": job[2]}
    except Exception as e:
        emit_log(f"get_next_job error: {e}", "WORKER", "#ef4444", is_error=True)
    return None

def update_job_status(job_id, status, error_msg=None):
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("UPDATE repost_queue SET status = %s, error = %s, updated_at = NOW() WHERE id = %s", (status, error_msg, job_id))
            conn.commit()
        conn.close()
    except Exception as e:
        emit_log(f"update_job_status error: {e}", "DB", "#ef4444", is_error=True)

def get_queue_size():
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM repost_queue WHERE status = 'not started'")
            count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        emit_log(f"get_queue_size error: {e}", "DB", "#ef4444", is_error=True)
        return 0

def is_video_already_uploaded(thumbnail_hash):
    if not thumbnail_hash:
        return False
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM uploaded_videos WHERE hash = %s LIMIT 1", (thumbnail_hash,))
            exists = cur.fetchone() is not None
        conn.close()
        return exists
    except Exception as e:
        emit_log(f"is_video_already_uploaded error: {e}", "DB", "#ef4444", is_error=True)
        return False

def mark_video_uploaded(thumbnail_hash, account_video_id, original_source_id):
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO uploaded_videos (hash, video_id, original_source_id) VALUES (%s, %s, %s) ON CONFLICT (hash) DO NOTHING",
                (thumbnail_hash, account_video_id, original_source_id)
            )
            conn.commit()
        conn.close()
    except Exception as e:
        emit_log(f"mark_video_uploaded error: {e}", "DB", "#ef4444", is_error=True)

# ---------------------------
#  SYNC UPLOADED TABLE
# ---------------------------
def sync_uploaded_videos_from_profile():
    conf = get_settings()
    domain = conf.get("main_domain")
    username = conf.get("my_user")
    headers = get_headers()
    emit_log("🔄 Syncing uploaded videos table with profile...", "CLEANER", "#06b6d4")
    all_videos = []
    page, empty_pages = 0, 0
    session = requests.Session()
    while empty_pages < 2 and page < 80:
        try:
            res = session.post(f"https://{domain}/profile/{username}/videos/latest", headers=headers, json={"page": page}, timeout=15)
            vids = re.findall(r'"videoId":"([^"]+)"', res.text)
            if not vids:
                empty_pages += 1
            else:
                empty_pages = 0
                all_videos.extend(vids)
            page += 1
            time.sleep(0.5)
        except Exception as e:
            emit_log(f"Sync page {page} error: {e}", "CLEANER", "#ef4444", is_error=True)
            break
    all_videos = list(dict.fromkeys(all_videos))
    emit_log(f"Found {len(all_videos)} videos in profile. Hashing thumbnails...", "CLEANER", "#06b6d4")
    conn = psycopg2.connect(NEON_DB_URL)
    cur = conn.cursor()
    for vid in all_videos:
        try:
            img_res = session.get(f"https://{domain}/media/images/{vid}.jpg", stream=True, timeout=8)
            if img_res.status_code == 200:
                img_hash = hashlib.md5(img_res.content).hexdigest()
                cur.execute(
                    "INSERT INTO uploaded_videos (hash, video_id, original_source_id) VALUES (%s, %s, %s) ON CONFLICT (hash) DO UPDATE SET video_id = EXCLUDED.video_id",
                    (img_hash, vid, vid)
                )
        except Exception as e:
            continue
    conn.commit()
    conn.close()
    emit_log(f"✅ Synced {len(all_videos)} uploaded videos into database.", "CLEANER", "#10b981")

# ---------------------------
#  DUPLICATE CLEANER
# ---------------------------
def native_cleaner_task():
    conf = get_settings()
    payload = conf.get("del_payload", "")
    username = conf.get("my_user")
    domain = conf.get("main_domain")
    if not payload:
        emit_log("❌ Cleaner Aborted: Missing Encrypted Delete Payload!", "CLEANER", "#ef4444", is_error=True)
        return
    emit_log("🧹 SWARM CLEANER: Mapping entire profile...", "CLEANER", "#06b6d4")
    current_status["scraper"] = "Purging Duplicates..."
    all_videos = []
    page, empty_pages = 0, 0
    headers = get_headers()
    session = requests.Session()
    while empty_pages < 2 and page < 80:
        try:
            res = session.post(f"https://{domain}/profile/{username}/videos/latest", headers=headers, json={"page": page}, timeout=15)
            vids = re.findall(r'"videoId":"([^"]+)"', res.text)
            if not vids:
                empty_pages += 1
            else:
                empty_pages = 0
                all_videos.extend(vids)
            page += 1
            time.sleep(0.5)
        except Exception as e:
            emit_log(f"Cleaner page {page} error: {e}", "CLEANER", "#ef4444", is_error=True)
            break
    all_videos = list(dict.fromkeys(all_videos))
    all_videos.reverse()
    emit_log(f"🧹 Found {len(all_videos)} videos. Hashing thumbnails against DB...", "CLEANER", "#06b6d4")
    conn = psycopg2.connect(NEON_DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT vid, hash FROM image_hashes")
    known_hashes = {row[0]: row[1] for row in cur.fetchall()}
    seen_hashes_this_run = {}
    deleted_count = 0
    for vid in all_videos:
        img_hash = known_hashes.get(vid)
        if not img_hash:
            try:
                img_res = session.get(f"https://{domain}/media/images/{vid}.jpg", stream=True, timeout=8)
                if img_res.status_code == 200:
                    img_hash = hashlib.md5(img_res.content).hexdigest()
                    cur.execute("INSERT INTO image_hashes (vid, hash) VALUES (%s, %s) ON CONFLICT DO NOTHING", (vid, img_hash))
                    conn.commit()
                    known_hashes[vid] = img_hash
                else:
                    continue
            except Exception as e:
                continue
        if img_hash in seen_hashes_this_run:
            emit_log(f"🚨 DUPLICATE SPOTTED: {vid[:8]}... Firing Vaporize Payload!", "CLEANER", "#f43f5e")
            try:
                del_res = session.post(f"https://{domain}/uservideo/delete/{vid}", json={"username": payload}, headers=headers, timeout=10)
                if del_res.status_code == 200:
                    deleted_count += 1
            except Exception as e:
                emit_log(f"Delete failed for {vid}: {e}", "CLEANER", "#ef4444", is_error=True)
            time.sleep(1.2)
        else:
            seen_hashes_this_run[img_hash] = vid
    conn.close()
    emit_log(f"✨ CLEANUP COMPLETE! Destroyed {deleted_count} duplicates.", "CLEANER", "#10b981")
    sync_uploaded_videos_from_profile()
    current_status["scraper"] = "Idle"

# ---------------------------
#  SCRAPER MODULE
# ---------------------------
def run_async(coroutine):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coroutine)
    loop.close()

async def async_scrape_ids(mode, query):
    conf = get_settings()
    s_headers = get_headers()
    all_vids = set()
    page, empty_count = 0, 0
    async with aiohttp.ClientSession() as session:
        while empty_count < 3 and page < 50:
            try:
                if mode == "username":
                    url = f"https://{conf['main_domain']}/profile/{query}/videos/latest"
                    async with session.post(url, headers=s_headers, json={"page": page}, timeout=10) as r:
                        text = await r.text()
                else:  # keyword
                    url = f"https://{conf['main_domain']}/searchVideo?q={query}&p={page}"
                    async with session.get(url, headers=s_headers, timeout=10) as r:
                        text = await r.text()
                vids = re.findall(r'"videoId":"([^"]+)"', text)
                if not vids:
                    empty_count += 1
                else:
                    empty_count = 0
                    all_vids.update(vids)
                page += 1
            except Exception as e:
                emit_log(f"Scrape error at page {page}: {e}", "SCRAPE", "#ef4444", is_error=True)
                break
    return list(all_vids)

def extract_video_id_from_input(input_str):
    input_str = input_str.strip()
    # Match both raw ID and URL like https://viraly.wtf/XXXXX
    match = re.search(r'(?:video/)?([A-Za-z0-9_-]{20,})', input_str)
    if match:
        return match.group(1)
    # Fallback: if input looks like a short ID (e.g., 20+ chars)
    if re.match(r'^[A-Za-z0-9_-]{20,}$', input_str):
        return input_str
    return None

# ---------------------------
#  WORKER ENGINE (with detailed logs)
# ---------------------------
def reposter_worker():
    global current_status
    emit_log(f"👷 SWARM NODE ONLINE. Polling database for jobs...", "WORKER", "#f59e0b")
    while True:
        job = get_next_job()
        if not job:
            current_status["reposter"] = "Idle (Queue Empty)"
            time.sleep(15)
            continue
        video_id, size_limit = job["video_id"], job["size_limit"]
        current_status["reposter"] = f"Processing: {video_id[:8]}"
        raw_file = watermarked_file = preview_file = None
        conf = get_settings()
        h_media = get_headers()
        try:
            # Thumbnail duplicate check
            domain = conf['main_domain']
            thumb_url = f"https://{domain}/media/images/{video_id}.jpg"
            thumb_resp = requests.get(thumb_url, headers=h_media, timeout=8)
            thumb_hash = None
            if thumb_resp.status_code == 200:
                thumb_hash = hashlib.md5(thumb_resp.content).hexdigest()
                if is_video_already_uploaded(thumb_hash):
                    emit_log(f"⏭️ SKIPPED (duplicate hash): {video_id[:8]} already uploaded", "REPOST", "#f43f5e")
                    update_job_status(job["id"], 'completed', "Duplicate skipped (hash match)")
                    continue
            else:
                emit_log(f"⚠️ Could not fetch thumbnail for hash check, proceeding anyway", "REPOST", "#f59e0b")
            # Fetch video metadata
            title = f"Viral Video {video_id[:6]}"
            desc = "#trending #viral #reels"
            category_tag = "18+"
            original_username = ""
            try:
                r_api = requests.get(f"https://{domain}/video/{urllib.parse.quote(video_id, safe='')}", headers=h_media, timeout=10).json()
                vid_data = r_api[0] if isinstance(r_api, list) and len(r_api) > 0 else (r_api if isinstance(r_api, dict) else {})
                if vid_data.get("title"): title = vid_data["title"]
                if vid_data.get("description"): desc = vid_data["description"]
                if vid_data.get("tag"): category_tag = vid_data["tag"]
                if vid_data.get("username"): original_username = vid_data["username"]
            except Exception as e:
                emit_log(f"Metadata fetch failed: {e}", "REPOST", "#ef4444", is_error=True)
            # Self-loop shield
            if original_username and original_username.lower() == conf['my_user'].lower():
                emit_log(f"⏭️ SKIPPED: Belongs to {conf['my_user']} (Self-Loop)", "REPOST", "#f43f5e")
                update_job_status(job["id"], 'completed', "Self-loop skipped")
                continue
            # Blacklist
            bl_words = [w.strip().lower() for w in conf.get("blacklist", "").split(",") if w.strip()]
            if any(w in f"{title} {desc} {category_tag}".lower() for w in bl_words):
                emit_log(f"🛑 BLACKLISTED: Trashing video.", "REPOST", "#ef4444")
                update_job_status(job["id"], 'failed', "Blacklisted Keyword")
                continue
            # Size check
            d_url = f"https://{domain}/media/videos/{video_id}.mp4"
            size_mb = 0
            with requests.get(d_url, headers=h_media, stream=True, timeout=10) as r_size:
                if r_size.status_code == 200 and 'content-length' in r_size.headers:
                    size_mb = round(int(r_size.headers['content-length']) / (1024 * 1024), 2)
            if size_limit != 9999 and size_mb > size_limit:
                emit_log(f"⏭️ SKIPPED ➔ {size_mb}MB > {size_limit}MB", "REPOST", "#f43f5e")
                update_job_status(job["id"], 'failed', f"Skipped: Too Large")
                continue
            emit_log(f"📥 DOWNLOADING ➔ {size_mb}MB", "REPOST", "#0ea5e9")
            safe_label = re.sub(r'[^a-zA-Z0-9]', '_', video_id)[-12:]
            raw_file = os.path.join(VIDEO_DIR, f"raw_{safe_label}.mp4")
            watermarked_file = os.path.join(VIDEO_DIR, f"video_{safe_label}.mp4")
            preview_file = os.path.join(PREVIEW_DIR, f"{safe_label}.jpg")
            with requests.get(d_url, headers=h_media, stream=True) as s_res:
                if s_res.status_code != 200:
                    raise Exception(f"404 Not Found for {video_id}")
                with open(raw_file, 'wb') as f:
                    for chunk in s_res.iter_content(8192):
                        f.write(chunk)
            file_to_upload = raw_file
            if size_limit == 9999 and size_mb > 40:
                emit_log(f"⚡ NO LIMIT & >40MB ➔ Skipping watermark", "REPOST", "#d946ef")
                subprocess.run(['ffmpeg', '-y', '-i', raw_file, '-ss', '1', '-vframes', '1', preview_file], capture_output=True)
            else:
                emit_log(f"👻 GHOST WATERMARKING (Anti-Ban)...", "REPOST", "#d946ef")
                vf = "hflip,eq=brightness=0.02:saturation=1.05,scale='min(720,iw)':-2,drawtext=text='telugu stuffs':fontcolor=yellow@0.6:fontsize=24:x=(w-text_w)/2:y=h-th-14"
                subprocess.run(['ffmpeg', '-y', '-i', raw_file, '-ss', '1', '-vframes', '1', preview_file], capture_output=True)
                subprocess.run(['ffmpeg', '-y', '-i', raw_file, '-vf', vf, '-c:v', 'libx264', '-crf', '28', '-preset', 'ultrafast', '-c:a', 'copy', watermarked_file], capture_output=True)
                file_to_upload = watermarked_file
            emit_log(f"📤 UPLOADING... [{category_tag}]", "REPOST", "#0ea5e9")
            base = ".".join(conf['main_domain'].split('.')[-2:])
            with open(file_to_upload, 'rb') as f:
                up = requests.post(
                    f"https://{conf['upload_domain']}/upload",
                    files={'files': (f"video_{safe_label}.mp4", f, 'video/mp4')},
                    data={"tag": category_tag, "title": title, "description": desc, "country": "IN", "username": conf['my_user'], "start": "0", "end": "0"},
                    headers={"Cookie": h_media["Cookie"], "Origin": f"https://{base}"}
                )
            response_text = up.text
            if up.status_code == 200 or (up.status_code == 400 and "allowedMimeTypes is not defined" in response_text):
                uploaded_video_id = re.search(r'"videoId":"([^"]+)"', response_text)
                uploaded_video_id = uploaded_video_id.group(1) if uploaded_video_id else video_id
                if thumb_hash:
                    mark_video_uploaded(thumb_hash, uploaded_video_id, video_id)
                emit_log(f"✅ SUCCESS ➔ {video_id[:8]} (Uploaded as {uploaded_video_id})", "REPOST", "#10b981")
                update_job_status(job["id"], 'completed')
            else:
                raise Exception(f"HTTP {up.status_code} | {response_text[:200]}")
        except Exception as e:
            emit_log(f"🔥 Error processing {video_id}: {e}", "REPOST", "#ef4444", is_error=True)
            update_job_status(job["id"], 'failed', str(e))
        finally:
            for f_path in [raw_file, watermarked_file, preview_file]:
                if f_path and os.path.exists(f_path):
                    try:
                        os.remove(f_path)
                    except:
                        pass

threading.Thread(target=reposter_worker, daemon=True).start()

# ---------------------------
#  FLASK WEB UI (Two Sections with better feedback)
# ---------------------------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>V9.5 Autonomous Swarm Node</title>
    <style>
        :root { --bg: #0f172a; --panel: #1e293b; --acc: #3b82f6; --text: #f8fafc; --grn: #10b981; }
        body { font-family: 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 15px; padding-bottom: 80px; }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
        .card { background: var(--panel); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0,0,0,0.5); border-top: 3px solid var(--acc); margin-bottom: 15px; }
        h2, h3 { margin-top: 0; color: #fff; }
        input, select, textarea, button { width: 100%; padding: 12px; margin-top: 8px; border-radius: 4px; border: 1px solid #334155; background: #020617; color: #fff; box-sizing: border-box; }
        button { background: var(--acc); color: #fff; font-weight: bold; border: none; cursor: pointer; transition: 0.2s; }
        button:hover { filter: brightness(1.2); }
        .btn-supabase { background: #8b5cf6; }
        .btn-repost { background: #f59e0b; }
        .btn-red { background: #ef4444; }
        .btn-sync { background: #10b981; }
        #logs { height: 400px; overflow-y: auto; background: #020617; padding: 15px; font-family: 'Consolas', monospace; font-size: 13px; border-radius: 6px; margin-top: 10px; border: 1px solid #334155; line-height: 1.6; }
        .status-bar { display: flex; justify-content: space-between; background: #020617; padding: 12px; border-radius: 6px; font-size: 14px; margin-bottom: 15px; border-left: 4px solid var(--grn); align-items: center; flex-wrap: wrap; gap: 10px; }
        .sm-label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; display: block; margin-top: 10px; }
        .inline-group { display: flex; gap: 10px; align-items: center; }
        .inline-group > * { margin-top: 0; }
        hr { border-color: #334155; margin: 15px 0; }
        .toast { position: fixed; bottom: 20px; right: 20px; background: #1e293b; border-left: 4px solid #10b981; padding: 12px 20px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); z-index: 1000; font-size: 14px; max-width: 300px; transition: opacity 0.3s; }
        @media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <h2>🐝 V9.5 DUAL-MODE SWARM NODE</h2>
    <div class="status-bar">
        <div>
            <b>[SCRAPER]</b> <span id="s-scrape" style="color:#3b82f6;">Idle</span> &nbsp;|&nbsp;
            <b>[WORKER]</b> <span id="s-repost" style="color:#f59e0b;">Idle</span> &nbsp;|&nbsp;
            <b>[QUEUE]</b> <span id="s-q" style="color:#10b981;">0</span>
        </div>
    </div>
    <div class="grid-2">
        <!-- SECTION 1: SUPABASE ARCHIVER -->
        <div class="card" style="border-top-color: #8b5cf6;">
            <h3>📦 SECTION 1: Supabase Archiver</h3>
            <p style="font-size:12px; color:#94a3b8;">Scrape IDs or single ID/link → store to your Supabase DB (no worker).</p>
            <div class="inline-group">
                <select id="arch_mode" style="width:35%;">
                    <option value="keyword">Keyword</option>
                    <option value="username">Username</option>
                    <option value="single">Single ID/Link</option>
                </select>
                <input id="arch_target" placeholder="Keyword, username, or video link/ID..." style="width:65%;">
            </div>
            <button onclick="startSupabaseArchive()" class="btn-supabase">🚀 ARCHIVE TO SUPABASE</button>
        </div>
        <!-- SECTION 2: WORKER QUEUE & REPOSTER -->
        <div class="card" style="border-top-color: #f59e0b;">
            <h3>🎬 SECTION 2: Worker Queue & Reposter</h3>
            <p style="font-size:12px; color:#94a3b8;">Scrape IDs → queue → download → watermark (skip if >40MB on No limit) → upload to your account.</p>
            <div class="inline-group">
                <select id="rep_mode" style="width:35%;">
                    <option value="keyword">Keyword</option>
                    <option value="username">Username</option>
                    <option value="manual">Manual IDs (line sep)</option>
                </select>
                <input id="rep_input" placeholder="Keyword, username, or paste IDs/links..." style="width:65%;">
            </div>
            <div class="inline-group">
                <select id="size_limit" style="width:40%;">
                    <option value="20">20 MB</option>
                    <option value="30">30 MB</option>
                    <option value="40">40 MB</option>
                    <option value="9999">No limit</option>
                </select>
                <button onclick="startReposter()" class="btn-repost" style="width:60%;">⚙️ SCRAPE & ADD TO QUEUE</button>
            </div>
            <hr>
            <div class="inline-group">
                <button onclick="runCleaner()" class="btn-red" style="width:50%;">🧹 DELETE DUPLICATES</button>
                <button onclick="syncUploadedTable()" class="btn-sync" style="width:50%;">🔄 SYNC UPLOADED TABLE</button>
            </div>
        </div>
    </div>
    <div class="card" style="margin-top: 15px; border-top-color:#10b981;">
        <div style="display:flex; justify-content:space-between; align-items:flex-start;">
            <h3>🖥️ Logs & Settings</h3>
            <button onclick="saveConfig()" style="width:auto; padding:5px 15px; font-size:12px; background:#475569;">💾 SAVE OVERRIDES</button>
        </div>
        <div class="grid-2" style="margin-bottom:10px;">
            <div><span class="sm-label">Account Token</span><input id="set_token" placeholder="Access Token"></div>
            <div><span class="sm-label">Username</span><input id="set_user" placeholder="Target Username"></div>
            <div><span class="sm-label">Encrypted Delete Payload</span><input id="set_del" placeholder="U2FsdGVkX1+0BWWOC9q0iGdVxX..."></div>
            <div><span class="sm-label">Smart Blacklist</span><input id="set_bl" placeholder="promo, link in bio..."></div>
        </div>
        <div><span class="sm-label">Full Browser Cookie Header</span><input id="set_cookie" placeholder="_ga=GA1.1...; accessToken=..."></div>
        <div id="logs">Loading logs...</div>
    </div>
    <div id="toast" class="toast" style="display:none;"></div>
    <script>
        function showToast(msg, isError=false) {
            let toast = document.getElementById('toast');
            toast.style.display = 'block';
            toast.style.borderLeftColor = isError ? '#ef4444' : '#10b981';
            toast.innerHTML = msg;
            setTimeout(() => { toast.style.display = 'none'; }, 4000);
        }
        async function startSupabaseArchive() {
            let mode = document.getElementById('arch_mode').value;
            let target = document.getElementById('arch_target').value.trim();
            if(!target) { showToast("Enter target!", true); return; }
            let resp = await fetch('/api/supabase_archive', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({mode:mode, target:target})});
            let data = await resp.json();
            showToast(data.message);
            document.getElementById('arch_target').value = '';
        }
        async function startReposter() {
            let mode = document.getElementById('rep_mode').value;
            let input = document.getElementById('rep_input').value.trim();
            let limit = document.getElementById('size_limit').value;
            if(!input) { showToast("Enter target or IDs!", true); return; }
            await fetch('/api/repost', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({mode:mode, input:input, size_limit: parseInt(limit)})});
            showToast("Scraping and adding to queue... Check logs.");
            document.getElementById('rep_input').value = '';
        }
        async function runCleaner() {
            if(confirm("Delete duplicate reels from your account?")) {
                await fetch('/api/cleaner', {method:'POST'});
                showToast("Duplicate cleaner started.");
            }
        }
        async function syncUploadedTable() {
            if(confirm("Sync uploaded videos table with your profile?")) {
                await fetch('/api/sync_uploaded', {method:'POST'});
                showToast("Sync started.");
            }
        }
        async function saveConfig() {
            let payload = {
                my_token: document.getElementById('set_token').value,
                my_user: document.getElementById('set_user').value,
                blacklist: document.getElementById('set_bl').value,
                del_payload: document.getElementById('set_del').value,
                full_cookie: document.getElementById('set_cookie').value
            };
            await fetch('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
            showToast("Settings saved.");
        }
        setInterval(async () => {
            try {
                let r = await fetch('/api/status');
                let d = await r.json();
                document.getElementById('s-scrape').innerText = d.scraper;
                document.getElementById('s-repost').innerText = d.reposter;
                document.getElementById('s-q').innerText = d.queue_size;
                let logsDiv = document.getElementById('logs');
                let isScrolledToBottom = logsDiv.scrollHeight - logsDiv.clientHeight <= logsDiv.scrollTop + 1;
                logsDiv.innerHTML = d.logs.map(l => `<span style='color:#64748b'>[${l.time}]</span> <span style='color:${l.color}'>[${l.category}]</span> ${l.message}`).join('<br>');
                if (isScrolledToBottom) logsDiv.scrollTop = logsDiv.scrollHeight;
            } catch(e) {}
        }, 1500);
        (async () => {
            let r = await fetch('/api/settings');
            let d = await r.json();
            document.getElementById('set_token').value = d.my_token || "";
            document.getElementById('set_user').value = d.my_user || "";
            document.getElementById('set_bl').value = d.blacklist || "";
            document.getElementById('set_del').value = d.del_payload || "";
            document.getElementById('set_cookie').value = d.full_cookie || "";
        })();
    </script>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(HTML_TEMPLATE)

@app.route('/health')
def health():
    return jsonify({"status": "awake"}), 200

@app.route('/api/status')
def api_status():
    current_status["queue_size"] = get_queue_size()
    # Convert log_messages to serializable list
    logs_serializable = []
    for log in log_messages:
        logs_serializable.append({
            "time": log.get("time", ""),
            "category": log.get("category", ""),
            "message": log.get("message", ""),
            "color": log.get("color", "#10b981"),
            "is_error": log.get("is_error", False)
        })
    return jsonify(current_status | {"logs": logs_serializable})

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    if request.method == 'GET':
        return jsonify(get_settings())
    data = request.json
    conf = get_settings()
    conf.update({
        "my_token": data.get("my_token", conf["my_token"]),
        "my_user": data.get("my_user", conf["my_user"]),
        "blacklist": data.get("blacklist", conf["blacklist"]),
        "del_payload": data.get("del_payload", conf["del_payload"]),
        "full_cookie": data.get("full_cookie", conf["full_cookie"])
    })
    json.dump(conf, open(SETTINGS_FILE, 'w'), indent=4)
    emit_log("Settings updated.", "SYS", "#10b981")
    return jsonify({"status": "ok"})

@app.route('/api/supabase_archive', methods=['POST'])
def api_supabase_archive():
    data = request.json
    mode = data.get('mode')
    target = data.get('target', '').strip()
    if not target:
        return jsonify({"error": "Empty target", "message": "Please enter a target"}), 400
    def archive_task():
        if mode == 'single':
            vid = extract_video_id_from_input(target)
            if vid:
                insert_into_supabase([vid])
                emit_log(f"Archived single video {vid} to Supabase", "ARCHIVE", "#8b5cf6")
            else:
                emit_log(f"Invalid video ID/link: {target}", "ARCHIVE", "#ef4444", is_error=True)
        else:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            vids = loop.run_until_complete(async_scrape_ids(mode, target))
            loop.close()
            if vids:
                insert_into_supabase(vids)
                emit_log(f"Archived {len(vids)} IDs from {mode} '{target}' to Supabase", "ARCHIVE", "#8b5cf6")
            else:
                emit_log(f"No IDs found for {mode} '{target}'", "ARCHIVE", "#ef4444", is_error=True)
    threading.Thread(target=archive_task, daemon=True).start()
    return jsonify({"message": "Archive task started. Check logs for details."})

@app.route('/api/repost', methods=['POST'])
def api_repost():
    data = request.json
    mode = data.get('mode', 'manual')
    input_val = data['input'].strip()
    size_limit = data['size_limit']
    def handle_queueing():
        if mode == "manual":
            ids = []
            for line in input_val.replace(',', '\n').split('\n'):
                line = line.strip()
                if not line: continue
                vid = extract_video_id_from_input(line)
                if vid:
                    ids.append(vid)
                else:
                    emit_log(f"Could not extract ID from: {line}", "QUEUE", "#ef4444", is_error=True)
            if not ids:
                emit_log("No valid IDs found in manual input", "QUEUE", "#ef4444", is_error=True)
                return
            new_ids = filter_existing_ids(ids)
            skipped = len(ids) - len(new_ids)
            added = 0
            for vid in new_ids:
                if add_to_neon_queue(vid, size_limit):
                    added += 1
            emit_log(f"Manual Input: Skipped {skipped} duplicates. Added {added} NEW jobs to Global Queue", "NODE", "#f59e0b")
        else:
            emit_log(f"Scraping {mode}: '{input_val}' to add to Worker Queue...", "REPOST", "#f59e0b")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            scraped_ids = loop.run_until_complete(async_scrape_ids(mode, input_val))
            loop.close()
            if not scraped_ids:
                emit_log(f"No IDs found for {mode} '{input_val}'", "QUEUE", "#ef4444", is_error=True)
                return
            new_ids = filter_existing_ids(scraped_ids)
            skipped = len(scraped_ids) - len(new_ids)
            added = 0
            for vid in new_ids:
                if add_to_neon_queue(vid, size_limit):
                    added += 1
            emit_log(f"Scrape Done! Found {len(scraped_ids)}. Skipped {skipped} existing. Added {added} NEW jobs to Global Queue.", "SCRAPE", "#3b82f6")
    threading.Thread(target=handle_queueing, daemon=True).start()
    return jsonify({"status": "queued", "message": "Scraping and queueing started."})

@app.route('/api/cleaner', methods=['POST'])
def api_cleaner():
    threading.Thread(target=native_cleaner_task, daemon=True).start()
    return jsonify({"status": "started", "message": "Duplicate cleaner started."})

@app.route('/api/sync_uploaded', methods=['POST'])
def api_sync_uploaded():
    threading.Thread(target=sync_uploaded_videos_from_profile, daemon=True).start()
    return jsonify({"status": "sync started", "message": "Sync started."})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5050))
    print(f"🚀 STARTING V9.5 DUAL-MODE SWARM NODE on Port {port}...")
    app.run(host='0.0.0.0', port=port, threaded=True)