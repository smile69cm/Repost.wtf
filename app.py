import os, json, time, subprocess, requests, threading, re, urllib.parse, hashlib, traceback, fcntl, sys
import asyncio, aiohttp
from flask import Flask, render_template_string, request, jsonify, session, redirect, url_for
from functools import wraps
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET', 'changeme_please_use_env_var_!@#$')

# ---------------------------
#  INDIAN TIMEZONE (IST)
# ---------------------------
IST = timezone(timedelta(hours=5, minutes=30))

def ist_now():
    return datetime.now(IST)

def ist_time_str():
    return ist_now().strftime("%H:%M:%S")

# ---------------------------
#  ENVIRONMENT VARIABLES (with hardcoded fallback for convenience)
# ---------------------------
NEON_DB_URL = os.environ.get('NEON_DB_URL', "postgresql://neondb_owner:npg_Rh0xIbmdFe5u@ep-quiet-block-a12aatzr-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require")
SUPABASE_URL = os.environ.get('SUPABASE_URL', "https://cnkbewgpguyojiebztbs.supabase.co/rest/v1/reels")
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNua2Jld2dwZ3V5b2ppZWJ6dGJzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQyODU0NzUsImV4cCI6MjA4OTg2MTQ3NX0.ldS5knPaT1imexuRH9jSlTDB1mRSpoozFXlmhbDw2fU")
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin123')  # Change this!
FFMPEG_PATH = os.environ.get('FFMPEG_PATH', 'ffmpeg')

# ---------------------------
#  DIRECTORIES & CONFIG
# ---------------------------
BASE_DIR = os.getcwd()
VIDEO_DIR = os.path.join(BASE_DIR, "watermarked_videos")
PREVIEW_DIR = os.path.join(BASE_DIR, "previews")
SETTINGS_FILE = "settings.json"
STATE_FILE = "state.json"          # Cross-process state (logs, status)
LOCK_FILE = "/tmp/swarm.lock"      # Simple file lock for critical sections

for d in [VIDEO_DIR, PREVIEW_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)

# ---------------------------
#  FFMPEG CHECK
# ---------------------------
def check_ffmpeg():
    try:
        subprocess.run([FFMPEG_PATH, '-version'], capture_output=True, check=True)
        return True
    except:
        return False

FFMPEG_AVAILABLE = check_ffmpeg()
if not FFMPEG_AVAILABLE:
    print("⚠️ WARNING: FFmpeg not found! Watermarking will be skipped.")

# ---------------------------
#  CROSS‑PROCESS STATE (file‑based)
# ---------------------------
def read_state():
    """Read state from JSON file with locking."""
    try:
        with open(STATE_FILE, 'r') as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            fcntl.flock(f, fcntl.LOCK_UN)
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {"logs": [], "scraper": "Idle", "reposter": "Idle", "queue_size": 0, "cleaner_running": False, "scraper_running": False}

def write_state(data):
    """Write state to JSON file with locking."""
    with open(STATE_FILE, 'w') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(data, f, indent=2)
        fcntl.flock(f, fcntl.LOCK_UN)

def emit_log(msg, category="SYS", color="#10b981", is_error=False):
    t = ist_time_str()
    full_msg = f"[{t}] [{category}] {msg}"
    print(full_msg)
    if is_error:
        print(traceback.format_exc())
    state = read_state()
    state["logs"].append({
        "time": t,
        "category": category,
        "message": msg,
        "color": color,
        "is_error": is_error
    })
    if len(state["logs"]) > 200:
        state["logs"] = state["logs"][-200:]
    write_state(state)

def update_status(scraper=None, reposter=None, queue_size=None):
    state = read_state()
    if scraper is not None:
        state["scraper"] = scraper
    if reposter is not None:
        state["reposter"] = reposter
    if queue_size is not None:
        state["queue_size"] = queue_size
    write_state(state)

# ---------------------------
#  DATABASE CONNECTION POOL
# ---------------------------
db_pool = None

def init_db_pool():
    global db_pool
    try:
        db_pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, dsn=NEON_DB_URL)
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        db_pool.putconn(conn)
        print("✅ Database connection pool established")
    except Exception as e:
        print(f"❌ Failed to create connection pool: {e}")
        db_pool = None

def get_db_connection():
    if db_pool:
        return db_pool.getconn()
    return psycopg2.connect(NEON_DB_URL)

def return_db_connection(conn):
    if db_pool:
        db_pool.putconn(conn)
    else:
        conn.close()

init_db_pool()

# ---------------------------
#  INIT DATABASE TABLES
# ---------------------------
def init_neon_db():
    conn = get_db_connection()
    try:
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
    except Exception as e:
        print(f"⚠️ Neon DB Init Error: {e}")
    finally:
        return_db_connection(conn)

init_neon_db()

# ---------------------------
#  SETTINGS MANAGER (local file, no cross‑process sharing needed)
# ---------------------------
DEFAULT_SETTINGS = {
    "my_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InRlbHVndXN0dWZmcyIsImlhdCI6MTc3NTMyMDc3OSwiZXhwIjoxNzc3OTEyNzc5fQ.48_8h8tDpZapGhFzMFgb9-DJSa9UZyArE2gvyJbk-1Y",
    "my_user": "telugustuffs",
    "main_domain": "love.viraly.wtf",
    "upload_domain": "loveupload.viraly.wtf",
    "blacklist": "",
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
#  AUTHENTICATION DECORATOR
# ---------------------------
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('home'))
        else:
            return render_template_string(LOGIN_TEMPLATE, error="Invalid password")
    return render_template_string(LOGIN_TEMPLATE, error=None)

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

# ---------------------------
#  THREAD LOCKS (prevent concurrent long operations)
# ---------------------------
cleaner_lock = threading.Lock()
scraper_lock = threading.Lock()

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
    records = [{"id": vid, "scraped_at": ist_now().strftime("%Y-%m-%d %H:%M:%S")} for vid in video_ids]
    try:
        r = requests.post(SUPABASE_URL, headers=headers, json=records, timeout=10)
        if r.status_code >= 400:
            emit_log(f"Supabase insert error: {r.text[:200]}", "SUPABASE", "#ef4444", is_error=True)
        else:
            emit_log(f"Archived {len(video_ids)} IDs to Supabase", "SUPABASE", "#10b981")
    except Exception as e:
        emit_log(f"Supabase connection error: {e}", "SUPABASE", "#ef4444", is_error=True)

# ---------------------------
#  QUEUE HELPERS (using connection pool)
# ---------------------------
def filter_existing_ids(vid_list):
    if not vid_list:
        return []
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            format_strings = ','.join(['%s'] * len(vid_list))
            cur.execute(f"SELECT video_id FROM repost_queue WHERE video_id IN ({format_strings})", tuple(vid_list))
            existing = {row[0] for row in cur.fetchall()}
        return [v for v in vid_list if v not in existing]
    except Exception as e:
        emit_log(f"filter_existing_ids error: {e}", "DB", "#ef4444", is_error=True)
        return vid_list
    finally:
        return_db_connection(conn)

def add_to_neon_queue(video_id, size_limit=10):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO repost_queue (video_id, size_limit, status, updated_at) VALUES (%s, %s, 'not started', NOW()) ON CONFLICT (video_id) DO NOTHING;",
                (video_id, size_limit)
            )
            inserted = cur.rowcount > 0
            conn.commit()
        return inserted
    except Exception as e:
        emit_log(f"add_to_neon_queue error for {video_id}: {e}", "DB", "#ef4444", is_error=True)
        return False
    finally:
        return_db_connection(conn)

def get_next_job():
    conn = get_db_connection()
    try:
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
        if job:
            return {"id": job[0], "video_id": job[1], "size_limit": job[2]}
    except Exception as e:
        emit_log(f"get_next_job error: {e}", "WORKER", "#ef4444", is_error=True)
        return None
    finally:
        return_db_connection(conn)

def update_job_status(job_id, status, error_msg=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE repost_queue SET status = %s, error = %s, updated_at = NOW() WHERE id = %s", (status, error_msg, job_id))
            conn.commit()
    except Exception as e:
        emit_log(f"update_job_status error: {e}", "DB", "#ef4444", is_error=True)
    finally:
        return_db_connection(conn)

def get_queue_size():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM repost_queue WHERE status = 'not started'")
            count = cur.fetchone()[0]
        return count
    except Exception as e:
        emit_log(f"get_queue_size error: {e}", "DB", "#ef4444", is_error=True)
        return 0
    finally:
        return_db_connection(conn)

def is_video_already_uploaded(thumbnail_hash):
    if not thumbnail_hash:
        return False
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM uploaded_videos WHERE hash = %s LIMIT 1", (thumbnail_hash,))
            exists = cur.fetchone() is not None
        return exists
    except Exception as e:
        emit_log(f"is_video_already_uploaded error: {e}", "DB", "#ef4444", is_error=True)
        return False
    finally:
        return_db_connection(conn)

def mark_video_uploaded(thumbnail_hash, account_video_id, original_source_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO uploaded_videos (hash, video_id, original_source_id) VALUES (%s, %s, %s) ON CONFLICT (hash) DO NOTHING",
                (thumbnail_hash, account_video_id, original_source_id)
            )
            conn.commit()
    except Exception as e:
        emit_log(f"mark_video_uploaded error: {e}", "DB", "#ef4444", is_error=True)
    finally:
        return_db_connection(conn)

# ---------------------------
#  SYNC UPLOADED TABLE
# ---------------------------
def sync_uploaded_videos_from_profile():
    if not cleaner_lock.acquire(blocking=False):
        emit_log("Sync already running, skipping.", "CLEANER", "#f59e0b")
        return
    try:
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
        conn = get_db_connection()
        try:
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
                except Exception:
                    continue
            conn.commit()
        except Exception as e:
            emit_log(f"Sync DB error: {e}", "CLEANER", "#ef4444", is_error=True)
        finally:
            return_db_connection(conn)
        emit_log(f"✅ Synced {len(all_videos)} uploaded videos into database.", "CLEANER", "#10b981")
    finally:
        cleaner_lock.release()

# ---------------------------
#  DUPLICATE CLEANER
# ---------------------------
def native_cleaner_task():
    if not cleaner_lock.acquire(blocking=False):
        emit_log("Cleaner already running, skipping.", "CLEANER", "#f59e0b")
        return
    try:
        conf = get_settings()
        payload = conf.get("del_payload", "")
        username = conf.get("my_user")
        domain = conf.get("main_domain")
        if not payload:
            emit_log("❌ Cleaner Aborted: Missing Encrypted Delete Payload!", "CLEANER", "#ef4444", is_error=True)
            return
        emit_log("🧹 SWARM CLEANER: Mapping entire profile...", "CLEANER", "#06b6d4")
        update_status(scraper="Purging Duplicates...")
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
        conn = get_db_connection()
        try:
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
                    except Exception:
                        continue
                if img_hash in seen_hashes_this_run:
                    emit_log(f"🚨 DUPLICATE SPOTTED: {vid[:8]}... Deleting", "CLEANER", "#f43f5e")
                    try:
                        del_res = session.post(f"https://{domain}/uservideo/delete/{vid}", json={"username": payload}, headers=headers, timeout=10)
                        if del_res.status_code == 200:
                            deleted_count += 1
                    except Exception as e:
                        emit_log(f"Delete failed for {vid}: {e}", "CLEANER", "#ef4444", is_error=True)
                    time.sleep(1.2)
                else:
                    seen_hashes_this_run[img_hash] = vid
            emit_log(f"✨ CLEANUP COMPLETE! Destroyed {deleted_count} duplicates.", "CLEANER", "#10b981")
        except Exception as e:
            emit_log(f"Cleaner DB error: {e}", "CLEANER", "#ef4444", is_error=True)
        finally:
            return_db_connection(conn)
        sync_uploaded_videos_from_profile()
    finally:
        update_status(scraper="Idle")
        cleaner_lock.release()

# ---------------------------
#  PERSISTENT ASYNC LOOP
# ---------------------------
async_loop = None
async_thread = None

def start_async_loop():
    global async_loop
    async_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(async_loop)
    async_loop.run_forever()

def run_coroutine(coro):
    if async_loop is None or not async_loop.is_running():
        return asyncio.run(coro)
    future = asyncio.run_coroutine_threadsafe(coro, async_loop)
    return future.result()

async_thread = threading.Thread(target=start_async_loop, daemon=True)
async_thread.start()
time.sleep(0.1)

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
                else:
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
    if input_str.startswith('http://') or input_str.startswith('https://'):
        parsed = urllib.parse.urlparse(input_str)
        last_segment = parsed.path.rstrip('/').split('/')[-1]
        if last_segment:
            return last_segment
    if re.match(r'^[A-Za-z0-9_\-=+/]+$', input_str):
        return input_str
    return None

# ---------------------------
#  WORKER ENGINE
# ---------------------------
def reposter_worker():
    emit_log(f"👷 SWARM NODE ONLINE. Polling database for jobs...", "WORKER", "#f59e0b")
    while True:
        try:
            job = get_next_job()
            if not job:
                update_status(reposter="Idle (Queue Empty)")
                time.sleep(15)
                continue
            video_id, size_limit = job["video_id"], job["size_limit"]
            update_status(reposter=f"Processing: {video_id[:8]}")
            raw_file = watermarked_file = preview_file = None
            conf = get_settings()
            h_media = get_headers()
            try:
                domain = conf['main_domain']
                # Thumbnail check
                thumb_url = f"https://{domain}/media/images/{video_id}.jpg"
                thumb_resp = requests.get(thumb_url, headers=h_media, timeout=8)
                thumb_hash = None
                if thumb_resp.status_code == 200:
                    thumb_hash = hashlib.md5(thumb_resp.content).hexdigest()
                    if is_video_already_uploaded(thumb_hash):
                        emit_log(f"⏭️ SKIPPED (duplicate hash): {video_id[:8]}", "REPOST", "#f43f5e")
                        update_job_status(job["id"], 'completed', "Duplicate hash")
                        continue
                # Metadata
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
                # Self-loop
                if original_username and original_username.lower() == conf['my_user'].lower():
                    emit_log(f"⏭️ SKIPPED: Belongs to {conf['my_user']}", "REPOST", "#f43f5e")
                    update_job_status(job["id"], 'completed', "Self-loop")
                    continue
                # Blacklist
                bl_words = [w.strip().lower() for w in conf.get("blacklist", "").split(",") if w.strip()]
                if any(w in f"{title} {desc} {category_tag}".lower() for w in bl_words):
                    emit_log(f"🛑 BLACKLISTED: Trashing video.", "REPOST", "#ef4444")
                    update_job_status(job["id"], 'failed', "Blacklisted")
                    continue
                # Size
                d_url = f"https://{domain}/media/videos/{video_id}.mp4"
                size_mb = 0
                with requests.get(d_url, headers=h_media, stream=True, timeout=10) as r_size:
                    if r_size.status_code == 200 and 'content-length' in r_size.headers:
                        size_mb = round(int(r_size.headers['content-length']) / (1024 * 1024), 2)
                if size_limit != 9999 and size_mb > size_limit:
                    emit_log(f"⏭️ SKIPPED ➔ {size_mb}MB > {size_limit}MB", "REPOST", "#f43f5e")
                    update_job_status(job["id"], 'failed', f"Too large: {size_mb}MB > {size_limit}MB")
                    continue
                emit_log(f"📥 DOWNLOADING ➔ {size_mb}MB", "REPOST", "#0ea5e9")
                safe_label = re.sub(r'[^a-zA-Z0-9]', '_', video_id)[-12:]
                raw_file = os.path.join(VIDEO_DIR, f"raw_{safe_label}.mp4")
                watermarked_file = os.path.join(VIDEO_DIR, f"video_{safe_label}.mp4")
                preview_file = os.path.join(PREVIEW_DIR, f"{safe_label}.jpg")
                with requests.get(d_url, headers=h_media, stream=True) as s_res:
                    if s_res.status_code != 200:
                        raise Exception(f"404 Not Found")
                    with open(raw_file, 'wb') as f:
                        for chunk in s_res.iter_content(8192):
                            f.write(chunk)
                file_to_upload = raw_file
                if size_limit == 9999 and size_mb > 40:
                    emit_log(f"⚡ NO LIMIT & >40MB ➔ Skipping watermark", "REPOST", "#d946ef")
                    subprocess.run([FFMPEG_PATH, '-y', '-i', raw_file, '-ss', '1', '-vframes', '1', preview_file], capture_output=True)
                elif not FFMPEG_AVAILABLE:
                    emit_log(f"⚠️ FFmpeg missing → uploading raw", "REPOST", "#f59e0b", is_error=True)
                else:
                    emit_log(f"👻 GHOST WATERMARKING...", "REPOST", "#d946ef")
                    vf = "hflip,eq=brightness=0.02:saturation=1.05,scale='min(720,iw)':-2,drawtext=text='telugu stuffs':fontcolor=yellow@0.6:fontsize=24:x=(w-text_w)/2:y=h-th-14"
                    subprocess.run([FFMPEG_PATH, '-y', '-i', raw_file, '-ss', '1', '-vframes', '1', preview_file], capture_output=True)
                    subprocess.run([FFMPEG_PATH, '-y', '-i', raw_file, '-vf', vf, '-c:v', 'libx264', '-crf', '28', '-preset', 'ultrafast', '-c:a', 'copy', watermarked_file], capture_output=True)
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
        except Exception as e:
            emit_log(f"Worker loop fatal: {e}", "WORKER", "#ef4444", is_error=True)
            time.sleep(10)

threading.Thread(target=reposter_worker, daemon=True).start()

# ---------------------------
#  FLASK UI WITH PROFESSIONAL LOOK
# ---------------------------
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Swarm Node Login</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%); display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .login-card { background: rgba(30, 41, 59, 0.9); backdrop-filter: blur(10px); padding: 40px; border-radius: 20px; box-shadow: 0 8px 32px rgba(0,0,0,0.3); width: 350px; text-align: center; border: 1px solid rgba(255,255,255,0.1); }
        h2 { color: #f8fafc; margin-bottom: 20px; }
        input { width: 100%; padding: 12px; margin: 10px 0; border-radius: 8px; border: none; background: #0f172a; color: white; }
        button { width: 100%; padding: 12px; background: #3b82f6; border: none; border-radius: 8px; color: white; font-weight: bold; cursor: pointer; transition: 0.2s; }
        button:hover { background: #2563eb; }
        .error { color: #ef4444; margin-top: 10px; }
    </style>
</head>
<body>
    <div class="login-card">
        <h2>🔐 Swarm Node Access</h2>
        <form method="POST">
            <input type="password" name="password" placeholder="Enter password" autofocus>
            <button type="submit">Authenticate</button>
            {% if error %}<div class="error">{{ error }}</div>{% endif %}
        </form>
    </div>
</body>
</html>
"""

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>V11.0 Swarm Node - Production</title>
    <style>
        :root { --bg: #0f172a; --panel: #1e293b; --acc: #3b82f6; --text: #f8fafc; --grn: #10b981; }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Inter', 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px; flex-wrap: wrap; gap: 15px; }
        h1 { font-size: 1.8rem; background: linear-gradient(135deg, #60a5fa, #a78bfa); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .logout-btn { background: #ef4444; padding: 8px 20px; border-radius: 8px; text-decoration: none; color: white; font-weight: 500; transition: 0.2s; }
        .logout-btn:hover { background: #dc2626; }
        .status-bar { background: var(--panel); border-radius: 12px; padding: 15px 20px; margin-bottom: 25px; display: flex; gap: 30px; flex-wrap: wrap; border-left: 4px solid var(--grn); box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); }
        .status-item { display: flex; align-items: baseline; gap: 8px; }
        .status-label { font-weight: 600; color: #94a3b8; text-transform: uppercase; font-size: 0.75rem; letter-spacing: 1px; }
        .status-value { font-weight: 700; font-size: 1.1rem; }
        .grid-2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 25px; margin-bottom: 25px; }
        .card { background: var(--panel); border-radius: 16px; padding: 24px; box-shadow: 0 8px 16px -4px rgba(0,0,0,0.3); transition: transform 0.2s; border-top: 3px solid var(--acc); }
        .card:hover { transform: translateY(-2px); }
        .card h2 { font-size: 1.4rem; margin-bottom: 16px; display: flex; align-items: center; gap: 10px; }
        .input-group { display: flex; gap: 12px; margin: 15px 0; flex-wrap: wrap; }
        .input-group select, .input-group input { flex: 1; padding: 12px; border-radius: 10px; border: 1px solid #334155; background: #020617; color: white; font-size: 0.9rem; }
        button { background: var(--acc); padding: 12px 20px; border: none; border-radius: 10px; color: white; font-weight: 600; cursor: pointer; transition: 0.2s; font-size: 0.9rem; }
        button:hover { filter: brightness(1.1); transform: scale(0.98); }
        .btn-purple { background: #8b5cf6; }
        .btn-orange { background: #f59e0b; }
        .btn-red { background: #ef4444; }
        .btn-green { background: #10b981; }
        hr { border-color: #334155; margin: 20px 0; }
        .logs-panel { background: #020617; border-radius: 16px; padding: 20px; margin-top: 25px; }
        .logs-header { display: flex; justify-content: space-between; margin-bottom: 15px; align-items: center; }
        #logs { height: 400px; overflow-y: auto; font-family: 'Fira Code', monospace; font-size: 12px; line-height: 1.6; }
        .settings-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .settings-grid input { width: 100%; }
        .toast { position: fixed; bottom: 20px; right: 20px; background: #1e293b; border-left: 4px solid #10b981; padding: 12px 20px; border-radius: 12px; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.3); z-index: 1000; font-size: 14px; transition: opacity 0.3s; backdrop-filter: blur(8px); }
        @media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🐝 V11.0 PRODUCTION SWARM NODE</h1>
        <a href="/logout" class="logout-btn">🚪 Logout</a>
    </div>
    <div class="status-bar">
        <div class="status-item"><span class="status-label">SCRAPER</span><span id="s-scrape" class="status-value" style="color:#3b82f6;">Idle</span></div>
        <div class="status-item"><span class="status-label">WORKER</span><span id="s-repost" class="status-value" style="color:#f59e0b;">Idle</span></div>
        <div class="status-item"><span class="status-label">QUEUE SIZE</span><span id="s-q" class="status-value" style="color:#10b981;">0</span></div>
        <div class="status-item"><span class="status-label">FFMPEG</span><span class="status-value" style="color:#{{ '10b981' if ffmpeg_ok else 'ef4444' }};">{{ '✓ Available' if ffmpeg_ok else '✗ Missing' }}</span></div>
    </div>
    <div class="grid-2">
        <div class="card" style="border-top-color:#8b5cf6;">
            <h2>📦 SECTION 1: Supabase Archiver</h2>
            <p style="font-size:0.85rem; color:#94a3b8;">Scrape IDs or single ID/link → store to your Supabase DB (no worker).</p>
            <div class="input-group">
                <select id="arch_mode">
                    <option value="keyword">Keyword</option>
                    <option value="username">Username</option>
                    <option value="single">Single ID/Link</option>
                </select>
                <input id="arch_target" placeholder="Keyword, username, or video link/ID...">
            </div>
            <button onclick="startSupabaseArchive()" class="btn-purple">🚀 ARCHIVE TO SUPABASE</button>
        </div>
        <div class="card" style="border-top-color:#f59e0b;">
            <h2>🎬 SECTION 2: Worker Queue & Reposter</h2>
            <p style="font-size:0.85rem; color:#94a3b8;">Scrape IDs → queue → download → watermark → upload to your account.</p>
            <div class="input-group">
                <select id="rep_mode">
                    <option value="keyword">Keyword</option>
                    <option value="username">Username</option>
                    <option value="manual">Manual IDs (line sep)</option>
                </select>
                <input id="rep_input" placeholder="Keyword, username, or paste IDs/links...">
            </div>
            <div class="input-group">
                <select id="size_limit">
                    <option value="20">20 MB</option>
                    <option value="30">30 MB</option>
                    <option value="40">40 MB</option>
                    <option value="9999">No limit</option>
                </select>
                <button onclick="startReposter()" class="btn-orange">⚙️ SCRAPE & ADD TO QUEUE</button>
            </div>
            <hr>
            <div class="input-group">
                <button onclick="runCleaner()" class="btn-red">🧹 DELETE DUPLICATES</button>
                <button onclick="syncUploadedTable()" class="btn-green">🔄 SYNC UPLOADED TABLE</button>
            </div>
        </div>
    </div>
    <div class="logs-panel">
        <div class="logs-header">
            <h2>📋 Live Logs (IST)</h2>
            <button onclick="clearLogs()" style="background:#475569; padding:6px 12px;">Clear</button>
        </div>
        <div id="logs">Loading logs...</div>
    </div>
    <div class="card" style="margin-top: 25px;">
        <h2>⚙️ Node Configuration</h2>
        <div class="settings-grid">
            <input id="set_token" placeholder="Access Token">
            <input id="set_user" placeholder="Username">
            <input id="set_del" placeholder="Delete Payload">
            <input id="set_bl" placeholder="Blacklist (comma)">
        </div>
        <input id="set_cookie" placeholder="Full Cookie Header">
        <button onclick="saveConfig()" style="margin-top: 15px; background:#475569;">💾 Save Overrides</button>
    </div>
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
        showToast("Scraping and queueing started.");
        document.getElementById('rep_input').value = '';
    }
    async function runCleaner() { if(confirm("Delete duplicate reels?")) { await fetch('/api/cleaner', {method:'POST'}); showToast("Cleaner started."); } }
    async function syncUploadedTable() { if(confirm("Sync uploaded table?")) { await fetch('/api/sync_uploaded', {method:'POST'}); showToast("Sync started."); } }
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
    async function clearLogs() { await fetch('/api/clear_logs', {method:'POST'}); showToast("Logs cleared."); }
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

# ---------------------------
#  FLASK ROUTES
# ---------------------------
@app.route('/')
@login_required
def home():
    return render_template_string(HTML_TEMPLATE, ffmpeg_ok=FFMPEG_AVAILABLE)

@app.route('/api/status')
@login_required
def api_status():
    state = read_state()
    state["queue_size"] = get_queue_size()
    return jsonify({
        "scraper": state.get("scraper", "Idle"),
        "reposter": state.get("reposter", "Idle"),
        "queue_size": state.get("queue_size", 0),
        "logs": state.get("logs", [])
    })

@app.route('/api/clear_logs', methods=['POST'])
@login_required
def clear_logs():
    state = read_state()
    state["logs"] = []
    write_state(state)
    return jsonify({"status": "ok"})

@app.route('/api/settings', methods=['GET', 'POST'])
@login_required
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
@login_required
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
            scraped_ids = run_coroutine(async_scrape_ids(mode, target))
            if scraped_ids:
                insert_into_supabase(scraped_ids)
                emit_log(f"Archived {len(scraped_ids)} IDs from {mode} '{target}'", "ARCHIVE", "#8b5cf6")
            else:
                emit_log(f"No IDs found for {mode} '{target}'", "ARCHIVE", "#ef4444", is_error=True)
    threading.Thread(target=archive_task, daemon=True).start()
    return jsonify({"message": "Archive task started. Check logs."})

@app.route('/api/repost', methods=['POST'])
@login_required
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
                emit_log("No valid IDs found", "QUEUE", "#ef4444", is_error=True)
                return
            new_ids = filter_existing_ids(ids)
            skipped = len(ids) - len(new_ids)
            added = 0
            for vid in new_ids:
                if add_to_neon_queue(vid, size_limit):
                    added += 1
            emit_log(f"Manual: Skipped {skipped}, Added {added} jobs", "NODE", "#f59e0b")
        else:
            emit_log(f"Scraping {mode}: '{input_val}'...", "REPOST", "#f59e0b")
            scraped_ids = run_coroutine(async_scrape_ids(mode, input_val))
            if not scraped_ids:
                emit_log(f"No IDs found for {mode} '{input_val}'", "QUEUE", "#ef4444", is_error=True)
                return
            new_ids = filter_existing_ids(scraped_ids)
            skipped = len(scraped_ids) - len(new_ids)
            added = 0
            for vid in new_ids:
                if add_to_neon_queue(vid, size_limit):
                    added += 1
            emit_log(f"Scraped {len(scraped_ids)} → Skipped {skipped} → Added {added} jobs", "SCRAPE", "#3b82f6")
    threading.Thread(target=handle_queueing, daemon=True).start()
    return jsonify({"status": "queued", "message": "Scraping started."})

@app.route('/api/cleaner', methods=['POST'])
@login_required
def api_cleaner():
    threading.Thread(target=native_cleaner_task, daemon=True).start()
    return jsonify({"status": "started", "message": "Duplicate cleaner started."})

@app.route('/api/sync_uploaded', methods=['POST'])
@login_required
def api_sync_uploaded():
    threading.Thread(target=sync_uploaded_videos_from_profile, daemon=True).start()
    return jsonify({"status": "sync started", "message": "Sync started."})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5050))
    print(f"🚀 STARTING V11.0 PRODUCTION SWARM NODE on Port {port}...")
    print(f"📹 FFmpeg available: {FFMPEG_AVAILABLE}")
    print(f"🔐 Admin password: {ADMIN_PASSWORD} (change via env ADMIN_PASSWORD)")
    # For production, use a proper WSGI server like gunicorn
    if os.environ.get('RUN_MAIN') or not os.environ.get('GUNICORN_CMD_ARGS'):
        # Use Flask dev server only for local testing
        app.run(host='0.0.0.0', port=port, threaded=True)