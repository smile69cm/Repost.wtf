import os, json, time, subprocess, requests, threading, re, urllib.parse, hashlib, traceback, fcntl, sys, uuid, socket
import asyncio, aiohttp
from flask import Flask, render_template_string, request, jsonify, session, redirect, url_for
from functools import wraps
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2 import pool

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET', 'changeme_production_secret_!@#$')

# ---------------------------
#  INDIAN TIMEZONE (IST) - 12 hour format
# ---------------------------
IST = timezone(timedelta(hours=5, minutes=30))
def ist_now(): return datetime.now(IST)
def ist_time_str(): return ist_now().strftime("%I:%M:%S %p")

# ---------------------------
#  SERVER ID
# ---------------------------
SERVER_ID = os.environ.get('SERVER_ID', socket.gethostname() + "_" + str(uuid.uuid4())[:8])
print(f"🖥️ Server ID: {SERVER_ID}")

# ---------------------------
#  ENVIRONMENT VARIABLES (with hardcoded fallbacks)
# ---------------------------
NEON_DB_URL = os.environ.get('NEON_DB_URL', "postgresql://neondb_owner:npg_Rh0xIbmdFe5u@ep-quiet-block-a12aatzr-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require")
SUPABASE_URL = os.environ.get('SUPABASE_URL', "https://cnkbewgpguyojiebztbs.supabase.co/rest/v1/reels")
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNua2Jld2dwZ3V5b2ppZWJ6dGJzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQyODU0NzUsImV4cCI6MjA4OTg2MTQ3NX0.ldS5knPaT1imexuRH9jSlTDB1mRSpoozFXlmhbDw2fU")
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin123')
FFMPEG_PATH = os.environ.get('FFMPEG_PATH', 'ffmpeg')

# ---------------------------
#  DIRECTORIES
# ---------------------------
BASE_DIR = os.getcwd()
VIDEO_DIR = os.path.join(BASE_DIR, "watermarked_videos")
PREVIEW_DIR = os.path.join(BASE_DIR, "previews")
SETTINGS_FILE = "settings.json"
STATE_FILE = "state.json"

for d in [VIDEO_DIR, PREVIEW_DIR]:
    if not os.path.exists(d): os.makedirs(d)

# ---------------------------
#  FFMPEG CHECK
# ---------------------------
def check_ffmpeg():
    try:
        subprocess.run([FFMPEG_PATH, '-version'], capture_output=True, check=True)
        return True
    except: return False
FFMPEG_AVAILABLE = check_ffmpeg()

# ---------------------------
#  CROSS‑PROCESS STATE (file‑based)
# ---------------------------
def read_state():
    try:
        with open(STATE_FILE, 'r') as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            fcntl.flock(f, fcntl.LOCK_UN)
            return data
    except:
        return {"logs": [], "scraper": "Idle", "reposter": "Idle", "queue_size": 0, "cleaner_running": False, "fast_mode": False, "current_operation": None}
def write_state(data):
    with open(STATE_FILE, 'w') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(data, f, indent=2)
        fcntl.flock(f, fcntl.LOCK_UN)
def emit_log(msg, category="SYS", color="#10b981", is_error=False):
    t = ist_time_str()
    # Print full message to console (no truncation)
    print(f"[{t}] [{category}] {msg}")
    if is_error:
        print(traceback.format_exc())
    state = read_state()
    state["logs"].append({"time": t, "category": category, "message": msg, "color": color, "is_error": is_error})
    if len(state["logs"]) > 200:
        state["logs"] = state["logs"][-200:]
    write_state(state)
def update_status(scraper=None, reposter=None, queue_size=None, current_op=None):
    state = read_state()
    if scraper is not None: state["scraper"] = scraper
    if reposter is not None: state["reposter"] = reposter
    if queue_size is not None: state["queue_size"] = queue_size
    if current_op is not None: state["current_operation"] = current_op
    write_state(state)

# ---------------------------
#  DATABASE CONNECTION POOL
# ---------------------------
db_pool = None
def init_db_pool():
    global db_pool
    try:
        db_pool = pool.SimpleConnectionPool(1, 10, dsn=NEON_DB_URL)
        conn = db_pool.getconn()
        with conn.cursor() as cur: cur.execute("SELECT 1")
        db_pool.putconn(conn)
        print("✅ DB pool ready")
    except Exception as e: print(f"❌ DB pool error: {e}")
init_db_pool()
def get_db_connection():
    return db_pool.getconn() if db_pool else psycopg2.connect(NEON_DB_URL)
def return_db_connection(conn):
    if db_pool: db_pool.putconn(conn)
    else: conn.close()

# ---------------------------
#  FIX: Ensure fast_mode is BOOLEAN and NOT NULL
# ---------------------------
def fix_fast_mode_column():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data_type FROM information_schema.columns 
                WHERE table_name='repost_queue' AND column_name='fast_mode';
            """)
            row = cur.fetchone()
            if row and row[0] != 'boolean':
                cur.execute("ALTER TABLE repost_queue ALTER COLUMN fast_mode TYPE BOOLEAN USING (fast_mode::boolean);")
                conn.commit()
                emit_log("Fixed fast_mode column type to BOOLEAN", "DB", "#10b981")
            elif not row:
                cur.execute("ALTER TABLE repost_queue ADD COLUMN fast_mode BOOLEAN DEFAULT FALSE;")
                conn.commit()
            cur.execute("UPDATE repost_queue SET fast_mode = FALSE WHERE fast_mode IS NULL;")
            conn.commit()
    except Exception as e:
        emit_log(f"fast_mode fix error: {e}", "DB", "#ef4444", True)
    finally:
        return_db_connection(conn)
fix_fast_mode_column()

# ---------------------------
#  DATABASE TABLES
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
                    updated_at TIMESTAMP DEFAULT NOW(),
                    server_id TEXT,
                    source_type TEXT,
                    source_value TEXT,
                    fast_mode BOOLEAN DEFAULT FALSE
                );
            """)
            for col, dtype in [('server_id','TEXT'), ('source_type','TEXT'), ('source_value','TEXT')]:
                try:
                    cur.execute(f"ALTER TABLE repost_queue ADD COLUMN IF NOT EXISTS {col} {dtype};")
                except: pass
            cur.execute("CREATE TABLE IF NOT EXISTS image_hashes (vid TEXT PRIMARY KEY, hash TEXT);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON repost_queue(status);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_fast ON repost_queue(fast_mode, status);")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS uploaded_videos (
                    hash TEXT PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    original_source_id TEXT,
                    uploaded_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
    except Exception as e: print(f"DB init error: {e}")
    finally: return_db_connection(conn)
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
if not os.path.exists(SETTINGS_FILE): json.dump(DEFAULT_SETTINGS, open(SETTINGS_FILE, 'w'), indent=4)
def get_settings(): return json.load(open(SETTINGS_FILE))
def get_headers():
    conf = get_settings()
    return {"Cookie": conf.get("full_cookie", f"accessToken={conf['my_token']}; allow18=%7B%22allow18%22%3Atrue%7D"),
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# ---------------------------
#  AUTH
# ---------------------------
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'): return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('home'))
        return render_template_string(LOGIN_TEMPLATE, error="Invalid password")
    return render_template_string(LOGIN_TEMPLATE, error=None)
@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

# ---------------------------
#  HEALTH CHECK FOR UPTIME ROBOT
# ---------------------------
@app.route('/health')
def health():
    return jsonify({"status": "healthy", "server_id": SERVER_ID, "time": ist_time_str()}), 200

# ---------------------------
#  QUEUE HELPERS
# ---------------------------
def add_to_neon_queue(video_id, size_limit, source_type, source_value, fast=False):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO repost_queue (video_id, size_limit, status, updated_at, server_id, source_type, source_value, fast_mode)
                VALUES (%s, %s, 'not started', NOW(), %s, %s, %s, %s)
                ON CONFLICT (video_id) DO NOTHING;
            """, (video_id, size_limit, SERVER_ID, source_type, source_value, fast))
            inserted = cur.rowcount > 0
            conn.commit()
        return inserted
    except Exception as e:
        emit_log(f"add_to_queue error {video_id}: {e}", "DB", "#ef4444", True)
        return False
    finally:
        return_db_connection(conn)

def get_next_job():
    """Fetch next job: fast mode first, then normal, using SKIP LOCKED."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE repost_queue SET status = 'doing', updated_at = NOW()
                WHERE id = (
                    SELECT id FROM repost_queue
                    WHERE fast_mode = TRUE AND (status = 'not started' OR (status = 'doing' AND updated_at < NOW() - INTERVAL '10 minutes'))
                    ORDER BY updated_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
                ) RETURNING id, video_id, size_limit, fast_mode;
            """)
            job = cur.fetchone()
            if not job:
                cur.execute("""
                    UPDATE repost_queue SET status = 'doing', updated_at = NOW()
                    WHERE id = (
                        SELECT id FROM repost_queue
                        WHERE fast_mode = FALSE AND (status = 'not started' OR (status = 'doing' AND updated_at < NOW() - INTERVAL '10 minutes'))
                        ORDER BY updated_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
                    ) RETURNING id, video_id, size_limit, fast_mode;
                """)
                job = cur.fetchone()
            conn.commit()
        if job:
            emit_log(f"🎯 Got job: {job[1][:8]} (fast={job[3]})", "WORKER", "#f59e0b")
            return {"id": job[0], "video_id": job[1], "size_limit": job[2], "fast_mode": job[3]}
        else:
            state = read_state()
            if state.get("queue_size", 0) > 0:
                emit_log(f"⚠️ Queue has {state['queue_size']} jobs but none acquired (locked by another worker)", "WORKER", "#f59e0b")
    except Exception as e:
        emit_log(f"get_next_job error: {e}", "WORKER", "#ef4444", True)
    finally:
        return_db_connection(conn)
    return None

def update_job_status(job_id, status, error_msg=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE repost_queue SET status = %s, error = %s, updated_at = NOW() WHERE id = %s", (status, error_msg, job_id))
            conn.commit()
    except Exception as e:
        emit_log(f"update_job_status error: {e}", "DB", "#ef4444", True)
    finally:
        return_db_connection(conn)

def get_queue_size():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM repost_queue WHERE status = 'not started'")
            return cur.fetchone()[0]
    except:
        return 0
    finally:
        return_db_connection(conn)

def get_queue_sources_grouped():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_type, source_value, COUNT(*) 
                FROM repost_queue WHERE status = 'not started' 
                GROUP BY source_type, source_value 
                ORDER BY source_type, COUNT(*) DESC;
            """)
            rows = cur.fetchall()
            result = {}
            for typ, val, cnt in rows:
                if typ not in result:
                    result[typ] = []
                result[typ].append({"value": val, "count": cnt})
            return result
    except:
        return {}
    finally:
        return_db_connection(conn)

def filter_existing_ids(vid_list):
    if not vid_list: return []
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            format_strings = ','.join(['%s'] * len(vid_list))
            cur.execute(f"SELECT video_id FROM repost_queue WHERE video_id IN ({format_strings})", tuple(vid_list))
            existing = {row[0] for row in cur.fetchall()}
        return [v for v in vid_list if v not in existing]
    except:
        return vid_list
    finally:
        return_db_connection(conn)

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
    return asyncio.run_coroutine_threadsafe(coro, async_loop).result()
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
                if not vids: empty_count += 1
                else: empty_count = 0; all_vids.update(vids)
                page += 1
            except Exception as e:
                emit_log(f"Scrape error: {e}", "SCRAPE", "#ef4444", True)
                break
    return list(all_vids)

def extract_video_id_from_input(input_str):
    input_str = input_str.strip()
    if input_str.startswith(('http://', 'https://')):
        parsed = urllib.parse.urlparse(input_str)
        last = parsed.path.rstrip('/').split('/')[-1]
        if last: return last
    if re.match(r'^[A-Za-z0-9_\-=+/]+$', input_str):
        return input_str
    return None

# ---------------------------
#  SINGLE OPERATION LOCK (for scrape/cleaner/sync, but NOT for worker)
# ---------------------------
operation_lock = threading.Lock()
def run_exclusive(func):
    def wrapper(*args, **kwargs):
        if not operation_lock.acquire(blocking=False):
            emit_log("Another operation is already running. Please wait.", "SYS", "#f59e0b")
            return
        try:
            update_status(current_op=func.__name__)
            func(*args, **kwargs)
        finally:
            update_status(current_op=None)
            operation_lock.release()
    return wrapper

# ---------------------------
#  WORKER ENGINE (runs continuously, no lock)
# ---------------------------
def reposter_worker():
    emit_log(f"👷 Worker online (Server: {SERVER_ID})", "WORKER", "#f59e0b")
    while True:
        try:
            qsize = get_queue_size()
            update_status(queue_size=qsize)
            job = get_next_job()
            if not job:
                time.sleep(5)
                continue
            update_status(reposter=f"Processing: {job['video_id'][:8]}")
            process_video_job(job)
        except Exception as e:
            emit_log(f"Worker loop error: {e}", "WORKER", "#ef4444", True)
            time.sleep(5)

def process_video_job(job):
    video_id, size_limit = job["video_id"], job["size_limit"]
    raw_file = watermarked_file = preview_file = None
    conf = get_settings()
    h_media = get_headers()
    try:
        domain = conf['main_domain']
        emit_log(f"🔍 Checking thumbnail for {video_id[:8]}...", "REPOST", "#0ea5e9")
        thumb_url = f"https://{domain}/media/images/{video_id}.jpg"
        thumb_resp = requests.get(thumb_url, headers=h_media, timeout=8)
        thumb_hash = None
        if thumb_resp.status_code == 200:
            thumb_hash = hashlib.md5(thumb_resp.content).hexdigest()
            emit_log(f"📸 Thumbnail hash: {thumb_hash[:8]}...", "REPOST", "#0ea5e9")
            if is_video_already_uploaded(thumb_hash):
                emit_log(f"⏭️ Duplicate hash: {video_id[:8]} already uploaded", "REPOST", "#f43f5e")
                update_job_status(job["id"], 'completed', "Duplicate hash")
                return
        else:
            emit_log(f"⚠️ Could not fetch thumbnail, proceeding anyway", "REPOST", "#f59e0b")
        # Fetch metadata
        emit_log(f"📝 Fetching metadata for {video_id[:8]}...", "REPOST", "#0ea5e9")
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
            emit_log(f"📝 Title: {title[:50]}...", "REPOST", "#0ea5e9")
        except Exception as e:
            emit_log(f"Metadata fetch failed: {e}", "REPOST", "#ef4444", True)
        # Self-loop & blacklist
        if original_username and original_username.lower() == conf['my_user'].lower():
            emit_log(f"⏭️ Self-loop: belongs to {conf['my_user']}", "REPOST", "#f43f5e")
            update_job_status(job["id"], 'completed', "Self-loop")
            return
        bl_words = [w.strip().lower() for w in conf.get("blacklist", "").split(",") if w.strip()]
        if any(w in f"{title} {desc} {category_tag}".lower() for w in bl_words):
            emit_log(f"🛑 Blacklisted keyword detected", "REPOST", "#ef4444")
            update_job_status(job["id"], 'failed', "Blacklisted")
            return
        # Size check
        d_url = f"https://{domain}/media/videos/{video_id}.mp4"
        size_mb = 0
        with requests.get(d_url, headers=h_media, stream=True, timeout=10) as r_size:
            if r_size.status_code == 200 and 'content-length' in r_size.headers:
                size_mb = round(int(r_size.headers['content-length']) / (1024 * 1024), 2)
                emit_log(f"📦 Video size: {size_mb} MB", "REPOST", "#0ea5e9")
        if size_limit != 9999 and size_mb > size_limit:
            emit_log(f"⏭️ Too large: {size_mb}MB > {size_limit}MB", "REPOST", "#f43f5e")
            update_job_status(job["id"], 'failed', f"Size {size_mb}MB > {size_limit}MB")
            return
        # Download
        emit_log(f"📥 Downloading {video_id[:8]}...", "REPOST", "#0ea5e9")
        safe_label = re.sub(r'[^a-zA-Z0-9]', '_', video_id)[-12:]
        raw_file = os.path.join(VIDEO_DIR, f"raw_{safe_label}.mp4")
        watermarked_file = os.path.join(VIDEO_DIR, f"video_{safe_label}.mp4")
        preview_file = os.path.join(PREVIEW_DIR, f"{safe_label}.jpg")
        with requests.get(d_url, headers=h_media, stream=True) as s_res:
            if s_res.status_code != 200: raise Exception("404 Not Found")
            with open(raw_file, 'wb') as f:
                for chunk in s_res.iter_content(8192): f.write(chunk)
        emit_log(f"✅ Downloaded {raw_file}", "REPOST", "#10b981")
        file_to_upload = raw_file
        # Watermark decision
        if size_limit == 9999 and size_mb > 40:
            emit_log(f"⚡ No watermark (size >40MB & no limit)", "REPOST", "#d946ef")
            subprocess.run([FFMPEG_PATH, '-y', '-i', raw_file, '-ss', '1', '-vframes', '1', preview_file], capture_output=True)
            emit_log(f"📸 Preview generated", "REPOST", "#0ea5e9")
        elif not FFMPEG_AVAILABLE:
            emit_log(f"⚠️ FFmpeg missing → uploading raw video", "REPOST", "#f59e0b")
        else:
            emit_log(f"👻 Applying ghost watermark...", "REPOST", "#d946ef")
            vf = "hflip,eq=brightness=0.02:saturation=1.05,scale='min(720,iw)':-2,drawtext=text='telugu stuffs':fontcolor=yellow@0.6:fontsize=24:x=(w-text_w)/2:y=h-th-14"
            subprocess.run([FFMPEG_PATH, '-y', '-i', raw_file, '-ss', '1', '-vframes', '1', preview_file], capture_output=True)
            subprocess.run([FFMPEG_PATH, '-y', '-i', raw_file, '-vf', vf, '-c:v', 'libx264', '-crf', '28', '-preset', 'ultrafast', '-c:a', 'copy', watermarked_file], capture_output=True)
            file_to_upload = watermarked_file
            emit_log(f"✅ Watermark applied", "REPOST", "#10b981")
        # Upload
        emit_log(f"📤 Uploading to {conf['upload_domain']}...", "REPOST", "#0ea5e9")
        base = ".".join(conf['main_domain'].split('.')[-2:])
        with open(file_to_upload, 'rb') as f:
            up = requests.post(
                f"https://{conf['upload_domain']}/upload",
                files={'files': (f"video_{safe_label}.mp4", f, 'video/mp4')},
                data={"tag": category_tag, "title": title, "description": desc, "country": "IN", "username": conf['my_user'], "start": "0", "end": "0"},
                headers={"Cookie": h_media["Cookie"], "Origin": f"https://{base}"}
            )
        if up.status_code == 200 or (up.status_code == 400 and "allowedMimeTypes is not defined" in up.text):
            uploaded_id = re.search(r'"videoId":"([^"]+)"', up.text)
            uploaded_id = uploaded_id.group(1) if uploaded_id else video_id
            if thumb_hash:
                mark_video_uploaded(thumb_hash, uploaded_id, video_id)
            emit_log(f"✅ Success: {video_id[:8]} → {uploaded_id}", "REPOST", "#10b981")
            update_job_status(job["id"], 'completed')
        else:
            raise Exception(f"Upload failed: {up.status_code} | {up.text[:200]}")
    except Exception as e:
        emit_log(f"🔥 Error processing {video_id}: {e}", "REPOST", "#ef4444", True)
        update_job_status(job["id"], 'failed', str(e))
    finally:
        for f in [raw_file, watermarked_file, preview_file]:
            if f and os.path.exists(f):
                try:
                    os.remove(f)
                    emit_log(f"🗑️ Deleted temp file: {os.path.basename(f)}", "REPOST", "#64748b")
                except: pass

# ---------------------------
#  UPLOADED TABLE HELPERS
# ---------------------------
def is_video_already_uploaded(thumbnail_hash):
    if not thumbnail_hash: return False
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM uploaded_videos WHERE hash = %s LIMIT 1", (thumbnail_hash,))
            return cur.fetchone() is not None
    except: return False
    finally: return_db_connection(conn)
def mark_video_uploaded(thumbnail_hash, account_video_id, original_source_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO uploaded_videos (hash, video_id, original_source_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (thumbnail_hash, account_video_id, original_source_id))
            conn.commit()
    except: pass
    finally: return_db_connection(conn)

# ---------------------------
#  CLEANER & SYNC (with exclusive lock)
# ---------------------------
@run_exclusive
def native_cleaner_task():
    conf = get_settings()
    payload = conf.get("del_payload", "")
    username = conf.get("my_user")
    domain = conf.get("main_domain")
    if not payload:
        emit_log("Cleaner aborted: no delete payload", "CLEANER", "#ef4444", True)
        return
    emit_log("🧹 Starting duplicate cleaner", "CLEANER", "#06b6d4")
    all_videos = []
    page, empty_pages = 0, 0
    headers = get_headers()
    session = requests.Session()
    emit_log("📄 Fetching profile videos...", "CLEANER", "#06b6d4")
    while empty_pages < 2 and page < 80:
        try:
            res = session.post(f"https://{domain}/profile/{username}/videos/latest", headers=headers, json={"page": page}, timeout=15)
            vids = re.findall(r'"videoId":"([^"]+)"', res.text)
            if not vids: empty_pages += 1
            else: empty_pages = 0; all_videos.extend(vids)
            emit_log(f"   Page {page}: found {len(vids)} videos (total {len(all_videos)})", "CLEANER", "#64748b")
            page += 1
            time.sleep(0.5)
        except Exception as e:
            emit_log(f"Cleaner page error: {e}", "CLEANER", "#ef4444", True)
            break
    all_videos = list(dict.fromkeys(all_videos))
    all_videos.reverse()
    emit_log(f"📊 Total unique videos in profile: {len(all_videos)}", "CLEANER", "#06b6d4")
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT vid, hash FROM image_hashes")
        known_hashes = {row[0]: row[1] for row in cur.fetchall()}
        seen_hashes = {}
        deleted = 0
        total = len(all_videos)
        for idx, vid in enumerate(all_videos, 1):
            emit_log(f"🔍 [{idx}/{total}] Processing {vid[:8]}...", "CLEANER", "#06b6d4")
            h = known_hashes.get(vid)
            if not h:
                try:
                    img = session.get(f"https://{domain}/media/images/{vid}.jpg", stream=True, timeout=8)
                    if img.status_code == 200:
                        h = hashlib.md5(img.content).hexdigest()
                        cur.execute("INSERT INTO image_hashes (vid, hash) VALUES (%s, %s) ON CONFLICT DO NOTHING", (vid, h))
                        conn.commit()
                        known_hashes[vid] = h
                        emit_log(f"   Hash generated: {h[:8]}", "CLEANER", "#64748b")
                    else:
                        emit_log(f"   ⚠️ No thumbnail, skipping", "CLEANER", "#f59e0b")
                        continue
                except Exception as e:
                    emit_log(f"   ⚠️ Hash error: {e}", "CLEANER", "#f59e0b")
                    continue
            if h in seen_hashes:
                emit_log(f"🚨 DUPLICATE: {vid[:8]} matches {seen_hashes[h][:8]} → deleting", "CLEANER", "#f43f5e")
                try:
                    del_res = session.post(f"https://{domain}/uservideo/delete/{vid}", json={"username": payload}, headers=headers, timeout=10)
                    if del_res.status_code == 200:
                        deleted += 1
                        emit_log(f"   ✅ Deleted {vid[:8]}", "CLEANER", "#10b981")
                    else:
                        emit_log(f"   ❌ Delete failed: {del_res.status_code}", "CLEANER", "#ef4444")
                except Exception as e:
                    emit_log(f"   ❌ Delete error: {e}", "CLEANER", "#ef4444", True)
                time.sleep(1.2)
            else:
                seen_hashes[h] = vid
                emit_log(f"   ✅ New hash recorded", "CLEANER", "#10b981")
        emit_log(f"✨ CLEANER FINISHED: Deleted {deleted} duplicate videos.", "CLEANER", "#10b981")
    except Exception as e:
        emit_log(f"Cleaner DB error: {e}", "CLEANER", "#ef4444", True)
    finally:
        return_db_connection(conn)
    sync_uploaded_videos_from_profile()

@run_exclusive
def sync_uploaded_videos_from_profile():
    conf = get_settings()
    domain = conf.get("main_domain")
    username = conf.get("my_user")
    headers = get_headers()
    emit_log("🔄 Syncing uploaded videos table with profile...", "CLEANER", "#06b6d4")
    all_videos = []
    page, empty = 0, 0
    session = requests.Session()
    while empty < 2 and page < 80:
        try:
            res = session.post(f"https://{domain}/profile/{username}/videos/latest", headers=headers, json={"page": page}, timeout=15)
            vids = re.findall(r'"videoId":"([^"]+)"', res.text)
            if not vids: empty += 1
            else: empty = 0; all_videos.extend(vids)
            emit_log(f"   Page {page}: {len(vids)} videos (total {len(all_videos)})", "CLEANER", "#64748b")
            page += 1
            time.sleep(0.5)
        except: break
    all_videos = list(dict.fromkeys(all_videos))
    emit_log(f"📊 Found {len(all_videos)} videos in profile", "CLEANER", "#06b6d4")
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        for idx, vid in enumerate(all_videos, 1):
            emit_log(f"🔍 [{idx}/{len(all_videos)}] Syncing {vid[:8]}...", "CLEANER", "#06b6d4")
            try:
                img = session.get(f"https://{domain}/media/images/{vid}.jpg", stream=True, timeout=8)
                if img.status_code == 200:
                    h = hashlib.md5(img.content).hexdigest()
                    cur.execute("INSERT INTO uploaded_videos (hash, video_id, original_source_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                                (h, vid, vid))
                    emit_log(f"   ✅ Recorded hash {h[:8]}", "CLEANER", "#10b981")
                else:
                    emit_log(f"   ⚠️ No thumbnail", "CLEANER", "#f59e0b")
            except Exception as e:
                emit_log(f"   ❌ Error: {e}", "CLEANER", "#ef4444")
        conn.commit()
    except Exception as e:
        emit_log(f"Sync DB error: {e}", "CLEANER", "#ef4444", True)
    finally:
        return_db_connection(conn)
    emit_log(f"✅ SYNC COMPLETE: {len(all_videos)} videos recorded.", "CLEANER", "#10b981")

# ---------------------------
#  SUPABASE HELPERS
# ---------------------------
def insert_into_supabase(video_ids):
    if not video_ids: return
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
            emit_log(f"Supabase error: {r.text[:200]}", "SUPABASE", "#ef4444", True)
        else:
            emit_log(f"Archived {len(video_ids)} IDs to Supabase", "SUPABASE", "#10b981")
    except Exception as e:
        emit_log(f"Supabase connection error: {e}", "SUPABASE", "#ef4444", True)

# ---------------------------
#  FLASK ROUTES
# ---------------------------
@app.route('/')
@login_required
def home():
    return render_template_string(HTML_TEMPLATE, ffmpeg_ok=FFMPEG_AVAILABLE, server_id=SERVER_ID)

@app.route('/api/status')
@login_required
def api_status():
    state = read_state()
    state["queue_size"] = get_queue_size()
    state["sources_grouped"] = get_queue_sources_grouped()
    return jsonify(state)

@app.route('/api/clear_logs', methods=['POST'])
@login_required
def clear_logs():
    state = read_state()
    state["logs"] = []
    write_state(state)
    emit_log("Logs cleared by user", "SYS", "#64748b")
    return jsonify({"status": "ok"})

@app.route('/api/settings', methods=['GET', 'POST'])
@login_required
def api_settings():
    if request.method == 'GET': return jsonify(get_settings())
    data = request.json
    conf = get_settings()
    conf.update(data)
    json.dump(conf, open(SETTINGS_FILE, 'w'), indent=4)
    emit_log("Settings updated", "SYS", "#10b981")
    return jsonify({"status": "ok"})

@app.route('/api/supabase_archive', methods=['POST'])
@login_required
def api_supabase_archive():
    data = request.json
    mode = data.get('mode')
    target = data.get('target', '').strip()
    if not target: return jsonify({"error": "Empty target"}), 400
    fast = data.get('fast', False)
    def task():
        if mode == 'single':
            vid = extract_video_id_from_input(target)
            if vid: insert_into_supabase([vid])
            else: emit_log(f"Invalid ID: {target}", "ARCHIVE", "#ef4444", True)
        else:
            emit_log(f"Scraping {mode} '{target}' for Supabase archive...", "ARCHIVE", "#8b5cf6")
            vids = run_coroutine(async_scrape_ids(mode, target))
            if vids:
                emit_log(f"Found {len(vids)} IDs, archiving...", "ARCHIVE", "#8b5cf6")
                insert_into_supabase(vids)
            else:
                emit_log(f"No IDs found for {mode} '{target}'", "ARCHIVE", "#ef4444", True)
    threading.Thread(target=task, daemon=True).start()
    return jsonify({"message": "Archive started"})

@app.route('/api/repost', methods=['POST'])
@login_required
def api_repost():
    data = request.json
    mode = data.get('mode', 'manual')
    target = data['input'].strip()
    size_limit = data['size_limit']
    fast = data.get('fast', False)
    if not target: return jsonify({"error": "Empty input"}), 400
    def task():
        if not operation_lock.acquire(blocking=False):
            emit_log("Another operation running, try later", "SYS", "#f59e0b")
            return
        try:
            update_status(current_op="scrape_queue")
            if mode == "manual":
                ids = []
                for line in target.replace(',', '\n').split('\n'):
                    line = line.strip()
                    if not line: continue
                    vid = extract_video_id_from_input(line)
                    if vid: ids.append(vid)
                if not ids:
                    emit_log("No valid IDs", "QUEUE", "#ef4444", True)
                    return
                emit_log(f"Manual: adding {len(ids)} IDs to queue (fast={fast})", "QUEUE", "#f59e0b")
                added = 0
                for vid in ids:
                    if add_to_neon_queue(vid, size_limit, "manual", "user_input", fast):
                        added += 1
                emit_log(f"Manual: added {added} jobs", "QUEUE", "#f59e0b")
            else:
                emit_log(f"Scraping {mode} '{target}' for queue (fast={fast})...", "SCRAPE", "#3b82f6")
                scraped_ids = run_coroutine(async_scrape_ids(mode, target))
                if not scraped_ids:
                    emit_log(f"No IDs found", "SCRAPE", "#ef4444", True)
                    return
                emit_log(f"Found {len(scraped_ids)} IDs, filtering existing...", "SCRAPE", "#3b82f6")
                new_ids = filter_existing_ids(scraped_ids)
                emit_log(f"{len(new_ids)} new IDs to add", "SCRAPE", "#3b82f6")
                added = 0
                for vid in new_ids:
                    if add_to_neon_queue(vid, size_limit, mode, target, fast):
                        added += 1
                emit_log(f"Scrape done: added {added} jobs to queue", "SCRAPE", "#3b82f6")
        finally:
            update_status(current_op=None)
            operation_lock.release()
    threading.Thread(target=task, daemon=True).start()
    return jsonify({"status": "queued"})

@app.route('/api/cleaner', methods=['POST'])
@login_required
def api_cleaner():
    threading.Thread(target=native_cleaner_task, daemon=True).start()
    return jsonify({"status": "started"})

@app.route('/api/sync_uploaded', methods=['POST'])
@login_required
def api_sync_uploaded():
    threading.Thread(target=sync_uploaded_videos_from_profile, daemon=True).start()
    return jsonify({"status": "started"})

@app.route('/api/force_process', methods=['POST'])
@login_required
def force_process():
    def force():
        emit_log("Manual force process triggered", "WORKER", "#f59e0b")
        job = get_next_job()
        if job:
            process_video_job(job)
        else:
            emit_log("No pending jobs", "WORKER", "#f59e0b")
    threading.Thread(target=force, daemon=True).start()
    return jsonify({"status": "forced"})

# ---------------------------
#  UI TEMPLATES (Mobile Responsive with styled inputs)
# ---------------------------
LOGIN_TEMPLATE = """
<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=yes"><title>Swarm Login</title><style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:linear-gradient(135deg,#0f172a,#1e1b4b);min-height:100vh;display:flex;align-items:center;justify-content:center;padding:16px}.login-card{background:rgba(30,41,59,0.9);backdrop-filter:blur(12px);padding:32px 24px;border-radius:28px;width:100%;max-width:380px;text-align:center;border:1px solid rgba(255,255,255,0.1);box-shadow:0 20px 35px -10px black}h2{color:#f8fafc;margin-bottom:24px;font-size:1.8rem}input{width:100%;padding:14px;margin:12px 0;border-radius:16px;border:none;background:#0f172a;color:white;font-size:1rem;border:1px solid #334155;transition:0.2s}input:focus{outline:none;border-color:#3b82f6;box-shadow:0 0 0 3px rgba(59,130,246,0.2)}button{width:100%;padding:14px;background:#3b82f6;border:none;border-radius:40px;color:white;font-weight:600;font-size:1rem;cursor:pointer;transition:0.2s}button:hover{background:#2563eb;transform:scale(0.98)}.error{color:#ef4444;margin-top:12px}
</style></head><body><div class="login-card"><h2>🔐 Swarm Node</h2><form method="POST"><input type="password" name="password" placeholder="Enter password" autofocus><button type="submit">Authenticate</button>{% if error %}<div class="error">{{ error }}</div>{% endif %}</form></div></body></html>
"""

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=yes, viewport-fit=cover">
    <title>V17.0 Swarm Node | Optimized</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0b1120; color: #f1f5f9; padding: 16px; }
        .container { max-width: 600px; margin: 0 auto; }
        .header { display: flex; flex-direction: column; gap: 12px; margin-bottom: 20px; }
        h1 { font-size: 1.6rem; background: linear-gradient(135deg, #60a5fa, #c084fc); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .top-bar { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 10px; }
        .badge { background: #1e293b; padding: 5px 12px; border-radius: 40px; font-size: 0.7rem; font-family: monospace; }
        .logout-btn { background: #ef4444; padding: 6px 16px; border-radius: 40px; text-decoration: none; color: white; font-size: 0.8rem; font-weight: 500; }
        .status-bar { background: #1e293b; border-radius: 20px; padding: 12px 16px; margin-bottom: 20px; display: flex; flex-wrap: wrap; gap: 12px; border-left: 3px solid #10b981; }
        .status-item { background: #0f172a; padding: 5px 12px; border-radius: 40px; font-size: 0.75rem; display: flex; align-items: baseline; gap: 6px; }
        .status-label { color: #94a3b8; text-transform: uppercase; font-size: 0.65rem; }
        .status-value { font-weight: 700; font-size: 0.85rem; }
        .card { background: #1e293b; border-radius: 24px; padding: 20px; margin-bottom: 20px; border-top: 3px solid #3b82f6; transition: transform 0.2s; }
        .card:hover { transform: translateY(-2px); }
        .card h2 { font-size: 1.3rem; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
        .input-group { display: flex; flex-direction: column; gap: 12px; margin: 16px 0; }
        .input-row { display: flex; flex-wrap: wrap; gap: 10px; }
        input, select, textarea {
            width: 100%;
            padding: 12px 16px;
            border-radius: 16px;
            border: 1px solid #334155;
            background: #0f172a;
            color: #f1f5f9;
            font-size: 0.9rem;
            transition: all 0.2s ease;
            outline: none;
        }
        input:focus, select:focus, textarea:focus {
            border-color: #3b82f6;
            box-shadow: 0 0 0 3px rgba(59,130,246,0.2);
        }
        select {
            appearance: none;
            background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%2394a3b8' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><polyline points='6 9 12 15 18 9'></polyline></svg>");
            background-repeat: no-repeat;
            background-position: right 16px center;
            background-size: 16px;
        }
        button {
            background: #3b82f6;
            padding: 12px 20px;
            border: none;
            border-radius: 40px;
            color: white;
            font-weight: 600;
            font-size: 0.9rem;
            cursor: pointer;
            transition: 0.2s;
            width: 100%;
        }
        button:active { transform: scale(0.97); }
        .btn-purple { background: #8b5cf6; }
        .btn-orange { background: #f59e0b; }
        .btn-red { background: #ef4444; }
        .btn-green { background: #10b981; }
        .fast-toggle { background: #334155; border: 1px solid #475569; }
        .fast-toggle.active { background: #f59e0b; color: #0f172a; }
        hr { border-color: #334155; margin: 16px 0; }
        .logs-panel { background: #0f172a; border-radius: 20px; padding: 16px; margin-top: 20px; }
        .logs-header { display: flex; justify-content: space-between; margin-bottom: 12px; flex-wrap: wrap; gap: 10px; align-items: center; }
        #logs { height: 320px; overflow-y: auto; font-family: 'JetBrains Mono', monospace; font-size: 11px; line-height: 1.5; background: #020617; padding: 12px; border-radius: 16px; }
        .settings-grid { display: flex; flex-direction: column; gap: 12px; margin-bottom: 16px; }
        .settings-grid input { width: 100%; }
        .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); backdrop-filter: blur(4px); z-index: 1000; justify-content: center; align-items: center; padding: 16px; }
        .modal-content { background: #1e293b; border-radius: 28px; padding: 20px; width: 100%; max-width: 500px; max-height: 80vh; overflow-y: auto; }
        .modal-content h3 { margin-bottom: 16px; font-size: 1.3rem; }
        .source-group { margin-bottom: 20px; }
        .source-group h4 { color: #94a3b8; font-size: 0.8rem; text-transform: uppercase; margin-bottom: 8px; }
        .source-table { width: 100%; border-collapse: collapse; }
        .source-table td, .source-table th { padding: 8px 6px; text-align: left; border-bottom: 1px solid #334155; font-size: 0.8rem; }
        .close-modal { float: right; font-size: 28px; cursor: pointer; background: none; border: none; color: white; line-height: 1; }
        .toast { position: fixed; bottom: 20px; left: 16px; right: 16px; background: #1e293b; border-left: 4px solid #10b981; padding: 12px 16px; border-radius: 40px; z-index: 1100; font-size: 0.8rem; backdrop-filter: blur(8px); text-align: center; }
        @media (min-width: 768px) { .container { max-width: 800px; } .input-row { flex-direction: row; } .status-bar { flex-wrap: nowrap; } button { width: auto; } .toast { left: auto; right: 20px; max-width: 350px; } }
        .log-entry { margin-bottom: 4px; word-break: break-word; }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <div class="top-bar"><h1>🐝 V17.0 OPTIMIZED SWARM</h1><a href="/logout" class="logout-btn">🚪 Logout</a></div>
        <div class="badge">🖥️ {{ server_id }}</div>
    </div>
    <div class="status-bar">
        <div class="status-item"><span class="status-label">SCRAPER</span><span id="s-scrape" style="color:#3b82f6;">Idle</span></div>
        <div class="status-item"><span class="status-label">WORKER</span><span id="s-repost" style="color:#f59e0b;">Idle</span></div>
        <div class="status-item"><span class="status-label">QUEUE</span><span id="s-q" style="color:#10b981;">0</span></div>
        <div class="status-item"><span class="status-label">FFMPEG</span><span style="color:{{ '#10b981' if ffmpeg_ok else '#ef4444' }};">{{ '✓' if ffmpeg_ok else '✗' }}</span></div>
        <div class="status-item"><span class="status-label">OP</span><span id="current-op">None</span></div>
    </div>
    <div class="card" style="border-top-color:#8b5cf6;">
        <h2>📦 Archiver</h2>
        <div class="input-group">
            <div class="input-row"><select id="arch_mode"><option value="keyword">Keyword</option><option value="username">Username</option><option value="single">Single ID/Link</option></select></div>
            <input id="arch_target" placeholder="Keyword, username, or link...">
            <div class="input-row"><button id="archFastBtn" class="fast-toggle">⚡ Fast OFF</button><button onclick="startSupabaseArchive()" class="btn-purple">🚀 Archive</button></div>
        </div>
    </div>
    <div class="card" style="border-top-color:#f59e0b;">
        <h2>🎬 Reposter</h2>
        <div class="input-group">
            <div class="input-row"><select id="rep_mode"><option value="keyword">Keyword</option><option value="username">Username</option><option value="manual">Manual IDs</option></select></div>
            <input id="rep_input" placeholder="Keyword, username, or IDs...">
            <div class="input-row"><select id="size_limit"><option value="20">20 MB</option><option value="30">30 MB</option><option value="40">40 MB</option><option value="9999">No limit</option></select></div>
            <div class="input-row"><button id="repFastBtn" class="fast-toggle">⚡ Fast OFF</button><button onclick="startReposter()" class="btn-orange">⚙️ Add to Queue</button></div>
        </div>
        <hr>
        <div class="input-row"><button onclick="runCleaner()" class="btn-red">🧹 Delete Duplicates</button><button onclick="syncUploadedTable()" class="btn-green">🔄 Sync Uploaded</button></div>
        <div class="input-row" style="margin-top:12px">
            <button id="showSourcesBtn" style="background:#334155;">📊 Queue Sources</button>
            <button id="forceProcessBtn" style="background:#f59e0b;">⚡ Force Process</button>
            <button id="settingsBtn" style="background:#334155;">⚙️ Settings</button>
        </div>
    </div>
    <div class="logs-panel">
        <div class="logs-header"><span>📋 Live Logs (IST 12hr)</span><button onclick="clearLogs()" style="background:#475569; padding:6px 12px; width:auto;">Clear</button></div>
        <div id="logs">Loading...</div>
    </div>
</div>
<div id="sourceModal" class="modal"><div class="modal-content"><span class="close-modal">&times;</span><h3>📌 Pending Jobs by Source</h3><div id="sourceTableBody">Loading...</div></div></div>
<div id="settingsModal" class="modal"><div class="modal-content"><span class="close-modal">&times;</span><h3>⚙️ Configuration</h3><div class="settings-grid"><input id="set_token" placeholder="Access Token"><input id="set_user" placeholder="Username"><input id="set_del" placeholder="Delete Payload"><input id="set_bl" placeholder="Blacklist"></div><input id="set_cookie" placeholder="Full Cookie Header"><button onclick="saveConfig()" style="margin-top:16px; background:#475569;">💾 Save</button></div></div>
<div id="toast" class="toast" style="display:none;"></div>
<script>
    let archFast=false, repFast=false;
    function showToast(msg,err){let t=document.getElementById('toast');t.style.display='block';t.style.borderLeftColor=err?'#ef4444':'#10b981';t.innerHTML=msg;setTimeout(()=>t.style.display='none',3500);}
    document.getElementById('archFastBtn').onclick=()=>{archFast=!archFast;document.getElementById('archFastBtn').innerHTML=archFast?'⚡ Fast ON':'⚡ Fast OFF';document.getElementById('archFastBtn').classList.toggle('active',archFast);};
    document.getElementById('repFastBtn').onclick=()=>{repFast=!repFast;document.getElementById('repFastBtn').innerHTML=repFast?'⚡ Fast ON':'⚡ Fast OFF';document.getElementById('repFastBtn').classList.toggle('active',repFast);};
    async function startSupabaseArchive(){let mode=document.getElementById('arch_mode').value,target=document.getElementById('arch_target').value.trim();if(!target){showToast("Enter target!",true);return;}let resp=await fetch('/api/supabase_archive',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode,target,fast:archFast})});let data=await resp.json();showToast(data.message);document.getElementById('arch_target').value='';}
    async function startReposter(){let mode=document.getElementById('rep_mode').value,input=document.getElementById('rep_input').value.trim(),limit=document.getElementById('size_limit').value;if(!input){showToast("Enter input!",true);return;}await fetch('/api/repost',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode,input,size_limit:parseInt(limit),fast:repFast})});showToast("Queueing started");document.getElementById('rep_input').value='';}
    async function runCleaner(){if(confirm("Delete duplicates?")){await fetch('/api/cleaner',{method:'POST'});showToast("Cleaner started");}}
    async function syncUploadedTable(){if(confirm("Sync uploaded table?")){await fetch('/api/sync_uploaded',{method:'POST'});showToast("Sync started");}}
    async function saveConfig(){let payload={my_token:document.getElementById('set_token').value,my_user:document.getElementById('set_user').value,blacklist:document.getElementById('set_bl').value,del_payload:document.getElementById('set_del').value,full_cookie:document.getElementById('set_cookie').value};await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});showToast("Settings saved");}
    async function clearLogs(){await fetch('/api/clear_logs',{method:'POST'});showToast("Logs cleared");}
    let srcModal=document.getElementById('sourceModal'),setModal=document.getElementById('settingsModal');
    document.getElementById('showSourcesBtn').onclick=()=>{let grouped=window.cachedSources||{};let html='';for(let [type,items] of Object.entries(grouped)){html+=`<div class="source-group"><h4>📁 ${type.toUpperCase()}</h4><table class="source-table"><tr><th>Source</th><th>Count</th></tr>`;items.forEach(i=>{html+=`<tr><td>${escapeHtml(i.value)}</td><td>${i.count}</td>`;});html+=`</table></div>`;}if(!Object.keys(grouped).length)html='<p>No pending jobs.</p>';document.getElementById('sourceTableBody').innerHTML=html;srcModal.style.display='flex';};
    function escapeHtml(s){return s.replace(/[&<>]/g,function(m){if(m==='&')return '&amp;';if(m==='<')return '&lt;';if(m==='>')return '&gt;';return m;});}
    document.getElementById('settingsBtn').onclick=async()=>{let r=await fetch('/api/settings');let d=await r.json();document.getElementById('set_token').value=d.my_token||'';document.getElementById('set_user').value=d.my_user||'';document.getElementById('set_bl').value=d.blacklist||'';document.getElementById('set_del').value=d.del_payload||'';document.getElementById('set_cookie').value=d.full_cookie||'';setModal.style.display='flex';};
    document.getElementById('forceProcessBtn').onclick=async()=>{await fetch('/api/force_process',{method:'POST'});showToast("Force process triggered");};
    document.querySelectorAll('.close-modal').forEach(btn=>btn.onclick=()=>{srcModal.style.display='none';setModal.style.display='none';});
    window.onclick=e=>{if(e.target==srcModal)srcModal.style.display='none';if(e.target==setModal)setModal.style.display='none';};
    setInterval(async()=>{try{let r=await fetch('/api/status');let d=await r.json();document.getElementById('s-scrape').innerText=d.scraper;document.getElementById('s-repost').innerText=d.reposter;document.getElementById('s-q').innerText=d.queue_size;document.getElementById('current-op').innerText=d.current_operation||'None';window.cachedSources=d.sources_grouped||{};let logsDiv=document.getElementById('logs');let isBottom=logsDiv.scrollHeight-logsDiv.clientHeight<=logsDiv.scrollTop+1;logsDiv.innerHTML=d.logs.map(l=>`<div class="log-entry"><span style='color:#64748b'>[${l.time}]</span> <span style='color:${l.color}'>[${l.category}]</span> ${l.message}</div>`).join('');if(isBottom)logsDiv.scrollTop=logsDiv.scrollHeight;}catch(e){}},1500);
    (async()=>{let r=await fetch('/api/settings');let d=await r.json();document.getElementById('set_token').value=d.my_token||'';document.getElementById('set_user').value=d.my_user||'';document.getElementById('set_bl').value=d.blacklist||'';document.getElementById('set_del').value=d.del_payload||'';document.getElementById('set_cookie').value=d.full_cookie||'';})();
</script>
</body>
</html>
"""

# ---------------------------
#  START WORKER
# ---------------------------
threading.Thread(target=reposter_worker, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5050))
    print(f"🚀 V17.0 Optimized Swarm Node on port {port} | Server: {SERVER_ID}")
    print(f"🔐 Admin password: {ADMIN_PASSWORD}")
    print(f"🏥 Health check: /health")
    app.run(host='0.0.0.0', port=port, threaded=True)