import os, json, time, requests, threading, re, urllib.parse, hashlib, traceback, fcntl, sys, uuid, socket, random
import asyncio, aiohttp
from flask import Flask, render_template_string, request, jsonify, session, redirect, url_for
from functools import wraps
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
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
#  ENVIRONMENT VARIABLES
# ---------------------------
NEON_DB_URL = os.environ.get('NEON_DB_URL', "postgresql://neondb_owner:npg_x7wj2CbVTWpL@ep-young-bar-a1w4zep5-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin123')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHANNEL_ID = os.environ.get('TELEGRAM_CHANNEL_ID', '-1003810911847')
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN)

# ---------------------------
#  DIRECTORIES & FILES
# ---------------------------
BASE_DIR = os.getcwd()
VIDEO_DIR = os.path.join(BASE_DIR, "downloads")
STATE_FILE = "state.json"

os.makedirs(VIDEO_DIR, exist_ok=True)

# ---------------------------
#  CROSS‑PROCESS STATE (file‑based)
# ---------------------------
def read_state():
    try:
        with open(STATE_FILE, 'r') as f:
            try:
                fcntl.flock(f, fcntl.LOCK_SH)
            except OSError:
                pass
            data = json.load(f)
            try:
                fcntl.flock(f, fcntl.LOCK_UN)
            except OSError:
                pass
            return data
    except:
        return {"logs": [], "scraper": "Idle", "worker": "Idle", "queue_size": 0, "current_operation": None}
def write_state(data):
    with open(STATE_FILE, 'w') as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX)
        except OSError:
            pass
        json.dump(data, f, indent=2)
        try:
            fcntl.flock(f, fcntl.LOCK_UN)
        except OSError:
            pass
def emit_log(msg, category="SYS", color="#10b981", is_error=False):
    t = ist_time_str()
    print(f"[{t}] [{category}] {msg}")
    if is_error:
        print(traceback.format_exc())
    state = read_state()
    state["logs"].append({"time": t, "category": category, "message": msg, "color": color, "is_error": is_error})
    if len(state["logs"]) > 200:
        state["logs"] = state["logs"][-200:]
    write_state(state)
def update_status(scraper=None, worker=None, queue_size=None, current_op=None):
    state = read_state()
    if scraper is not None: state["scraper"] = scraper
    if worker is not None: state["worker"] = worker
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
        db_pool = pool.SimpleConnectionPool(1, 5, dsn=NEON_DB_URL)
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
#  DATABASE TABLES (simplified)
# ---------------------------
def init_db():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Queue table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS repost_queue (
                    id SERIAL PRIMARY KEY,
                    video_id TEXT UNIQUE NOT NULL,
                    status TEXT DEFAULT 'pending',
                    error TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    server_id TEXT,
                    source_type TEXT,
                    source_value TEXT
                );
            """)
            # Hash table for deduplication
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sent_videos (
                    hash TEXT PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    sent_at TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON repost_queue(status);")
            conn.commit()
    except Exception as e: print(f"DB init error: {e}")
    finally: return_db_connection(conn)
init_db()

# ---------------------------
#  AUTH (simple password)
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

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "server_id": SERVER_ID, "telegram": TELEGRAM_ENABLED}), 200

# ---------------------------
#  QUEUE HELPERS
# ---------------------------
def add_to_queue(video_id, source_type, source_value):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO repost_queue (video_id, status, updated_at, server_id, source_type, source_value)
                VALUES (%s, 'pending', NOW(), %s, %s, %s)
                ON CONFLICT (video_id) DO NOTHING;
            """, (video_id, SERVER_ID, source_type, source_value))
            inserted = cur.rowcount > 0
            conn.commit()
        return inserted
    except Exception as e:
        emit_log(f"add_to_queue error {video_id}: {e}", "DB", "#ef4444", True)
        return False
    finally:
        return_db_connection(conn)

def get_next_job():
    """Only return a job if this server has Telegram enabled."""
    if not TELEGRAM_ENABLED:
        return None
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE repost_queue SET status = 'processing', updated_at = NOW()
                WHERE id = (
                    SELECT id FROM repost_queue
                    WHERE status = 'pending'
                    ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
                ) RETURNING id, video_id, source_type, source_value;
            """)
            job = cur.fetchone()
            conn.commit()
        if job:
            emit_log(f"🎯 Got job: {job[1][:8]}", "WORKER", "#f59e0b")
            return {"id": job[0], "video_id": job[1], "source_type": job[2], "source_value": job[3]}
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
            cur.execute("SELECT COUNT(*) FROM repost_queue WHERE status = 'pending'")
            return cur.fetchone()[0]
    except:
        return 0
    finally:
        return_db_connection(conn)

def is_already_sent(thumbnail_hash):
    if not thumbnail_hash:
        return False
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM sent_videos WHERE hash = %s LIMIT 1", (thumbnail_hash,))
            return cur.fetchone() is not None
    except:
        return False
    finally:
        return_db_connection(conn)

def mark_sent(thumbnail_hash, video_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO sent_videos (hash, video_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (thumbnail_hash, video_id))
            conn.commit()
    except:
        pass
    finally:
        return_db_connection(conn)

# ---------------------------
#  TELEGRAM SENDER
# ---------------------------
def send_video_to_telegram(video_path, caption):
    """Send video to Telegram channel. Returns True on success."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendVideo"
    # Telegram limit: 50 MB
    if os.path.getsize(video_path) > 50 * 1024 * 1024:
        emit_log(f"📤 Telegram skip: video >50MB", "TELEGRAM", "#f59e0b")
        return False
    for attempt in range(1, 4):
        try:
            with open(video_path, 'rb') as f:
                files = {'video': f}
                data = {
                    'chat_id': TELEGRAM_CHANNEL_ID,
                    'caption': caption[:1024],
                    'supports_streaming': True
                }
                resp = requests.post(url, data=data, files=files, timeout=60)
            if resp.status_code == 200:
                return True
            elif resp.status_code == 429:
                wait = 2 ** attempt + random.uniform(0, 2)
                emit_log(f"⚠️ Rate limit, retry {attempt}/3 after {wait:.1f}s", "TELEGRAM", "#f59e0b")
                time.sleep(wait)
            else:
                emit_log(f"⚠️ Telegram error {resp.status_code}: {resp.text[:100]}", "TELEGRAM", "#ef4444", True)
                return False
        except Exception as e:
            emit_log(f"⚠️ Exception: {e}, retry {attempt}/3", "TELEGRAM", "#ef4444", True)
            time.sleep(2 ** attempt)
    return False

# ---------------------------
#  SCRAPER (async, same as before)
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

def progress_print(page, new, total, empty=0, finished=False, error=None):
    msg = ""
    if error:
        msg = f"⚠️ Page {page}: error - {error}"
    elif finished:
        msg = f"✅ Scraping finished after {page+1} pages. Total IDs: {total}      "
    else:
        if new > 0:
            msg = f"📄 Page {page}: +{new:4} IDs | Total: {total:6} | Empty streak: {empty}/10"
        else:
            msg = f"📄 Page {page}: no new IDs  | Total: {total:6} | Empty streak: {empty}/10"
    sys.stdout.write(f"\r{msg}")
    sys.stdout.flush()
    if finished or error:
        print()

async def async_scrape_ids(mode, query, progress_callback=None):
    conf = {"main_domain": "love.viraly.wtf"}  # hardcoded
    s_headers = {"User-Agent": "Mozilla/5.0"}
    all_vids = set()
    page = 0
    empty_count = 0
    max_empty = 10
    async with aiohttp.ClientSession() as session:
        while True:
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
                    if progress_callback:
                        await progress_callback(page, 0, len(all_vids), empty=empty_count)
                    if empty_count >= max_empty:
                        if progress_callback:
                            await progress_callback(page, 0, len(all_vids), empty=empty_count, finished=True)
                        break
                else:
                    empty_count = 0
                    new_count = len([v for v in vids if v not in all_vids])
                    all_vids.update(vids)
                    if progress_callback:
                        await progress_callback(page, new_count, len(all_vids))
                page += 1
            except Exception as e:
                if progress_callback:
                    await progress_callback(page, 0, len(all_vids), error=str(e))
                break
    return list(all_vids)

def scrape_ids_sync(mode, query, report_func=None):
    async def _scrape():
        if report_func:
            async def progress(p, new_cnt, total, empty=0, finished=False, error=None):
                if error:
                    report_func(p, 0, total, empty=empty, error=error)
                elif finished:
                    report_func(p, 0, total, empty=empty, finished=True)
                else:
                    report_func(p, new_cnt, total, empty=empty)
            return await async_scrape_ids(mode, query, progress)
        else:
            return await async_scrape_ids(mode, query)
    return run_coroutine(_scrape())

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
#  WORKER ENGINE (only if Telegram enabled)
# ---------------------------
def worker_loop():
    if not TELEGRAM_ENABLED:
        emit_log("⚠️ TELEGRAM_BOT_TOKEN not set – worker will not process any jobs.", "WORKER", "#f59e0b")
        while True:
            time.sleep(60)
        return
    emit_log(f"👷 Worker online (Server: {SERVER_ID})", "WORKER", "#f59e0b")
    while True:
        try:
            qsize = get_queue_size()
            update_status(queue_size=qsize)
            job = get_next_job()
            if not job:
                time.sleep(5)
                continue
            update_status(worker=f"Processing: {job['video_id'][:8]}")
            process_job(job)
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            emit_log(f"Worker loop error: {e}", "WORKER", "#ef4444", True)
            time.sleep(10)

def process_job(job):
    video_id = job["video_id"]
    raw_file = None
    try:
        domain = "love.viraly.wtf"
        encoded_id = quote(video_id, safe='')
        # Thumbnail for duplicate detection
        thumb_url = f"https://{domain}/media/images/{encoded_id}.jpg"
        emit_log(f"🔍 Checking thumbnail for {video_id[:8]}...", "WORKER", "#0ea5e9")
        thumb_resp = requests.get(thumb_url, timeout=8)
        if thumb_resp.status_code == 200:
            thumb_hash = hashlib.md5(thumb_resp.content).hexdigest()
            if is_already_sent(thumb_hash):
                emit_log(f"⏭️ Duplicate: {video_id[:8]} already sent", "WORKER", "#f43f5e")
                update_job_status(job["id"], 'skipped', "Duplicate")
                return
        else:
            thumb_hash = None
        # Fetch metadata
        emit_log(f"📝 Fetching metadata for {video_id[:8]}...", "WORKER", "#0ea5e9")
        title = f"Video {video_id[:6]}"
        desc = ""
        try:
            r_api = requests.get(f"https://{domain}/video/{encoded_id}", timeout=10).json()
            vid_data = r_api[0] if isinstance(r_api, list) and len(r_api) > 0 else (r_api if isinstance(r_api, dict) else {})
            if vid_data.get("title"): title = vid_data["title"]
            if vid_data.get("description"): desc = vid_data["description"]
            emit_log(f"📝 Title: {title[:50]}", "WORKER", "#0ea5e9")
        except Exception as e:
            emit_log(f"Metadata fetch failed: {e}", "WORKER", "#ef4444", True)
        # Download video
        d_url = f"https://{domain}/media/videos/{encoded_id}.mp4"
        emit_log(f"📥 Downloading {video_id[:8]}...", "WORKER", "#0ea5e9")
        safe_label = re.sub(r'[^a-zA-Z0-9]', '_', video_id)[-12:]
        raw_file = os.path.join(VIDEO_DIR, f"{safe_label}.mp4")
        download_success = False
        for attempt in range(1, 4):
            try:
                with requests.get(d_url, stream=True, timeout=30) as s_res:
                    if s_res.status_code != 200:
                        raise Exception(f"HTTP {s_res.status_code}")
                    with open(raw_file, 'wb') as f:
                        for chunk in s_res.iter_content(8192):
                            f.write(chunk)
                download_success = True
                break
            except Exception as e:
                emit_log(f"⚠️ Download error (attempt {attempt}/3): {e}", "WORKER", "#f59e0b")
                time.sleep(2 ** attempt)
        if not download_success:
            raise Exception("Download failed")
        emit_log(f"✅ Downloaded {raw_file}", "WORKER", "#10b981")
        # Send to Telegram
        caption = f"{title}\n\n{desc}" if desc else title
        emit_log(f"📤 Sending to Telegram...", "WORKER", "#0ea5e9")
        if send_video_to_telegram(raw_file, caption):
            emit_log(f"✅ Sent to Telegram: {video_id[:8]}", "WORKER", "#10b981")
            if thumb_hash:
                mark_sent(thumb_hash, video_id)
            update_job_status(job["id"], 'done')
        else:
            raise Exception("Telegram send failed")
    except Exception as e:
        emit_log(f"🔥 Error processing {video_id}: {e}", "WORKER", "#ef4444", True)
        update_job_status(job["id"], 'failed', str(e))
    finally:
        if raw_file and os.path.exists(raw_file):
            try:
                os.remove(raw_file)
                emit_log(f"🗑️ Deleted temp file", "WORKER", "#64748b")
            except: pass

# ---------------------------
#  FLASK ROUTES (simplified UI)
# ---------------------------
@app.route('/')
@login_required
def home():
    return render_template_string(HTML_TEMPLATE, server_id=SERVER_ID, telegram_enabled=TELEGRAM_ENABLED)

@app.route('/api/status')
@login_required
def api_status():
    state = read_state()
    state["queue_size"] = get_queue_size()
    # Also compute sources grouped
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source_type, source_value, COUNT(*) FROM repost_queue WHERE status='pending' GROUP BY source_type, source_value ORDER BY COUNT(*) DESC;")
            rows = cur.fetchall()
            grouped = {}
            for typ, val, cnt in rows:
                if typ not in grouped: grouped[typ] = []
                grouped[typ].append({"value": val, "count": cnt})
            state["sources_grouped"] = grouped
    except:
        state["sources_grouped"] = {}
    finally:
        return_db_connection(conn)
    return jsonify(state)

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
        return jsonify({"telegram_enabled": TELEGRAM_ENABLED})
    return jsonify({"status": "ok"})

@app.route('/api/supabase_archive', methods=['POST'])  # kept for compatibility but no Supabase
@login_required
def api_supabase_archive():
    return jsonify({"message": "Supabase disabled – only Telegram"})

@app.route('/api/repost', methods=['POST'])
@login_required
def api_repost():
    data = request.json
    mode = data.get('mode', 'manual')
    target = data['input'].strip()
    if not target:
        return jsonify({"error": "Empty input"}), 400
    def task():
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
            added = 0
            for vid in ids:
                if add_to_queue(vid, "manual", "user_input"):
                    added += 1
            emit_log(f"Manual: added {added} jobs", "QUEUE", "#f59e0b")
        else:
            emit_log(f"Scraping {mode} '{target}'...", "SCRAPE", "#3b82f6")
            def progress(page, new_cnt, total, empty=0, finished=False, error=None):
                progress_print(page, new_cnt, total, empty, finished, error)
            scraped_ids = scrape_ids_sync(mode, target, progress)
            if scraped_ids:
                print()
                emit_log(f"Found {len(scraped_ids)} IDs, adding to queue...", "SCRAPE", "#3b82f6")
                added = 0
                for vid in scraped_ids:
                    if add_to_queue(vid, mode, target):
                        added += 1
                emit_log(f"Added {added} new jobs", "SCRAPE", "#3b82f6")
            else:
                print()
                emit_log(f"No IDs found", "SCRAPE", "#ef4444", True)
    threading.Thread(target=task, daemon=True).start()
    return jsonify({"status": "queued"})

@app.route('/api/force_process', methods=['POST'])
@login_required
def force_process():
    def force():
        emit_log("Manual force process triggered", "WORKER", "#f59e0b")
        job = get_next_job()
        if job:
            process_job(job)
        else:
            emit_log("No pending jobs", "WORKER", "#f59e0b")
    threading.Thread(target=force, daemon=True).start()
    return jsonify({"status": "forced"})

# ---------------------------
#  UI TEMPLATE
# ---------------------------
LOGIN_TEMPLATE = """
<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Telegram Swarm Login</title><style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Inter',sans-serif;background:linear-gradient(135deg,#0f172a,#1e1b4b);min-height:100vh;display:flex;align-items:center;justify-content:center;padding:16px}.login-card{background:rgba(30,41,59,0.9);backdrop-filter:blur(12px);padding:32px 24px;border-radius:28px;width:100%;max-width:380px;text-align:center;border:1px solid rgba(255,255,255,0.1)}h2{color:#f8fafc;margin-bottom:24px}input{width:100%;padding:14px;margin:12px 0;border-radius:16px;border:none;background:#0f172a;color:white;border:1px solid #334155}input:focus{outline:none;border-color:#3b82f6}button{width:100%;padding:14px;background:#3b82f6;border:none;border-radius:40px;color:white;font-weight:600;cursor:pointer}.error{color:#ef4444;margin-top:12px}
</style></head><body><div class="login-card"><h2>🤖 Telegram Swarm</h2><form method="POST"><input type="password" name="password" placeholder="Enter password" autofocus><button type="submit">Authenticate</button>{% if error %}<div class="error">{{ error }}</div>{% endif %}</form></div></body></html>
"""

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
    <title>Telegram Swarm Node</title>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Inter',sans-serif;background:#0b1120;color:#f1f5f9;padding:16px}
        .container{max-width:600px;margin:0 auto}
        .header{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap}
        h1{font-size:1.6rem;background:linear-gradient(135deg,#60a5fa,#c084fc);-webkit-background-clip:text;background-clip:text;color:transparent}
        .badge{background:#1e293b;padding:5px 12px;border-radius:40px;font-size:0.7rem}
        .logout-btn{background:#ef4444;padding:6px 16px;border-radius:40px;text-decoration:none;color:white;font-size:0.8rem}
        .status-bar{background:#1e293b;border-radius:20px;padding:12px 16px;margin-bottom:20px;display:flex;gap:12px;flex-wrap:wrap}
        .status-item{background:#0f172a;padding:5px 12px;border-radius:40px;font-size:0.75rem}
        .card{background:#1e293b;border-radius:24px;padding:20px;margin-bottom:20px;border-top:3px solid #3b82f6}
        .card h2{font-size:1.3rem;margin-bottom:16px}
        .input-group{display:flex;flex-direction:column;gap:12px;margin:16px 0}
        .input-row{display:flex;gap:10px;flex-wrap:wrap}
        select,input{flex:1;padding:12px;border-radius:16px;border:1px solid #334155;background:#0f172a;color:white}
        button{background:#3b82f6;padding:12px 20px;border:none;border-radius:40px;color:white;font-weight:600;cursor:pointer}
        .btn-orange{background:#f59e0b}
        .btn-red{background:#ef4444}
        hr{margin:16px 0;border-color:#334155}
        .logs-panel{background:#0f172a;border-radius:20px;padding:16px}
        .logs-header{display:flex;justify-content:space-between;margin-bottom:12px}
        #logs{height:320px;overflow-y:auto;font-family:monospace;font-size:11px;background:#020617;padding:12px;border-radius:16px}
        .toast{position:fixed;bottom:20px;right:20px;background:#1e293b;border-left:4px solid #10b981;padding:12px 20px;border-radius:40px;z-index:1000}
        @media (max-width:600px){.input-row{flex-direction:column}}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🤖 TELEGRAM SWARM</h1>
        <div><span class="badge">🖥️ {{ server_id }}</span><a href="/logout" class="logout-btn" style="margin-left:10px">Logout</a></div>
    </div>
    <div class="status-bar">
        <div class="status-item">📡 SCRAPER: <span id="s-scrape">Idle</span></div>
        <div class="status-item">⚙️ WORKER: <span id="s-worker">Idle</span></div>
        <div class="status-item">📊 QUEUE: <span id="s-q">0</span></div>
        <div class="status-item">🤖 TELEGRAM: <span style="color:{{ '#10b981' if telegram_enabled else '#ef4444' }}">{{ 'ON' if telegram_enabled else 'OFF' }}</span></div>
    </div>
    <div class="card">
        <h2>📥 Add Videos to Queue</h2>
        <div class="input-group">
            <div class="input-row"><select id="mode"><option value="keyword">Keyword</option><option value="username">Username</option><option value="manual">Manual IDs/Links</option></select></div>
            <textarea id="target" rows="2" placeholder="Keyword, username, or video links (one per line)"></textarea>
            <button onclick="startQueue()" class="btn-orange">🚀 ADD TO QUEUE</button>
        </div>
        <hr>
        <div class="input-row"><button onclick="showSources()" style="background:#334155">📊 Queue Sources</button><button onclick="forceProcess()" style="background:#f59e0b">⚡ Force Process One</button></div>
    </div>
    <div class="logs-panel">
        <div class="logs-header"><span>📋 Live Logs (IST 12hr)</span><button onclick="clearLogs()" style="background:#475569; padding:6px 12px">Clear</button></div>
        <div id="logs">Loading...</div>
    </div>
</div>
<div id="toast" class="toast" style="display:none"></div>
<script>
    function showToast(msg,err){let t=document.getElementById('toast');t.style.display='block';t.style.borderLeftColor=err?'#ef4444':'#10b981';t.innerHTML=msg;setTimeout(()=>t.style.display='none',3500);}
    async function startQueue(){let mode=document.getElementById('mode').value,target=document.getElementById('target').value.trim();if(!target){showToast("Enter target!",true);return;}await fetch('/api/repost',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode,input:target})});showToast("Queuing started");document.getElementById('target').value='';}
    async function forceProcess(){await fetch('/api/force_process',{method:'POST'});showToast("Force process triggered");}
    async function clearLogs(){await fetch('/api/clear_logs',{method:'POST'});showToast("Logs cleared");}
    async function showSources(){let r=await fetch('/api/status');let d=await r.json();let g=d.sources_grouped||{};let html='';for(let [type,items] of Object.entries(g)){html+=`<b>📁 ${type.toUpperCase()}</b><ul>`;items.forEach(i=>{html+=`<li>${i.value} : ${i.count}</li>`});html+=`</ul>`;}if(!html)html='<p>No pending jobs</p>';alert(html);}
    setInterval(async()=>{try{let r=await fetch('/api/status');let d=await r.json();document.getElementById('s-scrape').innerText=d.scraper;document.getElementById('s-worker').innerText=d.worker;document.getElementById('s-q').innerText=d.queue_size;let logsDiv=document.getElementById('logs');let isBottom=logsDiv.scrollHeight-logsDiv.clientHeight<=logsDiv.scrollTop+1;logsDiv.innerHTML=d.logs.map(l=>`<div><span style='color:#64748b'>[${l.time}]</span> <span style='color:${l.color}'>[${l.category}]</span> ${l.message}</div>`).join('');if(isBottom)logsDiv.scrollTop=logsDiv.scrollHeight;}catch(e){}},1500);
</script>
</body>
</html>
"""

# ---------------------------
#  START WORKER THREAD
# ---------------------------
threading.Thread(target=worker_loop, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5050))
    print(f"🚀 Telegram Swarm Node on port {port} | Server: {SERVER_ID}")
    print(f"🤖 Telegram enabled: {TELEGRAM_ENABLED}")
    app.run(host='0.0.0.0', port=port, threaded=True)