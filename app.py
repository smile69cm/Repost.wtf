import os, json, time, subprocess, requests, threading, random, re, urllib.parse
import asyncio, aiohttp, hashlib
from flask import Flask, render_template_string, request, jsonify, send_from_directory
import psycopg2

app = Flask(__name__)

# ---------------------------
#  ENVIRONMENT & ARCHITECTURE
# ---------------------------
SYSTEM_ID = int(os.environ.get("SYSTEM", "0")) 
SYS_PREFIX = "ADMIN" if SYSTEM_ID == 0 else f"W{SYSTEM_ID}"

BASE_DIR = os.getcwd()
VIDEO_DIR = os.path.join(BASE_DIR, "watermarked_videos")
PREVIEW_DIR = os.path.join(BASE_DIR, "previews")
SETTINGS_FILE = "settings.json"
DESC_FILE = "description.json"

for d in [VIDEO_DIR, PREVIEW_DIR]:
    if not os.path.exists(d): os.makedirs(d)

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://cnkbewgpguyojiebztbs.supabase.co/rest/v1/reels")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNua2Jld2dwZ3V5b2ppZWJ6dGJzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQyODU0NzUsImV4cCI6MjA4OTg2MTQ3NX0.ldS5knPaT1imexuRH9jSlTDB1mRSpoozFXlmhbDw2fU")
NEON_DB_URL = os.getenv("NEON_DB_URL", "postgresql://neondb_owner:npg_Rh0xIbmdFe5u@ep-quiet-block-a12aatzr-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require")

# ---------------------------
#  NEON DB INITIALIZATION
# ---------------------------
def init_neon_db():
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS repost_queue (
                    id SERIAL PRIMARY KEY, video_id TEXT UNIQUE NOT NULL,
                    size_limit INT DEFAULT 10, status TEXT DEFAULT 'not started',
                    error TEXT, worker_id INT DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            try: cur.execute("ALTER TABLE repost_queue ADD COLUMN IF NOT EXISTS size_limit INT DEFAULT 10;")
            except psycopg2.Error: conn.rollback()
            try: cur.execute("ALTER TABLE repost_queue ADD COLUMN IF NOT EXISTS worker_id INT DEFAULT NULL;")
            except psycopg2.Error: conn.rollback()
            
            cur.execute("CREATE TABLE IF NOT EXISTS image_hashes (vid TEXT PRIMARY KEY, hash TEXT);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON repost_queue(status);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_worker ON repost_queue(worker_id);")
            conn.commit()
        conn.close()
    except Exception as e: print(f"⚠️ Neon DB Init Error: {e}")

init_neon_db()

# ---------------------------
#  SETTINGS MANAGER
# ---------------------------
DEFAULT_SETTINGS = {
    "my_token": "", "my_user": "telugustuffs",
    "main_domain": "love.viraly.wtf", "upload_domain": "loveupload.viraly.wtf",
    "admin_url": "", "workers": {}, "active_workers": "1,2,3,4,5",
    "blacklist": "promo, link in bio, part 2, pt 2, subscribe",
    "autopilot_targets": "keyword:telugu, username:hotdesi",
    "autopilot_interval": 60, "del_payload": ""
}

if not os.path.exists(SETTINGS_FILE): json.dump(DEFAULT_SETTINGS, open(SETTINGS_FILE, 'w'), indent=4)
if not os.path.exists(DESC_FILE): json.dump(["#telugu", "#viral", "#trending", "#desi", "#hot", "#reels"], open(DESC_FILE, 'w'))

def get_settings():
    with open(SETTINGS_FILE, 'r') as f: return json.load(f)

def get_random_desc():
    try:
        tags = json.load(open(DESC_FILE, 'r'))
        return f"@telugustuffs {' '.join(random.sample(tags, min(len(tags), random.randint(5, 10))))}"
    except: return "@telugustuffs #telugu #viral"

# ---------------------------
#  LOGGER & STATUS
# ---------------------------
log_messages = []
current_status = {"reposter": "Idle", "scraper": "Idle", "queue_size": 0, "system": SYSTEM_ID}

def emit_log(msg, category="SYS", color="#10b981"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{SYS_PREFIX}] [{category}] {msg}")
    log_messages.append(f"<span style='color:#64748b'>[{t}]</span> <b style='color:#f8fafc'>[{SYS_PREFIX}]</b> <span style='color:{color}'>[{category}]</span> {msg}")
    if len(log_messages) > 150: log_messages.pop(0)

# ---------------------------
#  ADMIN: UPTIME & DISPATCHER
# ---------------------------
def uptime_manager():
    if SYSTEM_ID != 0: return 
    emit_log("🛡️ UPTIME MANAGER ONLINE. Preventing Render Sleep.", "UPTIME", "#3b82f6")
    while True:
        try:
            conf = get_settings()
            urls_to_ping = list(conf.get("workers", {}).values())
            if conf.get("admin_url"): urls_to_ping.append(conf.get("admin_url"))
            for u in urls_to_ping:
                if u and u.startswith("http"):
                    try: requests.get(f"{u.rstrip('/')}/health", timeout=5)
                    except: pass
        except: pass
        time.sleep(300)

def admin_dispatcher():
    if SYSTEM_ID != 0: return
    emit_log("👑 ADMIN DISPATCHER ONLINE. Monitoring queue...", "ADMIN", "#facc15")
    while True:
        try:
            conf = get_settings()
            active_worker_ids = [int(w.strip()) for w in conf.get("active_workers", "").split(",") if w.strip().isdigit()]
            
            if active_worker_ids:
                conn = psycopg2.connect(NEON_DB_URL)
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT worker_id FROM repost_queue WHERE status = 'failed' AND error LIKE '%524%' AND updated_at > NOW() - INTERVAL '5 minutes'")
                    penalized = {row[0] for row in cur.fetchall() if row[0] is not None}

                    for w in active_worker_ids:
                        if w in penalized: continue
                        cur.execute("SELECT COUNT(*) FROM repost_queue WHERE worker_id = %s AND status IN ('assigned', 'doing')", (w,))
                        active_count = cur.fetchone()[0]
                        if active_count < 5:
                            needed = 5 - active_count
                            cur.execute("""
                                UPDATE repost_queue SET worker_id = %s, status = 'assigned', updated_at = NOW()
                                WHERE id IN (SELECT id FROM repost_queue WHERE status = 'not started' LIMIT %s FOR UPDATE SKIP LOCKED)
                            """, (w, needed))
                            if cur.rowcount > 0: emit_log(f"📦 Dispatched {cur.rowcount} jobs to Worker {w}", "ADMIN", "#a855f7")
                    conn.commit()
                conn.close()
        except: pass
        current_status["queue_size"] = get_queue_size()
        time.sleep(15) 

# ---------------------------
#  ADMIN: AUTOPILOT (CRON)
# ---------------------------
def autopilot_manager():
    if SYSTEM_ID != 0: return
    time.sleep(20) 
    emit_log("🤖 AUTOPILOT ENGINE ONLINE. Ready for automated scraping.", "AUTOPILOT", "#a855f7")
    while True:
        try:
            conf = get_settings()
            targets = [t.strip() for t in conf.get("autopilot_targets", "").split(",") if t.strip()]
            interval = max(int(conf.get("autopilot_interval", 60)), 15)
            
            if targets:
                emit_log(f"🤖 AUTOPILOT WAKEUP: Scanning {len(targets)} active targets...", "AUTOPILOT", "#a855f7")
                for target in targets:
                    parts = target.split(":")
                    if len(parts) == 2:
                        mode, val = parts[0].strip().lower(), parts[1].strip()
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        ids = loop.run_until_complete(async_db_pipeline(mode, val, scrape_only=True))
                        loop.close()
                        
                        added = sum(1 for vid in ids if add_to_neon_queue(vid, 9999)) 
                        if added > 0: emit_log(f"🤖 AUTOPILOT: Found & Queued {added} new videos from '{val}'", "AUTOPILOT", "#f59e0b")
        except Exception as e:
            emit_log(f"⚠️ Autopilot Error: {e}", "AUTOPILOT", "#ef4444")
        
        time.sleep(interval * 60)

# ---------------------------
#  ADMIN: HIVE CLEANER THREAD
# ---------------------------
def hive_cleaner_task():
    conf = get_settings()
    payload = conf.get("del_payload", "")
    username = conf.get("my_user")
    token = conf.get("my_token")
    domain = conf.get("main_domain")
    
    if not payload:
        emit_log("❌ Cleaner Aborted: Missing Delete Payload in Settings!", "CLEANER", "#ef4444")
        current_status["scraper"] = "Idle"
        return
        
    emit_log("🧹 NATIVE CLEANER STARTED: Mapping profile...", "CLEANER", "#06b6d4")
    current_status["scraper"] = "Cleaning Duplicates..."
    
    all_videos = []
    page = 0
    empty_pages = 0
    headers = {"Cookie": f"accessToken={token}; allow18=%7B%22allow18%22%3Atrue%7D"}
    
    while empty_pages < 2 and page < 60:
        try:
            res = requests.post(f"https://{domain}/profile/{username}/videos/latest", headers=headers, json={"page": page}, timeout=10)
            vids = re.findall(r'"videoId":"([^"]+)"', res.text)
            if not vids: empty_pages += 1
            else:
                empty_pages = 0
                all_videos.extend(vids)
            page += 1
            time.sleep(0.5)
        except: break
        
    all_videos = list(dict.fromkeys(all_videos))
    all_videos.reverse() 
    emit_log(f"🧹 Found {len(all_videos)} total videos. Checking Neon DB Hashes...", "CLEANER", "#06b6d4")
    
    conn = psycopg2.connect(NEON_DB_URL)
    cur = conn.cursor()
    
    seen_hashes = {}
    deleted_count = 0
    
    for vid in all_videos:
        cur.execute("SELECT hash FROM image_hashes WHERE vid = %s", (vid,))
        row = cur.fetchone()
        
        if row: img_hash = row[0]
        else:
            try:
                img_res = requests.get(f"https://{domain}/media/images/{vid}.jpg", stream=True, timeout=5)
                if img_res.status_code == 200:
                    img_hash = hashlib.md5(img_res.content).hexdigest()
                    cur.execute("INSERT INTO image_hashes (vid, hash) VALUES (%s, %s) ON CONFLICT DO NOTHING", (vid, img_hash))
                    conn.commit()
                else: continue
            except: continue
            
        if img_hash in seen_hashes:
            original_vid = seen_hashes[img_hash]
            emit_log(f"🚨 DUPLICATE FOUND: {vid[:8]}... Firing Payload!", "CLEANER", "#f43f5e")
            try:
                del_res = requests.post(f"https://{domain}/uservideo/delete/{vid}", json={"username": payload}, headers=headers, timeout=10)
                if del_res.status_code == 200: deleted_count += 1
            except: pass
            time.sleep(1.5) 
        else:
            seen_hashes[img_hash] = vid

    conn.close()
    emit_log(f"✨ CLEANUP COMPLETE! Permanently deleted {deleted_count} duplicates.", "CLEANER", "#10b981")
    current_status["scraper"] = "Idle"

if SYSTEM_ID == 0:
    threading.Thread(target=admin_dispatcher, daemon=True).start()
    threading.Thread(target=uptime_manager, daemon=True).start()
    threading.Thread(target=autopilot_manager, daemon=True).start()

# ---------------------------
#  DATABASE HELPERS
# ---------------------------
def add_to_neon_queue(video_id, size_limit=10):
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO repost_queue (video_id, size_limit, status, updated_at) VALUES (%s, %s, 'not started', NOW()) ON CONFLICT (video_id) DO NOTHING;", (video_id, size_limit))
            inserted = cur.rowcount > 0
            conn.commit()
        conn.close()
        return inserted
    except: return False

def get_next_assigned_job(w_id):
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE repost_queue SET status = 'doing', updated_at = NOW()
                WHERE id = (SELECT id FROM repost_queue WHERE status = 'assigned' AND worker_id = %s ORDER BY id ASC LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id, video_id, size_limit;
            """, (w_id,))
            job = cur.fetchone()
            conn.commit()
        conn.close()
        if job: return {"id": job[0], "video_id": job[1], "size_limit": job[2]}
    except: pass
    return None

def update_job_status(job_id, status, error_msg=None):
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("UPDATE repost_queue SET status = %s, error = %s, updated_at = NOW() WHERE id = %s", (status, error_msg, job_id))
            conn.commit()
        conn.close()
    except: pass

def get_queue_size():
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM repost_queue WHERE status = 'not started'")
            count = cur.fetchone()[0]
        conn.close()
        return count
    except: return 0

# ---------------------------
#  SCRAPER MODULE
# ---------------------------
def run_async(coroutine):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coroutine)
    loop.close()

async def async_db_pipeline(mode, query, scrape_only=False):
    current_status["scraper"] = f"Scraping {query}"
    if not scrape_only: emit_log(f"🚀 SCRAPE INITIATED | Target: '{query}'", "SCRAPE", "#3b82f6")
    conf = get_settings()
    s_headers = {"Cookie": f"accessToken={conf['my_token']}; allow18=%7B%22allow18%22%3Atrue%7D", "User-Agent": "Mozilla/5.0"}
    db_headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}
    all_vids = set()
    page, empty_count = 0, 0
    async with aiohttp.ClientSession() as session:
        while empty_count < 3 and page < 50:
            try:
                url = f"https://{conf['main_domain']}/profile/{query}/videos/latest" if mode == "username" else f"https://{conf['main_domain']}/searchVideo?q={query}&p={page}"
                async with session.get(url, headers=s_headers, timeout=10) if mode != "username" else session.post(url, headers=s_headers, json={"page": page}, timeout=10) as r:
                    text = await r.text()
                vids = re.findall(r'"videoId":"([^"]+)"', text)
                if not vids: empty_count += 1
                else:
                    empty_count = 0
                    all_vids.update(vids)
                    if not scrape_only:
                        await session.post(SUPABASE_URL, headers=db_headers, json=[{"id": v, "name": None, "views": 0, "likes_count": 0} for v in vids])
                page += 1
            except: break
    if not scrape_only: emit_log(f"✨ Scrape Done! Extracted {len(all_vids)} IDs.", "SCRAPE", "#3b82f6")
    current_status["scraper"] = "Idle"
    return list(all_vids)

# ---------------------------
#  WORKER: VIDEO PROCESSING NODE
# ---------------------------
def reposter_worker():
    if SYSTEM_ID == 0: return 
    global current_status
    emit_log(f"👷 WORKER ONLINE. Awaiting assignments...", "WORKER", "#f59e0b")
    
    while True:
        job = get_next_assigned_job(SYSTEM_ID)
        if not job:
            current_status["reposter"] = "Idle (Awaiting Admin)"
            time.sleep(10)
            continue

        video_id, size_limit = job["video_id"], job["size_limit"]
        current_status["reposter"] = f"Processing: {video_id[:8]}"
        
        # Initialize variables before the try block so 'finally' doesn't crash
        raw_file, watermarked_file, preview_file = None, None, None
        conf = get_settings()

        try:
            # Metadata Extraction
            title = f"Telugu Stuffs {video_id[:6]}"
            desc = get_random_desc()
            category_tag = "18+" 
            try:
                r_api = requests.get(f"https://{conf['main_domain']}/video/{urllib.parse.quote(video_id, safe='')}", headers={"Cookie": f"accessToken={conf['my_token']}"}, timeout=10).json()
                if isinstance(r_api, list) and len(r_api) > 0:
                    vid_data = r_api[0]
                    if vid_data.get("title"): title = vid_data["title"]
                    if vid_data.get("description"): desc = vid_data["description"]
                    if vid_data.get("tag"): category_tag = vid_data["tag"]
            except: pass

            # Smart Blacklist Check
            bl_words = [w.strip().lower() for w in conf.get("blacklist", "").split(",") if w.strip()]
            text_to_check = f"{title} {desc} {category_tag}".lower()
            if any(w in text_to_check for w in bl_words):
                emit_log(f"🛑 BLACKLISTED: Spam keyword detected. Trashing video.", "REPOST", "#ef4444")
                update_job_status(job["id"], 'failed', "Blacklisted Keyword")
                continue

            # Size Check
            d_url = f"https://{conf['main_domain']}/media/videos/{video_id}.mp4"
            h_media = {"Cookie": f"accessToken={conf['my_token']}; allow18=%7B%22allow18%22%3Atrue%7D", "User-Agent": "Mozilla/5.0"}
            size_mb = 0
            with requests.get(d_url, headers=h_media, stream=True, timeout=10) as r_size:
                if r_size.status_code == 200 and 'content-length' in r_size.headers:
                    size_mb = round(int(r_size.headers['content-length']) / (1024 * 1024), 2)

            if size_limit != 9999 and size_mb > size_limit:
                emit_log(f"⏭️ SKIPPED ➔ {size_mb}MB > {size_limit}MB", "REPOST", "#f43f5e")
                update_job_status(job["id"], 'failed', f"Skipped: Too Large")
                continue

            emit_log(f"📥 DOWNLOADING ➔ {size_mb}MB", "REPOST", "#0ea5e9")
            
            # File Paths definition
            safe_label = re.sub(r'[^a-zA-Z0-9]', '_', video_id)[-12:]
            raw_file = os.path.join(VIDEO_DIR, f"raw_{safe_label}.mp4")
            watermarked_file = os.path.join(VIDEO_DIR, f"video_{safe_label}.mp4")
            preview_file = os.path.join(PREVIEW_DIR, f"{safe_label}.jpg")

            # Downloading stream
            with requests.get(d_url, headers=h_media, stream=True) as s_res:
                if s_res.status_code != 200: raise Exception(f"404 Not Found")
                with open(raw_file, 'wb') as f:
                    for chunk in s_res.iter_content(8192): f.write(chunk)

            file_to_upload = raw_file
            
            # Ghost Mode Processing
            if size_limit == 9999 and size_mb > 40:
                emit_log(f"⚡ UNLIMITED PASS ➔ Skipping watermark", "REPOST", "#d946ef")
                subprocess.run(['ffmpeg', '-y', '-i', raw_file, '-ss', '1', '-vframes', '1', preview_file], capture_output=True)
            else:
                emit_log(f"👻 GHOST WATERMARKING (Anti-Ban)...", "REPOST", "#d946ef")
                vf = "hflip,eq=brightness=0.02:saturation=1.05,scale='min(720,iw)':-2,drawtext=text='telugu stuffs':fontcolor=yellow@0.6:fontsize=24:x=(w-text_w)/2:y=h-th-14"
                subprocess.run(['ffmpeg', '-y', '-i', raw_file, '-ss', '1', '-vframes', '1', preview_file], capture_output=True)
                subprocess.run(['ffmpeg', '-y', '-i', raw_file, '-vf', vf, '-c:v', 'libx264', '-crf', '28', '-preset', 'ultrafast', '-c:a', 'copy', watermarked_file], capture_output=True)
                file_to_upload = watermarked_file

            # Upload Process
            emit_log(f"📤 UPLOADING... [{category_tag}]", "REPOST", "#0ea5e9")
            base = ".".join(conf['main_domain'].split('.')[-2:])
            with open(file_to_upload, 'rb') as f:
                up = requests.post(f"https://{conf['upload_domain']}/upload",
                    files={'files': (f"video_{safe_label}.mp4", f, 'video/mp4')},
                    data={"tag": category_tag, "title": title, "description": desc, "country": "IN", "username": conf['my_user'], "start": "0", "end": "0"},
                    headers={"Cookie": f"accessToken={conf['my_token']}", "Origin": f"https://{base}"})

            if up.status_code == 200:
                emit_log(f"✅ SUCCESS ➔ {video_id[:8]}", "REPOST", "#10b981")
                update_job_status(job["id"], 'completed')
            else: raise Exception(f"HTTP {up.status_code}")

        except Exception as e:
            emit_log(f"🔥 Error: {e}", "REPOST", "#ef4444")
            update_job_status(job["id"], 'failed', str(e))
        
        # -----------------------------------------------------
        # THE BULLETPROOF CLEANUP BLOCK - ALWAYS EXECUTES!
        # -----------------------------------------------------
        finally:
            if raw_file and os.path.exists(raw_file): 
                os.remove(raw_file)
            if watermarked_file and os.path.exists(watermarked_file): 
                os.remove(watermarked_file)
            if preview_file and os.path.exists(preview_file): 
                os.remove(preview_file)

if SYSTEM_ID > 0:
    threading.Thread(target=reposter_worker, daemon=True).start()

# ---------------------------
#  FLASK WEB UI
# ---------------------------
HTML_TEMPLATE = """
<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>V7.1 Ultimate Hive</title>
<style>
    :root { --bg: #0f172a; --panel: #1e293b; --acc: #3b82f6; --text: #f8fafc; --grn: #10b981; }
    body { font-family: 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 15px; padding-bottom: 80px;}
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
    .card { background: var(--panel); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0,0,0,0.5); border-top: 3px solid var(--acc); }
    h2, h3 { margin-top: 0; color: #fff; }
    input, select, textarea, button { width: 100%; padding: 12px; margin-top: 8px; border-radius: 4px; border: 1px solid #334155; background: #020617; color: #fff; box-sizing: border-box; }
    button { background: var(--acc); color: #fff; font-weight: bold; border: none; cursor: pointer; transition: 0.2s; }
    button:hover { filter: brightness(1.2); }
    .btn-sync { background: #8b5cf6; } .btn-repost { background: #f59e0b; } .btn-red { background: #ef4444; }
    #logs { height: 450px; overflow-y: auto; background: #020617; padding: 15px; font-family: 'Consolas', monospace; font-size: 13px; border-radius: 6px; margin-top: 10px; border: 1px solid #334155; line-height: 1.6;}
    .status-bar { display: flex; justify-content: space-between; background: #020617; padding: 12px; border-radius: 6px; font-size: 14px; margin-bottom: 15px; border-left: 4px solid var(--grn); align-items:center; flex-wrap: wrap; gap:10px;}
    .sys-badge { background: {% if sys_id == 0 %}#facc15{% else %}#f59e0b{% endif %}; color: #000; padding: 4px 10px; border-radius: 4px; font-weight: bold; font-size:16px; margin-left:10px; }
    .worker-stat { background: #334155; padding: 5px 10px; border-radius: 4px; font-size:12px; border: 1px solid #475569;}
    .sm-label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; display:block; margin-top: 10px; }
    @media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }
</style></head>
<body>
    <h2>🐝 V7.1 ULTIMATE HIVE <span class="sys-badge">{% if sys_id == 0 %}👑 GOD MODE{% else %}👷 WORKER {{ sys_id }}{% endif %}</span></h2>
    <div class="status-bar">
        <div>
            <b>[SCRAPER/CLEANER]</b> <span id="s-scrape" style="color:#3b82f6; margin-right:15px;">Idle</span>
            {% if sys_id > 0 %}<b>[WORKER]</b> <span id="s-repost" style="color:#f59e0b;">Idle</span> | {% endif %}
            <b>[UNASSIGNED IDs]</b> <span id="s-q" style="color:#10b981;">0</span>
        </div>
        <div id="worker-matrix" style="display:flex; gap:5px; flex-wrap:wrap;"></div>
    </div>
    
    {% if sys_id == 0 %}
    <div class="grid-2">
        <div class="card" style="border-top-color: #a855f7;">
            <h3>⚙️ Automation & Anti-Ban</h3>
            <span class="sm-label">Smart Blacklist (Comma separated)</span>
            <input id="set_bl" placeholder="promo, pt 2, link in bio...">
            <div style="display:flex; gap:10px;">
                <div style="width:70%;">
                    <span class="sm-label">Autopilot Targets</span>
                    <input id="set_auto_t" placeholder="username:desi, keyword:hot">
                </div>
                <div style="width:30%;">
                    <span class="sm-label">Interval (Mins)</span>
                    <input id="set_auto_i" type="number" value="60">
                </div>
            </div>
            <span class="sm-label">Enabled Workers (Remove a number to pause them!)</span>
            <input id="set_active_w" placeholder="1, 2, 3, 4, 5">
        </div>
        
        <div class="card" style="border-top-color: #f59e0b;">
            <h3>🎥 Manual Actions & Cleaner</h3>
            <div style="display:flex; gap:10px;">
                <input id="rep_input" placeholder="Paste manual IDs here..." style="width:70%;">
                <select id="size_limit" style="width:30%;"><option value="10">10 MB</option><option value="40">40 MB</option><option value="9999">Unlmt.</option></select>
            </div>
            <button onclick="startReposter()" class="btn-repost">⚙️ ADD TO NEON DB</button>
            <hr style="border-color:#334155; margin:15px 0;">
            <span class="sm-label">Encrypted Delete Payload</span>
            <input id="set_del" placeholder="U2FsdGVkX19Y2vJS8yrBP8...">
            <button onclick="runCleaner()" class="btn-red">🧹 RUN DB NATIVE CLEANER</button>
        </div>
    </div>
    {% endif %}

    <div class="card" style="margin-top: 15px; border-top-color:#10b981;">
        <div style="display:flex; justify-content:space-between; align-items:flex-start;">
            <h3>🖥️ {{ 'Global Hive Logs' if sys_id == 0 else 'Worker Local Logs' }}</h3>
            <button onclick="saveConfig()" style="width:auto; padding:5px 15px; font-size:12px; background:#475569;">💾 SAVE SETTINGS</button>
        </div>
        
        <div class="grid-2" style="margin-bottom:10px;">
            <div><span class="sm-label">Account Token</span><input id="set_token" placeholder="Access Token"></div>
            <div><span class="sm-label">Username</span><input id="set_user" placeholder="Target Username"></div>
            {% if sys_id == 0 %}
            <div><span class="sm-label">Admin URL (Keep-Alive)</span><input id="set_admin" placeholder="https://admin.onrender.com"></div>
            <div><span class="sm-label">Worker URLs (ID, URL)</span><textarea id="set_workers" placeholder="1, https://w1.onrender.com" rows="2"></textarea></div>
            {% endif %}
        </div>
        <div id="logs">Loading logs...</div>
    </div>
<script>
    const sysId = {{ sys_id }};
    async function startReposter() {
        let input = document.getElementById('rep_input').value, limit = document.getElementById('size_limit').value;
        if(!input) return alert("Enter IDs!");
        await fetch('/api/repost', {method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({input:input, size_limit: parseInt(limit)})});
        document.getElementById('rep_input').value = '';
    }
    async function runCleaner() {
        if(confirm("Are you sure? Make sure your Delete Payload is saved in settings!")) {
            await fetch('/api/cleaner', {method: 'POST'});
        }
    }
    async function saveConfig() {
        let payload = {my_token: document.getElementById('set_token').value, my_user: document.getElementById('set_user').value};
        if(sysId === 0) {
            payload.admin_url = document.getElementById('set_admin').value;
            payload.worker_text = document.getElementById('set_workers').value;
            payload.blacklist = document.getElementById('set_bl').value;
            payload.autopilot_targets = document.getElementById('set_auto_t').value;
            payload.autopilot_interval = parseInt(document.getElementById('set_auto_i').value);
            payload.active_workers = document.getElementById('set_active_w').value;
            payload.del_payload = document.getElementById('set_del').value;
        }
        await fetch('/api/settings', {method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
        alert("Configuration saved successfully!");
    }
    setInterval(async () => {
        let endpoint = sysId === 0 ? '/api/admin_data' : '/api/status';
        try {
            let r = await fetch(endpoint); let d = await r.json();
            
            if(sysId === 0) {
                document.getElementById('s-scrape').innerText = d.status.scraper;
                document.getElementById('s-q').innerText = d.status.queue_size;
                let matrix = "";
                for(let [wid, wstat] of Object.entries(d.worker_matrix)) {
                    let col = wstat === "OFFLINE" ? "color:#ef4444" : "color:#10b981";
                    if(wstat.includes("Processing")) col = "color:#facc15";
                    matrix += `<div class="worker-stat">W${wid}: <span style="${col}">${wstat}</span></div>`;
                }
                document.getElementById('worker-matrix').innerHTML = matrix;
            } else {
                document.getElementById('s-scrape').innerText = d.scraper; 
                document.getElementById('s-repost').innerText = d.reposter;
                document.getElementById('s-q').innerText = d.queue_size;
            }

            let logsDiv = document.getElementById('logs');
            let isScrolledToBottom = logsDiv.scrollHeight - logsDiv.clientHeight <= logsDiv.scrollTop + 1;
            logsDiv.innerHTML = (sysId === 0 ? d.logs : d.logs).join('<br>');
            if (isScrolledToBottom) logsDiv.scrollTop = logsDiv.scrollHeight;
        } catch(e) {}
    }, 1500);
    
    (async () => {
        let r = await fetch('/api/settings'); let d = await r.json();
        document.getElementById('set_token').value = d.my_token || ""; 
        document.getElementById('set_user').value = d.my_user || "";
        if(sysId === 0) {
            document.getElementById('set_admin').value = d.admin_url || "";
            document.getElementById('set_bl').value = d.blacklist || "";
            document.getElementById('set_auto_t').value = d.autopilot_targets || "";
            document.getElementById('set_auto_i').value = d.autopilot_interval || 60;
            document.getElementById('set_active_w').value = d.active_workers || "1,2,3,4,5";
            document.getElementById('set_del').value = d.del_payload || "";
            let wText = "";
            for(let [k,v] of Object.entries(d.workers || {})) { wText += `${k}, ${v}\n`; }
            document.getElementById('set_workers').value = wText.trim();
        }
    })();
</script></body></html>
"""

@app.route('/')
def home(): return render_template_string(HTML_TEMPLATE, sys_id=SYSTEM_ID)

@app.route('/health')
def health(): return jsonify({"status": "awake"}), 200

@app.route('/api/status')
def api_status(): 
    current_status["queue_size"] = get_queue_size()
    return jsonify(current_status | {"logs": log_messages})

@app.route('/api/admin_data')
def api_admin_data():
    if SYSTEM_ID != 0: return jsonify({})
    conf = get_settings()
    all_logs = list(log_messages)
    w_matrix = {}
    
    for wid, wurl in conf.get("workers", {}).items():
        if wurl and wurl.startswith("http"):
            try:
                r = requests.get(f"{wurl.rstrip('/')}/api/status", timeout=2).json()
                all_logs.extend(r.get("logs", []))
                w_matrix[wid] = r.get("reposter", "Idle")
            except: w_matrix[wid] = "OFFLINE"

    def get_time(log_str):
        m = re.search(r'\[(\d{2}:\d{2}:\d{2})\]', log_str)
        return m.group(1) if m else "00:00:00"
        
    all_logs.sort(key=get_time)
    current_status["queue_size"] = get_queue_size()
    return jsonify({"status": current_status, "logs": all_logs[-200:], "worker_matrix": w_matrix})

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    if request.method == 'GET': return jsonify(get_settings())
    data = request.json
    conf = get_settings()
    conf["my_token"] = data.get("my_token", conf["my_token"])
    conf["my_user"] = data.get("my_user", conf["my_user"])
    
    if SYSTEM_ID == 0:
        conf["admin_url"] = data.get("admin_url", "")
        conf["blacklist"] = data.get("blacklist", conf["blacklist"])
        conf["autopilot_targets"] = data.get("autopilot_targets", conf["autopilot_targets"])
        conf["autopilot_interval"] = data.get("autopilot_interval", conf["autopilot_interval"])
        conf["active_workers"] = data.get("active_workers", conf["active_workers"])
        conf["del_payload"] = data.get("del_payload", conf["del_payload"])
        
        worker_text = data.get("worker_text", "")
        w_dict = {}
        for line in worker_text.split('\n'):
            parts = line.split(',')
            if len(parts) == 2: w_dict[parts[0].strip()] = parts[1].strip()
        conf["workers"] = w_dict

    json.dump(conf, open(SETTINGS_FILE, 'w'), indent=4)
    emit_log("Settings applied safely.", "SYS", "#10b981")
    return jsonify({"status": "ok"})

@app.route('/api/repost', methods=['POST'])
def api_repost():
    if SYSTEM_ID != 0: return jsonify({"error": "Admin only"}), 403
    data = request.json
    def handle_queueing():
        ids = []
        for line in data['input'].strip().split('\n'):
            line = line.strip()
            if not line: continue
            match = re.search(r'/(?:video/)?([^/?]+)', line)
            ids.append(match.group(1) if match else line)
        added = sum(1 for vid in ids if add_to_neon_queue(vid, data['size_limit']))
        emit_log(f"⚡ Added {added} NEW unique manual jobs to DB Queue", "ADMIN", "#f59e0b")
    threading.Thread(target=handle_queueing, daemon=True).start()
    return jsonify({"status": "queued"})

@app.route('/api/cleaner', methods=['POST'])
def api_cleaner():
    if SYSTEM_ID != 0: return jsonify({"error": "Admin only"}), 403
    threading.Thread(target=hive_cleaner_task, daemon=True).start()
    return jsonify({"status": "started"})

@app.route('/videos/<path:f>')
def serv_v(f): return send_from_directory(VIDEO_DIR, f)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5050))
    print(f"🚀 STARTING RENDER HIVE {'👑 ADMIN' if SYSTEM_ID == 0 else f'👷 WORKER {SYSTEM_ID}'} on Port {port}...")
    app.run(host='0.0.0.0', port=port, threaded=True)
