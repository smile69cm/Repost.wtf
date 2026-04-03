import os, json, time, subprocess, requests, threading, random, re, urllib.parse
import asyncio, aiohttp
from flask import Flask, render_template_string, request, jsonify, send_from_directory
import psycopg2

app = Flask(__name__)

# ---------------------------
#  CONFIG & DIRECTORIES
# ---------------------------
BASE_DIR = os.getcwd()
VIDEO_DIR = os.path.join(BASE_DIR, "watermarked_videos")
PREVIEW_DIR = os.path.join(BASE_DIR, "previews")
DB_FILE = "processed_history.json"
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
    """Create distributed queue table and seamlessly upgrade old DB structure."""
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            # 1. Create table if it doesn't exist at all
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
            # 2. Upgrade old databases (adds size_limit column if missing)
            try:
                cur.execute("ALTER TABLE repost_queue ADD COLUMN IF NOT EXISTS size_limit INT DEFAULT 10;")
            except psycopg2.Error:
                conn.rollback() # Rollback if column already exists so we can continue

            cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON repost_queue(status);")
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Neon DB Init Error: {e}")

init_neon_db()

# ---------------------------
#  SETTINGS
# ---------------------------
DEFAULT_SETTINGS = {
    "my_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6Imxha3NobWluaWdodHkiLCJpYXQiOjE3NzUxOTE4ODIsImV4cCI6MTc3Nzc4Mzg4Mn0.z3ugmWosE8bL1mIA7Yxzf5hzfdbJQOIgyahon2tpuMY",
    "my_user": "telugustuffs",
    "main_domain": "love.viraly.wtf",
    "upload_domain": "loveupload.viraly.wtf"
}

if not os.path.exists(DB_FILE): json.dump([], open(DB_FILE, 'w'))
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
current_status = {"reposter": "Idle", "scraper": "Idle", "queue_size": 0, "db_count": 0}

def emit_log(msg, category="SYS", color="#10b981"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{category}] {msg}")
    log_messages.append(f"<span style='color:#64748b'>[{t}]</span> <span style='color:{color}'>[{category}]</span> {msg}")
    if len(log_messages) > 100: log_messages.pop(0)

# ---------------------------
#  DB TRACKER
# ---------------------------
def update_db_count_loop():
    while True:
        try:
            h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Prefer": "count=exact"}
            r = requests.get(f"{SUPABASE_URL}?select=id", headers=h, timeout=10)
            if 'Content-Range' in r.headers:
                current_status["db_count"] = int(r.headers['Content-Range'].split("/")[-1])
        except: pass
        time.sleep(30)

threading.Thread(target=update_db_count_loop, daemon=True).start()

# ---------------------------
#  CENTRAL QUEUE FUNCTIONS
# ---------------------------
def add_to_neon_queue(video_id, size_limit=10):
    """Adds to DB. Ignores if video_id already exists."""
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO repost_queue (video_id, size_limit, status, updated_at) 
                VALUES (%s, %s, 'not started', NOW())
                ON CONFLICT (video_id) DO NOTHING;
            """, (video_id, size_limit))
            inserted = cur.rowcount > 0
            conn.commit()
        conn.close()
        return inserted
    except Exception as e:
        emit_log(f"DB Insert Error: {e}", "SYS", "#ef4444")
        return False

def get_next_job():
    """Fetches jobs and locks them so servers don't overlap."""
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE repost_queue 
                SET status = 'doing', updated_at = NOW()
                WHERE id = (
                    SELECT id FROM repost_queue 
                    WHERE status = 'not started' 
                       OR (status = 'doing' AND updated_at < NOW() - INTERVAL '5 minutes')
                    ORDER BY updated_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, video_id, size_limit;
            """)
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
            cur.execute("SELECT COUNT(*) FROM repost_queue WHERE status != 'completed'")
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

async def async_fast_sync():
    current_status["scraper"] = "Syncing Titles..."
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
    conf = get_settings()

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{SUPABASE_URL}?select=id&name=is.null&limit=1000", headers=headers) as r:
            todo = await r.json() if r.status == 200 else []

    if not todo:
        current_status["scraper"] = "Idle"
        return

    emit_log(f"⚡ FAST SYNC: Updating {len(todo)} missing titles...", "SYNC", "#8b5cf6")
    sem = asyncio.Semaphore(20)
    
    async def fetch_and_patch(raw_id):
        async with sem:
            safe_id = urllib.parse.quote(raw_id, safe='')
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"https://{conf['main_domain']}/video/{safe_id}", timeout=10) as res:
                        if res.status == 200:
                            d = await res.json()
                            title = d[0].get("title", "Untitled") if isinstance(d, list) and d else d.get("title", "Untitled") if isinstance(d, dict) else "Untitled"
                            await s.patch(f"{SUPABASE_URL}?id=eq.{safe_id}", headers=headers, json={"name": title})
            except: pass

    await asyncio.gather(*(fetch_and_patch(item['id']) for item in todo))
    current_status["scraper"] = "Idle"

async def async_db_pipeline(mode, query, scrape_only=False):
    current_status["scraper"] = f"Scraping {query}"
    emit_log(f"🚀 SCRAPE INITIATED | Target: '{query}'", "SCRAPE", "#3b82f6")

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
                if not vids:
                    empty_count += 1
                else:
                    empty_count = 0
                    all_vids.update(vids)
                    if not scrape_only:
                        await session.post(SUPABASE_URL, headers=db_headers, json=[{"id": v, "name": None, "views": 0, "likes_count": 0} for v in vids])
                        emit_log(f"📡 [PAGE {page}] Extracted & DB Pushed {len(vids)} IDs", "SCRAPE", "#3b82f6")
                page += 1
                await asyncio.sleep(0.5)
            except: break

    emit_log(f"✨ Scrape Done! Extracted {len(all_vids)} IDs.", "SCRAPE", "#3b82f6")
    if scrape_only:
        current_status["scraper"] = "Idle"
        return list(all_vids)

    await async_fast_sync()
    return list(all_vids)

# ---------------------------
#  DISTRIBUTED REPOSTER WORKER
# ---------------------------
def reposter_worker():
    global current_status
    while True:
        job = get_next_job()
        
        if not job:
            current_status["reposter"] = "Idle (Sleeping)"
            time.sleep(30)
            continue

        video_id, size_limit = job["video_id"], job["size_limit"]
        current_status["reposter"] = f"Processing: {video_id[:8]}"
        current_status["queue_size"] = get_queue_size()

        raw_file, watermarked_file = None, None
        conf = get_settings()

        try:
            # Fetch Title
            title = f"Telugu Stuffs {video_id[:6]}"
            try:
                r_api = requests.get(f"https://{conf['main_domain']}/video/{urllib.parse.quote(video_id, safe='')}", headers={"Cookie": f"accessToken={conf['my_token']}"}, timeout=10).json()
                title = r_api[0].get("title", title) if isinstance(r_api, list) and r_api else r_api.get("title", title) if isinstance(r_api, dict) else title
            except: pass
            
            # Check size
            d_url = f"https://{conf['main_domain']}/media/videos/{video_id}.mp4"
            h_media = {"Cookie": f"accessToken={conf['my_token']}; allow18=%7B%22allow18%22%3Atrue%7D", "User-Agent": "Mozilla/5.0"}
            
            size_mb = 0
            with requests.get(d_url, headers=h_media, stream=True, timeout=10) as r_size:
                if r_size.status_code == 200 and 'content-length' in r_size.headers:
                    size_mb = round(int(r_size.headers['content-length']) / (1024 * 1024), 2)

            # --- DYNAMIC SIZE LIMIT LOGIC ---
            # If the size exceeds the chosen limit (and limit is not 9999 for unlimited)
            if size_limit != 9999 and size_mb > size_limit:
                emit_log(f"⏭️ SKIPPED ➔ Size: {size_mb}MB > {size_limit}MB Limit", "REPOST", "#f43f5e")
                update_job_status(job["id"], 'completed', f"Skipped: Size {size_mb}MB")
                continue

            emit_log(f"📥 DOWNLOADING ➔ {title[:20]} | {size_mb}MB", "REPOST", "#0ea5e9")
            safe_label = re.sub(r'[^a-zA-Z0-9]', '_', video_id)[-12:]
            raw_file = f"raw_{safe_label}.mp4"
            watermarked_file = os.path.join(VIDEO_DIR, f"video_{safe_label}.mp4")

            # Download Raw File
            with requests.get(d_url, headers=h_media, stream=True) as s_res:
                if s_res.status_code != 200: raise Exception(f"404 Not Found")
                with open(raw_file, 'wb') as f:
                    for chunk in s_res.iter_content(8192): f.write(chunk)

            # --- SMART WATERMARKING LOGIC ---
            file_to_upload = raw_file
            
            # If Unlimited mode is active AND file is > 40MB, skip FFmpeg to save RAM
            if size_limit == 9999 and size_mb > 40:
                emit_log(f"⚡ UNLIMITED PASS ➔ {size_mb}MB. Skipping watermark to prevent RAM crash!", "REPOST", "#d946ef")
                # We still need a thumbnail preview, this is RAM-safe
                subprocess.run(['ffmpeg', '-y', '-i', raw_file, '-ss', '1', '-vframes', '1', os.path.join(PREVIEW_DIR, f"{safe_label}.jpg")], capture_output=True)
                file_to_upload = raw_file
            else:
                emit_log(f"🎨 WATERMARKING...", "REPOST", "#d946ef")
                vf = "scale='min(720,iw)':-2,drawtext=text='telugu stuffs':fontcolor=yellow@0.6:fontsize=24:x=(w-text_w)/2:y=h-th-14"
                subprocess.run(['ffmpeg', '-y', '-i', raw_file, '-vf', vf, '-ss', '1', '-vframes', '1', os.path.join(PREVIEW_DIR, f"{safe_label}.jpg")], capture_output=True)
                subprocess.run(['ffmpeg', '-y', '-i', raw_file, '-vf', vf, '-c:v', 'libx264', '-crf', '28', '-preset', 'ultrafast', '-c:a', 'copy', watermarked_file], capture_output=True)
                file_to_upload = watermarked_file

            # Upload
            emit_log(f"📤 UPLOADING...", "REPOST", "#0ea5e9")
            base = ".".join(conf['main_domain'].split('.')[-2:])
            with open(file_to_upload, 'rb') as f:
                up = requests.post(f"https://{conf['upload_domain']}/upload",
                    files={'files': (f"video_{safe_label}.mp4", f, 'video/mp4')},
                    data={"tag": "18+", "title": title, "description": get_random_desc(), "country": "IN", "username": conf['my_user'], "start": "0", "end": "0"},
                    headers={"Cookie": f"accessToken={conf['my_token']}", "Origin": f"https://{base}"})

            if up.status_code == 200:
                emit_log(f"✅ SUCCESS ➔ {title[:20]}", "REPOST", "#10b981")
                update_job_status(job["id"], 'completed')
            else:
                raise Exception(f"Upload failed: HTTP {up.status_code}")

        except Exception as e:
            emit_log(f"🔥 Error: {e}", "REPOST", "#ef4444")
            update_job_status(job["id"], 'not started', str(e))

        finally:
            # File Cleanup
            if raw_file and os.path.exists(raw_file): os.remove(raw_file)
            if watermarked_file and os.path.exists(watermarked_file): os.remove(watermarked_file)

        current_status["queue_size"] = get_queue_size()

threading.Thread(target=reposter_worker, daemon=True).start()

# ---------------------------
#  FLASK WEB UI & ENDPOINTS
# ---------------------------
HTML_TEMPLATE = """
<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Render Hive Hub</title>
<style>
    :root { --bg: #0f172a; --panel: #1e293b; --acc: #3b82f6; --text: #f8fafc; --grn: #10b981; }
    body { font-family: 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 15px; padding-bottom: 80px;}
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
    .card { background: var(--panel); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0,0,0,0.5); border-top: 3px solid var(--acc); }
    h2, h3 { margin-top: 0; color: #fff; }
    input, select, textarea, button { width: 100%; padding: 12px; margin-top: 8px; border-radius: 4px; border: 1px solid #334155; background: #020617; color: #fff; box-sizing: border-box; }
    button { background: var(--acc); color: #fff; font-weight: bold; border: none; cursor: pointer; transition: 0.2s; }
    button:hover { filter: brightness(1.2); }
    .btn-sync { background: #8b5cf6; } .btn-repost { background: #f59e0b; }
    #logs { height: 350px; overflow-y: auto; background: #020617; padding: 15px; font-family: 'Consolas', monospace; font-size: 13px; border-radius: 6px; margin-top: 10px; border: 1px solid #334155; line-height: 1.6;}
    .status-bar { display: flex; justify-content: space-between; background: #020617; padding: 12px; border-radius: 6px; font-size: 14px; margin-bottom: 15px; border-left: 4px solid var(--grn); align-items:center;}
    .badge { background: #10b981; color: #000; padding: 4px 10px; border-radius: 20px; font-weight: bold; font-size:16px;}
    @media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }
</style></head>
<body>
    <h2>🐝 RENDER HIVE COMMAND CENTER</h2>
    <div class="status-bar">
        <div>
            <b>[SCRAPER]</b> <span id="s-scrape" style="color:#3b82f6; margin-right:15px;">Idle</span>
            <b>[WORKER]</b> <span id="s-repost" style="color:#f59e0b;">Idle</span> | <b>[NEON QUEUE]</b> <span id="s-q" style="color:#10b981;">0</span>
        </div>
        <div class="badge">SUPABASE IDs: <span id="s-db">0</span></div>
    </div>
    <div class="grid-2">
        <div class="card" style="border-top-color: #3b82f6;">
            <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                <h3>🔍 Fast DB Scraper</h3>
                <button onclick="fastSync()" class="btn-sync" style="width:auto; padding:6px 12px; margin:0; font-size:12px;">⚡ FORCE DB SYNC</button>
            </div>
            <p style="font-size:12px; color:#94a3b8;">Scrapes directly into Database with 0 views/likes, then fetches all titles.</p>
            <select id="db_mode"><option value="keyword">Keyword (Search)</option><option value="username">Username (Profile)</option></select>
            <input id="db_target" placeholder="Target keyword or username...">
            <button onclick="startScraper()">🚀 EXTRACT TO SUPABASE</button>
        </div>
        <div class="card" style="border-top-color: #f59e0b;">
            <h3>🎥 Smart Auto-Reposter</h3>
            <p style="font-size:12px; color:#94a3b8;">Supports: raw video ID, keyword, or username.</p>
            <div style="display:flex; gap:10px;">
                <select id="rep_mode" style="width:40%;"><option value="manual">Manual IDs</option><option value="keyword">Keyword</option><option value="username">Username</option></select>
                <input id="rep_input" placeholder="e.g. CAeAPfjrSB2J..." style="width:60%;">
            </div>
            
            <label style="display:flex; align-items:center; gap:10px; font-size:14px; margin-top:10px; background:#334155; padding:10px; border-radius:4px;">
                <b style="color:#f8fafc;">SIZE LIMIT:</b>
                <select id="size_limit" style="width:100%; margin:0; padding:8px;">
                    <option value="10">Max 10 MB (Safest)</option>
                    <option value="30">Max 30 MB</option>
                    <option value="40">Max 40 MB</option>
                    <option value="9999">Unlimited (Direct upload if >40MB)</option>
                </select>
            </label>
            
            <button onclick="startReposter()" class="btn-repost">⚙️ BROADCAST TO NEON DB</button>
        </div>
    </div>
    <div class="card" style="margin-top: 15px; border-top-color:#10b981;">
        <div style="display:flex; justify-content:space-between; align-items:center;">
            <h3>🖥️ Hive Terminal</h3>
            <button onclick="saveConfig()" style="width:auto; padding:5px 15px; font-size:12px; background:#475569;">💾 SAVE CONFIG</button>
        </div>
        <div class="grid-2" style="margin-bottom:10px;">
            <input id="set_token" placeholder="Access Token (Required)">
            <input id="set_user" placeholder="Target Upload Username">
        </div>
        <div id="logs">Loading logs...</div>
    </div>
<script>
    async function startScraper() {
        let m = document.getElementById('db_mode').value, q = document.getElementById('db_target').value;
        if(!q) return alert("Enter target!");
        await fetch('/api/scrape', {method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({mode:m, query:q})});
        document.getElementById('db_target').value = '';
    }
    async function fastSync() { await fetch('/api/fast_sync', {method: 'POST'}); }
    async function startReposter() {
        let mode = document.getElementById('rep_mode').value;
        let input = document.getElementById('rep_input').value;
        let limit = document.getElementById('size_limit').value;
        if(!input) return alert("Enter target or IDs!");
        await fetch('/api/repost', {method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({mode:mode, input:input, size_limit: parseInt(limit)})});
        document.getElementById('rep_input').value = '';
    }
    async function saveConfig() {
        let payload = {my_token: document.getElementById('set_token').value, my_user: document.getElementById('set_user').value};
        await fetch('/api/settings', {method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
        alert("Configuration saved!");
    }
    setInterval(async () => {
        let r = await fetch('/api/status'); let d = await r.json();
        document.getElementById('s-scrape').innerText = d.scraper; document.getElementById('s-repost').innerText = d.reposter;
        document.getElementById('s-q').innerText = d.queue_size; document.getElementById('s-db').innerText = d.db_count;
        let logsDiv = document.getElementById('logs');
        let isScrolledToBottom = logsDiv.scrollHeight - logsDiv.clientHeight <= logsDiv.scrollTop + 1;
        logsDiv.innerHTML = d.logs.join('<br>');
        if (isScrolledToBottom) logsDiv.scrollTop = logsDiv.scrollHeight;
    }, 1000);
    (async () => {
        let r = await fetch('/api/settings'); let d = await r.json();
        document.getElementById('set_token').value = d.my_token; document.getElementById('set_user').value = d.my_user;
    })();
</script></body></html>
"""

@app.route('/')
def home(): return render_template_string(HTML_TEMPLATE)

@app.route('/health')
def health(): return jsonify({"status": "ok", "message": "Render instance awake and tracking Neon DB."}), 200

@app.route('/api/status')
def api_status(): 
    current_status["queue_size"] = get_queue_size()
    return jsonify(current_status | {"logs": log_messages})

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    if request.method == 'GET': return jsonify(get_settings())
    data = request.json; conf = get_settings(); conf.update(data)
    json.dump(conf, open(SETTINGS_FILE, 'w'), indent=4)
    emit_log("Config updated across local instance.", "SYS", "#10b981")
    return jsonify({"status": "ok"})

@app.route('/api/scrape', methods=['POST'])
def api_scrape():
    data = request.json
    threading.Thread(target=run_async, args=(async_db_pipeline(data['mode'], data['query']),), daemon=True).start()
    return jsonify({"status": "started"})

@app.route('/api/fast_sync', methods=['POST'])
def api_fast_sync():
    threading.Thread(target=run_async, args=(async_fast_sync(),), daemon=True).start()
    return jsonify({"status": "started"})

@app.route('/api/repost', methods=['POST'])
def api_repost():
    data = request.json
    mode, input_val, size_limit = data['mode'], data['input'], data['size_limit']

    def handle_queueing():
        ids = []
        if mode == "manual":
            for line in input_val.strip().split('\n'):
                line = line.strip()
                if not line: continue
                match = re.search(r'/(?:video/)?([^/?]+)', line)
                ids.append(match.group(1) if match else line)
        else:
            emit_log(f"🔍 Extracting IDs for {mode}: '{input_val}'", "REPOST", "#f59e0b")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            ids = loop.run_until_complete(async_db_pipeline(mode, input_val, scrape_only=True))
            loop.close()

        added = sum(1 for vid in ids if add_to_neon_queue(vid, size_limit))
        limit_text = "Unlimited" if size_limit == 9999 else f"{size_limit}MB"
        emit_log(f"⚡ Added {added} NEW unique jobs (Limit: {limit_text})", "REPOST", "#f59e0b")

    threading.Thread(target=handle_queueing, daemon=True).start()
    return jsonify({"status": "queued"})

@app.route('/videos/<path:f>')
def serv_v(f): return send_from_directory(VIDEO_DIR, f)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5050))
    print(f"🚀 STARTING RENDER HIVE on Port {port}...")
    emit_log("Instance awake. Subscribing to Neon DB Queue.", "SYS", "#10b981")
    app.run(host='0.0.0.0', port=port, threaded=True)
