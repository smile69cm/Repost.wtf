import os, json, time, subprocess, requests, threading, random, re, urllib.parse
import asyncio, aiohttp, hashlib
from flask import Flask, render_template_string, request, jsonify, send_from_directory
import psycopg2

app = Flask(__name__)

# ---------------------------
#  DIRECTORIES & CONFIG
# ---------------------------
BASE_DIR = os.getcwd()
VIDEO_DIR = os.path.join(BASE_DIR, "watermarked_videos")
PREVIEW_DIR = os.path.join(BASE_DIR, "previews")
SETTINGS_FILE = "settings.json"

for d in [VIDEO_DIR, PREVIEW_DIR]:
    if not os.path.exists(d): os.makedirs(d)

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
                    error TEXT, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            try: cur.execute("ALTER TABLE repost_queue ADD COLUMN IF NOT EXISTS size_limit INT DEFAULT 10;")
            except: conn.rollback()
            
            cur.execute("CREATE TABLE IF NOT EXISTS image_hashes (vid TEXT PRIMARY KEY, hash TEXT);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON repost_queue(status);")
            conn.commit()
        conn.close()
    except Exception as e: print(f"⚠️ Neon DB Init Error: {e}")

init_neon_db()

# ---------------------------
#  SETTINGS MANAGER 
# ---------------------------
DEFAULT_SETTINGS = {
    "my_token": "",
    "my_user": "telugustuffs",
    "main_domain": "love.viraly.wtf", "upload_domain": "loveupload.viraly.wtf",
    "blacklist": "promo, link in bio, part 2, pt 2, subscribe",
    "del_payload": "", 
    "full_cookie": ""
}

if not os.path.exists(SETTINGS_FILE): json.dump(DEFAULT_SETTINGS, open(SETTINGS_FILE, 'w'), indent=4)

def get_settings():
    with open(SETTINGS_FILE, 'r') as f: return json.load(f)

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

def emit_log(msg, category="SYS", color="#10b981"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [NODE] [{category}] {msg}")
    log_messages.append(f"<span style='color:#64748b'>[{t}]</span> <span style='color:{color}'>[{category}]</span> {msg}")
    if len(log_messages) > 150: log_messages.pop(0)

# ---------------------------
#  DATABASE CO-OP HELPERS
# ---------------------------
def filter_existing_ids(vid_list):
    if not vid_list: return []
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        with conn.cursor() as cur:
            format_strings = ','.join(['%s'] * len(vid_list))
            cur.execute(f"SELECT video_id FROM repost_queue WHERE video_id IN ({format_strings})", tuple(vid_list))
            existing = {row[0] for row in cur.fetchall()}
        conn.close()
        return [v for v in vid_list if v not in existing]
    except: return vid_list

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
#  NATIVE DUPLICATE CLEANER
# ---------------------------
def native_cleaner_task():
    conf = get_settings()
    payload = conf.get("del_payload", "")
    username = conf.get("my_user")
    domain = conf.get("main_domain")
    
    if not payload:
        emit_log("❌ Cleaner Aborted: Missing Encrypted Delete Payload!", "CLEANER", "#ef4444")
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
            if not vids: empty_pages += 1
            else:
                empty_pages = 0
                all_videos.extend(vids)
            page += 1
            time.sleep(0.5)
        except: break
        
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
                else: continue
            except: continue
            
        if img_hash in seen_hashes_this_run:
            emit_log(f"🚨 DUPLICATE SPOTTED: {vid[:8]}... Firing Vaporize Payload!", "CLEANER", "#f43f5e")
            try:
                del_res = session.post(f"https://{domain}/uservideo/delete/{vid}", json={"username": payload}, headers=headers, timeout=10)
                if del_res.status_code == 200: deleted_count += 1
            except: pass
            time.sleep(1.2) 
        else:
            seen_hashes_this_run[img_hash] = vid

    conn.close()
    emit_log(f"✨ CLEANUP COMPLETE! Destroyed {deleted_count} duplicates.", "CLEANER", "#10b981")
    current_status["scraper"] = "Idle"

# ---------------------------
#  SCRAPER MODULE
# ---------------------------
def run_async(coroutine):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coroutine)
    loop.close()

async def async_db_pipeline(mode, query, size_limit=9999):
    current_status["scraper"] = f"Scraping {query}"
    emit_log(f"🚀 SCRAPE INITIATED | Target: '{query}'", "SCRAPE", "#3b82f6")
    conf = get_settings()
    s_headers = get_headers()
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
                page += 1
            except: break
            
    unique_vids = list(all_vids)
    
    new_vids = filter_existing_ids(unique_vids)
    skipped = len(unique_vids) - len(new_vids)
    
    added = 0
    for vid in new_vids:
        if add_to_neon_queue(vid, size_limit): added += 1
        
    emit_log(f"✨ Scrape Done! Found {len(unique_vids)}. Skipped {skipped} already processed. Added {added} NEW IDs to Queue.", "SCRAPE", "#3b82f6")
    current_status["scraper"] = "Idle"

# ---------------------------
#  CORE WORKER ENGINE
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
        
        raw_file, watermarked_file, preview_file = None, None, None
        conf = get_settings()
        h_media = get_headers()

        try:
            title = f"Viral Video {video_id[:6]}"
            desc = "#trending #viral #reels"
            category_tag = "18+" 
            
            try:
                r_api = requests.get(f"https://{conf['main_domain']}/video/{urllib.parse.quote(video_id, safe='')}", headers=h_media, timeout=10).json()
                vid_data = r_api[0] if isinstance(r_api, list) and len(r_api) > 0 else (r_api if isinstance(r_api, dict) else {})
                
                if vid_data.get("title"): title = vid_data["title"]
                if vid_data.get("description"): desc = vid_data["description"]
                if vid_data.get("tag"): category_tag = vid_data["tag"]
            except: pass

            bl_words = [w.strip().lower() for w in conf.get("blacklist", "").split(",") if w.strip()]
            if any(w in f"{title} {desc} {category_tag}".lower() for w in bl_words):
                emit_log(f"🛑 BLACKLISTED: Trashing video.", "REPOST", "#ef4444")
                update_job_status(job["id"], 'failed', "Blacklisted Keyword")
                continue

            d_url = f"https://{conf['main_domain']}/media/videos/{video_id}.mp4"
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
                if s_res.status_code != 200: raise Exception(f"404 Not Found")
                with open(raw_file, 'wb') as f:
                    for chunk in s_res.iter_content(8192): f.write(chunk)

            file_to_upload = raw_file
            
            if size_limit == 9999 and size_mb > 40:
                emit_log(f"⚡ UNLIMITED PASS ➔ Skipping watermark", "REPOST", "#d946ef")
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
                up = requests.post(f"https://{conf['upload_domain']}/upload",
                    files={'files': (f"video_{safe_label}.mp4", f, 'video/mp4')},
                    data={"tag": category_tag, "title": title, "description": desc, "country": "IN", "username": conf['my_user'], "start": "0", "end": "0"},
                    headers={"Cookie": h_media["Cookie"], "Origin": f"https://{base}"})

            response_text = up.text
            
            # 🛡️ THE BYPASS FIX: Server throws 400 but actually succeeds saving the file!
            if up.status_code == 200 or (up.status_code == 400 and "allowedMimeTypes is not defined" in response_text):
                emit_log(f"✅ SUCCESS ➔ {video_id[:8]} (Bypassed Bug)", "REPOST", "#10b981")
                update_job_status(job["id"], 'completed')
            else: 
                raise Exception(f"HTTP {up.status_code} | {response_text[:100]}")

        except Exception as e:
            emit_log(f"🔥 Error: {e}", "REPOST", "#ef4444")
            update_job_status(job["id"], 'failed', str(e))
            
        finally:
            for f_path in [raw_file, watermarked_file, preview_file]:
                if f_path and os.path.exists(f_path): os.remove(f_path)

threading.Thread(target=reposter_worker, daemon=True).start()

# ---------------------------
#  FLASK WEB UI
# ---------------------------
HTML_TEMPLATE = """
<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>V8.3 Autonomous Swarm</title>
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
    #logs { height: 400px; overflow-y: auto; background: #020617; padding: 15px; font-family: 'Consolas', monospace; font-size: 13px; border-radius: 6px; margin-top: 10px; border: 1px solid #334155; line-height: 1.6;}
    .status-bar { display: flex; justify-content: space-between; background: #020617; padding: 12px; border-radius: 6px; font-size: 14px; margin-bottom: 15px; border-left: 4px solid var(--grn); align-items:center; flex-wrap: wrap; gap:10px;}
    .sm-label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; display:block; margin-top: 10px; }
    @media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }
</style></head>
<body>
    <h2>🐝 V8.3 AUTONOMOUS SWARM NODE</h2>
    <div class="status-bar">
        <div>
            <b>[SCRAPER]</b> <span id="s-scrape" style="color:#3b82f6; margin-right:15px;">Idle</span>
            <b>[WORKER]</b> <span id="s-repost" style="color:#f59e0b;">Idle</span> | 
            <b>[GLOBAL QUEUE]</b> <span id="s-q" style="color:#10b981;">0</span>
        </div>
    </div>
    
    <div class="grid-2">
        <div class="card" style="border-top-color: #3b82f6;">
            <h3>🔍 Smart DB Scraper</h3>
            <p style="font-size:12px; color:#94a3b8; margin-top:0;">Skips IDs already in DB and pushes fresh ones to Queue!</p>
            <div style="display:flex; gap:10px;">
                <select id="db_mode" style="width:40%;"><option value="keyword">Keyword</option><option value="username">Username</option></select>
                <input id="db_target" placeholder="Target keyword..." style="width:60%;">
            </div>
            <button onclick="startScraper()">🚀 SCRAPE & ADD TO GLOBAL QUEUE</button>
        </div>
        
        <div class="card" style="border-top-color: #f59e0b;">
            <h3>🎥 Add Jobs & Cleaner</h3>
            <div style="display:flex; gap:10px;">
                <input id="rep_input" placeholder="Paste manual IDs here..." style="width:70%;">
                <select id="size_limit" style="width:30%;"><option value="10">10 MB</option><option value="40">40 MB</option><option value="9999">Unlmt.</option></select>
            </div>
            <button onclick="startReposter()" class="btn-repost">⚙️ ADD TO GLOBAL QUEUE</button>
            <hr style="border-color:#334155; margin:15px 0;">
            <button onclick="runCleaner()" class="btn-red">🧹 RUN GLOBAL DB CLEANER</button>
        </div>
    </div>

    <div class="card" style="margin-top: 15px; border-top-color:#10b981;">
        <div style="display:flex; justify-content:space-between; align-items:flex-start;">
            <h3>🖥️ Swarm Node Logs & Settings</h3>
            <button onclick="saveConfig()" style="width:auto; padding:5px 15px; font-size:12px; background:#475569;">💾 SAVE SETTINGS</button>
        </div>
        <div class="grid-2" style="margin-bottom:10px;">
            <div><span class="sm-label">Account Token</span><input id="set_token" placeholder="Access Token"></div>
            <div><span class="sm-label">Username</span><input id="set_user" placeholder="Target Username"></div>
            <div><span class="sm-label">Encrypted Delete Payload</span><input id="set_del" placeholder="U2FsdGVkX19Y2vJS8yrBP8..."></div>
            <div><span class="sm-label">Smart Blacklist</span><input id="set_bl" placeholder="promo, link in bio..."></div>
        </div>
        <div>
            <span class="sm-label">Full Browser Cookie Header (Anti-Bot)</span>
            <input id="set_cookie" placeholder="_ga=GA1.1...; accessToken=...">
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
    async function startReposter() {
        let input = document.getElementById('rep_input').value, limit = document.getElementById('size_limit').value;
        if(!input) return alert("Enter IDs!");
        await fetch('/api/repost', {method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({input:input, size_limit: parseInt(limit)})});
        document.getElementById('rep_input').value = '';
    }
    async function runCleaner() {
        if(confirm("Are you sure? This will map your profile and delete duplicates!")) {
            await fetch('/api/cleaner', {method: 'POST'});
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
        await fetch('/api/settings', {method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
        alert("Node Configuration Saved!");
    }
    setInterval(async () => {
        try {
            let r = await fetch('/api/status'); let d = await r.json();
            document.getElementById('s-scrape').innerText = d.scraper; 
            document.getElementById('s-repost').innerText = d.reposter;
            document.getElementById('s-q').innerText = d.queue_size;

            let logsDiv = document.getElementById('logs');
            let isScrolledToBottom = logsDiv.scrollHeight - logsDiv.clientHeight <= logsDiv.scrollTop + 1;
            logsDiv.innerHTML = d.logs.join('<br>');
            if (isScrolledToBottom) logsDiv.scrollTop = logsDiv.scrollHeight;
        } catch(e) {}
    }, 1500);
    
    (async () => {
        let r = await fetch('/api/settings'); let d = await r.json();
        document.getElementById('set_token').value = d.my_token || ""; 
        document.getElementById('set_user').value = d.my_user || "";
        document.getElementById('set_bl').value = d.blacklist || "";
        document.getElementById('set_del').value = d.del_payload || "";
        document.getElementById('set_cookie').value = d.full_cookie || "";
    })();
</script></body></html>
"""

@app.route('/')
def home(): return render_template_string(HTML_TEMPLATE)

@app.route('/health')
def health(): return jsonify({"status": "awake"}), 200

@app.route('/api/status')
def api_status(): 
    current_status["queue_size"] = get_queue_size()
    return jsonify(current_status | {"logs": log_messages})

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    if request.method == 'GET': return jsonify(get_settings())
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
    emit_log("Settings updated for this node.", "SYS", "#10b981")
    return jsonify({"status": "ok"})

@app.route('/api/scrape', methods=['POST'])
def api_scrape():
    data = request.json
    threading.Thread(target=run_async, args=(async_db_pipeline(data['mode'], data['query']),), daemon=True).start()
    return jsonify({"status": "started"})

@app.route('/api/repost', methods=['POST'])
def api_repost():
    data = request.json
    def handle_queueing():
        ids = []
        for line in data['input'].strip().split('\n'):
            line = line.strip()
            if not line: continue
            match = re.search(r'/(?:video/)?([^/?]+)', line)
            ids.append(match.group(1) if match else line)
            
        new_ids = filter_existing_ids(ids)
        skipped = len(ids) - len(new_ids)
        added = sum(1 for vid in new_ids if add_to_neon_queue(vid, data['size_limit']))
        emit_log(f"⚡ Manual Input: Skipped {skipped} duplicates. Added {added} NEW jobs to Global Queue", "NODE", "#f59e0b")
    threading.Thread(target=handle_queueing, daemon=True).start()
    return jsonify({"status": "queued"})

@app.route('/api/cleaner', methods=['POST'])
def api_cleaner():
    threading.Thread(target=native_cleaner_task, daemon=True).start()
    return jsonify({"status": "started"})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5050))
    print(f"🚀 STARTING V8.3 AUTONOMOUS SWARM NODE on Port {port}...")
    app.run(host='0.0.0.0', port=port, threaded=True)
