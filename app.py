import os, json, time, requests, threading, re, urllib.parse, hashlib, traceback, fcntl, sys, uuid, socket, random
import asyncio, aiohttp
from flask import Flask, render_template_string, request, jsonify, session, redirect, url_for
from functools import wraps
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from upstash_redis import Redis

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET', 'changeme_production_secret_!@#$')

# ---------------------------
#  INDIAN TIMEZONE (IST)
# ---------------------------
IST = timezone(timedelta(hours=5, minutes=30))
def ist_now(): return datetime.now(IST)
def ist_time_str(): return ist_now().strftime("%I:%M:%S %p")

# ---------------------------
#  SERVER ID & ENV VARS
# ---------------------------
SERVER_ID = os.environ.get('SERVER_ID', socket.gethostname() + "_" + str(uuid.uuid4())[:8])
print(f"🖥️ Server ID: {SERVER_ID}")

ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin123')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHANNEL_ID = os.environ.get('TELEGRAM_CHANNEL_ID', '-1003810911847')
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN)

# ---------------------------
#  HARDCODED UPSTASH REDIS REST CREDENTIALS
# ---------------------------
UPSTASH_REDIS_REST_URL = "https://top-grackle-133613.upstash.io"
UPSTASH_REDIS_REST_TOKEN = "gQAAAAAAAgntAAIgcDI4NWRmYmNkYThmMzA0ZDI5YjY0OWM2N2IyYWRiN2IxOA"

# ---------------------------
#  DIRECTORIES & FILES
# ---------------------------
BASE_DIR = os.getcwd()
VIDEO_DIR = os.path.join(BASE_DIR, "downloads")
STATE_FILE = "state.json"
os.makedirs(VIDEO_DIR, exist_ok=True)

# ---------------------------
#  UPSTASH REDIS CONNECTION
# ---------------------------
try:
    redis_client = Redis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)
    redis_client.ping()
    print("✅ Connected to Central Redis Queue (Upstash REST)")
except Exception as e:
    print(f"❌ Redis connection failed: {e}")
    redis_client = None

# ---------------------------
#  LOCAL STATE (For UI Logs)
# ---------------------------
def read_state():
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except:
        return {"logs": [], "scraper": "Idle", "worker": "Idle", "current_operation": None}

def write_state(data):
    with open(STATE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

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

def update_status(scraper=None, worker=None):
    state = read_state()
    if scraper is not None: state["scraper"] = scraper
    if worker is not None: state["worker"] = worker
    write_state(state)

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
#  CENTRALIZED QUEUE HELPERS (REDIS)
# ---------------------------
def add_to_queue(video_id, source_type, source_value):
    if not redis_client: return False
    
    # Use a Redis Set to ensure the exact same video ID is never queued twice
    is_new = redis_client.sadd("global_queued_ids", video_id)
    
    # Upstash REST returns 1 if added, 0 if already exists
    if is_new == 1 or is_new is True:
        job = {
            "id": str(uuid.uuid4()),
            "video_id": video_id,
            "source_type": source_type,
            "source_value": source_value
        }
        # Push to the main queue
        redis_client.rpush("global_job_queue", json.dumps(job))
        return True
    return False

def get_next_job():
    if not TELEGRAM_ENABLED or not redis_client: return None
    # Atomically pop a job from the list.
    job_str = redis_client.lpop("global_job_queue")
    if job_str:
        job = json.loads(job_str) if isinstance(job_str, str) else job_str
        emit_log(f"🎯 Got job: {job['video_id'][:8]}", "WORKER", "#f59e0b")
        return job
    return None

def get_queue_size():
    if not redis_client: return 0
    return redis_client.llen("global_job_queue") or 0

def is_already_sent(thumbnail_hash):
    if not thumbnail_hash or not redis_client: return False
    result = redis_client.sismember("global_sent_hashes", thumbnail_hash)
    return result == 1 or result is True

def mark_sent(thumbnail_hash):
    if redis_client and thumbnail_hash:
        redis_client.sadd("global_sent_hashes", thumbnail_hash)

# ---------------------------
#  TELEGRAM SENDER
# ---------------------------
def send_video_to_telegram(video_path, caption):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendVideo"
    if os.path.getsize(video_path) > 50 * 1024 * 1024:
        emit_log(f"📤 Telegram skip: video >50MB", "TELEGRAM", "#f59e0b")
        return False
    for attempt in range(1, 4):
        try:
            with open(video_path, 'rb') as f:
                files = {'video': f}
                data = {'chat_id': TELEGRAM_CHANNEL_ID, 'caption': caption[:1024], 'supports_streaming': True}
                resp = requests.post(url, data=data, files=files, timeout=60)
            if resp.status_code == 200:
                return True
            elif resp.status_code == 429:
                wait = 2 ** attempt + random.uniform(0, 2)
                emit_log(f"⚠️ Rate limit, retry {attempt}/3 after {wait:.1f}s", "TELEGRAM", "#f59e0b")
                time.sleep(wait)
            else:
                emit_log(f"⚠️ Telegram error {resp.status_code}", "TELEGRAM", "#ef4444")
                return False
        except Exception as e:
            time.sleep(2 ** attempt)
    return False

# ---------------------------
#  SCRAPER (async)
# ---------------------------
async_loop = None
def start_async_loop():
    global async_loop
    async_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(async_loop)
    async_loop.run_forever()

def run_coroutine(coro):
    if async_loop is None or not async_loop.is_running():
        return asyncio.run(coro)
    return asyncio.run_coroutine_threadsafe(coro, async_loop).result()

threading.Thread(target=start_async_loop, daemon=True).start()
time.sleep(0.1)

async def async_scrape_ids(mode, query):
    conf = {"main_domain": "love.viraly.wtf"}
    s_headers = {"User-Agent": "Mozilla/5.0"}
    all_vids = set()
    page = 0
    empty_count = 0
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if mode == "username":
                    url = f"https://{conf['main_domain']}/profile/{query}/videos/latest"
                    async with session.post(url, headers=s_headers, json={"page": page}, timeout=10) as r: text = await r.text()
                else:
                    url = f"https://{conf['main_domain']}/searchVideo?q={query}&p={page}"
                    async with session.get(url, headers=s_headers, timeout=10) as r: text = await r.text()
                
                vids = re.findall(r'"videoId":"([^"]+)"', text)
                if not vids:
                    empty_count += 1
                    if empty_count >= 10: break
                else:
                    empty_count = 0
                    all_vids.update(vids)
                page += 1
            except Exception:
                break
    return list(all_vids)

def extract_video_id_from_input(input_str):
    input_str = input_str.strip()
    if input_str.startswith(('http://', 'https://')):
        parsed = urllib.parse.urlparse(input_str)
        return parsed.path.rstrip('/').split('/')[-1] or None
    if re.match(r'^[A-Za-z0-9_\-=+/]+$', input_str): return input_str
    return None

# ---------------------------
#  WORKER ENGINE
# ---------------------------
def worker_loop():
    if not TELEGRAM_ENABLED or not redis_client:
        emit_log("⚠️ Missing Telegram Token or Redis connection – worker idle.", "WORKER", "#f59e0b")
        while True: time.sleep(60)
        
    emit_log(f"👷 Worker online (Server: {SERVER_ID})", "WORKER", "#f59e0b")
    while True:
        try:
            update_status(worker="Idle")
            job = get_next_job()
            if not job:
                time.sleep(5)
                continue
            update_status(worker=f"Processing: {job['video_id'][:8]}")
            process_job(job)
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            emit_log(f"Worker error: {e}", "WORKER", "#ef4444", True)
            time.sleep(10)

def process_job(job):
    video_id = job["video_id"]
    raw_file = None
    try:
        domain = "love.viraly.wtf"
        encoded_id = quote(video_id, safe='')
        
        # 1. Check Duplicate via Hash
        thumb_url = f"https://{domain}/media/images/{encoded_id}.jpg"
        thumb_resp = requests.get(thumb_url, timeout=8)
        if thumb_resp.status_code == 200:
            thumb_hash = hashlib.md5(thumb_resp.content).hexdigest()
            if is_already_sent(thumb_hash):
                emit_log(f"⏭️ Duplicate: {video_id[:8]} already sent globally", "WORKER", "#f43f5e")
                return
        else:
            thumb_hash = None

        # 2. Fetch Meta
        title = f"Video {video_id[:6]}"
        desc = ""
        try:
            r_api = requests.get(f"https://{domain}/video/{encoded_id}", timeout=10).json()
            vid_data = r_api[0] if isinstance(r_api, list) and len(r_api) > 0 else (r_api if isinstance(r_api, dict) else {})
            title = vid_data.get("title", title)
            desc = vid_data.get("description", desc)
        except: pass

        # 3. Download
        d_url = f"https://{domain}/media/videos/{encoded_id}.mp4"
        safe_label = re.sub(r'[^a-zA-Z0-9]', '_', video_id)[-12:]
        raw_file = os.path.join(VIDEO_DIR, f"{safe_label}.mp4")
        with requests.get(d_url, stream=True, timeout=30) as s_res:
            s_res.raise_for_status()
            with open(raw_file, 'wb') as f:
                for chunk in s_res.iter_content(8192): f.write(chunk)
        
        # 4. Telegram
        caption = f"{title}\n\n{desc}" if desc else title
        if send_video_to_telegram(raw_file, caption):
            emit_log(f"✅ Sent: {video_id[:8]}", "WORKER", "#10b981")
            mark_sent(thumb_hash)
            
    except Exception as e:
        emit_log(f"🔥 Error: {e}", "WORKER", "#ef4444")
    finally:
        if raw_file and os.path.exists(raw_file):
            try: os.remove(raw_file)
            except: pass

# ---------------------------
#  FLASK ROUTES
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
    return jsonify(state)

@app.route('/api/clear_logs', methods=['POST'])
@login_required
def clear_logs():
    state = read_state()
    state["logs"] = []
    write_state(state)
    return jsonify({"status": "ok"})

@app.route('/api/repost', methods=['POST'])
@login_required
def api_repost():
    data = request.json
    mode = data.get('mode', 'manual')
    target = data['input'].strip()
    if not target: return jsonify({"error": "Empty"}), 400
    
    def task():
        if mode == "manual":
            ids = [extract_video_id_from_input(l) for l in target.replace(',', '\n').split('\n') if l.strip()]
            ids = [vid for vid in ids if vid]
            added = sum(1 for vid in ids if add_to_queue(vid, "manual", "user_input"))
            emit_log(f"Manual: queued {added} new videos to Redis", "QUEUE", "#f59e0b")
        else:
            update_status(scraper=f"Scraping {mode} '{target}'...")
            emit_log(f"Scraping {mode} '{target}'...", "SCRAPE", "#3b82f6")
            scraped_ids = run_coroutine(async_scrape_ids(mode, target))
            if scraped_ids:
                added = sum(1 for vid in scraped_ids if add_to_queue(vid, mode, target))
                emit_log(f"Queued {added} new videos to Redis", "SCRAPE", "#3b82f6")
            else:
                emit_log(f"No IDs found", "SCRAPE", "#ef4444")
            update_status(scraper="Idle")
    
    threading.Thread(target=task, daemon=True).start()
    return jsonify({"status": "queued"})

@app.route('/api/force_process', methods=['POST'])
@login_required
def force_process():
    threading.Thread(target=lambda: process_job(get_next_job()) if get_queue_size() > 0 else None, daemon=True).start()
    return jsonify({"status": "forced"})

# ---------------------------
#  UI TEMPLATES
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
        select,input,textarea{flex:1;padding:12px;border-radius:16px;border:1px solid #334155;background:#0f172a;color:white}
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
            <div class="input-row">
                <select id="mode">
                    <option value="manual">Manual Links</option>
                    <option value="keyword">Keyword</option>
                    <option value="username">Username</option>
                </select>
            </div>
            <textarea id="target" rows="3" placeholder="Keyword, username, or video links (one per line)"></textarea>
            <button onclick="startQueue()" class="btn-orange">🚀 ADD TO QUEUE</button>
        </div>
        <hr>
        <div class="input-row">
            <button onclick="forceProcess()" style="background:#f59e0b; width: 100%;">⚡ Force Process One</button>
        </div>
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
    setInterval(async()=>{try{let r=await fetch('/api/status');let d=await r.json();document.getElementById('s-scrape').innerText=d.scraper;document.getElementById('s-worker').innerText=d.worker;document.getElementById('s-q').innerText=d.queue_size;let logsDiv=document.getElementById('logs');let isBottom=logsDiv.scrollHeight-logsDiv.clientHeight<=logsDiv.scrollTop+1;logsDiv.innerHTML=d.logs.map(l=>`<div><span style='color:#64748b'>[${l.time}]</span> <span style='color:${l.color}'>[${l.category}]</span> ${l.message}</div>`).join('');if(isBottom)logsDiv.scrollTop=logsDiv.scrollHeight;}catch(e){}},1500);
</script>
</body>
</html>
"""

threading.Thread(target=worker_loop, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5050))
    app.run(host='0.0.0.0', port=port, threaded=True)
