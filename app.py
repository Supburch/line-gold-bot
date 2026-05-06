import os, re, time, pytz, logging, requests, threading, signal, sys
from threading import Lock, Event
from collections import OrderedDict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, PushMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from apscheduler.schedulers.background import BackgroundScheduler
from supabase import create_client
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# ---------- 1. Logging & Flask Setup ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
app = Flask(__name__)

# ---------- 2. Config & Supabase ----------
LINE_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_SECRET = os.getenv("LINE_CHANNEL_SECRET")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

configuration = Configuration(access_token=LINE_TOKEN or "")
handler = WebhookHandler(LINE_SECRET or "")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL else None

# ---------- 3. Thread-local LINE Client ----------
_thread_local = threading.local()

def get_line_bot():
    if not hasattr(_thread_local, "bot"):
        # สร้าง ApiClient แยกต่อ Thread เพื่อความปลอดภัย (Thread-safety)
        _thread_local.bot = MessagingApi(ApiClient(configuration))
    return _thread_local.bot

# ---------- 4. Robust HTTP Client (Connection Pool Management) ----------
class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        # ตั้งค่า Retry และ Pool Size เพื่อรองรับการเรียกขนาน
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        self.cache = OrderedDict()
        self.lock = Lock()

    def fetch(self, url, ttl=60):
        now = time.monotonic()
        with self.lock:
            if url in self.cache:
                data, exp = self.cache[url]
                if now < exp:
                    return data
        
        try:
            # ใช้ Timeout ที่รัดกุม (Connect 3s, Read 7s)
            res = self.session.get(url, timeout=(3.1, 7.1))
            res.raise_for_status()
            data = res.json()
            with self.lock:
                self.cache[url] = (data, now + ttl)
                if len(self.cache) > 50: self.cache.popitem(last=False)
            return data
        except Exception as e:
            logger.error(f"Fetch error: {e}")
            return None

    def close(self):
        logger.info("Closing HTTP session...")
        self.session.close()

http = HttpClient()

# ---------- 5. Global Executors (Resource Controlled) ----------
# กำหนด thread name prefix เพื่อให้ง่ายต่อการ debug ใน log
webhook_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="WebhookExp")
fetch_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="FetchPool")

# ---------- 6. Safe Messaging ----------
def reply_safe(token, text):
    if not token: return
    try:
        get_line_bot().reply_message(
            ReplyMessageRequest(
                reply_token=token,
                messages=[TextMessage(text=(text[:2000] if text else "⚠️ Error").strip())]
            )
        )
    except Exception as e:
        logger.error(f"Reply failed: {e}")

def push_safe(uid, text):
    if not uid: return
    try:
        get_line_bot().push_message(
            PushMessageRequest(
                to=uid,
                messages=[TextMessage(text=(text[:2000] if text else "⚠️ Alert Error").strip())]
            ),
            timeout=5 # LINE server-side timeout
        )
    except Exception as e:
        logger.error(f"Push failed: {e}")

# ---------- 7. Parallel Gold Logic (Fixed Blocked Threads) ----------
def get_gold():
    # ยิงคู่ขนาน
    f1 = fetch_pool.submit(http.fetch, "https://api.frankfurter.app/latest?from=XAU&to=USD")
    f2 = fetch_pool.submit(http.fetch, "https://api.frankfurter.app/latest?from=USD&to=THB")

    try:
        # ❗ แก้ไขข้อ 4: เพิ่ม Timeout ให้ .result() ป้องกัน Thread ค้างตลอดกาล
        gold = f1.result(timeout=8)
        fx = f2.result(timeout=8)
    except (TimeoutError, Exception) as e:
        logger.error(f"Parallel fetch timeout/error: {e}")
        return None

    try:
        spot = float(gold["rates"]["USD"])
        rate = float(fx["rates"]["THB"])
        # สูตรคำนวณราคาทองไทย (โดยประมาณ)
        baht = (spot * rate / 31.1035) * 15.244 * 0.965
        return spot, rate, baht
    except Exception:
        return None

# ---------- 8. Alert Processing (Fixed Race Condition) ----------
RE_ALERT = re.compile(r"(เตือน|alert|สูงกว่า)\s*(\d+)")

def process_message(text, user_id):
    if not text: return "❌ ข้อความว่างเปล่า"
    
    m = RE_ALERT.search(text.lower())
    if m:
        try:
            val = float(m.group(2))
            if not supabase: return "⚠️ Database offline"
            
            # ❗ แก้ไขข้อ 3: ใช้ upsert + unique constraint (ต้องมี Index ใน DB จริงๆ)
            supabase.table("alerts").upsert({
                "user_id": user_id,
                "target_price": val,
                "direction": "above"
            }, on_conflict="user_id,target_price,direction").execute()
            return f"✅ บันทึกเตือนเมื่อราคา >= {val:,.2f} เรียบร้อย"
        except Exception as e:
            logger.error(f"DB Upsert Error: {e}")
            return "❌ บันทึกล้มเหลว"

    if any(x in text.lower() for x in ["ทอง", "gold", "ราคา"]):
        data = get_gold()
        if not data: return "❌ ไม่สามารถดึงข้อมูลได้ในขณะนี้"
        return f"🥇 ราคาทองคำ\n💵 Spot: ${data[0]:,.2f}\n🔸 ทองไทย: {data[2]:,.0f} บาท"

    return "💡 พิมพ์ 'ทอง' เพื่อดูราคา หรือ 'เตือน 2700' เพื่อตั้งปลุก"

# ---------- 9. Webhook Routes ----------
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    uid = getattr(event.source, "user_id", None)
    # ส่งเข้า ThreadPool เพื่อไม่ให้ Webhook ค้าง
    webhook_executor.submit(async_task, event.reply_token, event.message.text, uid)

def async_task(token, text, uid):
    res = process_message(text, uid)
    reply_safe(token, res)

# ---------- 10. Alert Engine ----------
def check_alerts():
    if not supabase: return
    data = get_gold()
    if not data: return

    price = data[0]
    try:
        # ดึงเฉพาะรายการที่ถึงเป้าหมาย (ลดโหลด DB)
        res = supabase.table("alerts").select("*").lte("target_price", price).execute()
        triggered = res.data or []
        
        for a in triggered:
            push_safe(a["user_id"], f"🔔 ราคาถึงเป้าหมายแล้ว!\n💵 Spot: ${price:,.2f}")
            # ลบหลังเตือน
            supabase.table("alerts").delete().eq("id", a["id"]).execute()
    except Exception as e:
        logger.error(f"Alert engine error: {e}")

# ---------- 11. Graceful Shutdown (แก้ไขข้อ 1, 2, 5) ----------
scheduler = BackgroundScheduler(timezone="Asia/Bangkok")

def graceful_shutdown(sig, frame):
    logger.info(f"Received signal {sig}. Performing graceful shutdown...")
    
    # 1. หยุดรับงานใหม่จาก Scheduler
    if scheduler.running:
        scheduler.shutdown(wait=True)
    
    # 2. ปิด Executors (Wait=True เพื่อให้งานที่ค้างอยู่ทำเสร็จก่อน)
    logger.info("Shutting down executors...")
    webhook_executor.shutdown(wait=True)
    fetch_pool.shutdown(wait=True)
    
    # 3. ปิด Session การเชื่อมต่อ
    http.close()
    
    logger.info("Exit success.")
    sys.exit(0)

# ลงทะเบียนสัญญาณสำหรับ Docker/Render (SIGTERM) และ Ctrl+C (SIGINT)
signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

# ---------- 12. Main Execution ----------
if __name__ == "__main__":
    # Scheduler hardening
    scheduler.add_job(check_alerts, "interval", minutes=5, max_instances=1, coalesce=True)
    scheduler.start()

    logger.info("Starting Gold Alert Bot Application...")
    try:
        # ใช้ threaded=True เพื่อให้ Flask รองรับการจัดการหลาย request พร้อมกัน
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), threaded=True)
    except Exception as e:
        logger.error(f"Flask startup failed: {e}")
