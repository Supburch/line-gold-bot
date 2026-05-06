import os, re, time, pytz, logging, requests
from threading import Lock, Event
from collections import OrderedDict
from urllib.parse import urlparse
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
from concurrent.futures import ThreadPoolExecutor

# ---------- 1. Logging Configuration ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---------- 2. Flask Setup ----------
app = Flask(__name__)

# ---------- 3. Environment Config Validation ----------
LINE_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_SECRET = os.getenv("LINE_CHANNEL_SECRET")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ตรวจสอบการกำหนดค่าสิ่งแวดล้อม (Environment Variables Validation)
if not LINE_TOKEN or not LINE_SECRET:
    logger.critical("❌ Missing critical LINE API credentials! Webhook handler may fail.")

configuration = Configuration(access_token=LINE_TOKEN or "")
handler = WebhookHandler(LINE_SECRET or "")

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("⚡ Supabase client initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Supabase initialization failed: {e}")
else:
    logger.warning("⚠️ Supabase credentials missing. Alert features are disabled.")

# ---------- 4. Thread-Safe HttpClient (Anti-Dogpiling & Cache Fallback) ----------
class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=50)
        self.session.mount("https://", adapter)

        self.cache = OrderedDict()
        self.inflight = {}
        self.lock = Lock()
        self.stats_lock = Lock()
        
        self.max_cache_size = 100
        self.default_timeout = (3.0, 7.0)  # (Connect, Read)
        self.stats = {"hit": 0, "miss": 0, "error": 0}

    def fetch(self, url, ttl=60):
        start_wait = time.monotonic()
        is_owner = False

        try:
            while True:
                now = time.monotonic()
                is_owner = False

                # Hard timeout เพื่อหลีกเลี่ยงการติด Loop ตลอดกาล
                if now - start_wait > 15:
                    logger.error(f"Hard timeout for URL: {url}")
                    return None

                with self.lock:
                    # 1. เช็ก LRU Cache (หากข้อมูลยังไม่หมดอายุ ดึงไปใช้งานทันที)
                    if url in self.cache:
                        data, expire = self.cache[url]
                        if now < expire:
                            self.cache.move_to_end(url)
                            with self.stats_lock:
                                self.stats["hit"] += 1
                            return data

                    # 2. ควบคุมการทำ In-flight Request (ป้องกันการยิง API ซ้ำซ้อนพร้อมกัน)
                    if url in self.inflight:
                        event = self.inflight[url]
                    else:
                        event = Event()
                        self.inflight[url] = event
                        event.clear()
                        is_owner = True

                if not is_owner:
                    # Thread อื่นรอให้ Thread เจ้าของประมวลผลดึงค่าเสร็จ
                    waited = event.wait(timeout=self.default_timeout[1] + 2)
                    if not waited:
                        logger.warning(f"In-flight timeout waiting for: {url}")
                    continue  # วนกลับไปอ่านค่าจาก Cache ที่ Thread หลักเขียนบันทึกไว้

                break

            with self.stats_lock:
                self.stats["miss"] += 1

            # ดึงข้อมูลจาก API จริง
            res = self.session.get(url, timeout=self.default_timeout)
            res.raise_for_status()
            data = res.json()

            with self.lock:
                self.cache[url] = (data, time.monotonic() + ttl)
                self.cache.move_to_end(url)
                if len(self.cache) > self.max_cache_size:
                    self.cache.popitem(last=False)
            return data

        except Exception as e:
            if is_owner:
                with self.stats_lock:
                    self.stats["error"] += 1
                logger.error(f"HTTP fetch failed on {url}: {e}")

            # Stale Fallback: ดึงข้อมูล Cache ที่หมดอายุไปใช้ชั่วคราวได้ไม่เกิน 5 นาทีหาก API หลักล่ม
            with self.lock:
                if url in self.cache:
                    data, expire = self.cache[url]
                    if time.monotonic() - expire < 300:
                        self.cache.move_to_end(url)
                        return data
            return None

        finally:
            if is_owner:
                with self.lock:
                    event = self.inflight.pop(url, None)
                    if event:
                        event.set()

    def cleanup(self):
        now = time.monotonic()
        with self.lock:
            expired = [k for k, (_, exp) in self.cache.items() if exp < now]
            for k in expired:
                del self.cache[k]
        logger.info(f"LRU Cache cleanup completed. Stats: {self.stats}")

http = HttpClient()

# ---------- 5. Thread-Pool Executor & Safe Messaging ----------
# ใช้ ThreadPool สำหรับประมวลผล Webhook ป้องกันอาการ Webhook Timeout (LINE จำกัด 1 วินาที)
executor = ThreadPoolExecutor(max_workers=10)

def reply_safe(token, text):
    if not token:
        return
    # ควบคุมความปลอดภัยของข้อความตอบกลับ ป้องกันข้อความว่างเปล่า และจำกัดความยาวสูงสุด
    safe_text = (text[:2000] if text else "⚠️ ระบบไม่สามารถประมวลผลข้อความได้").strip()
    try:
        with ApiClient(configuration) as api:
            MessagingApi(api).reply_message(
                ReplyMessageRequest(
                    reply_token=token,
                    messages=[TextMessage(text=safe_text)]
                )
            )
    except Exception as e:
        logger.error(f"Reply failed: {e}")

def push_safe(user_id, text):
    if not user_id:
        return
    safe_text = (text[:2000] if text else "⚠️ แจ้งเตือนข้อความขัดข้อง").strip()
    try:
        with ApiClient(configuration) as api:
            MessagingApi(api).push_message(
                PushMessageRequest(
                    to=user_id,
                    messages=[TextMessage(text=safe_text)]
                ),
                timeout=5  # กำหนด Timeout สูงสุดป้องกัน Thread ค้างชั่วคราว
            )
    except Exception as e:
        logger.error(f"Push failed: {e}")

# ---------- 6. Business Logic ----------
def get_gold():
    gold = http.fetch("https://api.frankfurter.app/latest?from=XAU&to=USD", ttl=60)
    fx = http.fetch("https://api.frankfurter.app/latest?from=USD&to=THB", ttl=300)

    if not gold or not fx:
        return None

    try:
        spot = float(gold["rates"]["USD"])
        rate = float(fx["rates"]["THB"])
        baht = (spot * rate / 31.1035) * 15.244 * 0.965
        return spot, rate, baht
    except Exception as e:
        logger.error(f"Error parsing gold price structures: {e}")
        return None

# ---------- 7. Database & Concurrency Safeguard ----------
PROCESSING = set()
PROCESS_LOCK = Lock()

def acquire(alert_id: int) -> bool:
    with PROCESS_LOCK:
        if alert_id in PROCESSING:
            return False
        PROCESSING.add(alert_id)
        return True

def release(alert_id: int):
    with PROCESS_LOCK:
        PROCESSING.discard(alert_id)

def alert_exists(user_id: str, target: float, direction: str) -> bool:
    if not supabase:
        return False
    try:
        res = supabase.table("alerts") \
            .select("id") \
            .eq("user_id", user_id) \
            .eq("target_price", target) \
            .eq("direction", direction) \
            .limit(1) \
            .execute()
        return bool(res.data)
    except Exception as e:
        logger.error(f"Error checking duplicate alerts: {e}")
        return False

def alert_add(user_id: str, target: float, direction: str) -> bool:
    if not supabase:
        return False
    try:
        if alert_exists(user_id, target, direction):
            return False  # มีข้อมูลที่ซ้ำกันอยู่แล้ว
        supabase.table("alerts").insert({
            "user_id": user_id,
            "target_price": target,
            "direction": direction
        }).execute()
        return True
    except Exception as e:
        logger.error(f"Failed to insert alert record: {e}")
        return False

# ---------- 8. Parser & Message Handler Logic ----------
RE_ALERT = re.compile(r"(เตือน|alert|สูงกว่า|ต่ำกว่า)\s*(\d+(\.\d+)?)")

def process_message(text, user_id):
    if not text or not text.strip():
        return "❌ ข้อความว่างเปล่า"

    raw_text = text.lower().strip()

    # 1. การจัดการคำสั่งแจ้งเตือนราคา (Alert Handler)
    m = RE_ALERT.search(raw_text)
    if m:
        try:
            value = float(m.group(2))
            if value <= 0 or value > 100000:
                return "❌ ระบุราคาเป้าหมายไม่ถูกต้อง (ค่าต้องอยู่ระหว่าง 1 ถึง 100,000)"
        except (ValueError, TypeError):
            return "❌ รูปแบบตัวเลขไม่ถูกต้อง"

        direction = "below" if any(x in raw_text for x in ["ต่ำ", "below", "ลง"]) else "above"

        if not supabase:
            return "⚠️ ระบบแจ้งเตือนปิดใช้งานชั่วคราว (Database Server Offline)"

        # ตรวจสอบการตั้งข้อมูลซ้ำก่อนดึงข้อมูลเข้าสู่ฐานข้อมูล
        if alert_exists(user_id, value, direction):
            return f"📢 คุณได้เคยตั้งแจ้งเตือนราคานี้ไว้แล้ว: ${value:,.2f}"

        if alert_add(user_id, value, direction):
            dir_label = "ต่ำกว่าหรือเท่ากับ" if direction == "below" else "สูงกว่าหรือเท่ากับ"
            return f"✅ บันทึกสำเร็จ: ระบบจะแจ้งเตือนเมื่อราคา {dir_label} ${value:,.2f}"
        return "❌ ตั้งเตือนไม่สำเร็จ กรุณาลองใหม่อีกครั้ง"

    # 2. การดึงราคาทองคำปัจจุบัน (Price Inquiry Handler)
    if any(k in raw_text for k in ["ทอง", "gold", "ราคา"]):
        data = get_gold()
        if not data:
            return "❌ ไม่สามารถตรวจสอบราคาทองคำได้ในขณะนี้ กรุณาลองใหม่อีกครั้ง"

        spot, rate, baht = data
        now = datetime.now(pytz.timezone("Asia/Bangkok")).strftime("%H:%M")
        return (
            f"🥇 ราคาทองคำ XAUUSD\n"
            f"💵 Spot: ${spot:,.2f}\n"
            f"💱 ค่าเงิน: {rate:.2f} THB\n"
            f"🔸 ทองแท่งไทย: ฿{baht:,.0f}\n"
            f"⏰ อัปเดตล่าสุด: {now}"
        )

    return "💡 แนะนำการพิมพ์คำสั่ง:\n• พิมพ์ 'ทอง' เพื่อดูราคาปัจจุบัน\n• พิมพ์ 'เตือน 2650' เพื่อตั้งแจ้งเตือนราคา"

# ---------- 9. Routing Controller (Flask Endpoints) ----------
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return "OK", 200

@app.route("/ping")
def ping():
    db_status = "Connected" if supabase else "Disconnected"
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "database": db_status
    }, 200

# ---------- 10. Webhook Event Router ----------
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text_message(event):
    # ป้องกันกรณีที่ Source Object ไม่มีแอตทริบิวต์ user_id
    user_id = getattr(event.source, "user_id", None)
    if not user_id:
        logger.warning("Event source lacks a valid 'user_id'. Message skipped.")
        return

    # ส่งงานไปรันใน Thread Pool เพื่อความเร็วในการตอบสนอง และส่ง HTTP 200 คืนให้ LINE ทันที
    executor.submit(async_process_event, event.reply_token, event.message.text, user_id)

def async_process_event(reply_token, message_text, user_id):
    try:
        reply_response = process_message(message_text, user_id)
        reply_safe(reply_token, reply_response)
    except Exception as e:
        logger.error(f"Internal Async Processing Error: {e}")
        reply_safe(reply_token, "❌ ขออภัย ระบบเกิดข้อผิดพลาดในการรันคำสั่ง")

# ---------- 11. Scheduler Alert Engine (Batch-deletion & Race condition lock) ----------
def check_alerts():
    if not supabase:
        return

    data = get_gold()
    if not data:
        return

    price = data[0]
    try:
        alerts = supabase.table("alerts").select("*").execute().data or []
    except Exception as e:
        logger.error(f"Failed to query alerts from Supabase: {e}")
        return

    delete_ids = []

    for a in alerts:
        alert_id = a.get("id")
        if alert_id is None:
            continue

        # การจอง Lock หน่วยความจำเพื่อระงับปัญหา Race Condition แจ้งเตือนเบิ้ลซ้ำ
        if not acquire(alert_id):
            continue

        released_early = False
        try:
            direction = a.get("direction")
            target_price = a.get("target_price")
            user_id = a.get("user_id")

            if not direction or target_price is None or not user_id:
                # บันทึกเป็น ID เสียหายเพื่อเตรียมทำความสะอาดฐานข้อมูลพร้อมกัน
                delete_ids.append(alert_id)
                continue

            target = float(target_price)

            if (direction == "above" and price >= target) or \
               (direction == "below" and price <= target):
                
                # ส่งข้อความผลักแจ้งเตือนผู้ใช้งานเมื่อถึงเงื่อนไข
                push_safe(user_id, f"🔔 แจ้งเตือนราคาทองคำถึงเป้าหมายแล้ว!\n💵 Spot ราคาปัจจุบัน: ${price:,.2f}")
                delete_ids.append(alert_id)
            else:
                # ปล่อย Lock คีย์ทันทีเมื่อยังไม่ทำงาน เพื่อประหยัดพื้นที่หน่วยความจำ
                release(alert_id)
                released_early = True

        except Exception as e:
            logger.error(f"Error executing checking cycle for Alert ID {alert_id}: {e}")
            if not released_early:
                release(alert_id)

    # ดำเนินการลบแบบมวลรวม (Batch delete) เพื่อรักษาประสิทธิภาพและประหยัดค่าใช้จ่ายฐานข้อมูล
    if delete_ids:
        try:
            supabase.table("alerts").delete().in_("id", delete_ids).execute()
            logger.info(f"Successfully processed and deleted alert IDs: {delete_ids}")
        except Exception as e:
            logger.error(f"Failed to batch delete triggered alerts: {e}")
        finally:
            for alert_id in delete_ids:
                release(alert_id)

# ---------- 12. App Initialization & Lifespans ----------
if __name__ == "__main__":
    scheduler = BackgroundScheduler(timezone="Asia/Bangkok")
    scheduler.add_job(check_alerts, "interval", minutes=5, jitter=30)
    scheduler.add_job(http.cleanup, "interval", minutes=10)
    
    is_render = os.getenv("RENDER") == "true"
    run_sched = os.getenv("RUN_SCHEDULER") == "true"

    if not is_render or run_sched:
        scheduler.start()
        logger.info("Scheduler service runs globally.")
        
        # ป้องกัน Web Service ของ Render หลับชั่วขณะ (Self-Ping Engine)
        self_url = os.getenv("SELF_URL")
        if self_url:
            scheduler.add_job(lambda: requests.get(self_url), "interval", minutes=14)

    port = int(os.getenv("PORT", 5000))
    app.run("0.0.0.0", port)
