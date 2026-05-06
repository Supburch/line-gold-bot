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

# --- [ 1. Logging Configuration ] ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- [ 2. Advanced HttpClient with Leak Protection ] ---
class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=50)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.cache = OrderedDict()
        self.inflight = {} 
        self.lock = Lock()
        self.throttle_lock = Lock()
        self.stats_lock = Lock()
        
        self.max_cache_size = 100
        self.default_timeout = (3.05, 7) # (connect, read)
        self.stats = {"hit": 0, "miss": 0, "error": 0}
        self._last_calls = {}

    def throttle(self, url, min_interval=1.0):
        host = urlparse(url).netloc or "default"
        with self.throttle_lock:
            now = time.monotonic()
            last_call = self._last_calls.get(host, now - min_interval)
            target_time = max(now, last_call + min_interval)
            sleep_time = target_time - now
            self._last_calls[host] = target_time
        if sleep_time > 0:
            time.sleep(sleep_time)

    def fetch_json(self, url, ttl=60, cache_key=None):
        key = cache_key or url
        start_wait = time.monotonic()
        is_owner = False # เก็บสถานะผู้ถือสิทธิ์ดึงข้อมูลสำหรับใช้ใน finally

        try:
            while True:
                now = time.monotonic()
                is_owner = False

                if now - start_wait > 15:
                    logger.error(f"Hard timeout for key: {key}")
                    return None

                with self.lock:
                    if key in self.cache:
                        data, expire = self.cache[key]
                        if now < expire:
                            self.cache.move_to_end(key)
                            with self.stats_lock: self.stats["hit"] += 1
                            return data

                    if key in self.inflight:
                        event = self.inflight[key]
                    else:
                        event = Event()
                        self.inflight[key] = event
                        event.clear()
                        is_owner = True

                if not is_owner:
                    waited = event.wait(timeout=self.default_timeout[1] + 2)
                    if not waited:
                        logger.warning(f"In-flight timeout for key: {key}")
                    continue # วนกลับไปตรวจสอบ Cache ใหม่หลังจากโดนปลุก

                break

            with self.stats_lock: self.stats["miss"] += 1
            self.throttle(url, 1.0)
            
            res = self.session.get(url, timeout=self.default_timeout)
            res.raise_for_status()
            data = res.json()
            
            with self.lock:
                self.cache[key] = (data, time.monotonic() + ttl)
                self.cache.move_to_end(key)
                if len(self.cache) > self.max_cache_size:
                    self.cache.popitem(last=False)
            return data

        except Exception as e:
            if is_owner:
                with self.stats_lock: self.stats["error"] += 1
                logger.error(f"Fetch failed {url}: {e}")
            
            with self.lock:
                if key in self.cache:
                    data, expire = self.cache[key]
                    if time.monotonic() - expire < 300:
                        self.cache.move_to_end(key)
                        return data
            return None

        finally:
            # ✅ แก้ไขข้อ 4: ป้องกัน Inflight leak แบบปลอดภัย 100% (เฉพาะ Thread เจ้าของเท่านั้นที่มีสิทธิ์ Set และ Pop)
            if is_owner:
                with self.lock:
                    event = self.inflight.pop(key, None)
                    if event:
                        event.set()

    def cleanup(self):
        now = time.monotonic()
        with self.lock:
            expired = [k for k, (_, exp) in self.cache.items() if exp < now]
            for k in expired: del self.cache[k]
        logger.info(f"Cleanup done. Stats: {self.stats}")

http = HttpClient()

# --- [ 3. Config & External Services ] ---
# ✅ แก้ไขข้อ 1: ป้องกัน crash จาก Flask(name) เปลี่ยนเป็น __name__
app = Flask(__name__)

LINE_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
LINE_SECRET = os.environ.get("LINE_CHANNEL_SECRET")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

configuration = Configuration(access_token=LINE_TOKEN)
handler = WebhookHandler(LINE_SECRET)
supabase = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        logger.error(f"Supabase init failed: {e}")

# --- [ 4. Alert In-Memory Lock & Deduplication ] ---
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
        logger.error(f"alert_exists error: {e}")
        return False

def alert_add(user_id: str, target: float, direction: str) -> bool:
    if not supabase:
        return False
    try:
        if alert_exists(user_id, target, direction):
            return False 
        supabase.table("alerts").insert({
            "user_id": user_id,
            "target_price": target,
            "direction": direction
        }).execute()
        return True
    except Exception as e:
        logger.error(f"alert_add error: {e}")
        return False

# ✅ แก้ไขข้อ 3: ป้องกัน Thread block / Scheduler lag จากการส่ง API ขัดข้องด้วยการตั้ง Timeout 5 วินาที
def push_safe(user_id, msg):
    try:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(
                    to=user_id,
                    messages=[TextMessage(text=msg)]
                ),
                timeout=5
            )
    except Exception as e:
        logger.error(f"push error: {e}")

# --- [ 5. Business Logic ] ---
def get_gold_price_thb():
    gold = http.fetch_json("https://api.frankfurter.app/latest?from=XAU&to=USD", ttl=60)
    usd_thb = http.fetch_json("https://api.frankfurter.app/latest?from=USD&to=THB", ttl=300)

    if not gold or not usd_thb:
        return None

    spot = gold["rates"]["USD"]
    rate = usd_thb["rates"]["THB"]
    thb_baht = (spot * rate / 31.1035) * 15.244 * 0.965
    return {"spot": spot, "rate": rate, "baht": thb_baht}

def process_message(text, user_id):
    raw_text = text.lower().strip()
    
    alert_pattern = r'^(?:เตือน|alert|สูงกว่า|ต่ำกว่า)\s*(\d+(?:\.\d+)?)'
    alert_match = re.search(alert_pattern, raw_text)
    
    if alert_match:
        target = float(alert_match.group(1))
        direction = "below" if any(x in raw_text for x in ["ต่ำ", "below", "ลง"]) else "above"
        
        if supabase:
            if alert_exists(user_id, target, direction):
                return f"📢 คุณเคยตั้งเตือนราคานี้ไว้แล้ว: ${target:,.2f}"
                
            if alert_add(user_id, target, direction):
                return f"✅ บันทึกสำเร็จ: จะเตือนเมื่อราคา {'ต่ำกว่า' if direction=='below' else 'ถึง'} ${target:,.2f}"
            return "❌ บันทึกข้อมูลแจ้งเตือนไม่สำเร็จ"
        return "⚠️ ระบบแจ้งเตือนยังไม่พร้อมใช้งาน"

    if any(k in raw_text for k in ["ทอง", "ราคา", "gold"]):
        data = get_gold_price_thb()
        if not data: 
            return "❌ ระบบดึงข้อมูลขัดข้อง กรุณาลองใหม่"
        
        now = datetime.now(pytz.timezone("Asia/Bangkok")).strftime("%H:%M")
        return (f"🥇 ราคาทองคำ XAUUSD\n"
                f"💵 Spot: ${data['spot']:,.2f}\n"
                f"💱 ค่าเงิน: {data['rate']:.2f} THB\n"
                f"🔸 ทองแท่ง: ฿{data['baht']:,.0f}\n"
                f"⏰ {now}")

    return "พิมพ์ 'ทอง' หรือ 'เตือน 2650' เพื่อเริ่มใช้งาน"

# --- [ 6. Webhook Handlers ] ---
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
    reply_text = process_message(event.message.text, event.source.user_id)
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message(ReplyMessageRequest(
            reply_token=event.reply_token,
            messages=[TextMessage(text=reply_text)]
        ))

# --- [ 7. Background Jobs ] ---
# ✅ แก้ไขข้อ 2: แก้ปัญหาส่งซ้ำซ้อนโดยใช้ In-memory Locking ควบคู่กับระบบควบคุมการคลาย Lock หลังทำการลบข้อมูลแล้ว
def check_alerts():
    if not supabase: 
        return
    data = get_gold_price_thb()
    if not data: 
        return
    
    price = data['spot']
    try:
        alerts = supabase.table("alerts").select("*").execute().data or []
    except Exception as e:
        logger.error(f"fetch error: {e}")
        return

    delete_ids = []

    for a in alerts:
        alert_id = a["id"]

        # ดึงสิทธิ์ Lock ในหน่วยความจำเพื่อป้องกัน Job อื่นทำงานซ้อนทับกัน
        if not acquire(alert_id):
            continue

        released_early = False
        try:
            direction = a["direction"]
            target = float(a["target_price"])

            if (
                (direction == "above" and price >= target) or
                (direction == "below" and price <= target)
            ):
                push_safe(
                    a["user_id"], 
                    f"🔔 ราคาทองถึง ${price:,.2f}"
                )
                delete_ids.append(alert_id)
            else:
                # ตัวที่ไม่ผ่านเงื่อนไขให้ปล่อย Lock ทันทีเพื่อให้คิวถัดไปตรวจสอบต่อได้
                release(alert_id)
                released_early = True

        except Exception as e:
            logger.error(f"alert loop error: {e}")
            if not released_early:
                release(alert_id)

    # ✅ แก้ไขข้อ 5: ใช้ Batch delete ลบข้อมูลทั้งหมดทีเดียว ประหยัดรอบการยิงฐานข้อมูลและลดภาระ Supabase
    if delete_ids:
        try:
            supabase.table("alerts").delete().in_("id", delete_ids).execute()
        except Exception as e:
            logger.error(f"batch delete error: {e}")
        finally:
            # คืนสิทธิ์การทำธุรกรรม (Release lock) ของแถวข้อมูลที่ลบออกแล้วอย่างปลอดภัย
            for alert_id in delete_ids:
                release(alert_id)

# --- [ 8. Runtime Runner ] ---
if __name__ == "__main__":
    scheduler = BackgroundScheduler(timezone="Asia/Bangkok")
    scheduler.add_job(check_alerts, 'interval', minutes=5, jitter=30)
    scheduler.add_job(http.cleanup, 'interval', minutes=10)
    
    is_render = os.environ.get("RENDER") == "true"
    run_sched = os.environ.get("RUN_SCHEDULER") == "true"
    
    if not is_render or run_sched:
        scheduler.start()
        logger.info("Scheduler started successfully.")
        
        self_url = os.environ.get("SELF_URL")
        if self_url:
            scheduler.add_job(lambda: requests.get(self_url), 'interval', minutes=14)

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
