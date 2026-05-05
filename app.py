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

# 1. Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- [ส่วนที่ 1: HttpClient Class] ---
class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        retry = Retry(
            total=3, connect=3, read=3, backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.cache = OrderedDict()
        self.inflight = {} 
        self.lock = Lock()
        self.throttle_lock = Lock()
        self.stats_lock = Lock()
        
        self.max_cache_size = 100
        self.default_timeout = (3, 7)
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
        now = time.monotonic()
        is_owner = False

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
            event.wait(timeout=5)
            return self.fetch_json(url, ttl, cache_key)

        with self.stats_lock: self.stats["miss"] += 1
        self.throttle(url, 1.0)
        
        try:
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
            with self.stats_lock: self.stats["error"] += 1
            logger.error(f"Fetch failed {url}: {e}")
            with self.lock:
                if key in self.cache:
                    data, expire = self.cache[key]
                    if time.monotonic() - expire < 300:
                        return data
            return None
        finally:
            with self.lock:
                if is_owner and key in self.inflight:
                    self.inflight[key].set()
                    del self.inflight[key]

    def cleanup(self):
        now = time.monotonic()
        with self.lock:
            expired = [k for k, (_, exp) in self.cache.items() if exp < now]
            for k in expired: del self.cache[k]

http = HttpClient()

# --- [ส่วนที่ 2: Config & Gold Logic] ---
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
    except:
        logger.error("Supabase init failed")

def get_gold_price_thb():
    gold = http.fetch_json("https://api.frankfurter.app/latest?from=XAU&to=USD", ttl=60)
    usd_thb = http.fetch_json("https://api.frankfurter.app/latest?from=USD&to=THB", ttl=300)
    if not gold or not usd_thb: return None
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
            supabase.table("alerts").insert({"user_id": user_id, "target_price": target, "direction": direction}).execute()
            return f"✅ บันทึกสำเร็จ: จะเตือนเมื่อราคา {'ต่ำกว่า' if direction=='below' else 'ถึง'} ${target:,.2f}"
        return "⚠️ ระบบฐานข้อมูลไม่พร้อม"

    if any(k in raw_text for k in ["ทอง", "ราคา", "gold"]):
        data = get_gold_price_thb()
        if not data: return "❌ ระบบดึงข้อมูลขัดข้อง"
        now = datetime.now(pytz.timezone("Asia/Bangkok")).strftime("%H:%M")
        return (f"🥇 ราคาทองคำ XAUUSD\nSpot: ${data['spot']:,.2f}\nTHB: {data['rate']:.2f}\nทองแท่ง: ฿{data['baht']:,.0f}\n⏰ {now}")

    return "พิมพ์ 'ทอง' หรือ 'เตือน 2600'"

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
    reply = process_message(event.message.text, event.source.user_id)
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(ReplyMessageRequest(
            reply_token=event.reply_token,
            messages=[TextMessage(text=reply)]
        ))

# --- [ส่วนที่ 3: Scheduler & Runner] ---
def check_alerts():
    if not supabase: return
    data = get_gold_price_thb()
    if not data: return
    current_spot = data['spot']
    try:
        res = supabase.table("alerts").select("*").execute()
        for alert in res.data:
            triggered = False
            if alert['direction'] == 'above' and current_spot >= alert['target_price']: triggered = True
            elif alert['direction'] == 'below' and current_spot <= alert['target_price']: triggered = True
            
            if triggered:
                msg = f"🔔 แจ้งเตือน! ราคาทองถึงเป้าหมาย: ${current_spot:,.2f}"
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).push_message(PushMessageRequest(to=alert['user_id'], messages=[TextMessage(text=msg)]))
                supabase.table("alerts").delete().eq("id", alert['id']).execute()
    except Exception as e:
        logger.error(f"Alert Job Error: {e}")

if __name__ == "__main__":
    scheduler = BackgroundScheduler(timezone="Asia/Bangkok")
    scheduler.add_job(check_alerts, 'interval', minutes=5)
    scheduler.add_job(http.cleanup, 'interval', minutes=10)
    
    run_sched = os.environ.get("RUN_SCHEDULER") == "true"
    if os.environ.get("RENDER") != "true" or run_sched:
        scheduler.start()
        self_url = os.environ.get("SELF_URL")
        if self_url:
            scheduler.add_job(lambda: requests.get(self_url), 'interval', minutes=14)

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
