"""
GoldBot - LINE Gold Price Bot
Production-ready:
- Render Free Tier
- Gunicorn (--workers 1 --threads 8 -t 60)

Features:
- Advisory Lock protection
- Optimized alert queries
- True LRU cache
- Retry-safe LINE messaging
- TTL cooldown cache
- Protected metrics route
- Graceful shutdown
"""

import os
import re
import time
import atexit
import logging
import requests
import pytz

from threading import Lock, Semaphore
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Any

from cachetools import TTLCache

from flask import Flask, request, abort, jsonify

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    PushMessageRequest,
    TextMessage,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

from apscheduler.schedulers.background import BackgroundScheduler


# =========================================================
# Logging
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# Flask
# =========================================================

app = Flask(__name__)


# =========================================================
# Environment
# =========================================================

LINE_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

METRICS_TOKEN = os.getenv("METRICS_TOKEN", "")

BANGKOK_TZ = pytz.timezone("Asia/Bangkok")

WAKE_WORD = "บอตเอ๋ย"

ALERT_MIN_USD = 100
ALERT_MAX_USD = 10_000

WEBHOOK_CONCURRENCY = 20


# =========================================================
# LINE SDK
# =========================================================

configuration = Configuration(access_token=LINE_TOKEN)

handler = WebhookHandler(LINE_SECRET)

WEBHOOK_SEMAPHORE = Semaphore(WEBHOOK_CONCURRENCY)


# =========================================================
# Supabase
# =========================================================

supabase = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client

        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

        logger.info("✅ Supabase connected")

    except Exception as e:
        logger.exception(f"Supabase init failed: {e}")

else:
    logger.warning("⚠️ Supabase not configured")


# =========================================================
# Schema Validation
# =========================================================

def verify_schema():
    if not supabase:
        return

    try:
        supabase.table("alerts").select("id").limit(1).execute()
        logger.info("✅ alerts table reachable")

    except Exception as e:
        logger.exception(f"Schema validation failed: {e}")


# =========================================================
# Typed Models
# =========================================================

@dataclass
class GoldPrice:
    spot: float
    usd_thb: float
    baht_gold: float


@dataclass
class Alert:
    id: Any
    user_id: str
    target_price: float
    direction: str


# =========================================================
# Cache Metrics
# =========================================================

@dataclass
class CacheMetrics:
    hits: int = 0
    misses: int = 0
    stale: int = 0

    _lock: Lock = field(default_factory=Lock, repr=False)

    def hit(self):
        with self._lock:
            self.hits += 1

    def miss(self):
        with self._lock:
            self.misses += 1

    def stale_served(self):
        with self._lock:
            self.stale += 1

    def snapshot(self):
        with self._lock:
            total = self.hits + self.misses or 1

            return {
                "hits": self.hits,
                "misses": self.misses,
                "stale_served": self.stale,
                "hit_ratio": round(self.hits / total, 3),
            }


cache_metrics = CacheMetrics()


# =========================================================
# Executors
# =========================================================

_db_executor = ThreadPoolExecutor(
    max_workers=8,
    thread_name_prefix="DB"
)

webhook_executor = ThreadPoolExecutor(
    max_workers=WEBHOOK_CONCURRENCY,
    thread_name_prefix="Webhook"
)

fetch_pool = ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="Fetch"
)


# =========================================================
# Safe DB Call
# =========================================================

def safe_db_call(fn):
    try:
        return _db_executor.submit(fn).result(timeout=5)

    except FuturesTimeout:
        logger.error("safe_db_call timeout")
        return None

    except Exception as e:
        logger.exception(f"safe_db_call error: {e}")
        return None


# =========================================================
# HTTP Client
# =========================================================

class HttpClient:

    def __init__(self):

        self.session = requests.Session()

        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[502, 503, 504]
        )

        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=retry
        )

        self.session.mount("https://", adapter)

        self.cache: OrderedDict = OrderedDict()

        self.fail_until: dict[str, float] = {}

        self.lock = Lock()

    def fetch(self, url: str, ttl: int = 60):

        now = time.monotonic()

        with self.lock:

            if url in self.fail_until and now < self.fail_until[url]:

                entry = self.cache.get(url)

                if entry:
                    data, _exp, stale_until = entry

                    if now < stale_until:
                        self.cache.move_to_end(url)

                        cache_metrics.stale_served()

                        return data

                return None

            entry = self.cache.get(url)

            if entry:
                data, exp, _stale = entry

                if now < exp:
                    self.cache.move_to_end(url)

                    cache_metrics.hit()

                    return data

        cache_metrics.miss()

        try:

            response = self.session.get(
                url,
                timeout=(3.1, 7.1)
            )

            response.raise_for_status()

            data = response.json()

            with self.lock:

                self.cache[url] = (
                    data,
                    now + ttl,
                    now + ttl + 300
                )

                self.cache.move_to_end(url)

                if len(self.cache) > 50:
                    self.cache.popitem(last=False)

                self.fail_until.pop(url, None)

            return data

        except Exception as e:

            logger.warning(f"fetch failed [{url}]: {e}")

            with self.lock:

                self.fail_until[url] = time.monotonic() + 30

                entry = self.cache.get(url)

                if entry:
                    data, _exp, stale_until = entry

                    if now < stale_until:
                        self.cache.move_to_end(url)

                        cache_metrics.stale_served()

                        return data

            return None

    def close(self):
        self.session.close()

    @property
    def cache_size(self):
        with self.lock:
            return len(self.cache)

    @property
    def open_circuits(self):
        now = time.monotonic()

        with self.lock:
            return sum(
                1 for t in self.fail_until.values()
                if now < t
            )


http = HttpClient()


# =========================================================
# LINE Messaging
# =========================================================

def reply_safe(token: str, text: str) -> bool:

    if not token:
        return False

    payload = (text[:2000] if text else "⚠️ Error").strip()

    for attempt in range(2):

        try:

            with ApiClient(configuration) as api:

                MessagingApi(api).reply_message(
                    ReplyMessageRequest(
                        reply_token=token,
                        messages=[TextMessage(text=payload)]
                    )
                )

            return True

        except Exception as e:

            logger.warning(
                f"reply_safe attempt {attempt + 1} failed: {e}"
            )

            if attempt == 0:
                time.sleep(1)

    return False


def push_safe(user_id: str, text: str) -> bool:

    if not user_id:
        return False

    payload = (text[:2000] if text else "⚠️ Error").strip()

    for attempt in range(2):

        try:

            with ApiClient(configuration) as api:

                MessagingApi(api).push_message(
                    PushMessageRequest(
                        to=user_id,
                        messages=[TextMessage(text=payload)]
                    )
                )

            return True

        except Exception as e:

            logger.warning(
                f"push_safe attempt {attempt + 1} failed: {e}"
            )

            if attempt == 0:
                time.sleep(1)

    return False


# =========================================================
# Gold API
# =========================================================

GOLD_API_URL = "https://api.gold-api.com/price/XAU"

FX_API_URL = "https://api.frankfurter.app/latest?from=USD&to=THB"


def _get_fx_rate_cached() -> float:

    with http.lock:
        entry = http.cache.get(FX_API_URL)

    if entry:

        data, _exp, stale_until = entry

        if time.monotonic() < stale_until:

            try:
                return float(data["rates"]["THB"])

            except Exception:
                pass

    return 35.0


def get_gold() -> Optional[GoldPrice]:

    f1 = fetch_pool.submit(http.fetch, GOLD_API_URL, 60)

    f2 = fetch_pool.submit(http.fetch, FX_API_URL, 300)

    try:
        gold = f1.result(timeout=8)

    except FuturesTimeout:
        f1.cancel()
        gold = None

    try:
        fx = f2.result(timeout=8)

    except FuturesTimeout:
        f2.cancel()
        fx = None

    if not gold or not fx:
        return None

    try:

        spot = float(gold["price"])

        usd_thb = float(fx["rates"]["THB"])

        baht = (
            (spot * usd_thb / 31.1035)
            * 15.244
        )

        return GoldPrice(
            spot=spot,
            usd_thb=usd_thb,
            baht_gold=baht
        )

    except Exception as e:

        logger.exception(f"get_gold parse failed: {e}")

        return None


def format_gold(g: GoldPrice):

    now = datetime.now(BANGKOK_TZ).strftime(
        "%d/%m/%Y %H:%M น."
    )

    return (
        f"🥇 ราคาทองคำ XAUUSD\n"
        f"{'─' * 25}\n"
        f"💵 USD/oz : ${g.spot:,.2f}\n"
        f"💱 USD/THB : {g.usd_thb:.2f}\n"
        f"🔸 ทอง 1 บาท : ฿{g.baht_gold:,.0f}\n"
        f"{'─' * 25}\n"
        f"⏰ {now}"
    )


# =========================================================
# Cooldown
# =========================================================

_COOLDOWN = TTLCache(
    maxsize=10_000,
    ttl=2
)

_COOLDOWN_LOCK = Lock()


def is_rate_limited(user_id: str):

    with _COOLDOWN_LOCK:

        if user_id in _COOLDOWN:
            return True

        _COOLDOWN[user_id] = True

        return False


# =========================================================
# Alert DB
# =========================================================

def alert_add(user_id, target, direction):

    if not supabase:
        return False

    def _fn():

        return supabase.table("alerts").upsert(
            {
                "user_id": user_id,
                "target_price": target,
                "direction": direction,
            },
            on_conflict="user_id,target_price,direction"
        ).execute()

    return safe_db_call(_fn) is not None


def alert_list(user_id):

    if not supabase:
        return []

    def _fn():

        return (
            supabase.table("alerts")
            .select("*")
            .eq("user_id", user_id)
            .order("created_at")
            .execute()
        )

    result = safe_db_call(_fn)

    if not result:
        return []

    return [
        Alert(
            id=row["id"],
            user_id=row["user_id"],
            target_price=float(row["target_price"]),
            direction=row["direction"]
        )
        for row in (result.data or [])
    ]


def alert_delete_id(alert_id):

    if not supabase:
        return False

    def _fn():

        return (
            supabase.table("alerts")
            .delete()
            .eq("id", alert_id)
            .execute()
        )

    return safe_db_call(_fn) is not None


def alert_delete_all(user_id):

    if not supabase:
        return False

    def _fn():

        return (
            supabase.table("alerts")
            .delete()
            .eq("user_id", user_id)
            .execute()
        )

    return safe_db_call(_fn) is not None


# =========================================================
# Alert Engine
# =========================================================

def process_alert_logic(price: float):

    def _fetch_above():

        return (
            supabase.table("alerts")
            .select("id,user_id,target_price,direction")
            .eq("direction", "above")
            .lte("target_price", price)
            .execute()
        )

    def _fetch_below():

        return (
            supabase.table("alerts")
            .select("id,user_id,target_price,direction")
            .eq("direction", "below")
            .gte("target_price", price)
            .execute()
        )

    res_above = safe_db_call(_fetch_above)

    res_below = safe_db_call(_fetch_below)

    triggered = []

    if res_above and res_above.data:
        triggered.extend(res_above.data)

    if res_below and res_below.data:
        triggered.extend(res_below.data)

    if not triggered:
        return

    delete_ids = []

    for alert in triggered:

        try:

            direction_text = (
                "ขึ้นถึง"
                if alert["direction"] == "above"
                else "ลงต่ำกว่า"
            )

            push_safe(
                alert["user_id"],
                (
                    "🔔 แจ้งเตือนราคาทอง!\n"
                    f"{'─' * 25}\n"
                    f"ราคา XAUUSD {direction_text} "
                    f"${float(alert['target_price']):,.2f}\n"
                    f"💵 ราคาปัจจุบัน: ${price:,.2f}\n"
                    f"{'─' * 25}\n"
                    "⚠️ การแจ้งเตือนนี้ถูกลบอัตโนมัติ"
                )
            )

            delete_ids.append(alert["id"])

        except Exception as e:
            logger.exception(f"push alert failed: {e}")

    if delete_ids:

        def _batch():

            return (
                supabase.table("alerts")
                .delete()
                .in_("id", delete_ids)
                .execute()
            )

        safe_db_call(_batch)


def check_alerts():

    if not supabase:
        return

    lock_id = 99999

    lock_res = safe_db_call(
        lambda: supabase.rpc(
            "try_advisory_lock",
            {"lock_id": lock_id}
        ).execute()
    )

    if not lock_res or lock_res.data is False:
        return

    try:

        gold = get_gold()

        if not gold:
            return

        process_alert_logic(gold.spot)

    finally:

        safe_db_call(
            lambda: supabase.rpc(
                "release_advisory_lock",
                {"lock_id": lock_id}
            ).execute()
        )


# =========================================================
# Regex
# =========================================================

RE_BELOW = re.compile(
    r"^(?:แจ้งเตือนต่ำกว่า|ต่ำกว่า|below|ลง)\s*(\d+(?:\.\d+)?)\s*(บาท|thb|฿)?$"
)

RE_ABOVE = re.compile(
    r"^(?:แจ้งเตือนสูงกว่า|สูงกว่า|แจ้งเตือน|เตือน|alert|above)\s*(\d+(?:\.\d+)?)\s*(บาท|thb|฿)?$"
)

RE_DELETE_INDEX = re.compile(r"^ลบ\s*(\d+)$")


# =========================================================
# Validation
# =========================================================

def _parse_target(value, unit):

    if unit in ["บาท", "thb", "฿"]:

        usd_thb = _get_fx_rate_cached()

        usd = (
            (value / 15.244 * 31.1035)
            / usd_thb
        )

        return (
            round(usd, 2),
            f"฿{value:,.0f} (≈ ${usd:,.2f})"
        )

    return value, f"${value:,.2f}"


def _validate_target(target):

    if not (
        ALERT_MIN_USD
        <= target
        <= ALERT_MAX_USD
    ):
        return (
            f"❌ กรุณาระบุราคา "
            f"${ALERT_MIN_USD:,}"
            f"–"
            f"${ALERT_MAX_USD:,}"
        )

    return None


# =========================================================
# Process Message
# =========================================================

def process_message(text, user_id):

    if is_rate_limited(user_id):
        return "⏳ ช้าหน่อยนะ..."

    lower = text.lower().strip()

    m = RE_BELOW.match(lower)

    if m:

        target, display = _parse_target(
            float(m.group(1)),
            m.group(2)
        )

        if err := _validate_target(target):
            return err

        if alert_add(user_id, target, "below"):
            return f"✅ ตั้งเตือนต่ำกว่า {display}"

        return "❌ เกิดข้อผิดพลาด"

    m = RE_ABOVE.match(lower)

    if m:

        target, display = _parse_target(
            float(m.group(1)),
            m.group(2)
        )

        if err := _validate_target(target):
            return err

        if alert_add(user_id, target, "above"):
            return f"✅ ตั้งเตือนสูงกว่า {display}"

        return "❌ เกิดข้อผิดพลาด"

    if any(
        kw in lower
        for kw in [
            "ดูการแจ้งเตือน",
            "การแจ้งเตือน",
            "myalert",
            "my alert",
        ]
    ):

        rows = alert_list(user_id)

        if not rows:
            return "📭 ไม่มีการแจ้งเตือน"

        lines = [
            "🔔 การแจ้งเตือนของคุณ",
            "─" * 20
        ]

        for i, alert in enumerate(rows, 1):

            icon = (
                "📈"
                if alert.direction == "above"
                else "📉"
            )

            sign = (
                "≥"
                if alert.direction == "above"
                else "≤"
            )

            lines.append(
                f"{i}. {icon} {sign} ${alert.target_price:,.2f}"
            )

        lines.append("─" * 20)

        return "\n".join(lines)

    m = RE_DELETE_INDEX.match(lower)

    if m:

        idx = int(m.group(1))

        rows = alert_list(user_id)

        if not rows:
            return "📭 ไม่มีการแจ้งเตือน"

        if not (1 <= idx <= len(rows)):
            return f"❌ เลือก 1-{len(rows)}"

        alert = rows[idx - 1]

        alert_delete_id(alert.id)

        return (
            f"🗑️ ลบแจ้งเตือน "
            f"${alert.target_price:,.2f}"
        )

    if any(
        kw in lower
        for kw in [
            "ยกเลิก",
            "cancel",
            "ลบการแจ้งเตือน"
        ]
    ):

        if alert_delete_all(user_id):
            return "🗑️ ลบทั้งหมดแล้ว"

        return "❌ เกิดข้อผิดพลาด"

    if any(
        kw in lower
        for kw in [
            "ราคาทอง",
            "gold",
            "xau",
            "xauusd"
        ]
    ):

        gold = get_gold()

        if not gold:
            return "❌ ดึงข้อมูลไม่ได้"

        return format_gold(gold)

    return (
        "👋 GoldBot\n\n"
        "คำสั่ง:\n"
        "- ราคาทอง\n"
        "- แจ้งเตือน 3500\n"
        "- แจ้งเตือนต่ำกว่า 3200\n"
        "- ดูการแจ้งเตือน\n"
        "- ยกเลิก"
    )


# =========================================================
# Routes
# =========================================================

@app.route("/")
def home():
    return "GoldBot Running", 200


@app.route("/ping")
def ping():
    return "pong", 200


@app.route("/callback", methods=["POST"])
def callback():

    signature = request.headers.get(
        "X-Line-Signature",
        ""
    )

    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)

    except InvalidSignatureError:
        abort(400)

    return "OK"


@app.route("/metrics")
def metrics():

    if METRICS_TOKEN:

        token = request.headers.get(
            "X-Metrics-Token",
            ""
        )

        if token != METRICS_TOKEN:
            abort(403)

    return jsonify({
        "cache_entries": http.cache_size,
        "open_circuits": http.open_circuits,
        "cooldowns": len(_COOLDOWN),
        "scheduler_running": scheduler.running,
        **cache_metrics.snapshot(),
    })


# =========================================================
# LINE Event
# =========================================================

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):

    text = event.message.text.strip()

    user_id = event.source.user_id

    if event.source.type in ["group", "room"]:

        if not text.startswith(WAKE_WORD):
            return

        text = (
            text[len(WAKE_WORD):].strip()
            or "ราคาทอง"
        )

    if not WEBHOOK_SEMAPHORE.acquire(blocking=False):

        push_safe(
            user_id,
            "⏳ ระบบกำลังยุ่ง"
        )

        return

    def _task():

        start = time.perf_counter()

        try:

            result = process_message(
                text,
                user_id
            )

            elapsed = (
                time.perf_counter()
                - start
            )

            logger.info(
                f"process_message {elapsed:.3f}s"
            )

            if not reply_safe(
                event.reply_token,
                result
            ):
                push_safe(user_id, result)

        except Exception as e:
            logger.exception(f"_task failed: {e}")

        finally:
            WEBHOOK_SEMAPHORE.release()

    webhook_executor.submit(_task)


# =========================================================
# Shutdown
# =========================================================

def _do_shutdown():

    logger.info("Shutdown initiated")

    try:

        if scheduler.running:
            scheduler.shutdown(wait=True)

    except Exception:
        pass

    webhook_executor.shutdown(wait=True)

    fetch_pool.shutdown(wait=True)

    _db_executor.shutdown(wait=True)

    http.close()

    logger.info("✅ Shutdown complete")


atexit.register(_do_shutdown)


# =========================================================
# Scheduler
# =========================================================

scheduler = BackgroundScheduler(
    timezone="Asia/Bangkok"
)

scheduler.add_job(
    check_alerts,
    "interval",
    minutes=5,
    id="check_alerts",
    max_instances=1,
    coalesce=True,
    replace_existing=True,
    jitter=30,
)


# =========================================================
# Entry
# =========================================================

if __name__ == "__main__":

    verify_schema()

    if not scheduler.running:
        scheduler.start()

    logger.info("🚀 GoldBot started")

    try:

        app.run(
            host="0.0.0.0",
            port=int(os.getenv("PORT", 5000)),
            threaded=True,
            use_reloader=False,
        )

    finally:
        _do_shutdown()

else:

    verify_schema()

    if not scheduler.running:
        scheduler.start()
