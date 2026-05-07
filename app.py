# file: app.py
"""
GoldBot - LINE Bot ราคาทองคำ
Deploy-ready: Render Free Tier + Gunicorn (--workers 1 --threads 8 -t 60)

Final patches:
  1. _get_fx_rate_cached: อ่าน cache ไม่ lock → ใส่ http.lock ครอบ
  2. graceful shutdown local mode: app.run() blocking ทำ _do_shutdown ไม่ถูกเรียก
     → try/finally แทน signal event
  3. _shutdown_event + _signal_handler: dead code หลัง fix #2 → ลบออก
  A. scheduler jitter=30 กัน thundering herd
  B. request timing log ใน _task
  C. cache hit/miss/stale metrics
  D. LINE push retry 1 รอบ transient errors
  E. cooldown purge: ย้ายออกจาก hot path → background thread ทุก 5 นาที
"""

import os
import re
import time
import logging
import threading
import pytz
import signal
import requests

from threading import Lock, Semaphore
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Any

from flask import Flask, request, abort, jsonify
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, PushMessageRequest, TextMessage,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from apscheduler.schedulers.background import BackgroundScheduler

# ===== 1. Logging & Flask =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)
app    = Flask(__name__)

# ===== 2. Config =====
LINE_TOKEN   = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_SECRET  = os.getenv("LINE_CHANNEL_SECRET", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
BANGKOK_TZ   = pytz.timezone("Asia/Bangkok")
WAKE_WORD    = "บอตเอ๋ย"

ALERT_MIN_USD       = 100
ALERT_MAX_USD       = 10_000
WEBHOOK_CONCURRENCY = 20   # semaphore == max_workers กัน queue ล้น

configuration = Configuration(access_token=LINE_TOKEN)
handler       = WebhookHandler(LINE_SECRET)
WEBHOOK_SEMAPHORE = Semaphore(WEBHOOK_CONCURRENCY)

# ===== 3. Supabase =====
supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✅ Supabase connected")
    except Exception as e:
        logger.error(f"❌ Supabase init failed: {e}")
else:
    logger.warning("⚠️  Supabase not configured — alert features disabled")


# ===== 4. Schema Validation =====
def verify_schema():
    if not supabase:
        return
    try:
        supabase.table("alerts") \
            .select("id,user_id,target_price,direction,created_at") \
            .limit(1).execute()
        logger.info("✅ Schema OK: alerts table + columns exist")
    except Exception as e:
        logger.error(
            f"❌ Schema check FAILED: {e}\n"
            "   ⚠️  รัน schema.sql ใน Supabase SQL Editor ก่อน deploy!\n"
            "   ⚠️  upsert on_conflict จะ error ถ้าไม่มี UNIQUE INDEX"
        )
        return
    logger.warning(
        "⚠️  Cannot auto-verify UNIQUE INDEX from app layer.\n"
        "   Ensure this exists:\n"
        "   CREATE UNIQUE INDEX alerts_user_target_direction_uidx\n"
        "   ON public.alerts (user_id, target_price, direction);"
    )


# ===== 5. Typed Models =====
@dataclass
class GoldPrice:
    spot:      float
    usd_thb:   float
    baht_gold: float


@dataclass
class Alert:
    id:           Any
    user_id:      str
    target_price: float
    direction:    str


# ===== 6. Cache Metrics =====
@dataclass
class CacheMetrics:
    hits:   int = 0
    misses: int = 0
    stale:  int = 0
    _lock: Lock = field(default_factory=Lock, repr=False)

    def hit(self):
        with self._lock: self.hits += 1

    def miss(self):
        with self._lock: self.misses += 1

    def stale_served(self):
        with self._lock: self.stale += 1

    def snapshot(self) -> dict:
        with self._lock:
            total = self.hits + self.misses or 1
            return {
                "hits":        self.hits,
                "misses":      self.misses,
                "stale_served": self.stale,
                "hit_ratio":   round(self.hits / total, 3),
            }

cache_metrics = CacheMetrics()


# ===== 7. safe_db_call =====
_db_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="DB")

def safe_db_call(fn) -> Optional[Any]:
    try:
        return _db_executor.submit(fn).result(timeout=5)
    except FuturesTimeout:
        logger.error("safe_db_call: timed out (5s)")
        return None
    except Exception as e:
        logger.exception(f"safe_db_call error: {e}")
        return None


# ===== 8. HTTP Client =====
class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        retry   = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
        self.session.mount("https://", adapter)
        self.cache:      OrderedDict      = OrderedDict()
        self.fail_until: dict[str, float] = {}
        self.lock = Lock()

    def fetch(self, url: str, ttl: int = 60) -> Optional[dict]:
        now = time.monotonic()

        with self.lock:
            # circuit open → return stale if available
            if url in self.fail_until and now < self.fail_until[url]:
                logger.warning(f"Circuit open: {url}")
                entry = self.cache.get(url)
                if entry:
                    data, _exp, stale_until = entry
                    if now < stale_until:
                        cache_metrics.stale_served()
                        return data
                return None

            # cache fresh
            entry = self.cache.get(url)
            if entry:
                data, exp, _stale = entry
                if now < exp:
                    cache_metrics.hit()
                    return data

        cache_metrics.miss()

        try:
            res = self.session.get(url, timeout=(3.1, 7.1))
            res.raise_for_status()
            data = res.json()
            with self.lock:
                self.cache[url] = (data, now + ttl, now + ttl + 300)
                if len(self.cache) > 50:
                    self.cache.popitem(last=False)
                self.fail_until.pop(url, None)
            return data
        except Exception as e:
            logger.error(f"Fetch error [{url}]: {e}")
            with self.lock:
                self.fail_until[url] = time.monotonic() + 30
                # FIX: stale fallback ตั้งแต่ fetch fail ครั้งแรก
                entry = self.cache.get(url)
                if entry:
                    data, _exp, stale_until = entry
                    if now < stale_until:
                        logger.warning(f"Returning stale on first fail: {url}")
                        cache_metrics.stale_served()
                        return data
            return None

    def close(self):
        self.session.close()
        logger.info("HTTP session closed")

    @property
    def cache_size(self) -> int:
        with self.lock: return len(self.cache)

    @property
    def open_circuits(self) -> int:
        now = time.monotonic()
        with self.lock:
            return sum(1 for t in self.fail_until.values() if now < t)


http = HttpClient()

# ===== 9. Thread Pools =====
webhook_executor = ThreadPoolExecutor(max_workers=WEBHOOK_CONCURRENCY, thread_name_prefix="Webhook")
fetch_pool       = ThreadPoolExecutor(max_workers=4,                   thread_name_prefix="Fetch")

# ===== 10. LINE Messaging =====
def reply_safe(token: str, text: str) -> bool:
    if not token:
        return False
    try:
        with ApiClient(configuration) as api:
            MessagingApi(api).reply_message(
                ReplyMessageRequest(
                    reply_token=token,
                    messages=[TextMessage(
                        text=(text[:2000] if text else "⚠️ Error").strip()
                    )]
                )
            )
        return True
    except Exception as e:
        logger.warning(f"reply_safe failed: {e}")
        return False


def push_safe(user_id: str, text: str, _retry: bool = True):
    """FIX D: retry 1 รอบ transient LINE errors"""
    if not user_id:
        return
    try:
        with ApiClient(configuration) as api:
            MessagingApi(api).push_message(
                PushMessageRequest(
                    to=user_id,
                    messages=[TextMessage(
                        text=(text[:2000] if text else "⚠️ Error").strip()
                    )]
                )
            )
    except Exception as e:
        logger.warning(f"push_safe error: {e}")
        if _retry:
            logger.info("push_safe: retrying once...")
            time.sleep(1)
            push_safe(user_id, text, _retry=False)


# ===== 11. Gold Price =====
GOLD_API_URL = "https://api.gold-api.com/price/XAU"
FX_API_URL   = "https://api.frankfurter.app/latest?from=USD&to=THB"


def _get_fx_rate_cached() -> float:
    """
    FIX #1: ครอบ http.lock ก่อนอ่าน cache
    OrderedDict ไม่ guarantee thread-safe ต่อ concurrent mutation
    """
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
    f2 = fetch_pool.submit(http.fetch, FX_API_URL,   300)

    gold = fx = None
    try:
        gold = f1.result(timeout=8)
    except FuturesTimeout:
        f1.cancel()
        logger.error("get_gold: XAU/USD timed out")
    except Exception as e:
        logger.exception(f"get_gold f1: {e}")

    try:
        fx = f2.result(timeout=8)
    except FuturesTimeout:
        f2.cancel()
        logger.error("get_gold: USD/THB timed out")
    except Exception as e:
        logger.exception(f"get_gold f2: {e}")

    if not gold or not fx:
        return None

    if not isinstance(gold, dict) or "price" not in gold:
        logger.error(f"Invalid gold API response: {str(gold)[:200]}")
        return None
    if not isinstance(fx, dict) or "rates" not in fx:
        logger.error(f"Invalid FX API response: {str(fx)[:200]}")
        return None

    try:
        spot    = float(gold["price"])
        usd_thb = float(fx["rates"]["THB"])
        baht    = (spot * usd_thb / 31.1035) * 15.244
        return GoldPrice(spot=spot, usd_thb=usd_thb, baht_gold=baht)
    except (KeyError, TypeError, ValueError) as e:
        logger.exception(f"get_gold parse: {e}")
        return None


def format_gold(g: GoldPrice) -> str:
    now = datetime.now(BANGKOK_TZ).strftime("%d/%m/%Y %H:%M น.")
    return (
        f"🥇 ราคาทองคำ XAUUSD\n"
        f"{'─' * 25}\n"
        f"💵 USD/oz  : ${g.spot:,.2f}\n"
        f"💱 USD/THB : {g.usd_thb:.2f} บาท\n"
        f"🔸 ทอง 1 บาท : ฿{g.baht_gold:,.0f}\n"
        f"{'─' * 25}\n"
        f"⏰ {now}\n"
        f"📊 ข้อมูลจาก: gold-api.com"
    )


# ===== 12. Rate Limit =====
# FIX E: แยก purge ออกไปเป็น background thread ทุก 5 นาที
# ไม่รัน O(n) ทุก request อีกต่อไป
_COOLDOWN:     dict = {}
_COOLDOWN_LOCK      = Lock()
COOLDOWN_SEC        = 2
PURGE_AFTER_SEC     = 300


def _purge_cooldown():
    """รันใน background ทุก 5 นาที"""
    now = time.time()
    with _COOLDOWN_LOCK:
        expired = [k for k, v in _COOLDOWN.items() if now - v > PURGE_AFTER_SEC]
        for k in expired:
            del _COOLDOWN[k]
    if expired:
        logger.info(f"Purged {len(expired)} cooldown entries")


def is_rate_limited(user_id: str) -> bool:
    now = time.time()
    with _COOLDOWN_LOCK:
        if now - _COOLDOWN.get(user_id, 0) < COOLDOWN_SEC:
            return True
        _COOLDOWN[user_id] = now
        return False


# ===== 13. Alert DB =====
def alert_add(user_id: str, target: float, direction: str) -> bool:
    if not supabase:
        return False
    def _fn():
        return supabase.table("alerts").upsert(
            {"user_id": user_id, "target_price": target, "direction": direction},
            on_conflict="user_id,target_price,direction"
        ).execute()
    return safe_db_call(_fn) is not None


def alert_list(user_id: str) -> List[Alert]:
    if not supabase:
        return []
    def _fn():
        return supabase.table("alerts").select("*") \
            .eq("user_id", user_id) \
            .order("created_at") \
            .execute()
    result = safe_db_call(_fn)
    if not result:
        return []
    return [
        Alert(
            id=r["id"],
            user_id=r["user_id"],
            target_price=float(r["target_price"]),
            direction=r["direction"]
        )
        for r in (result.data or [])
    ]


def alert_delete_id(alert_id: Any) -> bool:
    if not supabase:
        return False
    def _fn():
        return supabase.table("alerts").delete().eq("id", alert_id).execute()
    return safe_db_call(_fn) is not None


def alert_delete_all(user_id: str) -> bool:
    if not supabase:
        return False
    def _fn():
        return supabase.table("alerts").delete().eq("user_id", user_id).execute()
    return safe_db_call(_fn) is not None


# ===== 14. Alert Engine =====
def check_alerts():
    if not supabase:
        return
    gold = get_gold()
    if not gold:
        return
    price = gold.spot

    def _fetch():
        return supabase.table("alerts") \
            .select("id,user_id,target_price,direction").execute()

    result     = safe_db_call(_fetch)
    all_alerts = (result.data or []) if result else []
    if not all_alerts:
        return

    triggered = [
        a for a in all_alerts
        if (a["direction"] == "above" and price >= float(a["target_price"])) or
           (a["direction"] == "below" and price <= float(a["target_price"]))
    ]
    if not triggered:
        return

    delete_ids = []
    for a in triggered:
        try:
            dir_text = "ขึ้นถึง" if a["direction"] == "above" else "ลงต่ำกว่า"
            push_safe(
                a["user_id"],
                f"🔔 แจ้งเตือนราคาทอง!\n"
                f"{'─' * 25}\n"
                f"ราคา XAUUSD {dir_text} ${float(a['target_price']):,.2f} แล้ว!\n"
                f"💵 ราคาปัจจุบัน: ${price:,.2f}\n"
                f"{'─' * 25}\n"
                f"⚠️ การแจ้งเตือนนี้ถูกลบออกแล้ว"
            )
            delete_ids.append(a["id"])
        except Exception as e:
            logger.exception(f"push failed alert {a.get('id')}: {e}")

    if delete_ids:
        def _batch():
            return supabase.table("alerts").delete().in_("id", delete_ids).execute()
        safe_db_call(_batch)
        logger.info(f"✅ Batch deleted {len(delete_ids)} alert(s)")


# ===== 15. Regex & Validation =====
RE_BELOW        = re.compile(
    r"^(?:แจ้งเตือนต่ำกว่า|ต่ำกว่า|below|ลง)\s*(\d+(?:\.\d+)?)\s*(บาท|thb|฿)?$"
)
RE_ABOVE        = re.compile(
    r"^(?:แจ้งเตือนสูงกว่า|สูงกว่า|แจ้งเตือน|เตือน|alert|above)\s*(\d+(?:\.\d+)?)\s*(บาท|thb|฿)?$"
)
RE_DELETE_INDEX = re.compile(r"^ลบ\s*(\d+)$")


def _parse_target(value: float, unit: Optional[str]) -> Tuple[float, str]:
    if unit in ["บาท", "thb", "฿"]:
        usd_thb = _get_fx_rate_cached()
        usd     = (value / 15.244 * 31.1035) / usd_thb
        return round(usd, 2), f"฿{value:,.0f} (≈ ${usd:,.2f})"
    return value, f"${value:,.2f}"


def _validate_target(target_usd: float) -> Optional[str]:
    if not (ALERT_MIN_USD <= target_usd <= ALERT_MAX_USD):
        return (
            f"❌ ราคาไม่สมเหตุสมผล\n"
            f"กรุณาระบุราคาระหว่าง ${ALERT_MIN_USD:,}–${ALERT_MAX_USD:,} USD"
        )
    return None


# ===== 16. Message Handler =====
def process_message(text: str, user_id: str) -> str:
    if is_rate_limited(user_id):
        return "⏳ ช้าหน่อยนะ..."

    lower = text.lower().strip()

    m = RE_BELOW.match(lower)
    if m:
        target, display = _parse_target(float(m.group(1)), m.group(2))
        err = _validate_target(target)
        if err:
            return err
        if alert_add(user_id, target, "below"):
            return (
                f"✅ ตั้งการแจ้งเตือนสำเร็จ!\n"
                f"📉 จะแจ้งเมื่อราคาลงต่ำกว่า {display}\n"
                f"🕐 ตรวจสอบราคาทุก 5 นาที"
            )
        return "❌ เกิดข้อผิดพลาด กรุณาลองใหม่"

    m = RE_ABOVE.match(lower)
    if m:
        target, display = _parse_target(float(m.group(1)), m.group(2))
        err = _validate_target(target)
        if err:
            return err
        if alert_add(user_id, target, "above"):
            return (
                f"✅ ตั้งการแจ้งเตือนสำเร็จ!\n"
                f"📈 จะแจ้งเมื่อราคาขึ้นถึง {display}\n"
                f"🕐 ตรวจสอบราคาทุก 5 นาที"
            )
        return "❌ เกิดข้อผิดพลาด กรุณาลองใหม่"

    if any(kw in lower for kw in ["ดูการแจ้งเตือน", "การแจ้งเตือน", "myalert", "my alert"]):
        rows = alert_list(user_id)
        if not rows:
            return "📭 คุณยังไม่มีการแจ้งเตือนที่ตั้งไว้"
        lines = ["🔔 การแจ้งเตือนของคุณ:", "─" * 20]
        for i, a in enumerate(rows, 1):
            icon = "📈" if a.direction == "above" else "📉"
            sign = "≥"  if a.direction == "above" else "≤"
            lines.append(f"{i}. {icon} {sign} ${a.target_price:,.2f}")
        lines += ["─" * 20, "💡 พิมพ์ 'ลบ 1' เพื่อลบรายการที่ 1"]
        return "\n".join(lines)

    m = RE_DELETE_INDEX.match(lower)
    if m:
        idx  = int(m.group(1))
        rows = alert_list(user_id)
        if not rows:
            return "📭 ไม่มีการแจ้งเตือนที่ตั้งไว้"
        if idx < 1 or idx > len(rows):
            return f"❌ กรุณาระบุหมายเลข 1-{len(rows)}"
        a        = rows[idx - 1]
        dir_text = "ขึ้นถึง" if a.direction == "above" else "ลงต่ำกว่า"
        alert_delete_id(a.id)
        return f"🗑️ ลบการแจ้งเตือน {dir_text} ${a.target_price:,.2f} แล้ว"

    if any(kw in lower for kw in ["ยกเลิก", "ลบการแจ้งเตือน", "cancel"]):
        if alert_delete_all(user_id):
            return "🗑️ ลบการแจ้งเตือนทั้งหมดแล้ว"
        return "❌ เกิดข้อผิดพลาด"

    if any(kw in lower for kw in ["ราคาทอง", "gold", "xau", "xauusd"]):
        gold = get_gold()
        if not gold:
            return "❌ ไม่สามารถดึงข้อมูลได้ในขณะนี้\nกรุณาลองใหม่อีกครั้งนะ"
        return format_gold(gold)

    return (
        "👋 สวัสดี! ฉันคือ GoldBot 🥇\n\n"
        "📌 คำสั่งที่ใช้งานได้:\n"
        "─────────────────────\n"
        "💰 ราคาทอง\n"
        "📈 แจ้งเตือน [ราคา USD]\n"
        "   หรือ แจ้งเตือนสูงกว่า [ราคา]\n"
        "📉 แจ้งเตือนต่ำกว่า [ราคา]\n"
        "📋 ดูการแจ้งเตือน\n"
        "🗑️ ยกเลิกการแจ้งเตือน\n\n"
        "💡 ใส่ บาท/฿ หลังราคาเพื่อแปลงจากราคาบาท\n"
        f"   ช่วงราคาที่รับได้: ${ALERT_MIN_USD:,}–${ALERT_MAX_USD:,} USD"
    )


# ===== 17. Routes =====
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body      = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"


@app.route("/")
def health():
    return "GoldBot is running! 🥇", 200


@app.route("/ping")
def ping():
    return "pong", 200


@app.route("/metrics")
def metrics():
    with _COOLDOWN_LOCK:
        active_cooldowns = len(_COOLDOWN)
    return jsonify({
        "webhook_max_workers":    getattr(webhook_executor, "_max_workers", None),
        "fetch_max_workers":      getattr(fetch_pool,       "_max_workers", None),
        "db_max_workers":         getattr(_db_executor,     "_max_workers", None),
        "cache_entries":          http.cache_size,
        "circuit_breakers_open":  http.open_circuits,
        "active_cooldowns":       active_cooldowns,
        "supabase_connected":     supabase is not None,
        "scheduler_running":      scheduler.running,
        **cache_metrics.snapshot(),   # hits, misses, stale_served, hit_ratio
        "timestamp":              datetime.now(BANGKOK_TZ).isoformat(),
    })


# ===== 18. LINE Event Handler =====
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    text     = event.message.text.strip()
    user_id  = event.source.user_id
    is_group = event.source.type in ["group", "room"]

    if is_group:
        if not text.startswith(WAKE_WORD):
            return
        text = text[len(WAKE_WORD):].strip() or "ราคาทอง"

    token = event.reply_token

    if not WEBHOOK_SEMAPHORE.acquire(blocking=False):
        logger.warning("Webhook semaphore full — rejecting")
        push_safe(user_id, "⏳ ระบบยุ่งอยู่ กรุณาลองใหม่อีกสักครู่")
        return

    def _task():
        # FIX B: request timing log
        start = time.perf_counter()
        try:
            result  = process_message(text, user_id)
            elapsed = time.perf_counter() - start
            logger.info(f"process_message: {elapsed:.3f}s | user={user_id[:8]}...")
            if not reply_safe(token, result):
                push_safe(user_id, result)
        except Exception as e:
            logger.exception(f"_task error: {e}")
        finally:
            WEBHOOK_SEMAPHORE.release()

    webhook_executor.submit(_task)


# ===== 19. Shutdown =====
def _do_shutdown():
    logger.info("Shutting down scheduler...")
    if scheduler.running:
        scheduler.shutdown(wait=True)
    logger.info("Shutting down thread pools...")
    webhook_executor.shutdown(wait=True)
    fetch_pool.shutdown(wait=True)
    _db_executor.shutdown(wait=True)
    http.close()
    logger.info("✅ Shutdown complete")


# ===== 20. Scheduler =====
scheduler = BackgroundScheduler(timezone="Asia/Bangkok")
scheduler.add_job(
    check_alerts, "interval", minutes=5,
    id="check_alerts", max_instances=1, coalesce=True,
    replace_existing=True,
    jitter=30,          # FIX A: กัน thundering herd
)
# FIX E: cooldown purge background job แทน O(n) ทุก request
scheduler.add_job(
    _purge_cooldown, "interval", minutes=5,
    id="purge_cooldown", max_instances=1, coalesce=True,
    replace_existing=True,
)


# ===== 21. Entry Point =====
if __name__ == "__main__":
    # FIX #2: try/finally แทน signal event
    # app.run() blocking → finally รับประกัน _do_shutdown() ถูกเรียกเสมอ
    # ไม่ว่า Ctrl+C, SIGTERM, หรือ exception
    verify_schema()
    if not scheduler.running:
        scheduler.start()
    logger.info("🚀 GoldBot starting (direct mode)...")
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
    # gunicorn mode (--workers 1 กัน scheduler ซ้ำ)
    verify_schema()
    if not scheduler.running:
        scheduler.start()
        logger.info("🚀 GoldBot starting (gunicorn mode)...")
