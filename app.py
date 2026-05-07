# app.py
"""
GoldBot - LINE Bot ราคาทองคำ
Production-ready: Render Free Tier + Gunicorn (--workers 1 --threads 8 -t 60)

Best-of-both merged:
  จากโค้ด 1:
    - /internal/check-alerts endpoint + INTERNAL_SECRET (Render Cron Job)
    - Advisory lock ผ่าน Supabase RPC กัน race condition multi-process

  จากโค้ด 2:
    - TTLCache (cachetools) แทน dict + purge thread
    - process_alert_logic แยก query ตาม direction ใช้ DB index จริง
    - reply_safe retry loop (ไม่ recursive)
    - atexit.register(_do_shutdown) ครอบคลุม gunicorn mode ด้วย
    - METRICS_TOKEN guard บน /metrics
    - LRU move_to_end ใน OrderedDict
    - Graceful shutdown: try/finally ใน __main__
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
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, PushMessageRequest, TextMessage,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from apscheduler.schedulers.background import BackgroundScheduler


# =========================================================
# Logging & Flask
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)
app    = Flask(__name__)


# =========================================================
# Environment
# =========================================================

LINE_TOKEN       = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_SECRET      = os.getenv("LINE_CHANNEL_SECRET", "")
SUPABASE_URL     = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY     = os.getenv("SUPABASE_KEY", "")
METRICS_TOKEN    = os.getenv("METRICS_TOKEN", "")        # guard /metrics
INTERNAL_SECRET  = os.getenv("INTERNAL_SECRET", "")     # guard /internal/check-alerts

BANGKOK_TZ          = pytz.timezone("Asia/Bangkok")
WAKE_WORD           = "บอตเอ๋ย"
ALERT_MIN_USD       = 100
ALERT_MAX_USD       = 10_000
WEBHOOK_CONCURRENCY = 20
ADVISORY_LOCK_ID    = 99999

configuration     = Configuration(access_token=LINE_TOKEN)
handler           = WebhookHandler(LINE_SECRET)
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
    logger.warning("⚠️  Supabase not configured — alert features disabled")


# =========================================================
# Schema Validation
# =========================================================

def verify_schema():
    if not supabase:
        return
    try:
        supabase.table("alerts") \
            .select("id,user_id,target_price,direction,created_at") \
            .limit(1).execute()
        logger.info("✅ Schema OK: alerts table reachable")
    except Exception as e:
        logger.error(
            f"❌ Schema check FAILED: {e}\n"
            "   ⚠️  รัน schema.sql ใน Supabase SQL Editor ก่อน deploy!\n"
            "   ⚠️  ต้องมี UNIQUE INDEX: (user_id, target_price, direction)"
        )
    else:
        logger.warning(
            "⚠️  UNIQUE INDEX ไม่สามารถตรวจสอบจาก app layer ได้\n"
            "   ตรวจสอบใน Supabase dashboard ว่ามี:\n"
            "   alerts_user_target_direction_uidx"
        )


# =========================================================
# Typed Models
# =========================================================

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


# =========================================================
# Cache Metrics
# =========================================================

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


# =========================================================
# Thread Pools
# =========================================================

_db_executor     = ThreadPoolExecutor(max_workers=8,                    thread_name_prefix="DB")
webhook_executor = ThreadPoolExecutor(max_workers=WEBHOOK_CONCURRENCY,  thread_name_prefix="Webhook")
fetch_pool       = ThreadPoolExecutor(max_workers=4,                    thread_name_prefix="Fetch")


# =========================================================
# Safe DB Call
# =========================================================

def safe_db_call(fn) -> Optional[Any]:
    try:
        return _db_executor.submit(fn).result(timeout=5)
    except FuturesTimeout:
        logger.error("safe_db_call: timed out (5s)")
        return None
    except Exception as e:
        logger.exception(f"safe_db_call error: {e}")
        return None


# =========================================================
# HTTP Client (LRU Cache + Circuit Breaker + Stale Fallback)
# =========================================================

class HttpClient:

    def __init__(self):
        self.session = requests.Session()
        retry   = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
        self.session.mount("https://", adapter)
        self.cache:      OrderedDict      = OrderedDict()   # (data, exp, stale_until)
        self.fail_until: dict[str, float] = {}
        self.lock = Lock()

    def fetch(self, url: str, ttl: int = 60) -> Optional[dict]:
        now = time.monotonic()

        with self.lock:
            # circuit open → stale fallback
            if url in self.fail_until and now < self.fail_until[url]:
                entry = self.cache.get(url)
                if entry:
                    data, _exp, stale_until = entry
                    if now < stale_until:
                        self.cache.move_to_end(url)
                        cache_metrics.stale_served()
                        return data
                return None

            # cache fresh (LRU touch)
            entry = self.cache.get(url)
            if entry:
                data, exp, _stale = entry
                if now < exp:
                    self.cache.move_to_end(url)
                    cache_metrics.hit()
                    return data

        cache_metrics.miss()

        try:
            res = self.session.get(url, timeout=(3.1, 7.1))
            res.raise_for_status()
            data = res.json()
            with self.lock:
                self.cache[url] = (data, now + ttl, now + ttl + 300)
                self.cache.move_to_end(url)
                if len(self.cache) > 50:
                    self.cache.popitem(last=False)
                self.fail_until.pop(url, None)
            return data

        except Exception as e:
            logger.warning(f"Fetch failed [{url}]: {e}")
            with self.lock:
                self.fail_until[url] = time.monotonic() + 30
                # stale fallback ตั้งแต่ fetch fail ครั้งแรก
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
    def cache_size(self) -> int:
        with self.lock: return len(self.cache)

    @property
    def open_circuits(self) -> int:
        now = time.monotonic()
        with self.lock:
            return sum(1 for t in self.fail_until.values() if now < t)


http = HttpClient()


# =========================================================
# LINE Messaging (retry loop — ไม่ recursive)
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
            logger.warning(f"reply_safe attempt {attempt+1} failed: {e}")
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
            logger.warning(f"push_safe attempt {attempt+1} failed: {e}")
            if attempt == 0:
                time.sleep(1)
    return False


# =========================================================
# Gold API
# =========================================================

GOLD_API_URL = "https://api.gold-api.com/price/XAU"
FX_API_URL   = "https://api.frankfurter.app/latest?from=USD&to=THB"


def _get_fx_rate_cached() -> float:
    """อ่าน FX rate จาก cache เท่านั้น — ไม่ยิง API ใหม่"""
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
    """ยิง 2 endpoints พร้อมกัน + validate response"""
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
        logger.error(f"Invalid gold response: {str(gold)[:200]}")
        return None
    if not isinstance(fx, dict) or "rates" not in fx:
        logger.error(f"Invalid FX response: {str(fx)[:200]}")
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


# =========================================================
# Cooldown (TTLCache — thread-safe, no manual purge)
# =========================================================

_COOLDOWN      = TTLCache(maxsize=10_000, ttl=2)
_COOLDOWN_LOCK = Lock()


def is_rate_limited(user_id: str) -> bool:
    with _COOLDOWN_LOCK:
        if user_id in _COOLDOWN:
            return True
        _COOLDOWN[user_id] = True
        return False


# =========================================================
# Alert DB
# =========================================================

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


# =========================================================
# Advisory Lock (จากโค้ด 1 — กัน race condition multi-process)
# =========================================================

def _acquire_advisory_lock() -> bool:
    """คืน True ถ้าได้ lock, False ถ้ามี process อื่นถือ lock อยู่"""
    res = safe_db_call(
        lambda: supabase.rpc(
            "try_advisory_lock",
            {"lock_id": ADVISORY_LOCK_ID}
        ).execute()
    )
    return bool(res and res.data is True)


def _release_advisory_lock():
    safe_db_call(
        lambda: supabase.rpc(
            "release_advisory_lock",
            {"lock_id": ADVISORY_LOCK_ID}
        ).execute()
    )


# =========================================================
# Alert Engine
# (จากโค้ด 2 — แยก query ตาม direction ใช้ DB index จริง)
# =========================================================

def _process_alert_logic(price: float):
    """fetch triggered alerts แยก above/below เพื่อใช้ DB index"""

    def _fetch_above():
        return supabase.table("alerts") \
            .select("id,user_id,target_price,direction") \
            .eq("direction", "above") \
            .lte("target_price", price) \
            .execute()

    def _fetch_below():
        return supabase.table("alerts") \
            .select("id,user_id,target_price,direction") \
            .eq("direction", "below") \
            .gte("target_price", price) \
            .execute()

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
                f"⚠️ การแจ้งเตือนนี้ถูกลบอัตโนมัติ"
            )
            delete_ids.append(a["id"])
        except Exception as e:
            logger.exception(f"push alert {a.get('id')} failed: {e}")

    if delete_ids:
        def _batch():
            return supabase.table("alerts").delete().in_("id", delete_ids).execute()
        safe_db_call(_batch)
        logger.info(f"✅ Batch deleted {len(delete_ids)} alert(s)")


def check_alerts():
    if not supabase:
        return

    if not _acquire_advisory_lock():
        logger.info("check_alerts: lock held by another process, skipping")
        return

    try:
        gold = get_gold()
        if not gold:
            return
        _process_alert_logic(gold.spot)
    finally:
        _release_advisory_lock()


# =========================================================
# Regex & Validation
# =========================================================

RE_BELOW = re.compile(
    r"^(?:แจ้งเตือนต่ำกว่า|ต่ำกว่า|below|ลง)\s*(\d+(?:\.\d+)?)\s*(บาท|thb|฿)?$"
)
RE_ABOVE = re.compile(
    r"^(?:แจ้งเตือนสูงกว่า|สูงกว่า|แจ้งเตือน|เตือน|alert|above)\s*(\d+(?:\.\d+)?)\s*(บาท|thb|฿)?$"
)
RE_DELETE_INDEX = re.compile(r"^ลบ\s*(\d+)$")


def _parse_target(value: float, unit: Optional[str]) -> Tuple[float, str]:
    if unit in ["บาท", "thb", "฿"]:
        usd_thb = _get_fx_rate_cached()
        usd     = (value / 15.244 * 31.1035) / usd_thb
        return round(usd, 2), f"฿{value:,.0f} (≈ ${usd:,.2f})"
    return value, f"${value:,.2f}"


def _validate_target(target: float) -> Optional[str]:
    if not (ALERT_MIN_USD <= target <= ALERT_MAX_USD):
        return f"❌ กรุณาระบุราคา ${ALERT_MIN_USD:,}–${ALERT_MAX_USD:,} USD"
    return None


# =========================================================
# Message Handler
# =========================================================

def process_message(text: str, user_id: str) -> str:
    if is_rate_limited(user_id):
        return "⏳ ช้าหน่อยนะ..."

    lower = text.lower().strip()

    # ── Alert ต่ำกว่า ─────────────────────────────────────────────────
    m = RE_BELOW.match(lower)
    if m:
        target, display = _parse_target(float(m.group(1)), m.group(2))
        if err := _validate_target(target):
            return err
        if alert_add(user_id, target, "below"):
            return (
                f"✅ ตั้งการแจ้งเตือนสำเร็จ!\n"
                f"📉 จะแจ้งเมื่อราคาลงต่ำกว่า {display}\n"
                f"🕐 ตรวจสอบราคาทุก 5 นาที"
            )
        return "❌ เกิดข้อผิดพลาด กรุณาลองใหม่"

    # ── Alert สูงกว่า ─────────────────────────────────────────────────
    m = RE_ABOVE.match(lower)
    if m:
        target, display = _parse_target(float(m.group(1)), m.group(2))
        if err := _validate_target(target):
            return err
        if alert_add(user_id, target, "above"):
            return (
                f"✅ ตั้งการแจ้งเตือนสำเร็จ!\n"
                f"📈 จะแจ้งเมื่อราคาขึ้นถึง {display}\n"
                f"🕐 ตรวจสอบราคาทุก 5 นาที"
            )
        return "❌ เกิดข้อผิดพลาด กรุณาลองใหม่"

    # ── ดูรายการ ──────────────────────────────────────────────────────
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

    # ── ลบรายการเดียว ────────────────────────────────────────────────
    m = RE_DELETE_INDEX.match(lower)
    if m:
        idx  = int(m.group(1))
        rows = alert_list(user_id)
        if not rows:
            return "📭 ไม่มีการแจ้งเตือนที่ตั้งไว้"
        if not (1 <= idx <= len(rows)):
            return f"❌ กรุณาระบุหมายเลข 1-{len(rows)}"
        a        = rows[idx - 1]
        dir_text = "ขึ้นถึง" if a.direction == "above" else "ลงต่ำกว่า"
        alert_delete_id(a.id)
        return f"🗑️ ลบการแจ้งเตือน {dir_text} ${a.target_price:,.2f} แล้ว"

    # ── ยกเลิกทั้งหมด ─────────────────────────────────────────────────
    if any(kw in lower for kw in ["ยกเลิก", "cancel", "ลบการแจ้งเตือน"]):
        if alert_delete_all(user_id):
            return "🗑️ ลบการแจ้งเตือนทั้งหมดแล้ว"
        return "❌ เกิดข้อผิดพลาด"

    # ── ราคาทอง ───────────────────────────────────────────────────────
    if any(kw in lower for kw in ["ราคาทอง", "gold", "xau", "xauusd"]):
        gold = get_gold()
        if not gold:
            return "❌ ไม่สามารถดึงข้อมูลได้ในขณะนี้\nกรุณาลองใหม่อีกครั้งนะ"
        return format_gold(gold)

    # ── Help ──────────────────────────────────────────────────────────
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


# =========================================================
# Routes
# =========================================================

@app.route("/")
def home():
    return "GoldBot Running 🥇", 200


@app.route("/ping")
def ping():
    return "pong", 200


@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body      = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"


@app.route("/internal/check-alerts", methods=["POST"])
def internal_check_alerts():
    """
    จากโค้ด 1: Render Cron Job เรียก endpoint นี้แทน APScheduler
    แม่นยำกว่า scheduler ใน free tier ที่อาจ drift
    ต้องส่ง header: X-Internal-Secret
    """
    if not INTERNAL_SECRET:
        return "INTERNAL_SECRET not configured", 500
    if request.headers.get("X-Internal-Secret") != INTERNAL_SECRET:
        return "Unauthorized", 403
    check_alerts()
    return "OK", 200


@app.route("/metrics")
def metrics():
    """Protected endpoint — ต้องส่ง header: X-Metrics-Token"""
    if METRICS_TOKEN:
        if request.headers.get("X-Metrics-Token") != METRICS_TOKEN:
            abort(403)
    return jsonify({
        "webhook_max_workers":   getattr(webhook_executor, "_max_workers", None),
        "fetch_max_workers":     getattr(fetch_pool,       "_max_workers", None),
        "db_max_workers":        getattr(_db_executor,     "_max_workers", None),
        "cache_entries":         http.cache_size,
        "circuit_breakers_open": http.open_circuits,
        "cooldown_entries":      len(_COOLDOWN),
        "supabase_connected":    supabase is not None,
        "scheduler_running":     scheduler.running,
        **cache_metrics.snapshot(),
        "timestamp":             datetime.now(BANGKOK_TZ).isoformat(),
    })


# =========================================================
# LINE Event Handler
# =========================================================

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    text     = event.message.text.strip()
    user_id  = event.source.user_id
    is_group = event.source.type in ["group", "room"]

    if is_group:
        if not text.startswith(WAKE_WORD):
            return
        text = text[len(WAKE_WORD):].strip() or "ราคาทอง"

    if not WEBHOOK_SEMAPHORE.acquire(blocking=False):
        push_safe(user_id, "⏳ ระบบยุ่งอยู่ กรุณาลองใหม่อีกสักครู่")
        return

    token = event.reply_token

    def _task():
        start = time.perf_counter()
        try:
            result  = process_message(text, user_id)
            elapsed = time.perf_counter() - start
            logger.info(f"process_message {elapsed:.3f}s uid={user_id[:8]}...")
            if not reply_safe(token, result):
                push_safe(user_id, result)
        except Exception as e:
            logger.exception(f"_task failed: {e}")
        finally:
            WEBHOOK_SEMAPHORE.release()

    webhook_executor.submit(_task)


# =========================================================
# Graceful Shutdown
# =========================================================

def _do_shutdown():
    logger.info("Shutdown initiated...")
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


atexit.register(_do_shutdown)   # ครอบคลุมทั้ง gunicorn และ direct mode


# =========================================================
# Scheduler (APScheduler เป็น fallback)
# =========================================================

scheduler = BackgroundScheduler(timezone="Asia/Bangkok")
scheduler.add_job(
    check_alerts,
    "interval",
    minutes=5,
    id="check_alerts",
    max_instances=1,
    coalesce=True,
    replace_existing=True,
    jitter=30,          # กัน thundering herd
)


# =========================================================
# Entry Point
# =========================================================

if __name__ == "__main__":
    verify_schema()
    if not scheduler.running:
        scheduler.start()
    logger.info("🚀 GoldBot started (direct mode)")
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
    # gunicorn mode — atexit.register ครอบคลุม shutdown แล้ว
    verify_schema()
    if not scheduler.running:
        scheduler.start()
        logger.info("🚀 GoldBot started (gunicorn mode)")
