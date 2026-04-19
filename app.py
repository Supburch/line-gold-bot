import os
import re
import requests
from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, PushMessageRequest, TextMessage,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from datetime import datetime
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from supabase import create_client

app = Flask(__name__)

# ====== LINE Config ======
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")

# ====== Supabase Config ======
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ====== ดึงราคาทอง ======
def get_gold_price():
    try:
        api_key = os.environ.get("GOLDAPI_KEY", "")
        headers = {"x-access-token": api_key}
        res = requests.get("https://www.goldapi.io/api/XAU/USD", headers=headers, timeout=10)
        data = res.json()
        return float(data["price"])
    except Exception as e:
        print(f"Gold price error: {e}")
        return None


def get_usd_thb_rate():
    try:
        res = requests.get("https://api.frankfurter.app/latest?from=USD&to=THB", timeout=10)
        data = res.json()
        return float(data["rates"]["THB"])
    except:
        return 34.5


def format_gold_message(price_usd, thb_rate):
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    now = datetime.now(bangkok_tz)
    time_str = now.strftime("%d/%m/%Y %H:%M น.")
    return (
        f"🥇 ราคาทองคำ XAUUSD\n"
        f"{'─' * 25}\n"
        f"💵 USD/oz  : ${price_usd:,.2f}\n"
        f"💱 USD/THB : {thb_rate:.2f} บาท\n"
        f"{'─' * 25}\n"
        f"⏰ {time_str}\n"
        f"📊 ข้อมูลจาก: GoldAPI.io"
    )


# ====== จัดการ Alert ======
def add_alert(user_id, target_price, direction):
    if not supabase:
        return False
    try:
        supabase.table("alerts").insert({
            "user_id": user_id,
            "target_price": target_price,
            "direction": direction
        }).execute()
        return True
    except Exception as e:
        print(f"Add alert error: {e}")
        return False


def get_alerts(user_id):
    if not supabase:
        return []
    try:
        res = supabase.table("alerts").select("*").eq("user_id", user_id).execute()
        return res.data
    except:
        return []


def delete_all_alerts(user_id):
    if not supabase:
        return False
    try:
        supabase.table("alerts").delete().eq("user_id", user_id).execute()
        return True
    except:
        return False


def delete_alert_by_id(alert_id):
    try:
        supabase.table("alerts").delete().eq("id", alert_id).execute()
    except:
        pass


# ====== ตรวจ Alert ทุก 5 นาที ======
def check_alerts():
    if not supabase:
        return
    price = get_gold_price()
    if price is None:
        return
    try:
        res = supabase.table("alerts").select("*").execute()
        alerts = res.data
    except:
        return

    triggered = [
        a for a in alerts
        if (a["direction"] == "above" and price >= a["target_price"]) or
           (a["direction"] == "below" and price <= a["target_price"])
    ]

    if not triggered:
        return

    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        for alert in triggered:
            dir_text = "ขึ้นถึง" if alert["direction"] == "above" else "ลงต่ำกว่า"
            msg = (
                f"🔔 แจ้งเตือนราคาทอง!\n"
                f"{'─' * 25}\n"
                f"ราคา XAUUSD {dir_text} ${alert['target_price']:,.2f} แล้ว!\n"
                f"💵 ราคาปัจจุบัน: ${price:,.2f}\n"
                f"{'─' * 25}\n"
                f"⚠️ การแจ้งเตือนนี้ถูกลบออกแล้ว"
            )
            try:
                line_bot_api.push_message(
                    PushMessageRequest(
                        to=alert["user_id"],
                        messages=[TextMessage(text=msg)]
                    )
                )
                delete_alert_by_id(alert["id"])
            except Exception as e:
                print(f"Push error: {e}")


# ====== ประมวลผลข้อความ ======
def handle_message_text(text, user_id):
    lower = text.lower().strip()

    if any(kw in lower for kw in ["ราคาทอง", "ทอง", "gold", "xauusd", "xau", "ราคา"]):
        price = get_gold_price()
        if price is None:
            return "❌ ขออภัย ไม่สามารถดึงข้อมูลได้ อีกสักครู่กรุณาลองใหม่นะ"
        return format_gold_message(price, get_usd_thb_rate())

    match_below = re.search(r'(?:แจ้งเตือนต่ำกว่า|ต่ำกว่า|below|ลง)\s*(\d+(?:\.\d+)?)', lower)
    if match_below:
        target = float(match_below.group(1))
        if add_alert(user_id, target, "below"):
            return (
                f"✅ ตั้งการแจ้งเตือนสำเร็จ!\n"
                f"📉 จะแจ้งเมื่อราคาลงต่ำกว่า ${target:,.2f}\n"
                f"🕐 ตรวจสอบราคาทุก 5 นาที"
            )
        return "❌ เกิดข้อผิดพลาด กรุณาลองใหม่นะ"

    match_above = re.search(r'(?:แจ้งเตือนสูงกว่า|แจ้งเตือน|เตือน|alert|ถึง)\s*(\d+(?:\.\d+)?)', lower)
    if match_above:
        target = float(match_above.group(1))
        if add_alert(user_id, target, "above"):
            return (
                f"✅ ตั้งการแจ้งเตือนสำเร็จ!\n"
                f"📈 จะแจ้งเมื่อราคาขึ้นถึง ${target:,.2f}\n"
                f"🕐 ตรวจสอบราคาทุก 5 นาที"
            )
        return "❌ เกิดข้อผิดพลาด กรุณาลองใหม่นะ"

    if any(kw in lower for kw in ["ดูการแจ้งเตือน", "การแจ้งเตือน", "myalert", "my alert"]):
        alerts = get_alerts(user_id)
        if not alerts:
            return "📭 คุณยังไม่มีการแจ้งเตือนที่ตั้งไว้\n\nพิมพ์ เช่น:\nแจ้งเตือน 3400\nต่ำกว่า 3300"
        lines = ["🔔 การแจ้งเตือนของคุณ:", "─" * 20]
        for i, a in enumerate(alerts, 1):
            dir_text = "📈 ขึ้นถึง ≥" if a["direction"] == "above" else "📉 ลงต่ำกว่า ≤"
            lines.append(f"{i}. {dir_text} ${a['target_price']:,.2f}")
        return "\n".join(lines)

    if any(kw in lower for kw in ["ยกเลิก", "ลบการแจ้งเตือน", "cancel"]):
        if delete_all_alerts(user_id):
            return "🗑️ ลบการแจ้งเตือนทั้งหมดแล้วครับ"
        return "❌ เกิดข้อผิดพลาด"

    return (
        "👋 สวัสดี ! ฉันคือ GoldBot 🥇\n\n"
        "📌 นี่คือคำสั่งที่ใช้กับฉันได้:\n"
        "─────────────────────\n"
        "💰 ขอราคาทอง\n"
        "   → เพื่อดูราคา XAUUSD ณ ปัจจุบัน\n\n"
       "📈 แจ้งเตือนสูงกว่า [ราคา]\n"
        "   → เตือนเมื่อราคาขึ้นถึงราคาที่กำหนด\n\n"
        "📉 แจ้งเตือนต่ำกว่า [ราคา]\n"
        "   → เตือนเมื่อราคาลงต่ำกว่าที่กำหนด\n\n"
        "📋 ดูการแจ้งเตือน\n"
        "   → แสดง alerts ที่ตั้งไว้\n\n"
        "🗑️ ยกเลิกการแจ้งเตือน\n"
        "   → ลบ alerts ทั้งหมด"
    )


# ====== LINE Webhook ======
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"


@app.route("/", methods=["GET"])
def health():
    return "GoldBot is running! 🥇"


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    reply_text = handle_message_text(event.message.text, user_id)
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)],
            )
        )


# ====== Background Scheduler ======
scheduler = BackgroundScheduler(timezone="Asia/Bangkok")
scheduler.add_job(check_alerts, "interval", minutes=5)
scheduler.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
