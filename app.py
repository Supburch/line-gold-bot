import os
import requests
import yfinance as yf
from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from datetime import datetime
import pytz

app = Flask(__name__)

# ====== ใส่ค่า Token ที่ได้จาก LINE Developers ======
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "YOUR_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "YOUR_CHANNEL_SECRET")

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)


def get_gold_price():
    """ดึงราคาทอง XAUUSD แบบ real-time"""
    try:
        ticker = yf.Ticker("GC=F")  # Gold Futures (ใกล้เคียง spot มาก)
        data = ticker.history(period="1d", interval="1m")
        if data.empty:
            raise ValueError("No data from yfinance")
        price_usd = data["Close"].iloc[-1]
        return float(price_usd)
    except Exception as e:
        print(f"yfinance error: {e}")
        return None


def get_usd_thb_rate():
    """ดึงอัตราแลกเปลี่ยน USD/THB"""
    try:
        ticker = yf.Ticker("USDTHB=X")
        data = ticker.history(period="1d", interval="1m")
        if data.empty:
            raise ValueError("No THB rate")
        rate = data["Close"].iloc[-1]
        return float(rate)
    except Exception as e:
        print(f"THB rate error: {e}")
        return 34.5  # fallback rate


def format_gold_message(price_usd, thb_rate):
    """สร้างข้อความแสดงราคาทอง"""
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    now = datetime.now(bangkok_tz)
    time_str = now.strftime("%d/%m/%Y %H:%M น.")

    # คำนวณราคา
    price_thb_oz = price_usd * thb_rate          # บาท/ทรอยออนซ์
    price_thb_per_gram = price_thb_oz / 31.1035   # บาท/กรัม
    price_thb_per_baht_gold = price_thb_per_gram * 15.244  # บาท/บาทไทย (1 บาทไทย = 15.244 กรัม)

    msg = (
        f"🥇 ราคาทองคำ XAUUSD\n"
        f"{'─' * 25}\n"
        f"💵 USD/oz  : ${price_usd:,.2f}\n"
        f"💱 USD/THB : {thb_rate:.2f} บาท\n"
        f"{'─' * 25}\n"
        f"🇹🇭 บาท/ออนซ์ : ฿{price_thb_oz:,.0f}\n"
        f"🔹 บาท/กรัม  : ฿{price_thb_per_gram:,.2f}\n"
        f"🔸 บาท/บาท   : ฿{price_thb_per_baht_gold:,.0f}\n"
        f"{'─' * 25}\n"
        f"⏰ {time_str}\n"
        f"📊 ข้อมูล: Gold Futures (GC=F)"
    )
    return msg


def handle_message_text(text):
    """ประมวลผลข้อความที่ได้รับ"""
    keywords = ["ราคาทอง", "ทอง", "gold", "xauusd", "xau", "ราคา", "goldprice", "price"]
    lower_text = text.lower().strip()

    is_gold_request = any(kw in lower_text for kw in keywords)

    if is_gold_request:
        price_usd = get_gold_price()
        if price_usd is None:
            return "❌ ขออภัย ไม่สามารถดึงข้อมูลราคาทองได้ในขณะนี้\nกรุณาลองใหม่อีกครั้งครับ"
        thb_rate = get_usd_thb_rate()
        return format_gold_message(price_usd, thb_rate)
    else:
        return (
            "👋 สวัสดีครับ! ผมคือ GoldBot 🥇\n\n"
            "พิมพ์คำเหล่านี้เพื่อดูราคาทอง:\n"
            "• ราคาทอง\n"
            "• ทอง\n"
            "• gold\n"
            "• XAUUSD\n\n"
            "ราคาที่แสดงเป็น XAUUSD แบบ real-time ครับ"
        )


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
    user_text = event.message.text
    reply_text = handle_message_text(user_text)

    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)],
            )
        )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
