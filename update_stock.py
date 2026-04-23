import gspread
import os
import json
import time
import requests

def get_stock_price(symbol):
    try:
        # ใช้ดึงราคาจาก Yahoo Finance แบบไม่ผ่าน Library (ป้องกัน Error websockets)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        data = response.json()
        price = data['chart']['result'][0]['meta']['regularMarketPrice']
        return price
    except:
        return "N/A"

def update_stock():
    try:
        creds_json = os.environ.get("GOOGLE_SHEETS_CREDS")
        if not creds_json:
            print("Error: ไม่พบกุญแจใน Secrets")
            return
            
        creds_dict = json.loads(creds_json)
        gc = gspread.service_account_from_dict(creds_dict)

        sh = gc.open("stock-updater") 
        worksheet = sh.get_worksheet(0)

        stocks = [
            "THAIBEV19.BK", "DBS19.BK", "UOB19.BK", "SEMB19.BK", "SGX19.BK",
            "FERRARI80.BK", "HERMES80.BK", "LOREAL80.BK", "SANOFI80.BK", "NOVOB80.BK",
            "TRIPCOM80.BK", "POPMART80.BK", "MEITUAN80.BK", "JD80.BK", "NONGFU80.BK",
            "SMIC23.BK", "KUAISH23.BK", "HUAHONG23.BK", "AIA23.BK", "HKEX23.BK",
            "VNM19.BK", "FPTVN19.BK", "VCB19.BK", "MWG19.BK", "GEELY80.BK",
            "ADVANT19.BK", "ADVANT23.BK", "HONDA19.BK", "ITOCHU19.BK", "KEYENCE23.BK",
            "MITSU19.BK", "MUFG19.BK", "NINTENDO19.BK", "SANRIO23.BK", "SMFG19.BK",
            "SOFTBANK23.BK", "SUSHI23.BK", "TEL23.BK", "TOYOTA80.BK", "UNIQLO80.BK",
            "ASML01.BK", "XIAOMI80.BK", "TENCENT80.BK", "PINGAN80.BK", "SINGTEL80.BK",
            "NETEASE80.BK", "VENTURE19.BK", "STEG19.BK"
        ]
        
        values = []
        for symbol in stocks:
            price = get_stock_price(symbol)
            values.append([price])
            print(f"Fetched {symbol}: {price}")
            time.sleep(0.5)

        range_to_update = f"AI2:AI{1 + len(stocks)}"
        worksheet.update(range_to_update, values)
        print("--- Update Completed ---")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_stock()
