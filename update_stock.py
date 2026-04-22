import yfinance as yf
import gspread
import os
import json
import time

def update_stock():
    try:
        # 1. ดึงกุญแจจาก GitHub Secrets
        creds_json = os.environ.get("GOOGLE_SHEETS_CREDS")
        if not creds_json:
            print("Error: ไม่พบกุญแจใน Secrets")
            return
            
        creds_dict = json.loads(creds_json)
        gc = gspread.service_account_from_dict(creds_dict)

        # 2. เปิดไฟล์ Google Sheets (แก้ชื่อให้ตรงกับไฟล์ของคุณ)
        # อย่าลืมกด Share ชีตนี้ให้ Email ของบอตด้วยนะ!
        sh = gc.open("stock-updater") 
        worksheet = sh.get_worksheet(0)

        # 3. รายชื่อหุ้นทั้งหมดที่ต้องการ (เติม .BK ให้เรียบร้อย)
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
        
        # 4. เตรียมข้อมูลเพื่ออัปเดตแบบทีละเยอะๆ (Batch Update) เพื่อความรวดเร็ว
        values = []
        for symbol in stocks:
            try:
                ticker = yf.Ticker(symbol)
                # ดึงราคาปัจจุบัน
                price = ticker.fast_info['last_price']
                values.append([price])
                print(f"ดึงข้อมูลสำเร็จ: {symbol} = {price}")
            except Exception as e:
                values.append(["N/A"])
                print(f"ไม่พบข้อมูลหุ้น: {symbol} ({e})")
            
            # ป้องกันโดนแบนจากการดึงข้อมูลเร็วเกินไป
            time.sleep(0.2)

        # 5. เขียนลงในคอลัมน์ AI (เริ่มตั้งแต่ AI2 เป็นต้นไป)
        # ใช้ range AI2:AI... ตามจำนวนหุ้น
        range_to_update = f"AI2:AI{1 + len(stocks)}"
        worksheet.update(range_to_update, values)

        print(f"--- อัปเดตราคาหุ้น {len(stocks)} ตัว ลงในคอลัมน์ AI เรียบร้อยแล้ว ---")

    except Exception as e:
        print(f"เกิดข้อผิดพลาดใหญ่: {e}")

if __name__ == "__main__":
    update_stock()
