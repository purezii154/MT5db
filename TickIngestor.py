import MetaTrader5 as mt5
import time
import threading
import requests # <--- ใช้ Library requests แทน
import datetime # <--- ใช้สำหรับสร้าง Timestamp

# ==========================================================
# == ไม่ต้อง Import Sender จาก questdb แล้ว ===
# ==========================================================
# (เราจะสร้าง Line Protocol string เอง)

# ==============================================================================
# == CLASS DEFINITION: TickIngestor
# ==============================================================================
class TickIngestor:
    """
    ดึงข้อมูล Tick Data จริงจาก MT5 แล้วยิง (ingest) เข้า QuestDB
    โดยใช้ HTTP POST และ Line Protocol
    """
    def __init__(self, symbol):
        self.symbol = symbol
        self.is_connected = False
        self._connect()

    def _connect(self):
        """เชื่อมต่อกับ MT5 ที่รันอยู่"""
        if mt5.initialize():
            print(f"[{self.symbol}] เชื่อมต่อ MT5 สำเร็จ (Ingestor)")
            self.is_connected = True
        else:
            print(f"[{self.symbol}] เชื่อมต่อ MT5 ล้มเหลว (Ingestor)")
            self.is_connected = False

    def run(self):
        """Main loop: ดึง Tick และส่งเข้า QuestDB"""
        if not self.is_connected:
            print(f"[{self.symbol}] ไม่สามารถเริ่มทำงานได้: การเชื่อมต่อล้มเหลว")
            return

        # URL สำหรับส่งข้อมูลเข้า QuestDB (ผ่าน HTTP port 9000)
        questdb_url = "http://localhost:9000/write" # <--- ใช้ Port 9000

        print(f"[{self.symbol}] เตรียมส่งข้อมูลไปยัง QuestDB ที่ {questdb_url}")
        last_tick_time = 0

        while True:
            tick = mt5.symbol_info_tick(self.symbol)

            if tick is not None and tick.time_msc > last_tick_time:
                last_tick_time = tick.time_msc

                try:
                    # ============================================================
                    # == นี่คือจุดแก้ไข: สร้าง Line Protocol String เอง ==
                    # ============================================================
                    # รูปแบบ: table_name,tag_key=tag_value field_key=field_value timestamp_nanos
                    timestamp_ns = tick.time_msc * 1_000_000 # Milliseconds to Nanoseconds
                    line_protocol_data = f"ticks,symbol={self.symbol} bid={tick.bid},ask={tick.ask} {timestamp_ns}"

                    # ============================================================
                    # == นี่คือจุดแก้ไข: ส่งข้อมูลด้วย requests.post ==
                    # ============================================================
                    response = requests.post(questdb_url, data=line_protocol_data.encode('utf-8'))
                    response.raise_for_status() # เช็คว่าส่งสำเร็จหรือไม่ (ถ้าไม่สำเร็จจะโยน Error)

                    print(f"[{self.symbol}] Tick! Bid: {tick.bid}, Ask: {tick.ask}")

                except requests.exceptions.RequestException as e:
                    print(f"[{self.symbol}] เกิดข้อผิดพลาดในการส่งข้อมูล QuestDB (HTTP): {e}")
                except Exception as e: # ดัก Error ทั่วไป
                    print(f"[{self.symbol}] เกิดข้อผิดพลาดอื่นๆ: {e}")

            time.sleep(0.01) # หน่วงเวลาเล็กน้อย


# ==============================================================================
# == MAIN PROGRAM (เหมือนเดิม)
# ==============================================================================
if __name__ == "__main__":

    symbols_to_track = ["BTC", "EURUSD", "GBPUSD"]
    threads = []

    if not mt5.initialize():
        print("ไม่สามารถเชื่อมต่อ MT5 หลักได้! (โปรดตรวจสอบ MT5)")
        exit()

    print("MT5 (Main) เชื่อมต่อสำเร็จ... กำลังเริ่ม Ingestor Threads...")

    for symbol in symbols_to_track:
        if not mt5.symbol_select(symbol, True):
            print(f"ไม่สามารถเปิดสัญลักษณ์ {symbol} ใน Market Watch ได้ (โปรดตรวจสอบ MT5)")
            continue

        ingestor_bot = TickIngestor(symbol=symbol)
        t = threading.Thread(target=ingestor_bot.run, daemon=True)
        threads.append(t)
        t.start()
        time.sleep(0.5)

    print(f"บอท Ingestor ทั้ง {len(threads)} ตัว กำลังทำงาน...")

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("กำลังหยุดการทำงาน...")
        mt5.shutdown() # ปิด MT5 ด้วย