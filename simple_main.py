print("[BOOT] FILE EXECUTED", flush=True)

import os
import time
import threading
import requests
from flask import Flask

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
PORT = int(os.getenv("PORT", "10000"))

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running", 200

def run_web():
    print(f"[WEB] basic health server listening on port {PORT}", flush=True)
    app.run(host="0.0.0.0", port=PORT)

def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("[WARN] Missing TELEGRAM_TOKEN or CHAT_ID", flush=True)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": message}, timeout=10)
        print(f"[TELEGRAM] status={r.status_code}", flush=True)
    except Exception as e:
        print("[ERROR] Telegram failed:", e, flush=True)

def run_scanner():
    print("[SCAN] Cycle started", flush=True)
    print("[SCAN] Running scan placeholder", flush=True)
    print("[SCAN] Cycle complete", flush=True)

def scanner_loop():
    print("[BOOT] Scanner started", flush=True)

    if not FINNHUB_API_KEY:
        print("[BOOT] Missing FINNHUB_API_KEY. Scanner cannot start.", flush=True)
        return

    print("[BOOT] FINNHUB_API_KEY loaded", flush=True)
    send_telegram("✅ Scanner bot started")

    while True:
        print("[HEARTBEAT] alive", flush=True)
        run_scanner()
        time.sleep(10)

if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    scanner_loop()

