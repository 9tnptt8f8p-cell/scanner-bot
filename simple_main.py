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
    print(f"[WEB] basic health server listening on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)


def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("[WARN] Missing TELEGRAM_TOKEN or CHAT_ID")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": message}, timeout=10)
    except Exception as e:
        print("[ERROR] Telegram failed:", e)


def run_scanner():
    print("[SCAN] Running scan...")
    # Real scanner logic goes here.
    # No repeated test alerts.


def scanner_loop():
    print("[BOOT] Scanner started")

    if not FINNHUB_API_KEY:
        print("[BOOT] Missing FINNHUB_API_KEY. Scanner cannot start.")
        return

    send_telegram("✅ Scanner bot started")

    while True:
        run_scanner()
        time.sleep(60)


if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    scanner_loop()

