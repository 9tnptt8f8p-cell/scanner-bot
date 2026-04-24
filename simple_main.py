import time
import requests

TELEGRAM_TOKEN = "PASTE_YOUR_TOKEN_HERE"
CHAT_ID = "PASTE_YOUR_CHAT_ID_HERE"

WATCHLIST = ["SOUN", "RGTI", "AKAN"]

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, data=data)
    except:
        print("Telegram failed")

def run_scanner():
    print("Running scan...")

    for symbol in WATCHLIST:
        message = f"""🚨 TEST ALERT

{symbol}
Bot is running.
"""
        send_telegram(message)

[4/24/2026 7:32 AM] Blackmonday: TELEGRAM_TOKEN=8710285209:AAHt42ariJ2McMPqNGrDmqR35Lmad_eiI6g
CHAT_ID=1296637203,1184831083
[4/24/2026 7:41 AM] Blackmonday: if name == "__main__":
    send_telegram("✅ Bot started")

    while True:
        run_scanner()
        time.sleep(60)
