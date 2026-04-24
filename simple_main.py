import os
import time
import requests

# Get secrets from Render environment
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

WATCHLIST = ["SOUN", "RGTI", "AKAN"]


def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("Missing TELEGRAM_TOKEN or CHAT_ID")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}

    try:
        requests.post(url, data=data)
    except Exception as e:
        print("Telegram failed:", e)


def run_scanner():
    print("Running scan...")

    for symbol in WATCHLIST:
        message = f"""🚨 TEST ALERT

{symbol}
Bot is running.
"""
        send_telegram(message)


if __name__ == "__main__":
    send_telegram("✅ Bot started")

    while True:
        run_scanner()
        time.sleep(60)

