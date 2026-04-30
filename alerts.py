import requests
import os

TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

print(f"[TELEGRAM DEBUG] token={bool(TOKEN)} chat_id={bool(CHAT_ID)}")

def send_alert(message):
    if not TOKEN or not CHAT_ID:
        print("[ALERT ERROR] Missing TELEGRAM token or chat id")
        return False

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": message
    }

    try:
        r = requests.post(url, json=payload, timeout=10)

        if r.status_code == 200:
            print("[ALERT SENT]")
            return True
        else:
            print(f"[ALERT ERROR] status={r.status_code} body={r.text}")
            return False

    except Exception as e:
        print(f"[ALERT ERROR] {e}")
        return False
