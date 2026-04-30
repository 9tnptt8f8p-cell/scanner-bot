import os
import requests

TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")  # comma separated

def get_chat_ids():
    ids = []

    if CHAT_IDS:
        ids.extend([x.strip() for x in CHAT_IDS.split(",") if x.strip()])

    if CHAT_ID:
        ids.append(CHAT_ID.strip())

    return list(set(ids))  # remove duplicates


def send_alert(message):
    chat_ids = get_chat_ids()

    print(f"[TELEGRAM DEBUG] token={bool(TOKEN)} chats={chat_ids}")

    if not TOKEN or not chat_ids:
        print("[ALERT LOCAL]", message)
        return False

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

    success = True

    for chat_id in chat_ids:
        try:
            r = requests.post(
                url,
                json={"chat_id": chat_id, "text": message},
                timeout=10
            )

            print(f"[TELEGRAM RESPONSE] chat={chat_id} status={r.status_code}")

            if r.status_code != 200:
                success = False

        except Exception as e:
            print(f"[TELEGRAM ERROR] {e}")
            success = False

    return success
