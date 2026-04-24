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


# Track recent alerts to avoid spam
last_alert_time = {}

TICKERS = [
    "AKAN","AUUD","SOUN","RGTI","PLTR",
    "SKLZ","CPXI","EUDA","PAPL","MITI"
]


def fetch_quote(ticker):
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        print(f"[DEBUG] {ticker} quote: {data}", flush=True)
        return data
    except Exception as e:
        print(f"[ERROR] {ticker} fetch failed: {e}", flush=True)
        return None


def score_ticker(price, prev_close):
    if not price or not prev_close or prev_close == 0:
        return 0, 0

    pct = ((price - prev_close) / prev_close) * 100
    score = 0

    if pct >= 10:
        score += 3
    elif pct >= 5:
        score += 2

    if price > 0:
        score += 2

    if price < 20:
        score += 1

    return score, pct


def run_scanner():
    print("[SCAN] Cycle started", flush=True)

    best = None
    best_score = 0

    for ticker in TICKERS:
        data = fetch_quote(ticker)

        if not data or data.get("c") in [0, None]:
            print(f"[SCAN] Skipping {ticker}: invalid live quote", flush=True)
            continue

        price = data.get("c")
        prev_close = data.get("pc")

        score, pct = score_ticker(price, prev_close)

        print(f"[SCAN] {ticker} price={price} pct={pct:.2f} score={score}", flush=True)

        if score > best_score:
            best_score = score
            best = (ticker, price, pct, score)

    if best and best_score >= 7:
        ticker, price, pct, score = best

        now = time.time()
        last_time = last_alert_time.get(ticker, 0)

        if now - last_time > 1800:  # 30 min cooldown
            msg = (
                f"🚨 {ticker}\n"
                f"Price: {price}\n"
                f"Change: {pct:.2f}%\n"
                f"Score: {score}"
            )

            send_telegram(msg)
            last_alert_time[ticker] = now

            print(f"[ALERT] Sent: {ticker}", flush=True)
        else:
            print(f"[SCAN] Skipped alert (cooldown): {ticker}", flush=True)

        print(f"[SCAN] Top ranked: {ticker} score={score} price={price}", flush=True)
    else:
        print("[SCAN] Top ranked: none", flush=True)

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
        time.sleep(15)  # fast testing


if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    scanner_loop()
