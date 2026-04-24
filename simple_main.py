import os
import time
import requests
from collections import defaultdict, deque

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SCAN_SECONDS = 30
FAST_MOVE_PCT = 12
EARLY_VOLUME = 50000
FAST_VOLUME = 150000
MOMO_VOLUME = 300000
COOLDOWN_SECONDS = 300

price_history = defaultdict(lambda: deque(maxlen=20))
last_alert_time = {}


def send_alert(msg):
    print(f"[ALERT] {msg}", flush=True)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARNING] Telegram not set", flush=True)
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10
        )
    except Exception as e:
        print(f"[ERROR] Telegram: {e}", flush=True)


def get_movers():
    print("[INFO] Fetching movers...", flush=True)

    try:
        url = f"https://financialmodelingprep.com/api/v3/stock_market/gainers?apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=10)
        data = r.json()

        symbols = [x["symbol"] for x in data[:30] if "symbol" in x]

        print(f"[INFO] Movers: {symbols}", flush=True)

        return symbols

    except Exception as e:
        print(f"[ERROR] Movers: {e}", flush=True)
        return []


def get_quote(symbol):
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": symbol, "token": FINNHUB_API_KEY},
            timeout=10
        )
        d = r.json()

        price = d.get("c")
        prev = d.get("pc")

        if not price or not prev:
            return None

        price = float(price)
        prev = float(prev)

        if price <= 0 or prev <= 0:
            return None

        pct = ((price - prev) / prev) * 100

        return price, pct

    except Exception as e:
        print(f"[ERROR] Quote {symbol}: {e}", flush=True)
        return None


def get_volume(symbol):
    now = int(time.time())
    start = now - 1800

    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/candle",
            params={
                "symbol": symbol,
                "resolution": "1",
                "from": start,
                "to": now,
                "token": FINNHUB_API_KEY
            },
            timeout=10
        )

        d = r.json()

        if d.get("s") != "ok":
            return 0

        return int(sum(d.get("v", [])))

    except Exception as e:
        print(f"[ERROR] Volume {symbol}: {e}", flush=True)
        return 0


def can_alert(symbol):
    now = time.time()
    return symbol not in last_alert_time or now - last_alert_time[symbol] > COOLDOWN_SECONDS


def mark_alert(symbol):
    last_alert_time[symbol] = time.time()


def check(symbol, price, daily_pct, volume):
    price_history[symbol].append((time.time(), price))

    quick_pct = 0
    if len(price_history[symbol]) >= 2:
        old_price = price_history[symbol][0][1]
        if old_price > 0:
            quick_pct = ((price - old_price) / old_price) * 100

    move = max(daily_pct, quick_pct)

    if move < FAST_MOVE_PCT or volume < EARLY_VOLUME or not can_alert(symbol):
        return

    if volume >= MOMO_VOLUME:
        tag = "🔥 MOMO"
    elif volume >= FAST_VOLUME:
        tag = "🚨 FAST"
    else:
        tag = "⚠️ EARLY"

    print(f"[TRIGGER] {symbol} {tag} {move:.1f}%", flush=True)

    send_alert(f"{tag} {symbol} +{move:.1f}% | Vol {volume:,}")
    mark_alert(symbol)


def run():
    print("TEST LOG — NEW FILE RUNNING", flush=True)
    print("🚀 MOVERS SCANNER STARTED", flush=True)

    while True:
        print("\n===== NEW SCAN =====", flush=True)

        symbols = get_movers()

        for s in symbols:
            q = get_quote(s)
            if not q:
                print(f"[SKIP] {s}", flush=True)
                continue

            price, pct = q
            vol = get_volume(s)

            print(f"[SCAN] {s} | {pct:.1f}% | Vol {vol:,}", flush=True)

            check(s, price, pct, vol)

            time.sleep(0.5)

        print("[DONE] Cycle complete", flush=True)
        time.sleep(SCAN_SECONDS)


if __name__ == "__main__":
    run()
