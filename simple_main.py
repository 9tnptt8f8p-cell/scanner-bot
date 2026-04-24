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
EARLY_VOLUME = 50_000
FAST_VOLUME = 150_000
MOMO_VOLUME = 300_000
COOLDOWN_SECONDS = 300

price_history = defaultdict(lambda: deque(maxlen=20))
last_alert_time = {}


def send_alert(msg):
    print(f"[TELEGRAM] {msg}")

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARNING] Telegram not configured")
        return

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
        timeout=10
    )


def get_movers():
    if not FMP_API_KEY:
        print("[ERROR] Missing FMP_API_KEY")
        return []

    url = f"https://financialmodelingprep.com/api/v3/stock_market/gainers?apikey={FMP_API_KEY}"

    try:
        r = requests.get(url, timeout=10)
        data = r.json()

        symbols = []
        for item in data[:50]:
            symbol = item.get("symbol")
            if symbol:
                symbols.append(symbol)

        return symbols

    except Exception as e:
        print(f"[ERROR] Movers: {e}")
        return []


def get_quote(symbol):
    if not FINNHUB_API_KEY:
        print("[ERROR] Missing FINNHUB_API_KEY")
        return None

    try:
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": symbol, "token": FINNHUB_API_KEY},
            timeout=10
        )

        data = r.json()

        price = data.get("c")
        previous_close = data.get("pc")

        if not price or not previous_close:
            return None

        price = float(price)
        previous_close = float(previous_close)

        if price <= 0 or previous_close <= 0:
            return None

        daily_pct = ((price - previous_close) / previous_close) * 100

        return price, daily_pct

    except Exception as e:
        print(f"[ERROR] Quote {symbol}: {e}")
        return None


def get_30m_volume(symbol):
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

        data = r.json()

        if data.get("s") != "ok":
            return 0

        return int(sum(data.get("v", [])))

    except Exception as e:
        print(f"[ERROR] Volume {symbol}: {e}")
        return 0


def can_alert(symbol):
    now = time.time()
    return symbol not in last_alert_time or now - last_alert_time[symbol] > COOLDOWN_SECONDS


def mark_alerted(symbol):
    last_alert_time[symbol] = time.time()


def check_fast_move(symbol, price, daily_pct, volume):
    now = time.time()
    price_history[symbol].append((now, price))

    quick_pct = 0

    if len(price_history[symbol]) >= 2:
        old_price = price_history[symbol][0][1]
        if old_price > 0:
            quick_pct = ((price - old_price) / old_price) * 100

    move_pct = max(daily_pct, quick_pct)

    if move_pct < FAST_MOVE_PCT:
        return

    if volume < EARLY_VOLUME:
        return

    if not can_alert(symbol):
        return

    if volume >= MOMO_VOLUME:
        label = "🔥 MOMO RUNNER"
    elif volume >= FAST_VOLUME:
        label = "🚨 FAST MOVE"
    else:
        label = "⚠️ EARLY SPIKE"

    print(f"[TRIGGER] {symbol} {label} +{move_pct:.1f}%")

    msg = (
        f"{label}\n"
        f"{symbol}\n"
        f"Move: +{move_pct:.1f}%\n"
        f"Daily: +{daily_pct:.1f}%\n"
        f"Quick: +{quick_pct:.1f}%\n"
        f"Price: ${price:.4f}\n"
        f"30m Volume: {volume:,}"
    )

    send_alert(msg)
    mark_alerted(symbol)


def run():
    print("🚀 SCANNER STARTED — MOVERS ONLY")
    send_alert("🚀 SCANNER STARTED — MOVERS ONLY / +12% alerts active")

    while True:
        print("\n===== NEW MOVERS SCAN =====")

        symbols = get_movers()
        print(f"[INFO] Scanning {len(symbols)} movers only")

        for symbol in symbols:
            quote = get_quote(symbol)

            if not quote:
                print(f"[SKIP] {symbol} no quote")
                continue

            price, daily_pct = quote
            volume = get_30m_volume(symbol)

            print(
                f"[SCAN] {symbol:<6} | "
                f"Price ${price:<8.4f} | "
                f"Daily {daily_pct:>6.1f}% | "
                f"Vol30m {volume:>8,}"
            )

            check_fast_move(symbol, price, daily_pct, volume)

            time.sleep(0.5)

        print("[SCAN] Movers cycle complete")
        time.sleep(SCAN_SECONDS)


if __name__ == "__main__":
    run()
