import os
import time
import requests
from collections import defaultdict, deque
from dotenv import load_dotenv

load_dotenv()

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


# ======================
# ALERT
# ======================

def send_alert(message):
    print(f"[TELEGRAM] {message}")

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARNING] Telegram not configured")
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=10
        )
    except Exception as e:
        print(f"[ERROR][ALERT] {e}")


# ======================
# MOVERS (FMP)
# ======================

def get_top_movers():
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
        print(f"[ERROR][GAINERS] {e}")
        return []


def get_symbols_to_scan():
    symbols = get_top_movers()

    print("\n==================== NEW SCAN ====================")
    print(f"[SYMBOLS] Scanning {len(symbols)} movers")

    if not symbols:
        print("[WARNING] No movers returned")

    return symbols


# ======================
# DATA (FINNHUB)
# ======================

def get_quote(symbol):
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": symbol, "token": FINNHUB_API_KEY},
            timeout=10
        )

        data = r.json()

        price = data.get("c")
        prev_close = data.get("pc")

        if not price or not prev_close:
            return None

        price = float(price)
        prev_close = float(prev_close)

        if price <= 0 or prev_close <= 0:
            return None

        return {
            "price": price,
            "daily_pct": ((price - prev_close) / prev_close) * 100
        }

    except Exception as e:
        print(f"[ERROR][QUOTE] {symbol}: {e}")
        return None


def get_30m_volume(symbol):
    now = int(time.time())
    start = now - 60 * 30

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

        volumes = data.get("v", [])
        return int(sum(volumes)) if volumes else 0

    except Exception as e:
        print(f"[ERROR][VOLUME] {symbol}: {e}")
        return 0


# ======================
# ALERT LOGIC
# ======================

def can_alert(symbol):
    now = time.time()
    return symbol not in last_alert_time or now - last_alert_time[symbol] > COOLDOWN_SECONDS


def mark_alert(symbol):
    last_alert_time[symbol] = time.time()


def check_fast_move(symbol, price, daily_pct, volume):
    now = time.time()
    price_history[symbol].append((now, price))

    quick_pct = 0
    if len(price_history[symbol]) >= 2:
        old_time, old_price = price_history[symbol][0]
        if old_price > 0:
            quick_pct = ((price - old_price) / old_price) * 100

    move_pct = max(daily_pct, quick_pct)

    if move_pct < FAST_MOVE_PCT or volume < EARLY_VOLUME or not can_alert(symbol):
        return

    if volume >= MOMO_VOLUME:
        label = "🔥 MOMO RUNNER"
    elif volume >= FAST_VOLUME:
        label = "🚨 FAST MOVE"
    else:
        label = "⚠️ EARLY SPIKE"

    print(f"[ALERT] {symbol} TRIGGERED {label} (+{move_pct:.1f}%)")

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
    mark_alert(symbol)


# ======================
# MAIN LOOP
# ======================

def run():
    if not FINNHUB_API_KEY:
        print("[ERROR] Missing FINNHUB_API_KEY")
        return

    send_alert("🚀 SCANNER STARTED — live movers / 12% alerts active")

    while True:
        symbols = get_symbols_to_scan()

        for symbol in symbols:
            quote = get_quote(symbol)
            if not quote:
                continue

            price = quote["price"]
            daily_pct = quote["daily_pct"]
            volume = get_30m_volume(symbol)

            print(
                f"[SCAN] {symbol:<6} | "
                f"Price: ${price:<8.4f} | "
                f"Daily: {daily_pct:>6.1f}% | "
                f"Vol30m: {volume:>8,}"
            )

            check_fast_move(symbol, price, daily_pct, volume)

            time.sleep(0.5)

        print("[SCAN] Cycle complete")
        time.sleep(SCAN_SECONDS)


if __name__ == "__main__":
    run()

