import os
import time
import requests
from collections import defaultdict, deque
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ======================
# SETTINGS
# ======================

SCAN_SECONDS = 30

FAST_MOVE_PCT = 12
EARLY_VOLUME = 50_000
FAST_VOLUME = 150_000
MOMO_VOLUME = 300_000

COOLDOWN_SECONDS = 300  # 5 min per ticker

WATCHLIST = [
    "ATOM", "AKAN", "AUUD", "PAPL", "SOUN", "SKLZ",
    "RGTI", "PLTR", "CPXI", "EUDA", "MITI"
]

# stores recent prices per ticker
price_history = defaultdict(lambda: deque(maxlen=20))
last_alert_time = {}


# ======================
# TELEGRAM
# ======================

def send_alert(message):
    print(message)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[ALERT] Missing Telegram token/chat id")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message
            },
            timeout=10
        )
    except Exception as e:
        print(f"[ALERT ERROR] {e}")


# ======================
# FINNHUB DATA
# ======================

def get_quote(symbol):
    url = "https://finnhub.io/api/v1/quote"

    try:
        r = requests.get(
            url,
            params={
                "symbol": symbol,
                "token": FINNHUB_API_KEY
            },
            timeout=10
        )

        data = r.json()

        price = data.get("c")
        prev_close = data.get("pc")

        if not price or not prev_close:
            return None

        return {
            "symbol": symbol,
            "price": float(price),
            "prev_close": float(prev_close),
            "daily_pct": ((float(price) - float(prev_close)) / float(prev_close)) * 100
        }

    except Exception as e:
        print(f"[QUOTE ERROR] {symbol}: {e}")
        return None


def get_volume(symbol):
    """
    Finnhub free quote endpoint does not always give reliable live volume.
    This tries candle volume from today's 1-minute data.
    """
    now = int(time.time())
    start = now - 60 * 30

    url = "https://finnhub.io/api/v1/stock/candle"

    try:
        r = requests.get(
            url,
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
        return int(sum(volumes))

    except Exception as e:
        print(f"[VOLUME ERROR] {symbol}: {e}")
        return 0


# ======================
# ALERT LOGIC
# ======================

def can_alert(symbol):
    now = time.time()

    if symbol not in last_alert_time:
        return True

    return now - last_alert_time[symbol] >= COOLDOWN_SECONDS


def mark_alerted(symbol):
    last_alert_time[symbol] = time.time()


def check_fast_move(symbol, price, daily_pct, volume):
    now = time.time()

    price_history[symbol].append((now, price))

    if len(price_history[symbol]) < 2:
        return

    old_time, old_price = price_history[symbol][0]

    if old_price <= 0:
        return

    quick_pct = ((price - old_price) / old_price) * 100

    # Use whichever is stronger: quick intraday move or daily move
    move_pct = max(quick_pct, daily_pct)

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

    msg = (
        f"{label}\n"
        f"{symbol}\n"
        f"Move: +{move_pct:.1f}%\n"
        f"Price: ${price:.4f}\n"
        f"30m Volume: {volume:,}\n"
        f"Alert rule: +12% move / 50k+ volume"
    )

    send_alert(msg)
    mark_alerted(symbol)


# ======================
# SCANNER LOOP
# ======================

def run_scanner():
    if not FINNHUB_API_KEY:
        print("[BOOT] Missing FINNHUB_API_KEY")
        return

    send_alert("✅ Scanner started — fast move alerts active")

    while True:
        print("[SCAN] Starting cycle")

        for symbol in WATCHLIST:
            quote = get_quote(symbol)

            if not quote:
                print(f"[SCAN] No quote for {symbol}")
                continue

            price = quote["price"]
            daily_pct = quote["daily_pct"]
            volume = get_volume(symbol)

            print(
                f"[SCAN] {symbol} ${price:.4f} "
                f"daily={daily_pct:.1f}% vol30m={volume:,}"
            )

            # IMPORTANT:
            # This runs BEFORE any score filter.
            check_fast_move(
                symbol=symbol,
                price=price,
                daily_pct=daily_pct,
                volume=volume
            )

            time.sleep(1)

        print("[SCAN] Cycle complete")
        time.sleep(SCAN_SECONDS)


if __name__ == "__main__":
    run_scanner()

