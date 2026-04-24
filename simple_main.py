import os
import time
import requests
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import defaultdict, deque

print("✅ NEW SIMPLE_MAIN.PY LOADED — MOVERS ONLY", flush=True)

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


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot running")

    def log_message(self, format, *args):
        return


def start_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("", port), Handler)
    print(f"[WEB] Server running on port {port}", flush=True)
    server.serve_forever()


def send_alert(msg):
    print(f"[ALERT] {msg}", flush=True)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARNING] Telegram not configured", flush=True)
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
    print("[INFO] Fetching movers from FMP...", flush=True)

    if not FMP_API_KEY:
        print("[ERROR] Missing FMP_API_KEY", flush=True)
        return []

    try:
        url = f"https://financialmodelingprep.com/api/v3/stock_market/gainers?apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=10)
        print(f"[FMP STATUS] {r.status_code}", flush=True)

        data = r.json()

        if not isinstance(data, list):
            print(f"[FMP ERROR DATA] {data}", flush=True)
            return []

        symbols = []
        for item in data[:30]:
            symbol = item.get("symbol")
            if symbol:
                symbols.append(symbol)

        print(f"[INFO] Movers only: {symbols}", flush=True)
        return symbols

    except Exception as e:
        print(f"[ERROR] Movers: {e}", flush=True)
        return []


def get_quote(symbol):
    if not FINNHUB_API_KEY:
        print("[ERROR] Missing FINNHUB_API_KEY", flush=True)
        return None

    try:
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": symbol, "token": FINNHUB_API_KEY},
            timeout=10
        )

        data = r.json()
        price = data.get("c")
        prev = data.get("pc")

        if not price or not prev:
            return None

        price = float(price)
        prev = float(prev)

        if price <= 0 or prev <= 0:
            return None

        daily_pct = ((price - prev) / prev) * 100
        return price, daily_pct

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

        data = r.json()

        if data.get("s") != "ok":
            return 0

        return int(sum(data.get("v", [])))

    except Exception as e:
        print(f"[ERROR] Volume {symbol}: {e}", flush=True)
        return 0


def can_alert(symbol):
    now = time.time()
    return symbol not in last_alert_time or now - last_alert_time[symbol] > COOLDOWN_SECONDS


def mark_alert(symbol):
    last_alert_time[symbol] = time.time()


def check_fast_move(symbol, price, daily_pct, volume):
    price_history[symbol].append((time.time(), price))

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
        tag = "🔥 MOMO RUNNER"
    elif volume >= FAST_VOLUME:
        tag = "🚨 FAST MOVE"
    else:
        tag = "⚠️ EARLY SPIKE"

    msg = (
        f"{tag}\n"
        f"{symbol}\n"
        f"Move: +{move_pct:.1f}%\n"
        f"Daily: +{daily_pct:.1f}%\n"
        f"Quick: +{quick_pct:.1f}%\n"
        f"Price: ${price:.4f}\n"
        f"30m Volume: {volume:,}"
    )

    send_alert(msg)
    mark_alert(symbol)


def run_scanner():
    print("🚀 MOVERS-ONLY SCANNER STARTED", flush=True)
    send_alert("🚀 Scanner started — movers only / +12% alerts active")

    while True:
        print("\n===== NEW MOVERS-ONLY SCAN =====", flush=True)

        symbols = get_movers()

        for symbol in symbols:
            quote = get_quote(symbol)

            if not quote:
                print(f"[SKIP] {symbol} no quote", flush=True)
                continue

            price, daily_pct = quote
            volume = get_volume(symbol)

            print(
                f"[SCAN] {symbol:<6} | "
                f"Price ${price:<8.4f} | "
                f"Daily {daily_pct:>6.1f}% | "
                f"Vol30m {volume:>8,}",
                flush=True
            )

            check_fast_move(symbol, price, daily_pct, volume)
            time.sleep(0.5)

        print("[DONE] Movers-only cycle complete", flush=True)
        time.sleep(SCAN_SECONDS)


if __name__ == "__main__":
    threading.Thread(target=start_web_server, daemon=True).start()
    run_scanner()
