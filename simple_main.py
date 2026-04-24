import os
import time
import requests
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import defaultdict, deque

print("✅ NEW SIMPLE_MAIN.PY LOADED — MOVERS ONLY + NEWS/DILUTION RANKS", flush=True)

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
SEC_TICKER_CACHE = None


# ======================
# RENDER WEB SERVER
# ======================

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


# ======================
# TELEGRAM
# ======================

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


# ======================
# MOVERS ONLY
# ======================

def get_movers():
    print("[INFO] Fetching movers from FMP...", flush=True)

    if not FMP_API_KEY:
        print("[ERROR] Missing FMP_API_KEY", flush=True)
        return []

    try:
        url = f"https://financialmodelingprep.com/api/v3/stock_market/gainers?apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=10)
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


# ======================
# PRICE / VOLUME
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


# ======================
# NEWS RANK AFTER TRIGGER ONLY
# ======================

def get_news_rank(symbol):
    today = datetime.utcnow().date()
    start = today - timedelta(days=3)

    try:
        r = requests.get(
            "https://finnhub.io/api/v1/company-news",
            params={
                "symbol": symbol,
                "from": start.isoformat(),
                "to": today.isoformat(),
                "token": FINNHUB_API_KEY
            },
            timeout=10
        )

        news = r.json()

        if not isinstance(news, list) or not news:
            return "D", "No clear news found"

        headline = news[0].get("headline", "Headline unavailable")
        text = headline.lower()

        a_words = [
            "fda", "approval", "contract", "partnership", "acquisition",
            "merger", "patent", "positive", "phase", "trial", "award",
            "launch", "record revenue"
        ]

        b_words = [
            "earnings", "revenue", "growth", "guidance", "expands",
            "agreement", "collaboration", "order"
        ]

        weak_words = [
            "announces", "update", "conference", "presentation"
        ]

        if any(w in text for w in a_words):
            return "A", headline

        if any(w in text for w in b_words):
            return "B", headline

        if any(w in text for w in weak_words):
            return "C", headline

        return "C", headline

    except Exception as e:
        print(f"[ERROR] News {symbol}: {e}", flush=True)
        return "D", "News check failed"


# ======================
# DILUTION RANK AFTER TRIGGER ONLY
# ======================

def load_sec_tickers():
    global SEC_TICKER_CACHE

    if SEC_TICKER_CACHE is not None:
        return SEC_TICKER_CACHE

    try:
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "movers-scanner/1.0 contact@example.com"},
            timeout=10
        )

        data = r.json()
        mapping = {}

        for _, item in data.items():
            ticker = item.get("ticker", "").upper()
            cik = str(item.get("cik_str", "")).zfill(10)
            if ticker and cik:
                mapping[ticker] = cik

        SEC_TICKER_CACHE = mapping
        return mapping

    except Exception as e:
        print(f"[ERROR] SEC ticker map: {e}", flush=True)
        SEC_TICKER_CACHE = {}
        return {}


def get_dilution_rank(symbol):
    dilution_words = [
        "offering", "registered direct", "private placement", "atm",
        "at-the-market", "shelf", "s-1", "f-1", "warrant",
        "convertible", "securities purchase agreement", "prospectus",
        "resale", "equity line"
    ]

    try:
        cik = load_sec_tickers().get(symbol.upper())

        if not cik:
            return "C", "Could not verify SEC filings"

        url = f"https://data.sec.gov/submissions/CIK{cik}.json"

        r = requests.get(
            url,
            headers={"User-Agent": "movers-scanner/1.0 contact@example.com"},
            timeout=10
        )

        data = r.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])[:20]
        descriptions = recent.get("primaryDocDescription", [])[:20]

        combined = " ".join(forms + descriptions).lower()

        high_risk_forms = ["s-1", "f-1", "424b", "424b5", "s-3", "f-3"]

        if any(form in combined for form in high_risk_forms):
            return "D", "High risk — recent registration/prospectus-type filing found"

        if any(word in combined for word in dilution_words):
            return "C", "Possible risk — dilution/offering language found"

        return "A", "No obvious recent dilution filing found"

    except Exception as e:
        print(f"[ERROR] Dilution {symbol}: {e}", flush=True)
        return "C", "Dilution check failed"


# ======================
# ALERT LOGIC
# ======================

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

    print(f"[TRIGGER] {symbol} {tag} +{move_pct:.1f}%", flush=True)

    news_rank, news_text = get_news_rank(symbol)
    dilution_rank, dilution_text = get_dilution_rank(symbol)

    msg = (
        f"{tag}\n"
        f"{symbol}\n"
        f"Move: +{move_pct:.1f}%\n"
        f"Daily: +{daily_pct:.1f}%\n"
        f"Quick: +{quick_pct:.1f}%\n"
        f"Price: ${price:.4f}\n"
        f"30m Volume: {volume:,}\n\n"
        f"News Rank: {news_rank}\n"
        f"News: {news_text}\n\n"
        f"Dilution Rank: {dilution_rank}\n"
        f"Dilution: {dilution_text}"
    )

    send_alert(msg)
    mark_alert(symbol)


# ======================
# MAIN
# ======================

def run_scanner():
    print("🚀 MOVERS-ONLY SCANNER STARTED", flush=True)
    send_alert("🚀 Scanner started — movers only / +12% alerts + news/dilution ranks")

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
