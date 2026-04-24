import os
import time
import requests
from threading import Thread
from flask import Flask
from dotenv import load_dotenv

load_dotenv()

# ============================
# BOOT MARKER
# ============================

BOOT_MARKER = "quote-only rebuild 2026-04-24 v3"

# ============================
# ENV
# ============================

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")

# ============================
# SETTINGS
# ============================

MIN_GAIN = 30
MIN_SCORE = 7
SCAN_SLEEP = 180
ALERT_COOLDOWN_SECONDS = 900

WATCHLIST = [
    "AKAN", "AUUD", "SOUN", "RGTI", "PLTR",
    "SKLZ", "CPXI", "EUDA", "PAPL", "MITI"
]

# ============================
# FLASK HEALTH SERVER
# ============================

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot alive"

@app.route("/health")
def health():
    return "OK"

# ============================
# TELEGRAM
# ============================

def get_chat_ids():
    ids = []

    if TELEGRAM_CHAT_IDS:
        ids.extend([x.strip() for x in TELEGRAM_CHAT_IDS.split(",") if x.strip()])

    if TELEGRAM_CHAT_ID:
        ids.append(TELEGRAM_CHAT_ID.strip())

    return list(set(ids))


def send_telegram(message):
    chat_ids = get_chat_ids()

    print(f"[TELEGRAM DEBUG] token_exists={bool(TELEGRAM_BOT_TOKEN)} chat_ids={chat_ids}")

    if not TELEGRAM_BOT_TOKEN or not chat_ids:
        print("[ALERT LOCAL]", message)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    ok = True

    for chat_id in chat_ids:
        try:
            r = requests.post(
                url,
                json={
                    "chat_id": chat_id,
                    "text": message
                },
                timeout=10
            )

            print(f"[TELEGRAM RESPONSE] chat={chat_id} status={r.status_code} body={r.text}")

            if r.status_code != 200:
                ok = False

        except Exception as e:
            print(f"[TELEGRAM EXCEPTION] chat={chat_id} error={e}")
            ok = False

    if not ok:
        print("[ALERT LOCAL]", message)

    return ok

# ============================
# FINNHUB QUOTE
# ============================

def get_quote(ticker):
    if not FINNHUB_API_KEY:
        print("[BOOT] Missing FINNHUB_API_KEY")
        return None

    url = "https://finnhub.io/api/v1/quote"
    params = {
        "symbol": ticker,
        "token": FINNHUB_API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        price = float(data.get("c") or 0)
        prev_close = float(data.get("pc") or 0)
        timestamp = int(data.get("t") or 0)

        if price <= 0 or prev_close <= 0:
            return None

        daily_gain = ((price - prev_close) / prev_close) * 100

        return {
            "ticker": ticker,
            "price": price,
            "prev_close": prev_close,
            "daily_gain": daily_gain,
            "timestamp": timestamp,
            "raw": data
        }

    except Exception as e:
        print(f"[QUOTE ERROR] {ticker}: {e}")
        return None


def is_stale_quote(quote, max_age_seconds=900):
    timestamp = quote.get("timestamp", 0)

    if timestamp <= 0:
        return True

    age = time.time() - timestamp

    return age > max_age_seconds

# ============================
# NEWS / CATALYST
# ============================

def get_news_catalyst(ticker):
    if not FINNHUB_API_KEY:
        return "unknown", "Missing Finnhub key"

    today = time.strftime("%Y-%m-%d")

    url = "https://finnhub.io/api/v1/company-news"
    params = {
        "symbol": ticker,
        "from": today,
        "to": today,
        "token": FINNHUB_API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        news = r.json()

        if not isinstance(news, list) or not news:
            return "none", "No fresh catalyst found"

        headline = news[0].get("headline", "")
        h = headline.lower()

        if "earnings" in h or "results" in h:
            return "earnings", headline

        if "patent" in h:
            return "patent", headline

        if "contract" in h or "agreement" in h:
            return "contract", headline

        if "fda" in h or "trial" in h:
            return "biotech", headline

        if "lawsuit" in h or "jury" in h or "damages" in h:
            return "legal", headline

        if "offering" in h or "warrant" in h or "registered direct" in h:
            return "offering", headline

        return "news", headline

    except Exception as e:
        print(f"[NEWS ERROR] {ticker}: {e}")
        return "unknown", "News check failed"

# ============================
# RISK
# ============================

def check_dilution_risk(text):
    danger_words = [
        "offering",
        "registered direct",
        "securities purchase agreement",
        "warrant",
        "atm",
        "shelf",
        "f-1",
        "s-1",
        "convertible",
        "pipe",
        "reverse split"
    ]

    text = str(text).lower()
    hits = [word for word in danger_words if word in text]

    return len(hits) > 0, hits

# ============================
# SCORING
# ============================

def score_ticker(quote, catalyst_type, catalyst_text):
    score = 0
    reasons = []
    risk_flags = []

    ticker = quote["ticker"]
    price = quote["price"]
    gain = quote["daily_gain"]

    if gain >= 100:
        score += 4
        reasons.append("100%+ move")
    elif gain >= 75:
        score += 3
        reasons.append("75%+ move")
    elif gain >= 50:
        score += 2
        reasons.append("50%+ move")
    elif gain >= 30:
        score += 1
        reasons.append("30%+ move")

    if catalyst_type not in ["none", "unknown"]:
        score += 3
        reasons.append("fresh catalyst")
    else:
        risk_flags.append("no clear catalyst")

    if catalyst_type in ["earnings", "patent", "contract", "legal", "biotech"]:
        score += 1
        reasons.append(f"strong catalyst type: {catalyst_type}")

    if price < 1:
        risk_flags.append("sub-$1 stock")

    if price > 50:
        risk_flags.append("high price mover")

    has_dilution, hits = check_dilution_risk(catalyst_text)

    if has_dilution:
        score -= 3
        risk_flags.append("dilution risk: " + ", ".join(hits))

    score = max(0, min(score, 10))

    return {
        "ticker": ticker,
        "price": price,
        "gain": gain,
        "score": score,
        "reasons": reasons,
        "risk_flags": risk_flags,
        "catalyst_type": catalyst_type,
        "catalyst_text": catalyst_text
    }

# ============================
# ALERT MESSAGE
# ============================

def build_alert(result, rank):
    reasons = ", ".join(result["reasons"]) if result["reasons"] else "none"
    risks = ", ".join(result["risk_flags"]) if result["risk_flags"] else "none"

    return f"""
🚨 MOMENTUM ALERT

Rank: #{rank}
{result['ticker']} | Score: {result['score']}/10
Price: ${result['price']:.4f}
Daily Gain: {result['gain']:.1f}%

Catalyst: {result['catalyst_type']}
{result['catalyst_text']}

Reasons: {reasons}
Risk: {risks}
""".strip()

# ============================
# SCANNER LOOP
# ============================

def run_scanner():
    print(f"[BOOT] Scanner started | {BOOT_MARKER}")
    print(f"[BOOT] Watchlist: {', '.join(WATCHLIST)}")

    if not FINNHUB_API_KEY:
        print("[BOOT] Missing FINNHUB_API_KEY. Scanner cannot start.")
        return

    alert_history = {}

    while True:
        print("[SCAN] Cycle started")
        results = []

        for ticker in WATCHLIST:
            quote = get_quote(ticker)

            if not quote:
                print(f"[SKIP] {ticker} no quote")
                continue

            if is_stale_quote(quote):
                print(f"[SKIP] {ticker} stale quote")
                continue

            print(
                f"[SCAN] {ticker:<6} | Price ${quote['price']:<8.4f} | "
                f"Daily {quote['daily_gain']:6.1f}%"
            )

            if quote["daily_gain"] < MIN_GAIN:
                print(
                    f"[FILTER] {ticker} skipped — daily gain "
                    f"{quote['daily_gain']:.1f}% under {MIN_GAIN}%"
                )
                continue

            catalyst_type, catalyst_text = get_news_catalyst(ticker)

            result = score_ticker(
                quote=quote,
                catalyst_type=catalyst_type,
                catalyst_text=catalyst_text
            )

            results.append(result)

            time.sleep(0.5)

        results.sort(key=lambda x: x["score"], reverse=True)

        if results:
            top_line = " | ".join(
                [
                    f"#{i + 1} {r['ticker']} {r['score']}/10 ${r['price']:.2f}"
                    for i, r in enumerate(results[:10])
                ]
            )

            print(f"[SCAN] Top ranked: {top_line}")
        else:
            print("[SCAN] No ranked tickers this cycle")

        now = time.time()

        for rank, result in enumerate(results, start=1):
            ticker = result["ticker"]
            last_alert = alert_history.get(ticker, 0)
            cooldown_left = int(ALERT_COOLDOWN_SECONDS - (now - last_alert))

            if result["score"] >= MIN_SCORE and now - last_alert >= ALERT_COOLDOWN_SECONDS:
                sent = send_telegram(build_alert(result, rank))

                if sent:
                    alert_history[ticker] = now
                    print(f"[ALERT SENT] #{rank} {ticker} score {result['score']}/10")
                else:
                    print(f"[ALERT FAILED] #{rank} {ticker} score {result['score']}/10")

            elif result["score"] >= MIN_SCORE:
                print(f"[NO ALERT] #{rank} {ticker} cooldown active {cooldown_left}s left")

            else:
                print(
                    f"[NO ALERT] #{rank} {ticker} score "
                    f"{result['score']}/10 below MIN_SCORE {MIN_SCORE}"
                )

        print("[SCAN] Cycle complete")
        print("[HEARTBEAT] alive")

        time.sleep(SCAN_SLEEP)

# ============================
# START
# ============================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))

    print(f"[WEB] starting server on port {port}")

    web_thread = Thread(
        target=lambda: app.run(
            host="0.0.0.0",
            port=port,
            debug=False,
            use_reloader=False
        ),
        daemon=True
    )

    web_thread.start()

    time.sleep(2)

    print("[BOOT] starting scanner")
    run_scanner()
