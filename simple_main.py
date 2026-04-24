import os
import time
import requests
from flask import Flask
from threading import Thread
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MIN_GAIN = 30
MIN_SCORE = 7
SCAN_SLEEP = 60

WATCHLIST = [
    "MXL", "SCNI", "LINT", "CAST", "ATOM", "LIDR", "ONMD", "WYHG", "ENVB",
    "AKAN", "AUUD", "PAPL", "SOUN", "RGTI", "SKLZ", "EUDA"
]


# ----------------------------
# Render health server
# ----------------------------

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot alive"

def run_health_server():
    port = int(os.getenv("PORT", 10000))
    print(f"[WEB] basic health server listening on port {port}")
    app.run(host="0.0.0.0", port=port)


# ----------------------------
# Telegram alerts
# ----------------------------

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[ALERT]", message)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message
        }, timeout=10)
    except Exception as e:
        print(f"[TELEGRAM ERROR] {e}")


# ----------------------------
# Finnhub quote
# ----------------------------

def get_quote(ticker):
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

        if price <= 0 or prev_close <= 0:
            return None

        daily_gain = ((price - prev_close) / prev_close) * 100

        return {
            "ticker": ticker,
            "price": price,
            "prev_close": prev_close,
            "daily_gain": daily_gain,
            "raw": data
        }

    except Exception as e:
        print(f"[QUOTE ERROR] {ticker}: {e}")
        return None


# ----------------------------
# Volume fix system
# ----------------------------

def get_volume_data(ticker, quote_data):
    """
    Fixes Vol30m = 0 issue.

    Finnhub quote endpoint often does NOT provide true intraday volume.
    So this function prevents the bot from blindly killing every ticker
    just because Vol30m is missing.

    It:
    - checks multiple possible volume keys
    - falls back safely
    - flags volume as unconfirmed instead of hard-skipping
    """

    raw = quote_data.get("raw", {})

    vol_30m = (
        raw.get("vol_30m")
        or raw.get("volume_30m")
        or raw.get("v30")
        or quote_data.get("vol_30m")
        or 0
    )

    daily_volume = (
        raw.get("volume")
        or raw.get("daily_volume")
        or raw.get("v")
        or quote_data.get("volume")
        or 0
    )

    try:
        vol_30m = int(float(vol_30m))
    except:
        vol_30m = 0

    try:
        daily_volume = int(float(daily_volume))
    except:
        daily_volume = 0

    estimated = False

    # Fallback: estimate 30m volume from daily volume if available
    if vol_30m == 0 and daily_volume > 0:
        vol_30m = int(daily_volume / 13)
        estimated = True

    volume_missing = vol_30m == 0

    return {
        "vol_30m": vol_30m,
        "daily_volume": daily_volume,
        "volume_missing": volume_missing,
        "estimated": estimated
    }


# ----------------------------
# News / catalyst check
# ----------------------------

def get_news_catalyst(ticker):
    url = "https://finnhub.io/api/v1/company-news"
    today = time.strftime("%Y-%m-%d")
    params = {
        "symbol": ticker,
        "from": today,
        "to": today,
        "token": FINNHUB_API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        news = r.json()

        if not isinstance(news, list) or len(news) == 0:
            return "none", "No fresh catalyst found"

        headline = news[0].get("headline", "").lower()

        if "earnings" in headline or "results" in headline:
            return "earnings", "Fresh earnings / results catalyst"
        if "patent" in headline:
            return "patent", "Fresh patent catalyst"
        if "contract" in headline or "agreement" in headline:
            return "contract", "Fresh contract / agreement catalyst"
        if "fda" in headline or "trial" in headline:
            return "biotech", "Fresh biotech / FDA catalyst"
        if "lawsuit" in headline or "damages" in headline or "jury" in headline:
            return "legal", "Fresh legal catalyst"

        return "news", news[0].get("headline", "Fresh news catalyst")

    except Exception as e:
        print(f"[NEWS ERROR] {ticker}: {e}")
        return "unknown", "News check failed"


# ----------------------------
# Dilution / trap check
# ----------------------------

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

    text = text.lower()

    hits = [word for word in danger_words if word in text]

    if hits:
        return True, hits

    return False, []


# ----------------------------
# Scoring engine
# ----------------------------

def score_ticker(quote, volume_info, catalyst_type, catalyst_text):
    score = 0
    reasons = []
    risk_flags = []

    ticker = quote["ticker"]
    gain = quote["daily_gain"]
    price = quote["price"]

    # Percent gain scoring
    if gain >= 75:
        score += 3
        reasons.append("75%+ move")
    elif gain >= 50:
        score += 2
        reasons.append("50%+ move")
    elif gain >= 30:
        score += 1
        reasons.append("30%+ move")

    # Volume scoring
    vol_30m = volume_info["vol_30m"]

    if volume_info["volume_missing"]:
        risk_flags.append("missing volume data")
    else:
        if vol_30m >= 100_000:
            score += 2
            reasons.append("strong volume")
        elif vol_30m >= 25_000:
            score += 1
            reasons.append("some volume")

    if volume_info["estimated"]:
        risk_flags.append("volume estimated")

    # Catalyst scoring
    if catalyst_type not in ["none", "unknown"]:
        score += 2
        reasons.append("fresh catalyst")
    else:
        risk_flags.append("no clear catalyst")

    # Price sanity
    if price < 1:
        risk_flags.append("sub-$1 stock")
    elif price > 50:
        risk_flags.append("high price mover")

    # Dilution check from catalyst text/headline
    has_dilution, dilution_hits = check_dilution_risk(catalyst_text)

    if has_dilution:
        score -= 2
        risk_flags.append("dilution risk: " + ", ".join(dilution_hits))

    # Safety rule: do not let missing volume create fake high score
    if volume_info["volume_missing"] and score >= 7:
        score -= 2
        risk_flags.append("score reduced: volume unconfirmed")

    return {
        "ticker": ticker,
        "price": price,
        "gain": gain,
        "score": score,
        "reasons": reasons,
        "risk_flags": risk_flags,
        "catalyst_type": catalyst_type,
        "catalyst_text": catalyst_text,
        "vol_30m": vol_30m,
        "daily_volume": volume_info["daily_volume"]
    }


# ----------------------------
# Alert message
# ----------------------------

def build_alert(result):
    reasons = ", ".join(result["reasons"]) if result["reasons"] else "none"
    risks = ", ".join(result["risk_flags"]) if result["risk_flags"] else "none"

    return f"""
🚨 MOMENTUM ALERT

{result['ticker']} | Score: {result['score']}/10
Price: ${result['price']:.4f}
Daily Gain: {result['gain']:.1f}%
Vol30m: {result['vol_30m']:,}

Catalyst: {result['catalyst_type']}
{result['catalyst_text']}

Reasons: {reasons}
Risk: {risks}
""".strip()


# ----------------------------
# Scanner loop
# ----------------------------

def run_scanner():
    print("[BOOT] Scanner started")

    if not FINNHUB_API_KEY:
        print("[BOOT] Missing FINNHUB_API_KEY. Scanner cannot start.")
        return

    already_alerted = set()

    while True:
        results = []

        print("[SCAN] Starting cycle")

        for ticker in WATCHLIST:
            quote = get_quote(ticker)

            if not quote:
                print(f"[SKIP] {ticker} no quote")
                continue

            gain = quote["daily_gain"]
            price = quote["price"]

            volume_info = get_volume_data(ticker, quote)

            print(
                f"[SCAN] {ticker:<6} | Price ${price:<8.4f} | "
                f"Daily {gain:6.1f}% | Vol30m {volume_info['vol_30m']:>8,}"
            )

            if gain < MIN_GAIN:
                print(f"[FILTER] {ticker} skipped — daily gain {gain:.1f}% under {MIN_GAIN}%")
                continue

            catalyst_type, catalyst_text = get_news_catalyst(ticker)

            result = score_ticker(
                quote=quote,
                volume_info=volume_info,
                catalyst_type=catalyst_type,
                catalyst_text=catalyst_text
            )

            results.append(result)

        results.sort(key=lambda x: x["score"], reverse=True)

        if results:
            top_line = " | ".join(
                [f"{r['ticker']} {r['score']}/10 ${r['price']:.2f}" for r in results[:5]]
            )
            print(f"[SCAN] Top ranked: {top_line}")

        for result in results:
            ticker = result["ticker"]

            if result["score"] >= MIN_SCORE and ticker not in already_alerted:
                send_telegram(build_alert(result))
                already_alerted.add(ticker)
                print(f"[ALERT SENT] {ticker} score {result['score']}/10")
            else:
                print(f"[NO ALERT] {ticker} score {result['score']}/10")

        print("[SCAN] Cycle complete")
        print("[HEARTBEAT] alive")

        time.sleep(SCAN_SLEEP)


# ----------------------------
# Start bot
# ----------------------------

if __name__ == "__main__":
    Thread(target=run_health_server, daemon=True).start()
    run_scanner()
