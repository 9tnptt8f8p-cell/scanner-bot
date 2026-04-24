import os
import time
import requests
from datetime import datetime, timedelta
from threading import Thread
from flask import Flask
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

MIN_GAIN = 30
MIN_SCORE = 7
SCAN_SLEEP = 180
ALERT_COOLDOWN_SECONDS = 900
MAX_MOVERS = 40

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

    if not TELEGRAM_BOT_TOKEN or not chat_ids:
        print("[ALERT LOCAL]", message)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    all_sent = True

    for chat_id in chat_ids:
        try:
            r = requests.post(
                url,
                json={"chat_id": chat_id, "text": message},
                timeout=10
            )

            if r.status_code != 200:
                all_sent = False
                print(f"[TELEGRAM FAILED] chat={chat_id} {r.status_code} {r.text}")

        except Exception as e:
            all_sent = False
            print(f"[TELEGRAM ERROR] chat={chat_id}: {e}")

    if not all_sent:
        print("[ALERT LOCAL BACKUP]", message)

    return all_sent


# ============================
# AUTO MOVER LIST
# ============================

def get_market_movers():
    """
    Removes fixed watchlist.
    Pulls current day gainers from Yahoo Finance.
    """

    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"

    params = {
        "scrIds": "day_gainers",
        "count": MAX_MOVERS
    }

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()

        quotes = (
            data.get("finance", {})
            .get("result", [{}])[0]
            .get("quotes", [])
        )

        tickers = []

        for q in quotes:
            symbol = q.get("symbol")
            gain = q.get("regularMarketChangePercent", 0)

            if not symbol:
                continue

            if "." in symbol or "-" in symbol:
                continue

            try:
                gain = float(gain)
            except:
                gain = 0

            if gain >= 10:
                tickers.append(symbol)

        print(f"[MOVERS] Loaded {len(tickers)} gainers: {', '.join(tickers[:15])}")

        return tickers

    except Exception as e:
        print(f"[MOVERS ERROR] {e}")
        return []


# ============================
# PRICE
# ============================

def get_quote(ticker):
    if FINNHUB_API_KEY:
        try:
            url = "https://finnhub.io/api/v1/quote"
            params = {"symbol": ticker, "token": FINNHUB_API_KEY}

            r = requests.get(url, params=params, timeout=10)
            data = r.json()

            price = float(data.get("c") or 0)
            prev_close = float(data.get("pc") or 0)

            if price > 0 and prev_close > 0:
                daily_gain = ((price - prev_close) / prev_close) * 100

                return {
                    "ticker": ticker,
                    "price": price,
                    "prev_close": prev_close,
                    "daily_gain": daily_gain
                }

        except Exception as e:
            print(f"[FINNHUB QUOTE ERROR] {ticker}: {e}")

    return None


# ============================
# VOLUME
# ============================

def get_intraday_volume(ticker):
    if ALPACA_API_KEY and ALPACA_SECRET_KEY:
        try:
            now_dt = datetime.utcnow()
            start_dt = now_dt - timedelta(minutes=30)

            url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"

            params = {
                "timeframe": "1Min",
                "start": start_dt.isoformat() + "Z",
                "end": now_dt.isoformat() + "Z",
                "limit": 1000,
                "adjustment": "raw",
                "feed": "iex"
            }

            headers = {
                "APCA-API-KEY-ID": ALPACA_API_KEY,
                "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
            }

            r = requests.get(url, params=params, headers=headers, timeout=10)
            data = r.json()

            bars = data.get("bars", [])

            if bars:
                vol = int(sum(bar.get("v", 0) for bar in bars))
                if vol > 0:
                    return vol, "alpaca"

        except Exception as e:
            print(f"[ALPACA VOL ERROR] {ticker}: {e}")

    if TWELVEDATA_API_KEY:
        try:
            url = "https://api.twelvedata.com/time_series"

            params = {
                "symbol": ticker,
                "interval": "1min",
                "outputsize": 30,
                "apikey": TWELVEDATA_API_KEY
            }

            r = requests.get(url, params=params, timeout=10)
            data = r.json()

            values = data.get("values", [])

            if values:
                vol = int(sum(int(float(v.get("volume", 0))) for v in values))
                if vol > 0:
                    return vol, "twelvedata"

        except Exception as e:
            print(f"[TWELVEDATA 30M ERROR] {ticker}: {e}")

    return 0, "none"


def get_daily_volume(ticker):
    if TWELVEDATA_API_KEY:
        try:
            url = "https://api.twelvedata.com/time_series"

            params = {
                "symbol": ticker,
                "interval": "1day",
                "outputsize": 1,
                "apikey": TWELVEDATA_API_KEY
            }

            r = requests.get(url, params=params, timeout=10)
            data = r.json()

            values = data.get("values", [])

            if values:
                vol = int(float(values[0].get("volume", 0)))
                if vol > 0:
                    return vol, "twelvedata"

        except Exception as e:
            print(f"[TWELVEDATA DAILY ERROR] {ticker}: {e}")

    if ALPHAVANTAGE_API_KEY:
        try:
            url = "https://www.alphavantage.co/query"

            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": ticker,
                "apikey": ALPHAVANTAGE_API_KEY
            }

            r = requests.get(url, params=params, timeout=10)
            data = r.json()

            series = data.get("Time Series (Daily)", {})

            if series:
                latest = list(series.keys())[0]
                vol = int(float(series[latest].get("5. volume", 0)))

                if vol > 0:
                    return vol, "alphavantage"

        except Exception as e:
            print(f"[ALPHA DAILY ERROR] {ticker}: {e}")

    return 0, "none"


def get_volume_data(ticker):
    vol_30m, source_30m = get_intraday_volume(ticker)
    daily_vol, source_day = get_daily_volume(ticker)

    return {
        "vol_30m": vol_30m,
        "daily_volume": daily_vol,
        "volume_missing": vol_30m == 0 and daily_vol == 0,
        "source": source_30m,
        "daily_source": source_day
    }


# ============================
# NEWS
# ============================

def get_news_catalyst(ticker):
    if not FINNHUB_API_KEY:
        return "unknown", "Missing Finnhub key"

    today = time.strftime("%Y-%m-%d")

    try:
        url = "https://finnhub.io/api/v1/company-news"

        params = {
            "symbol": ticker,
            "from": today,
            "to": today,
            "token": FINNHUB_API_KEY
        }

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

        return "news", headline

    except Exception as e:
        print(f"[NEWS ERROR] {ticker}: {e}")
        return "unknown", "News check failed"


# ============================
# RISK + SCORING
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


def score_ticker(quote, volume_info, catalyst_type, catalyst_text):
    score = 0
    reasons = []
    risk_flags = []

    price = quote["price"]
    gain = quote["daily_gain"]
    vol_30m = volume_info["vol_30m"]
    daily_vol = volume_info["daily_volume"]

    if gain >= 75:
        score += 3
        reasons.append("75%+ move")
    elif gain >= 50:
        score += 2
        reasons.append("50%+ move")
    elif gain >= 30:
        score += 1
        reasons.append("30%+ move")

    if daily_vol >= 2_000_000:
        score += 3
        reasons.append("2M+ daily volume")
    elif daily_vol >= 500_000:
        score += 2
        reasons.append("500k+ daily volume")
    elif daily_vol >= 100_000:
        score += 1
        reasons.append("100k+ daily volume")

    if vol_30m >= 500_000:
        score += 2
        reasons.append("huge 30m volume")
    elif vol_30m >= 100_000:
        score += 1
        reasons.append("strong 30m volume")
    elif vol_30m >= 5_000:
        score += 1
        reasons.append("active 30m volume")

    if volume_info["daily_source"] != "none":
        score += 1
        reasons.append(f"daily volume source: {volume_info['daily_source']}")

    if volume_info["source"] != "none":
        reasons.append(f"30m volume source: {volume_info['source']}")

    if catalyst_type not in ["none", "unknown"]:
        score += 2
        reasons.append("fresh catalyst")
    else:
        risk_flags.append("no clear catalyst")

    if price < 1:
        risk_flags.append("sub-$1 stock")

    if price > 50:
        risk_flags.append("high price mover")

    has_dilution, hits = check_dilution_risk(catalyst_text)

    if has_dilution:
        score -= 2
        risk_flags.append("dilution risk: " + ", ".join(hits))

    score = max(0, min(score, 10))

    return {
        "ticker": quote["ticker"],
        "price": price,
        "gain": gain,
        "score": score,
        "reasons": reasons,
        "risk_flags": risk_flags,
        "catalyst_type": catalyst_type,
        "catalyst_text": catalyst_text,
        "vol_30m": vol_30m,
        "daily_volume": daily_vol,
        "volume_source": volume_info["source"],
        "daily_volume_source": volume_info["daily_source"]
    }


def build_alert(result, rank):
    reasons = ", ".join(result["reasons"]) if result["reasons"] else "none"
    risks = ", ".join(result["risk_flags"]) if result["risk_flags"] else "none"

    return f"""
🚨 MOMENTUM ALERT

Rank: #{rank}
{result['ticker']} | Score: {result['score']}/10
Price: ${result['price']:.4f}
Daily Gain: {result['gain']:.1f}%

Daily Volume: {result['daily_volume']:,}
Daily Volume Source: {result['daily_volume_source']}

Vol30m: {result['vol_30m']:,}
30m Volume Source: {result['volume_source']}

Catalyst: {result['catalyst_type']}
{result['catalyst_text']}

Reasons: {reasons}
Risk: {risks}
""".strip()


# ============================
# SCANNER
# ============================

def run_scanner():
    print("[BOOT] Scanner started")

    alert_history = {}

    while True:
        print("[SCAN] Starting cycle")

        tickers = get_market_movers()

        if not tickers:
            print("[SCAN] No movers found")
            time.sleep(SCAN_SLEEP)
            continue

        results = []

        for ticker in tickers:
            quote = get_quote(ticker)

            if not quote:
                print(f"[SKIP] {ticker} no quote")
                continue

            volume_info = get_volume_data(ticker)

            print(
                f"[SCAN] {ticker:<6} | Price ${quote['price']:<8.4f} | "
                f"Daily {quote['daily_gain']:6.1f}% | "
                f"DayVol {volume_info['daily_volume']:>10,} | "
                f"Vol30m {volume_info['vol_30m']:>8,} | "
                f"DaySrc {volume_info['daily_source']} | "
                f"30mSrc {volume_info['source']}"
            )

            if quote["daily_gain"] < MIN_GAIN:
                print(f"[FILTER] {ticker} skipped — daily gain {quote['daily_gain']:.1f}% under {MIN_GAIN}%")
                continue

            catalyst_type, catalyst_text = get_news_catalyst(ticker)

            result = score_ticker(
                quote=quote,
                volume_info=volume_info,
                catalyst_type=catalyst_type,
                catalyst_text=catalyst_text
            )

            results.append(result)

            time.sleep(0.5)

        results.sort(key=lambda x: x["score"], reverse=True)

        if results:
            top_line = " | ".join(
                [f"#{i+1} {r['ticker']} {r['score']}/10 ${r['price']:.2f}" for i, r in enumerate(results[:10])]
            )
            print(f"[SCAN] Top ranked: {top_line}")

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
                print(f"[NO ALERT] #{rank} {ticker} score {result['score']}/10 below MIN_SCORE {MIN_SCORE}")

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
print("[BOOT] sending telegram test...")

test_sent = send_telegram("✅ BOT STARTED — TELEGRAM TEST")

if test_sent:
    print("[BOOT] Telegram working")
else:
    print("[BOOT] Telegram FAILED")

print("[BOOT] starting scanner")
run_scanner()
