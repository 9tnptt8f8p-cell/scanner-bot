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

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")

MIN_GAIN = 30
MIN_SCORE = 7
SCAN_SLEEP = 60

WATCHLIST = [
    "MXL", "SCNI", "LINT", "CAST", "ATOM", "LIDR", "ONMD", "WYHG", "ENVB",
    "AKAN", "AUUD", "PAPL", "SOUN", "RGTI", "SKLZ", "EUDA"
]

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot alive"

@app.route("/health")
def health():
    return "OK"


def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[ALERT LOCAL]", message)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=10
        )
    except Exception as e:
        print(f"[TELEGRAM ERROR] {e}")


def get_quote(ticker):
    if not FINNHUB_API_KEY:
        return None

    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": ticker, "token": FINNHUB_API_KEY}

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


def get_intraday_volume(ticker):
    """
    Gets last-30-minute volume.
    Used for timing / active momentum.
    """

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

            print(f"[VOLUME FALLBACK] {ticker} Alpaca no usable 30m volume")

        except Exception as e:
            print(f"[ALPACA VOLUME ERROR] {ticker}: {e}")

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

            print(f"[VOLUME FALLBACK] {ticker} TwelveData no usable 30m volume")

        except Exception as e:
            print(f"[TWELVEDATA VOLUME ERROR] {ticker}: {e}")

    if ALPHAVANTAGE_API_KEY:
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": ticker,
                "interval": "1min",
                "apikey": ALPHAVANTAGE_API_KEY
            }

            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            series = data.get("Time Series (1min)", {})

            volumes = []
            for k in list(series.keys())[:30]:
                volumes.append(int(float(series[k].get("5. volume", 0))))

            if volumes:
                vol = int(sum(volumes))
                if vol > 0:
                    return vol, "alphavantage"

            print(f"[VOLUME FALLBACK] {ticker} AlphaVantage no usable 30m volume")

        except Exception as e:
            print(f"[ALPHA VOLUME ERROR] {ticker}: {e}")

    if FINNHUB_API_KEY:
        try:
            now = int(time.time())
            thirty_minutes_ago = now - 30 * 60

            url = "https://finnhub.io/api/v1/stock/candle"
            params = {
                "symbol": ticker,
                "resolution": "1",
                "from": thirty_minutes_ago,
                "to": now,
                "token": FINNHUB_API_KEY
            }

            r = requests.get(url, params=params, timeout=10)
            data = r.json()

            if data.get("s") == "ok":
                vol = int(sum(data.get("v", [])))
                if vol > 0:
                    return vol, "finnhub"

            print(f"[VOLUME FALLBACK] {ticker} Finnhub no usable 30m volume")

        except Exception as e:
            print(f"[FINNHUB VOLUME ERROR] {ticker}: {e}")

    return 0, "none"


def get_daily_volume(ticker):
    """
    Gets total day volume.
    Used for liquidity / scanner strength.
    """

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
            print(f"[DAILY VOL ERROR] {ticker}: {e}")

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
                latest_day = list(series.keys())[0]
                vol = int(float(series[latest_day].get("5. volume", 0)))
                if vol > 0:
                    return vol, "alphavantage"

        except Exception as e:
            print(f"[ALPHA DAILY VOL ERROR] {ticker}: {e}")

    return 0, "none"


def get_volume_data(ticker, quote_data=None):
    vol_30m, intraday_source = get_intraday_volume(ticker)
    daily_vol, daily_source = get_daily_volume(ticker)

    volume_missing = vol_30m == 0 and daily_vol == 0

    return {
        "vol_30m": vol_30m,
        "daily_volume": daily_vol,
        "volume_missing": volume_missing,
        "estimated": False,
        "source": intraday_source,
        "daily_source": daily_source
    }


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

        if not isinstance(news, list) or len(news) == 0:
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

    ticker = quote["ticker"]
    price = quote["price"]
    gain = quote["daily_gain"]
    vol_30m = volume_info["vol_30m"]
    daily_vol = volume_info.get("daily_volume", 0)

    if gain >= 75:
        score += 3
        reasons.append("75%+ move")
    elif gain >= 50:
        score += 2
        reasons.append("50%+ move")
    elif gain >= 30:
        score += 1
        reasons.append("30%+ move")

    # DAILY VOLUME = liquidity / real scanner strength
    if daily_vol >= 2_000_000:
        score += 3
        reasons.append("2M+ daily volume")
    elif daily_vol >= 500_000:
        score += 2
        reasons.append("500k+ daily volume")
    elif daily_vol >= 100_000:
        score += 1
        reasons.append("100k+ daily volume")

    # 30M VOLUME = active timing / current momentum
    if vol_30m >= 500_000:
        score += 2
        reasons.append("huge 30m volume")
    elif vol_30m >= 100_000:
        score += 1
        reasons.append("strong 30m volume")
    elif vol_30m >= 5_000:
        score += 1
        reasons.append("active 30m volume")

    if volume_info.get("daily_source") != "none":
        score += 1
        reasons.append(f"daily volume source: {volume_info.get('daily_source')}")

    if volume_info.get("source") != "none":
        reasons.append(f"30m volume source: {volume_info.get('source')}")

    if volume_info["volume_missing"]:
        risk_flags.append("missing volume data")

    if catalyst_type not in ["none", "unknown"]:
        score += 2
        reasons.append("fresh catalyst")
    else:
        risk_flags.append("no clear catalyst")

    if price < 1:
        risk_flags.append("sub-$1 stock")

    if price > 50:
        risk_flags.append("high price mover")

    has_dilution, dilution_hits = check_dilution_risk(catalyst_text)

    if has_dilution:
        score -= 2
        risk_flags.append("dilution risk: " + ", ".join(dilution_hits))

    score = max(0, min(score, 10))

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
        "daily_volume": daily_vol,
        "volume_source": volume_info.get("source", "none"),
        "daily_volume_source": volume_info.get("daily_source", "none")
    }


def build_alert(result):
    reasons = ", ".join(result["reasons"]) if result["reasons"] else "none"
    risks = ", ".join(result["risk_flags"]) if result["risk_flags"] else "none"

    return f"""
🚨 MOMENTUM ALERT

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


def run_scanner():
    print("[BOOT] Scanner started")

    if not FINNHUB_API_KEY:
        print("[BOOT] Missing FINNHUB_API_KEY. Scanner cannot start.")
        return

    already_alerted = set()

    while True:
        print("[SCAN] Starting cycle")
        results = []

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
                f"Daily {gain:6.1f}% | "
                f"DayVol {volume_info['daily_volume']:>10,} | "
                f"Vol30m {volume_info['vol_30m']:>8,} | "
                f"DaySrc {volume_info.get('daily_source', 'none')} | "
                f"30mSrc {volume_info.get('source', 'none')}"
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
