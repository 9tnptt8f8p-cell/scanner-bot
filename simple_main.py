import os
import time
import requests
from threading import Thread
from flask import Flask
from dotenv import load_dotenv
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from structure_engine import analyze_structure

load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

ET = ZoneInfo("America/New_York")

MARKET_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-19", "2026-02-16",
    "2026-04-03", "2026-05-25", "2026-06-19",
    "2026-07-03", "2026-09-07", "2026-11-26",
    "2026-12-25",
}

   
def should_scan_now():
    now = datetime.now(ET)

    print(f"[TIME] Market clock ET: {now.strftime('%Y-%m-%d %I:%M:%S %p %Z')}", flush=True)

    if now.weekday() >= 5:
        return False

    if now.date().isoformat() in MARKET_HOLIDAYS_2026:
        return False

    if not (dtime(4, 0) <= now.time() <= dtime(20, 0)):
        return False

    return True

BOOT_MARKER = "tight 27pct spike scanner v1"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Supports one or both:
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")

MIN_GAIN = 27
MIN_SCORE = 6
SCAN_SLEEP = 180
ALERT_COOLDOWN_SECONDS = 1800
MAX_GAINERS = 40
MAX_ALERTS_PER_CYCLE = 3

MIN_VOLUME = 500_000
MAX_PRICE = 100

app = Flask(__name__)


@app.route("/")
def home():
    return "Bot alive"


@app.route("/health")
def health():
    return "OK"


def get_chat_ids():
    ids = []

    if TELEGRAM_CHAT_IDS:
        ids.extend([x.strip() for x in TELEGRAM_CHAT_IDS.split(",") if x.strip()])

    if TELEGRAM_CHAT_ID:
        ids.append(TELEGRAM_CHAT_ID.strip())

    # remove duplicates
    return list(dict.fromkeys(ids))


def send_telegram(message):
    chat_ids = get_chat_ids()

    print(f"[TELEGRAM DEBUG] token={bool(TELEGRAM_BOT_TOKEN)} chats={chat_ids}", flush=True)

    if not TELEGRAM_BOT_TOKEN or not chat_ids:
        print("[ALERT LOCAL]", message, flush=True)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    success = True

    for chat_id in chat_ids:
        try:
            r = requests.post(
                url,
                json={"chat_id": chat_id, "text": message},
                timeout=10
            )

            print(
                f"[TELEGRAM RESPONSE] chat={chat_id} status={r.status_code} body={r.text}",
                flush=True
            )

            if r.status_code != 200:
                success = False

        except Exception as e:
            print(f"[TELEGRAM ERROR] chat={chat_id} error={e}", flush=True)
            success = False

    if not success:
        print("[ALERT LOCAL]", message, flush=True)

    return success


def get_percent_gainers():
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"

    params = {
        "scrIds": "day_gainers",
        "count": MAX_GAINERS
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

        movers = []

        for q in quotes:
            ticker = q.get("symbol")
            price = q.get("regularMarketPrice")
            gain = q.get("regularMarketChangePercent")
            volume = q.get("regularMarketVolume", 0)

            if not ticker:
                continue

            if "." in ticker or "-" in ticker:
                continue

            try:
                price = float(price or 0)
                gain = float(gain or 0)
                volume = int(volume or 0)
            except Exception:
                continue

            if price <= 0:
                continue

            if gain < MIN_GAIN:
                continue

            if volume < MIN_VOLUME:
                print(f"[FILTER] {ticker} skipped — volume {volume:,} under {MIN_VOLUME:,}", flush=True)
                continue

            if price > MAX_PRICE:
                print(f"[FILTER] {ticker} skipped — price ${price:.2f} over ${MAX_PRICE}", flush=True)
                continue

            movers.append({
                "ticker": ticker,
                "price": price,
                "gain": gain,
                "volume": volume
            })

        movers.sort(key=lambda x: x["gain"], reverse=True)

        print(f"[GAINERS] Found {len(movers)} qualified movers over {MIN_GAIN}%:", flush=True)
        print("[GAINERS] " + ", ".join([f"{m['ticker']} {m['gain']:.1f}%" for m in movers[:20]]), flush=True)

        return movers

    except Exception as e:
        print(f"[GAINERS ERROR] {e}", flush=True)
        return []


def get_yahoo_candles(ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"

    params = {
        "interval": "5m",
        "range": "1d"
    }

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()

        result = data["chart"]["result"][0]
        quote = result["indicators"]["quote"][0]

        candles = []

        for o, h, l, c, v in zip(
            quote["open"],
            quote["high"],
            quote["low"],
            quote["close"],
            quote["volume"]
        ):
            if None in (o, h, l, c, v):
                continue

            candles.append({
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v
            })

        return candles

    except Exception as e:
        print(f"[CANDLE ERROR] {ticker}: {e}", flush=True)
        return []


def get_alpaca_candles(ticker):
    url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
    }

    params = {
        "timeframe": "5Min",
        "limit": 50
    }

    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        data = r.json()

        bars = data.get("bars", [])
        candles = []

        for b in bars:
            candles.append({
                "open": b["o"],
                "high": b["h"],
                "low": b["l"],
                "close": b["c"],
                "volume": b["v"]
            })

         
        return candles
    except Exception as e:
        print(f"[ALPACA ERROR] {ticker}: {e}", flush=True)
        return []


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
        print(f"[NEWS ERROR] {ticker}: {e}", flush=True)
        return "unknown", "News check failed"


def check_dilution_risk(text):
    text = str(text).lower()

    danger_words = {
        "public offering": "public offering",
        "registered direct": "registered direct",
        "direct offering": "direct offering",
        "private placement": "private placement",
        "securities purchase agreement": "securities purchase agreement",
        "at-the-market": "ATM",
        "atm": "ATM",
        "shelf": "shelf registration",
        "s-1": "S-1",
        "s-3": "S-3",
        "f-1": "F-1",
        "f-3": "F-3",
        "424b": "424B",
        "424b5": "424B5",
        "warrant": "warrants",
        "exercise price": "warrant exercise price",
        "convertible": "convertible",
        "convertible note": "convertible note",
        "pipe": "PIPE",
        "equity line": "equity line",
        "resale": "resale registration",
        "selling stockholder": "selling stockholder",
        "reverse split": "reverse split",
        "offering": "offering"
    }

    hits = []

    for word, label in danger_words.items():
        if word in text and label not in hits:
            hits.append(label)

    return hits


def score_mover(mover, catalyst_type, catalyst_text):
    score = 0
    reasons = []
    risks = []

    gain = mover["gain"]
    price = mover["price"]
    volume = mover["volume"]

    if gain >= 100:
        score += 5
        reasons.append("100%+ gainer")
    elif gain >= 75:
        score += 4
        reasons.append("75%+ gainer")
    elif gain >= 50:
        score += 3
        reasons.append("50%+ gainer")
    elif gain >= 27:
        score += 2
        reasons.append("27%+ spike")

    if volume >= 10_000_000:
        score += 3
        reasons.append("10M+ volume")
    elif volume >= 2_000_000:
        score += 2
        reasons.append("2M+ volume")
    elif volume >= 500_000:
        score += 1
        reasons.append("500k+ volume")

    if catalyst_type not in ["none", "unknown"]:
        score += 2
        reasons.append("fresh news")
    else:
        risks.append("no clear fresh news")

    if catalyst_type in ["earnings", "patent", "contract", "legal", "biotech"]:
        score += 1
        reasons.append(f"strong catalyst: {catalyst_type}")

    dilution_hits = check_dilution_risk(catalyst_text)

    if dilution_hits:
        if len(dilution_hits) >= 3:
            score -= 5
            risks.append("HIGH dilution risk: " + ", ".join(dilution_hits))
        elif len(dilution_hits) == 2:
            score -= 4
            risks.append("MEDIUM/HIGH dilution risk: " + ", ".join(dilution_hits))
        else:
            score -= 3
            risks.append("dilution risk: " + ", ".join(dilution_hits))

    if price < 1:
        risks.append("sub-$1 stock")

    score = max(0, min(score, 10))

    return {
        "ticker": mover["ticker"],
        "price": price,
        "gain": gain,
        "volume": volume,
        "score": score,
        "catalyst_type": catalyst_type,
        "catalyst_text": catalyst_text,
        "reasons": reasons,
        "risks": risks
    }


def build_alert(result, rank):
    reasons = ", ".join(result["reasons"]) if result["reasons"] else "none"
    risks = ", ".join(result["risks"]) if result["risks"] else "none"

    session_block = f"""

🕓 MARKET SESSION: {result.get('session', 'UNKNOWN')}

🧠 Session Notes:
{chr(10).join(['- ' + n for n in result.get('session_notes', [])])}
"""

    regime_block = f"""

📊 MARKET REGIME: {result.get('market_regime', 'UNKNOWN')}

🧠 Regime Notes:
{chr(10).join(['- ' + n for n in result.get('regime_notes', [])])}
"""

    return f"""
🚨 27%+ SPIKE ALERT

Rank: #{rank}
{result['ticker']} | Score: {result['score']}/10

Price: ${result['price']:.4f}
Gain: {result['gain']:.1f}
%Session Gain: {result.get('candle_session_gain', 0):.1f}%
Yahoo Volume: {result['volume']:,}
Recent Candle Vol: {result.get('recent_volume', 0):,}
Candle Total Vol: {result.get('total_candle_volume', 0):,}

Catalyst: {result['catalyst_type']}
{result['catalyst_text']}

Reasons: {reasons}
Risk: {risks}{session_block}{regime_block}
""".strip()

def get_market_session():
    now = datetime.now(ET).time()

    if dtime(4, 0) <= now < dtime(9, 30):
        return "PREMARKET", [
            "lower liquidity",
            "wider spreads",
            "wait for open confirmation"
        ]

    if dtime(9, 30) <= now < dtime(11, 0):
        return "OPEN", [
            "highest opportunity window",
            "watch VWAP and first pullback"
        ]

    if dtime(11, 0) <= now < dtime(14, 0):
        return "MIDDAY", [
            "slower tape",
            "avoid forcing trades"
        ]

    if dtime(14, 0) <= now < dtime(16, 0):
        return "POWER HOUR", [
            "watch continuation or breakdown"
        ]

    if dtime(16, 0) <= now <= dtime(20, 0):
        return "AFTERHOURS", [
            "thin liquidity",
            "only trust strong news moves"
        ]

    return "CLOSED", ["market closed"]


def detect_market_regime(results):
    if not results:
        return "UNKNOWN", ["no qualified movers yet"]

    strong = 0
    mid = 0

    for r in results[:10]:
        if r["score"] >= 8:
            strong += 1
        elif r["score"] >= 6:
            mid += 1

    notes = []

    if strong >= 3:
        return "HOT", ["multiple strong setups", "momentum market active"]

    if strong == 0 and mid <= 2:
        return "CHOP", ["lack of strong setups", "be defensive / avoid forcing trades"]

    return "MIXED", ["some setups but inconsistent", "only take A+ charts"]


def run_scanner():
    print(f"[BOOT] Scanner started | {BOOT_MARKER}", flush=True)
    print("[BOOT] No watchlist — scanning 27%+ percent gainers only", flush=True)

    alert_history = {}

    while True:
        if not should_scan_now():
            print("[SLEEP] Market inactive — skipping scan", flush=True)
            time.sleep(300)
            continue

        print("[SCAN] Market active — running scan", flush=True)
        session, session_notes = get_market_session()
        movers = get_percent_gainers()
        results = []

        for mover in movers:
            ticker = mover["ticker"]

            print(
                f"[QUALIFIED] {ticker:<6} | Price ${mover['price']:<8.4f} | "
                f"Gain {mover['gain']:6.1f}% | Volume {mover['volume']:,}",
                flush=True
            )

            catalyst_type, catalyst_text = get_news_catalyst(ticker)   

            result = score_mover(
                mover=mover,
                catalyst_type=catalyst_type,
                catalyst_text=catalyst_text
            )
            result["session"] = session
            result["session_notes"] = session_notes
            candles = get_alpaca_candles(ticker)

            if not candles:
                print(f"[DATA FALLBACK] {ticker} Alpaca failed — using Yahoo", flush=True)
                candles = get_yahoo_candles(ticker)
            else:
                print(f"[DATA] {ticker} candles from Alpaca", flush=True)

            recent_volume = sum(c["volume"] for c in candles[-5:]) if candles else 0
            total_candle_volume = sum(c["volume"] for c in candles) if candles else 0

            result["recent_volume"] = recent_volume
            result["total_candle_volume"] = total_candle_volume

            if candles:
                first_close = float(candles[0]["close"])
                last_close = float(candles[-1]["close"])
                candle_session_gain = ((last_close - first_close) / first_close) * 100 if first_close > 0 else 0
            else:
                candle_session_gain = 0

            result["candle_session_gain"] = candle_session_gain

            if result.get("session") == "PREMARKET":
                if recent_volume < 200000:
                    result["risks"].append(f"low premarket candle volume: {recent_volume:,}")

                if candle_session_gain < 15:
                    result["risks"].append(f"only up {candle_session_gain:.1f}% this session")
            structure = analyze_structure(ticker, candles)
            result["structure"] = structure
            result["score"] += structure["structure_score"]
            result["score"] = max(0, min(result["score"], 10))

            if structure["risk_flags"]:
                result["risks"].extend(structure["risk_flags"])

            if structure["reasons"]:
                result["reasons"].extend(structure["reasons"])

            results.append(result)

            time.sleep(0.5)

        results.sort(key=lambda x: x["score"], reverse=True)

        regime, regime_notes = detect_market_regime(results)

        for r in results:
            r["market_regime"] = regime
            r["regime_notes"] = regime_notes

        if results:
            top_line = " | ".join(
                [
                    f"#{i + 1} {r['ticker']} {r['score']}/10 {r['gain']:.1f}%"
                    for i, r in enumerate(results[:10])
                ]
            )
            print(f"[SCAN] Top ranked: {top_line}", flush=True)
        else:
            print("[SCAN] No qualified 27%+ gainers found", flush=True)

                       ]
            )
            print(f"[SCAN] Top ranked: {top_line}", flush=True)
        else:
            print("[SCAN] No qualified 27%+ gainers found", flush=True)

        now = time.time()
        alerts_sent_this_cycle = 0

       for rank, result in enumerate(results, start=1):
    if alerts_sent_this_cycle >= MAX_ALERTS_PER_CYCLE:
        print("[ALERT LIMIT] Max alerts reached this cycle", flush=True)
        break

    ticker = result["ticker"]
    last_alert = alert_history.get(ticker, 0)
    cooldown_done = now - last_alert >= ALERT_COOLDOWN_SECONDS

    valid_27pct_alert = (
        result["gain"] >= 27
        and result.get("candle_session_gain", 0) >= 15
        and result.get("recent_volume", 0) >= 200000
        and result["score"] >= MIN_SCORE
    )

    valid_fast_12pct_alert = (
        result.get("candle_session_gain", 0) >= 12
        and result.get("recent_volume", 0) >= 200000
        and result["score"] >= MIN_SCORE
    )

    should_alert = valid_27pct_alert or valid_fast_12pct_alert

    if should_alert and cooldown_done:
        sent = send_telegram(build_alert(result, rank))

                if sent:
                    alert_history[ticker] = now
                    alerts_sent_this_cycle += 1
                    print(f"[ALERT SENT] #{rank} {ticker} score {result['score']}/10", flush=True)
                else:
                    print(f"[ALERT FAILED] #{rank} {ticker} score {result['score']}/10", flush=True)

            elif result["score"] >= MIN_SCORE:
                left = int(ALERT_COOLDOWN_SECONDS - (now - last_alert))
                print(f"[NO ALERT] #{rank} {ticker} cooldown active {left}s left", flush=True)

            else:
                print(
                    f"[NO ALERT] #{rank} {ticker} score {result['score']}/10 below MIN_SCORE {MIN_SCORE}",
                    flush=True
                )

        print("[SCAN] Cycle complete", flush=True)
        print("[HEARTBEAT] alive", flush=True)

        time.sleep(SCAN_SLEEP)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))

    print(f"[WEB] starting server on port {port}", flush=True)

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

   

                )

        print("[SCAN] Cycle complete", flush=True)
        print("[HEARTBEAT] alive", flush=True)

        time.sleep(SCAN_SLEEP)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))

    print(f"[WEB] starting server on port {port}", flush=True)

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

   

    print("[BOOT] starting scanner", flush=True)
    run_scanner()
