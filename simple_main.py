import os
import time
import requests
from threading import Thread
from flask import Flask
from dotenv import load_dotenv
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from structure_engine import analyze_structure

ET = ZoneInfo("America/New_York")

MARKET_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-19", "2026-02-16",
    "2026-04-03", "2026-05-25", "2026-06-19",
    "2026-07-03", "2026-09-07", "2026-11-26",
    "2026-12-25",
}
def get_dilution_risk(text):
    text = str(text).lower()
    reasons = []

    if "atm" in text or "at-the-market" in text:
        reasons.append("ATM active")

    if "offering" in text or "securities purchase agreement" in text:
        reasons.append("Recent offering filed")

    if "warrant" in text or "exercise price" in text:
        reasons.append("Warrants in play")

    if "s-3" in text or "f-3" in text or "shelf" in text:
        reasons.append("Shelf registration")

    if len(reasons) >= 2:
        risk = "HIGH"
    elif len(reasons) == 1:
        risk = "MEDIUM"
    else:
        risk = "LOW"

    return risk, reasons
def should_scan_now():
    now = datetime.now(ET)

    # ❌ Weekend
    if now.weekday() >= 5:
        return False

    # ❌ Holiday
    if now.date().isoformat() in MARKET_HOLIDAYS_2026:
        return False

    # ❌ Outside 4AM–8PM
    if not (dtime(4, 0) <= now.time() <= dtime(20, 0)):
        return False

    return True
import requests
from threading import Thread
from flask import Flask
from dotenv import load_dotenv

load_dotenv()

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
    danger_words = [
        "offering",
        "registered direct",
        "securities purchase agreement",
        "warrant",
        "warrants",
        "atm",
        "shelf",
        "f-1",
        "s-1",
        "convertible",
        "pipe",
        "reverse split",
        "direct offering",
        "public offering"
    ]

    text = str(text).lower()
    return [word for word in danger_words if word in text]


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

    dilution_risk, dilution_reasons = get_dilution_risk(result["catalyst_text"])

    dilution_block = f"\n\n💀 DILUTION RISK: {dilution_risk}"

    for item in dilution_reasons:
        dilution_block += f"\n- {item}"

    return f"""
🚨 27%+ SPIKE ALERT

Rank: #{rank}
{result['ticker']} | Score: {result['score']}/10

Price: ${result['price']:.4f}
Gain: {result['gain']:.1f}%
Volume: {result['volume']:,}

Catalyst: {result['catalyst_type']}
{result['catalyst_text']}

Reasons: {reasons}
Risk: {risks}{dilution_block}
""".strip()


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

            candles = get_yahoo_candles(ticker)
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

        now = time.time()
        alerts_sent_this_cycle = 0

        for rank, result in enumerate(results, start=1):
            if alerts_sent_this_cycle >= MAX_ALERTS_PER_CYCLE:
                print("[ALERT LIMIT] Max alerts reached this cycle", flush=True)
                break

            ticker = result["ticker"]
            last_alert = alert_history.get(ticker, 0)
            cooldown_done = now - last_alert >= ALERT_COOLDOWN_SECONDS

            if result["score"] >= MIN_SCORE and cooldown_done:
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

   

    print("[BOOT] starting scanner", flush=True)
    run_scanner()
