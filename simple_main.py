import os
import time
import requests
from threading import Thread
from flask import Flask
from dotenv import load_dotenv
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from risk_engine import build_risk
from structure_engine import analyze_structure
from msg_builder import build_alert
from alerts import send_alert
from rank_engine import rank_result
load_dotenv()


def detect_dilution_type(text):
    text = (text or "").lower()
    signals = []

    if "at the market" in text or "atm offering" in text:
        signals.append("ATM offering")

    if "warrant" in text:
        signals.append("Warrants")

    if "convertible" in text or "convertible note" in text:
        signals.append("Convertible notes")

    if "securities purchase agreement" in text:
        signals.append("Securities purchase agreement")

    if "registered direct" in text:
        signals.append("Registered direct offering")

    if "shelf registration" in text or "form s-3" in text or "form f-3" in text:
        signals.append("Shelf registration")

    if "resale" in text:
        signals.append("Resale registration")

    if "offering" in text and not signals:
        signals.append("Offering language detected")

    return signals


def analyze_news(headline):
    h = (headline or "").lower()

    if not h:
        return "UNKNOWN", "❓ No clear headline found"

    if any(x in h for x in ["here are", "stocks moving", "top movers", "why shares are trading"]):
        return "WEAK", "📰 Mover-list headline, not company-specific news"

    if any(x in h for x in ["offering", "priced", "registered direct", "atm"]):
        return "NEGATIVE", "💸 Offering / dilution news"

    if any(x in h for x in ["contract", "agreement", "partnership", "collaboration"]):
        return "STRONG", "🤝 Deal / partnership news"

    if any(x in h for x in ["fda", "approval", "phase", "trial", "clinical", "data"]):
        return "STRONG", "💊 FDA / clinical news"

    if any(x in h for x in ["earnings", "revenue", "guidance", "profit", "sales"]):
        return "STRONG", "📊 Earnings / financial news"

    if any(x in h for x in ["merger", "acquisition", "buyout"]):
        return "STRONG", "🏢 Merger / acquisition news"

    return "UNKNOWN", "❓ Unclear catalyst"


def build_trade_bias(result):
    risks = " ".join(result.get("risks", [])).lower()
    news_quality = result.get("news_quality", "")
    structure = " ".join(result.get("reasons", []) + result.get("risks", [])).lower()

    if "offering" in risks or "dilution" in risks or "warrant" in risks:
        return "⚠️ High risk — dilution/financing overhang"

    if news_quality == "NEGATIVE":
        return "❌ Negative catalyst — avoid unless extreme scalp only"

    if news_quality == "WEAK":
        return "⚠️ Weak catalyst — could fade fast"

    if "below vwap" in structure:
        return "⚠️ Below VWAP — wait for reclaim"

    if "upper wick" in structure or "trap" in structure:
        return "⚠️ Trap risk — wait for cleaner setup"

    if news_quality == "STRONG":
        return "✅ Strong catalyst — watch for continuation"

    return "🤔 Mixed/unclear — wait for confirmation"

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
second_leg_tracker = {}
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

BOOT_MARKER = "20pct runner re-alert v1"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Supports one or both:
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")

MIN_GAIN = 12
SCAN_MIN_GAIN = MIN_GAIN
SCAN_SLEEP = 100
ALERT_COOLDOWN_SECONDS = 1800
MAX_GAINERS = 25
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


MAX_GAINERS = 50
SCAN_MIN_GAIN = 12
ALERT_MIN_GAIN = 27
MIN_VOLUME = 50000


MAX_MARKET_CAP = 1_000_000_000
TREND_BUILDER_MIN_GAIN = 12
PREMARKET_MIN_GAIN = 8
PREMARKET_MIN_VOLUME = 50_000
def get_yahoo_market_cap(ticker):
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"

    params = {
        "modules": "price"
    }

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()

        result = data.get("quoteSummary", {}).get("result", [])
        if not result:
            return 0

        market_cap = (
            result[0]
            .get("price", {})
            .get("marketCap", {})
            .get("raw", 0)
        )

        return int(market_cap or 0)

    except Exception as e:
        print(f"[MARKET CAP ERROR] {ticker}: {e}", flush=True)
        return 0

def get_float_shares(ticker):   # 👈 NO INDENT (top level)
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"

    params = {
        "modules": "defaultKeyStatistics"
    }

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()

        result = data.get("quoteSummary", {}).get("result", [])
        if not result:
            return 0

        float_shares = (
            result[0]
            .get("defaultKeyStatistics", {})
            .get("floatShares", {})
            .get("raw", 0)
        )

        return int(float_shares or 0)

    except Exception as e:
        print(f"[FLOAT ERROR] {ticker}: {e}", flush=True)
        return 0
def get_finnhub_profile(ticker):
    if not FINNHUB_API_KEY:
        return 0, 0

    url = "https://finnhub.io/api/v1/stock/profile2"

    params = {
        "symbol": ticker,
        "token": FINNHUB_API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        market_cap_millions = float(data.get("marketCapitalization", 0) or 0)
        share_outstanding_millions = float(data.get("shareOutstanding", 0) or 0)

        market_cap = int(market_cap_millions * 1_000_000)
        float_shares = int(share_outstanding_millions * 1_000_000)

        return market_cap, float_shares

    except Exception as e:
        print(f"[FINNHUB PROFILE ERROR] {ticker}: {e}", flush=True)
        return 0, 0
def get_nasdaq_gainers():
    url = "https://api.nasdaq.com/api/screener/stocks"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/"
    }

    params = {
        "tableonly": "true",
        "limit": 200,
        "offset": 0,
        "download": "true"
    }

    movers = []

    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        data = r.json()

        rows = (
            data.get("data", {})
            .get("rows", [])
        )

        for row in rows:
            ticker = row.get("symbol")
            price_raw = str(row.get("lastsale", "0")).replace("$", "").replace(",", "")
            pct_raw = str(row.get("pctchange", "0")).replace("%", "").replace("+", "")
            vol_raw = str(row.get("volume", "0")).replace(",", "")

            if not ticker:
                continue

            if "." in ticker or "-" in ticker:
                continue

            try:
                price = float(price_raw or 0)
                gain = float(pct_raw or 0)
                volume = int(float(vol_raw or 0))
            except Exception:
                continue

            if price <= 0:
                continue

            if gain < SCAN_MIN_GAIN:
                continue

            if volume < MIN_VOLUME:
                continue

            if price > MAX_PRICE:
                continue

            movers.append({
                "ticker": ticker,
                "price": price,
                "gain": gain,
                "volume": volume
            })

        print(f"[NASDAQ] Found {len(movers)} candidates over {SCAN_MIN_GAIN}%", flush=True)
        return movers

    except Exception as e:
        print(f"[NASDAQ ERROR] {e}", flush=True)
        return []
def get_percent_gainers():
    # Yahoo expanded scanner: day gainers + most actives
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    screeners = [
    "day_gainers",
    "most_actives",
    "small_cap_gainers",
    "aggressive_small_caps",
    "undervalued_growth_stocks"
]
    all_movers = {}

    for screener in screeners:
        params = {
            "scrIds": screener,
            "count": 200
        }

        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            data = r.json()

            quotes = (
                data.get("finance", {})
                .get("result", [{}])[0]
                .get("quotes", [])
            )

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

                if gain < SCAN_MIN_GAIN:
                    continue
                    
                if volume < MIN_VOLUME:
                    continue
                    
                if price > MAX_PRICE:
                    continue

                # keep best data if duplicate
                all_movers[ticker] = {
                    "ticker": ticker,
                    "price": price,
                    "gain": gain,
                    "volume": volume
                }
        except Exception as e:
            print(f"[YAHOO {screener.upper()} ERROR] {e}", flush=True)

    nasdaq_movers = get_nasdaq_gainers()

    for m in nasdaq_movers:
        all_movers[m["ticker"]] = m

    movers = list(all_movers.values())

    movers.sort(key=lambda x: x["gain"], reverse=True)

    print(f"[YAHOO EXPANDED] Found {len(movers)} scan candidates over {SCAN_MIN_GAIN}%:", flush=True)
    print("[YAHOO EXPANDED] " + ", ".join([f"{m['ticker']} {m['gain']:.1f}%" for m in movers[:20]]), flush=True)

    return movers[:MAX_GAINERS]

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
def get_finnhub_quote(ticker):
    if not FINNHUB_API_KEY:
        return None

    url = "https://finnhub.io/api/v1/quote"

    params = {
        "symbol": ticker,
        "token": FINNHUB_API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        current = float(data.get("c", 0) or 0)
        previous_close = float(data.get("pc", 0) or 0)

        if current <= 0 or previous_close <= 0:
            return None

        gain = ((current - previous_close) / previous_close) * 100

        return {
            "price": current,
            "gain": gain
        }

    except Exception as e:
        print(f"[FINNHUB QUOTE ERROR] {ticker}: {e}", flush=True)
        return None


def check_dilution_risk(text):
    text = (text or "").lower()

    danger_words = {
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
       pass
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

    if gain > 30 and volume < 1_000_000:
        score -= 2
        risks.append("low volume spike")

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
def get_alert_title(result):
    gain = result.get("gain", 0)
    score = result.get("score", 0)
    recent_vol = result.get("recent_volume", 0)

    if result.get("trend_builder_alert"):
        return "🚨 TREND BUILDER"

    if gain >= 35 and score >= 8 and recent_vol >= 200_000:
        return "🔥 MOMENTUM RUNNER"

    if gain >= 20 and score >= 6 and recent_vol >= 100_000:
        return "🚨 BUILDING MOMENTUM"

    return "⚠️ EARLY SPIKE"
def build_alert(result, rank):
    clean_reasons = [r for r in result.get("reasons", []) if "market cap" not in r.lower()]
    reasons = ", ".join(clean_reasons) or "None"
    risks_text = "\n".join(result.get("risks", [])) or "None"

    gain = result["gain"]
    float_shares = result.get("float", 0)
    title = get_alert_title(result)

    return (
        f"{title}\n\n"
        f"Rank: #{rank}\n"
        f"{result['ticker']} | Score: {result['score']}/10\n\n"
        f"Price: ${result['price']:.4f}\n"
        f"Gain: {result['gain']:.1f}%\n"
        f"Float: {float_shares/1_000_000:.1f}M\n\n"
        f"Catalyst: {result.get('catalyst_type', 'none')}\n"
        f"{result.get('catalyst_text', '')}\n\n"
        f"Reasons:\n{reasons}\n\n"
        f"Risk:\n{risks_text}\n\n"
        f"📊 MARKET REGIME: {result.get('market_regime', 'UNKNOWN')}\n"
    )
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
      
def ema(values, period):
    if len(values) < period:
        return None

    k = 2 / (period + 1)
    ema_value = values[0]

    for price in values[1:]:
        ema_value = price * k + ema_value * (1 - k)

    return ema_value


def higher_lows_forming(candles, count=4):
    if len(candles) < count:
        return False

    lows = [float(c["low"]) for c in candles[-count:]]

    return all(lows[i] >= lows[i - 1] for i in range(1, len(lows)))


def is_big_upper_wick(candle):
    high = float(candle["high"])
    low = float(candle["low"])
    close = float(candle["close"])

    full_range = high - low
    upper_wick = high - close

    if full_range <= 0:
        return False

    return upper_wick / full_range >= 0.45


def is_trend_builder(result, candles):
    if len(candles) < 20:
        return False

    closes = [float(c["close"]) for c in candles]

    ema9 = ema(closes[-20:], 9)
    ema20 = ema(closes[-30:], 20) if len(closes) >= 30 else ema(closes, 20)
    ema50 = ema(closes[-50:], 50) if len(closes) >= 50 else None

    if ema9 is None or ema20 is None:
        return False

    above_vwap = "Price above VWAP" in result.get("reasons", [])

    volume_steady = result.get("recent_volume", 0) >= 75_000
    holding_gains = result.get("candle_session_gain", 0) >= 2
    higher_lows = higher_lows_forming(candles, count=4)
    no_bad_wick = not is_big_upper_wick(candles[-1])

    ema_stack = ema9 > ema20
    if ema50:
        ema_stack = ema9 > ema20 > ema50

    return (
        result.get("gain", 0) >= TREND_BUILDER_MIN_GAIN
        and above_vwap
        and ema_stack
        and higher_lows
        and volume_steady
        and holding_gains
        and no_bad_wick
    )
def check_sec_offering_risk(ticker):
    try:
        headers = {"User-Agent": "scanner-bot your-email@example.com"}

        tickers_url = "https://www.sec.gov/files/company_tickers.json"
        r = requests.get(tickers_url, headers=headers, timeout=10)
        companies = r.json()

        cik = None
        for item in companies.values():
            if item.get("ticker", "").upper() == ticker.upper():
                cik = str(item["cik_str"]).zfill(10)
                break

        if not cik:
            return False, "SEC CIK not found"

        filings_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = requests.get(filings_url, headers=headers, timeout=10)
        data = r.json()

        forms = data.get("filings", {}).get("recent", {}).get("form", [])
        dates = data.get("filings", {}).get("recent", {}).get("filingDate", [])

        risky_forms = {"S-1", "S-3", "424B5", "424B3", "F-1", "F-3", "6-K", "8-K"}

        hits = []
        for form, date in zip(forms[:20], dates[:20]):
            if form in risky_forms:
                hits.append(f"{form} filed {date}")

        if hits:
            return True, "; ".join(hits[:5])

        return False, "No recent offering-type SEC forms found"

    except Exception as e:
        return False, f"SEC check error: {e}"
        
    port = int(os.getenv("PORT", 10000))
def classify_news_quality(headline):
    h = (headline or "").lower()

    BAD_NEWS_KEYWORDS = [
        "top gainers",
        "stocks moving",
        "market movers",
        "premarket session",
        "here are",
        "why these stocks",
        "market update",
        "roundup",
        "shares are trading higher",
    ]

    STRONG_KEYWORDS = [
        "fda",
        "approval",
        "contract",
        "acquisition",
        "merger",
        "earnings",
        "guidance",
        "partnership",
        "deal",
    ]

    # ❌ Fake / aggregator news
    if any(k in h for k in BAD_NEWS_KEYWORDS):
        return "NONE"

    # ✅ Real catalyst
    if any(k in h for k in STRONG_KEYWORDS):
        return "STRONG"

    # ⚠️ Weak / unclear
    if h:
        return "WEAK"

    return "NONE"


def run_scanner():
    print(f"[BOOT] Scanner started | {BOOT_MARKER}", flush=True)
    print(f"[BOOT] No watchlist — scanning {SCAN_MIN_GAIN}%+ gainers with VWAP filter", flush=True)

    alert_history = {}
    runner_prices = {}

    while True:
        if not should_scan_now():
            print("[SLEEP] Market inactive — skipping scan", flush=True)
            time.sleep(60)
            continue

        print("[SCAN] Market active — running scan", flush=True)

        session, session_notes = get_market_session()
        movers = get_percent_gainers()
        results = []

        for mover in movers:
            sec_risk = False
            sec_note = ""

            ticker = mover["ticker"]

            # 🔥 Finnhub quote confirmation
            finnhub_quote = get_finnhub_quote(ticker)

            if finnhub_quote:
                mover["price"] = finnhub_quote["price"]
                mover["gain"] = finnhub_quote["gain"]
                print(f"[FINNHUB] {ticker} quote confirmed ${mover['price']:.4f} {mover['gain']:.1f}%", flush=True)
            else:
                print(f"[FINNHUB] {ticker} quote unavailable — using scanner price/gain", flush=True)

            if mover.get("volume", 0) == 0:
                mover["volume"] = 500_000

            catalyst_type, catalyst_text = get_news_catalyst(ticker)

            result = score_mover(
                mover=mover,
                catalyst_type=catalyst_type,
                catalyst_text=catalyst_text
            )

            market_cap, float_shares = get_finnhub_profile(ticker)

            result["market_cap"] = market_cap
            result["float"] = float_shares

            print(f"[MARKET CAP] {ticker}: {market_cap}", flush=True)
            print(f"[FLOAT] {ticker}: {float_shares}", flush=True)
            
            if market_cap:
                result["reasons"].append(f"Market cap: ${market_cap:,}")

            sec_risk, sec_note = check_sec_offering_risk(ticker)
            result["sec_note"] = sec_note

            if sec_risk:
                result["risks"].append(f"⚠️ SEC offering risk: {sec_note}")
                result["score"] -= 2

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
                result["high"] = max(float(c["high"]) for c in candles[-10:])
                result["prev_volume"] = sum(c["volume"] for c in candles[-10:-5]) if len(candles) >= 10 else 0

                first_close = float(candles[0]["close"])
                last_close = float(candles[-1]["close"])
                result["candle_session_gain"] = (
                    ((last_close - first_close) / first_close) * 100
                    if first_close > 0 else 0
                )
            else:
                result["candle_session_gain"] = 0

            structure = analyze_structure(ticker, candles)
            result["structure"] = structure
            result["score"] += structure.get("structure_score", 0)
            result["score"] = max(0, min(result["score"], 10))

            result["risks"].extend(structure.get("risk_flags", []))
            result["reasons"].extend(structure.get("reasons", []))

            trend_builder_alert = is_trend_builder(result, candles)
            result["trend_builder_alert"] = trend_builder_alert

            if trend_builder_alert:
                result["score"] += 2
                result["score"] = max(0, min(result["score"], 10))
                result["reasons"].append("Trend Builder: VWAP + EMAs + higher lows")

            results.append(result)
            time.sleep(0.5)

        results.sort(key=lambda x: x["score"], reverse=True)

        regime, regime_notes = detect_market_regime(results)

        for r in results:
            r["market_regime"] = regime
            r["regime_notes"] = regime_notes

        if results:
            top_line = " | ".join(
                f"#{i + 1} {r['ticker']} {r['score']}/10 {r['gain']:.1f}%"
                for i, r in enumerate(results[:10])
            )
            print(f"[SCAN] Top ranked: {top_line}", flush=True)
        else:
            print("[SCAN] No qualified gainers found", flush=True)

        now = time.time()

        for rank, result in enumerate(results, start=1):
            ticker = result["ticker"]
            price = result.get("price", 0)
            recent_vol = result.get("recent_volume", 0)
            market_cap = result.get("market_cap", 0)
            float_shares = result.get("float", 0)

            # --- RISK HOOK ---
            filing_text = result.get("filing_text", "") or result.get("catalyst_text", "")
            filing_date = result.get("filing_date", None)
            headline = result.get("catalyst_text", "") or result.get("headline", "")

            # your filter
            news_quality = classify_news_quality(headline)

            # your existing analyzer
            _, news_summary = analyze_news(headline)

            result["news_quality"] = news_quality
            result["news_summary"] = news_summary
            # --- NEWS QUALITY SCORE ADJUSTMENT ---
            if news_quality == "NONE":
               result.setdefault("risks", []).append("⚠️ No confirmed catalyst / technical momentum only")
               result["catalyst_type"] = "⚠️ TECHNICAL MOMENTUM ONLY"

            elif news_quality == "WEAK":
               result["score"] = max(0, result.get("score", 0) - 1)
               result.setdefault("risks", []).append("⚠️ Weak/unclear news")
               result["catalyst_type"] = "⚠️ WEAK NEWS"

            elif news_quality == "STRONG":
               result["score"] = min(10, result.get("score", 0) + 1)
               result["catalyst_type"] = "⚡ STRONG NEWS"
            
                 
            # --- SEC FILING CLEANUP (SMART) ---
            risk_list = build_risk(filing_text, filing_date)
            
            clean_risks = []
            
            for risk in risk_list:
                r = risk.lower()
        
            # 🚨 TRUE DILUTION / FINANCING
            if any(x in r for x in [
                "offering",
                "dilution",
                "warrant",
                "atm",
                "convertible",
                "securities purchase"
            ]):
                clean_risks.append("🚨 " + risk.replace("⚠️ ", ""))
        
            # ⚠️ JUST A FILING (NOT AUTOMATIC RISK)
            else:
                clean_risks.append(
                    risk.replace("⚠️ SEC offering risk:", "⚠️ SEC filing nearby:")
                )
            
            if clean_risks:
                result["risks"] = result.get("risks", []) + clean_risks
            # ===== TRASH FILTERS =====

            if price < 0.5 or price > 500:
                print(f"[FILTER] {ticker} skipped — price ${price:.2f} outside range", flush=True)
                continue

                print(f"[WARN] {ticker} no market cap data — allowing through", flush=True)

            elif market_cap > 1_000_000_000:
                print(f"[FILTER] {ticker} skipped — market cap over 1B", flush=True)
                continue

            if float_shares == 0:
                print(f"[WARN] {ticker} no float data — allowing through", flush=True)

            elif float_shares > 50_000_000:
                print(f"[FILTER] {ticker} skipped — float too high", flush=True)
                continue

            if result.get("gain", 0) < 25 and recent_vol < 200_000:
                print(f"[FILTER] {ticker} skipped — slow mover", flush=True)
                continue
            early_momentum_alert = (
                result["gain"] >= 12
                and result.get("volume", 0) >= 500_000
                and result.get("recent_volume", 0) >= 50_000
            )

            if early_momentum_alert:
                print(f"[EARLY] {ticker} building momentum", flush=True)

            if result["gain"] < 20 and not early_momentum_alert:
                continue

            above_vwap = "Price above VWAP" in result.get("reasons", [])
            recent_vol = result.get("recent_volume", 0)
            total_vol = result.get("total_candle_volume", 0)

            valid_early_alert = (
                result["gain"] >= 15
                and recent_vol >= 100_000
                and above_vwap
            )

            valid_runner_alert = (
                result["gain"] >= ALERT_MIN_GAIN
                and recent_vol >= 200_000
                and above_vwap
            )

            valid_emergency_runner_alert = (
                result["gain"] >= 35
                and total_vol >= 1_000_000
            )
       
            # ===== SECOND LEG + BREAKOUT BURST =====

            price = result.get("price", 0)
            gain = result.get("gain", 0)
            vwap = result.get("vwap", 0)

            above_vwap = price > vwap if vwap else "Price above VWAP" in result.get("reasons", [])

            recent_high = result.get("high", price)
            recent_vol = result.get("recent_volume", 0)
            prev_vol = result.get("prev_volume", 0)

            volume_spike = recent_vol > (prev_vol * 1.5) if prev_vol > 0 else False
            pullback = price < recent_high * 0.95

            second_leg_alert = (
                ticker in second_leg_tracker
                and not second_leg_tracker[ticker]["sent"]
                and gain >= 25
                and above_vwap
                and price > second_leg_tracker[ticker]["high"] * 1.03
            )

            breakout_burst_alert = (
                gain >= 25
                and price > recent_high
                and volume_spike
            )

            # ===== ENTRY SETUP ALERTS =====

            vwap_reclaim_setup = (
                gain >= 15
                and above_vwap
                and recent_vol >= 150_000
            )

            breakout_hold_setup = (
                gain >= 20
                and price >= recent_high * 0.98
                and recent_vol >= 200_000
            )

            dip_buy_setup = (
                gain >= 20
                and above_vwap
                and pullback
                and recent_vol >= 150_000
            )
            trend_builder_alert = result.get("trend_builder_alert", False)

            should_alert = (
                valid_early_alert
                or valid_runner_alert
                or valid_emergency_runner_alert
                or early_momentum_alert
                or trend_builder_alert
                or second_leg_alert
                or breakout_burst_alert
                or vwap_reclaim_setup
                or breakout_hold_setup
                or dip_buy_setup
            )

            if second_leg_alert:
                print(f"🟢 SECOND LEG BUILDING {ticker} {price}", flush=True)

            if breakout_burst_alert:
                print(f"🚀 BREAKOUT BURST {ticker} {price}", flush=True)
            last_alert = alert_history.get(ticker, 0)
            cooldown_done = now - last_alert >= ALERT_COOLDOWN_SECONDS
            current_price = float(result.get("price", 0))
            last_alert_price = runner_prices.get(ticker, 0)
            new_high_realert = current_price > last_alert_price

            result["rank_score"] = rank_result(result)
            result["trade_bias"] = build_trade_bias(result)

            alert_tag = ""

            if trend_builder_alert:
                alert_tag = "\n\n🚨 TREND BUILDER\nControlled trend forming: VWAP hold + EMAs stacked + higher lows"
            elif second_leg_alert:
                alert_tag = "\n\n🔥 SECOND LEG CONFIRMED"
            elif breakout_burst_alert:
                alert_tag = "\n\n🚀 BREAKOUT BURST"
            elif vwap_reclaim_setup:
                alert_tag = "\n\n🟢 VWAP RECLAIM SETUP"
            elif breakout_hold_setup:
                alert_tag = "\n\n🚀 BREAKOUT HOLD SETUP"
            elif dip_buy_setup:
                alert_tag = "\n\n📈 DIP BUY SETUP"

            if should_alert and result["score"] >= 6:
                if cooldown_done or new_high_realert:
                    sent = send_alert(build_alert(result, rank) + alert_tag)

                    if sent:
                        alert_history[ticker] = now
                        runner_prices[ticker] = current_price
                        print(f"[ALERT SENT] #{rank} {ticker}", flush=True)
                    else:
                        print(f"[ALERT FAILED] #{rank} {ticker}", flush=True)
                else:
                    print(f"[NO ALERT] #{rank} {ticker} cooldown active", flush=True)
            else:
                print(
                    f"[NO ALERT] #{rank} {ticker} blocked | "
                    f"gain={result['gain']:.1f}% recent_vol={recent_vol:,}",
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


   
