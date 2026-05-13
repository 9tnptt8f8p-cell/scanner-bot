import os
import re
import time
import requests
from bs4 import BeautifulSoup
from threading import Thread
from flask import Flask
from dotenv import load_dotenv
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from risk_engine import build_risk
from structure_engine import analyze_structure
from alerts import send_alert
from rank_engine import rank_result

load_dotenv()
def build_trade_bias(result):
    """Bias engine. Dilution is awareness-only and must not override runner/trap bias."""
    news_quality = result.get("news_quality", "")
    structure = " ".join(result.get("reasons", []) + result.get("risks", [])).lower()

    price = result.get("price_float", float(result.get("price", 0) or 0))
    vwap = result.get("vwap_float", float(result.get("vwap", 0) or 0))
    recent_volume = int(result.get("recent_volume", result.get("volume", 0)) or 0)

    above_vwap = True
    vwap_distance = 0
    if vwap and price:
        vwap_distance = ((price - vwap) / vwap) * 100
        above_vwap = price >= vwap
        result["above_vwap"] = above_vwap
        result["vwap_distance"] = round(vwap_distance, 2)

    has_higher_lows = result.get("has_higher_lows", False) or "higher lows" in structure
    breakout_confirmed = result.get("breakout_confirmed", False) or "breakout" in structure or "strong candle close" in structure

    true_second_leg = above_vwap and has_higher_lows and breakout_confirmed and recent_volume >= 150000
    result["true_second_leg"] = true_second_leg

    result["momentum_decay"] = bool((vwap and price and price < vwap) or result.get("volume_fading", False))

    if news_quality == "NEGATIVE":
        return "❌ Negative catalyst — avoid unless extreme scalp only"

    if vwap and price:
        if vwap_distance <= -12:
            return "🚨 Way below VWAP — failed momentum / avoid"
        if vwap_distance < 0:
            return "👀 Slightly below VWAP — reclaim watch"

    if result.get("momentum_decay", False) and not true_second_leg:
        return "⚠️ Momentum faded — wait for reclaim"

    if "upper wick" in structure or "trap" in structure:
        return "⚠️ Trap risk — wait for cleaner setup"

    if true_second_leg:
        return "🚀 RUNNER WATCH — VWAP hold + second-leg setup"

    if news_quality == "STRONG":
        return "✅ Strong catalyst — watch for continuation"

    if news_quality == "WEAK":
        return "⚠️ Weak catalyst — could fade fast"

    return "🤔 Mixed/unclear — wait for confirmation"


    price = result.get("price_float", float(result.get("price", 0) or 0))
    vwap = result.get("vwap_float", float(result.get("vwap", 0) or 0))
    gain = float(
        result.get("gain_percent", result.get("gain", 0)) or 0
    )
    recent_volume = int(result.get("recent_volume", result.get("volume", 0)) or 0)

    above_vwap = False
    if vwap and price:
        vwap_distance = ((price - vwap) / vwap) * 100
        above_vwap = price >= vwap
        result["above_vwap"] = above_vwap
        result["vwap_distance"] = round(vwap_distance, 2)
    else:
        vwap_distance = 0

    has_higher_lows = (
        result.get("has_higher_lows", False)
        or "higher lows" in structure
    )

    breakout_confirmed = (
        result.get("breakout_confirmed", False)
        or "breakout" in structure
        or "strong candle close" in structure
    )

    true_second_leg = (
        above_vwap
        and has_higher_lows
        and breakout_confirmed
        and recent_volume >= 150000
    )

    result["true_second_leg"] = true_second_leg

    # --- MOMENTUM DECAY ---
    if gain > 20 and vwap and price and price < vwap:
        result["momentum_decay"] = True
    else:
        result["momentum_decay"] = False

    if "offering" in risks or "dilution" in risks or "warrant" in risks:
        return "⚠️ High risk — dilution/financing overhang"

    if news_quality == "NEGATIVE":
        return "❌ Negative catalyst — avoid unless extreme scalp only"

    if vwap and price:
        if vwap_distance <= -12:
            return "🚨 Way below VWAP — failed momentum / avoid"

        elif vwap_distance < 0:
            return "👀 Slightly below VWAP — reclaim watch"

    if result.get("momentum_decay", False):
        return "⚠️ MOMENTUM FADED — wait for reclaim"

    if "upper wick" in structure or "trap" in structure:
        return "⚠️ Trap risk — wait for cleaner setup"

    if true_second_leg:
        return "🚀 RUNNER WATCH — VWAP hold + second-leg setup"

    if news_quality == "STRONG":
        return "✅ Strong catalyst — watch for continuation"

    if news_quality == "WEAK":
        return "⚠️ Weak catalyst — could fade fast"

    return "🤔 Mixed/unclear — wait for confirmation"
    
def is_above_vwap(price, vwap):
    if not vwap or not price:
        return True
    return price > (vwap * 0.995)
    
    
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

    # Scan only 7:30 AM ET to 4:10 PM ET
    if not (dtime(7, 30) <= now.time() < dtime(16, 10)):
        return False

    return True

BOOT_MARKER = "elite scanner rebuild v4 — cleaner alerts + awareness-only dilution + fresh gainer tiers"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Supports one or both:
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")

MIN_GAIN = 12
SCAN_MIN_GAIN = 12
SCAN_SLEEP = 90

ALERT_COOLDOWN_SECONDS = 900
EARLY_ALERT_COOLDOWN = 600
MAX_ALERTS_PER_CYCLE = 5

MAX_GAINERS = 60
ALERT_MIN_GAIN = 20
MIN_VOLUME = 50_000
MAX_PRICE = 100

MAX_MARKET_CAP = 1_000_000_000
TREND_BUILDER_MIN_GAIN = 12
PREMARKET_MIN_GAIN = 8
PREMARKET_MIN_VOLUME = 50_000

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

# Legacy fallback only — active alerts use alerts.send_alert()
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

MAX_MARKET_CAP = 1_000_000_000
TREND_BUILDER_MIN_GAIN = 12
PREMARKET_MIN_GAIN = 8
PREMARKET_MIN_VOLUME = 50_000

def get_finnhub_profile(ticker):
    now = time.time()

    if ticker in PROFILE_CACHE:
        cached = PROFILE_CACHE[ticker]

        if now - cached["time"] < CACHE_TTL_SECONDS:
            return cached["data"]
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
        market_cap = float(data.get("marketCapitalization") or 0) * 1_000_000
        float_shares = float(data.get("shareOutstanding") or 0) * 1_000_000

        PROFILE_CACHE[ticker] = {
            "time": now,
            "data": (market_cap, float_shares)
        }
        return market_cap, float_shares

        market_cap = int(market_cap_millions * 1_000_000)
        float_shares = int(share_outstanding_millions * 1_000_000)

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
            ticker = ticker.upper()
            
            BAD_SUFFIXES = ("WS", "WT", "WQ", "WSA", "WSC", "IW", "WARRANT")
            
            if ticker.endswith(BAD_SUFFIXES) or (ticker.endswith("W") and len(ticker) > 4):
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
        ticker = m["ticker"].upper()

        BAD_SUFFIXES = ("WS", "WT", "WQ", "WSA", "WSC", "IW", "WARRANT")

        if ticker.endswith(BAD_SUFFIXES) or (
            ticker.endswith("W") and len(ticker) > 4
        ):
            continue

        m["ticker"] = ticker
        all_movers[ticker] = m

    movers = list(all_movers.values())
    movers.sort(key=lambda x: x["gain"], reverse=True)

    print(
        f"[YAHOO EXPANDED] Found {len(movers)} scan candidates over {SCAN_MIN_GAIN}%:",
        flush=True
    )

    print(
        "[YAHOO EXPANDED] "
        + ", ".join([f"{m['ticker']} {m['gain']:.1f}%" for m in movers[:20]]),
        flush=True
    )
 
    return movers[:max(MAX_GAINERS, 100)]

def get_yahoo_candles(ticker):
    """Robust Yahoo candle fallback. Handles malformed/missing quote arrays cleanly."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "5m", "range": "1d"}
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json() if r.content else {}
        chart = data.get("chart", {})
        results = chart.get("result") or []
        if not results:
            return []

        quote_list = (results[0].get("indicators", {}) or {}).get("quote") or []
        if not quote_list:
            return []
        quote = quote_list[0] or {}

        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []

        candles = []
        for o, h, l, c, v in zip(opens, highs, lows, closes, volumes):
            if None in (o, h, l, c, v):
                continue
            try:
                candles.append({
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(c),
                    "volume": int(float(v or 0)),
                })
            except Exception:
                continue

        return candles
    except Exception as e:
        print(f"[CANDLE ERROR] {ticker}: {e}", flush=True)
        return []


    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()

        result = data["chart"]["result"][0]
        quote = result["indicators"]["quote"][0]

        candles = []

        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])

        for o, h, l, c, v in zip(
            opens,
            highs,
            lows,
            closes,
            volumes
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

def score_mover(mover, catalyst_type, catalyst_text):
    """
    Cleaner base score. Structure/news adjustments happen later after candles.
    Goal: do NOT kill clean no-news runners too early.
    """
    score = 0
    reasons = []
    risks = []

    ticker = str(mover.get("ticker", "")).upper()
    gain = float(mover.get("gain", mover.get("gain_percent", 0)) or 0)
    price = float(mover.get("price", 0) or 0)
    volume = int(float(mover.get("volume", 0) or 0))

    if gain >= 100:
        score += 5
        reasons.append("100%+ gainer")
    elif gain >= 75:
        score += 4
        reasons.append("75%+ gainer")
    elif gain >= 50:
        score += 3
        reasons.append("50%+ gainer")
    elif gain >= 25:
        score += 2
        reasons.append("25%+ mover")
    elif gain >= 15:
        score += 1
        reasons.append("15%+ early mover")

    if volume >= 20_000_000:
        score += 4
        reasons.append("20M+ volume")
    elif volume >= 10_000_000:
        score += 3
        reasons.append("10M+ volume")
    elif volume >= 2_000_000:
        score += 2
        reasons.append("2M+ volume")
    elif volume >= 500_000:
        score += 1
        reasons.append("500k+ volume")

    news_quality = classify_news_quality(catalyst_text)
    if news_quality == "STRONG":
        score += 2
        reasons.append("confirmed catalyst")
    elif news_quality == "WEAK":
        score += 1
        reasons.append("weak catalyst")
    elif news_quality == "JUNK":
        risks.append("⚠️ Aggregator headline only")
    else:
        # small warning only, no heavy penalty here
        risks.append("⚠️ No confirmed catalyst / technical momentum only")

    if catalyst_type in ["earnings", "patent", "contract", "legal", "biotech"]:
        score += 1
        reasons.append(f"strong catalyst: {catalyst_type}")

    if gain > 30 and volume < 500_000:
        score -= 2
        risks.append("thin-volume spike")

    if price < 1:
        risks.append("sub-$1 stock")

    return {
        "ticker": ticker,
        "price": price,
        "gain": gain,
        "gain_percent": gain,
        "volume": volume,
        "score": max(0, min(score, 10)),
        "catalyst_type": catalyst_type,
        "catalyst_text": catalyst_text,
        "news_quality": news_quality,
        "reasons": reasons,
        "risks": risks,
    }

def get_alert_title(result):
    risks_text = " ".join(result.get("risks", [])).lower()
    structure_text = " ".join(result.get("reasons", [])).lower()

    score = result.get("score", 0)
    recent_vol = result.get("recent_volume", 0)
    gain = result.get("gain", 0)

    # --- HARD TRAP FILTER ---
    if detect_bad_structure(risks_text):
        return "⚠️ TRAP / AVOID"

    # --- MOMENTUM DECAY ---
    if result.get("momentum_decay", False):
        return "⚠️ MOMENTUM FADED"

    # --- TREND BUILDER (TOP PRIORITY) ---
    if result.get("trend_builder_alert", False):
        return "🚨 TREND BUILDER"

    # --- TRUE SECOND LEG ---
    if result.get("true_second_leg", False):

        # tighter elite condition
        if (
            result.get("price", 0) >= result.get("recent_high", 0) * 0.97
            and recent_vol >= 150000
            and (
                result.get("has_higher_lows", False)
                or "higher lows" in structure_text
            )
        ):
            return "🚨 SECOND LEG COIL BREAKOUT"

        return "🟢 RUNNER WATCH"

    # --- SCORE-BASED TITLES ---
    if score >= 9 and recent_vol >= 200000:
        return "🔥 MOMENTUM RUNNER"

    if score == 8:
        return "🚨 BUILDING MOMENTUM"

    if score == 7:
        return "👀 POTENTIAL RUNNER"

    if score == 6:
        return "⚠️ EARLY MOMENTUM WATCH"

    return "⚠️ EARLY SPIKE WATCH"
    
def get_alert_status(result):
    score = result.get("score", 0)

    # --- MOMENTUM DECAY ---
    if result.get("momentum_decay", False):
        return "Momentum faded — lost trend strength, reclaim needed."

    # --- TRUE SECOND LEG ---
    if result.get("true_second_leg", False):
        return "Confirmed continuation setup — VWAP hold + higher lows."

    # --- TREND BUILDER ---
    if result.get("trend_builder_alert", False):
        return "Strong trend structure — continuation possible."

    # --- SCORE STATUS ---
    if score >= 9:
        return "Confirmed momentum — strong runner conditions."

    elif score == 8:
        return "Building momentum — wait for clean entry confirmation."

    elif score == 7:
        return "Potential runner forming — needs more confirmation."

    elif score == 6:
        return "Early momentum forming — watch only, needs confirmation."

    return "Early move detected — NOT confirmed yet."
        
def build_alert(result):

    # --- CLEAN REASONS ---
    clean_reasons = []
    seen = set()

    news_quality = result.get("news_quality", "")

    for r in result.get("reasons", []):
        if not r:
            continue

        r = str(r).strip()
        low = r.lower()

        if "market cap" in low:
            continue
        if "daily" in low:
            continue
        if "fresh daily breakout" in low:
            continue
        if "fresh news" in low and news_quality in ["NONE", "UNKNOWN", "JUNK"]:
            continue

        if r in seen:
            continue

        seen.add(r)
        clean_reasons.append(r)

    reasons_text = "\n".join(clean_reasons) if clean_reasons else "None"

    # --- CLEAN RISKS ---
    clean_risks = []
    seen = set()

    for r in result.get("risks", []):
        if not r:
            continue

        r = str(r).strip()

        if r.lower() in ["none", "n/a", ""]:
            continue

        if r in seen:
            continue

        seen.add(r)
        clean_risks.append(r)

    risk_text = "\n".join(clean_risks) if clean_risks else "None"

    float_shares = result.get("float", 0) or 0

    # ONE TITLE ONLY
    title = result.get("title") or get_alert_title(result)

    status = get_alert_status(result)

    catalyst_text = result.get("catalyst_text", "") or ""
    catalyst_type = result.get("catalyst_type", "none")

    no_news_warning = ""
    if news_quality in ["NONE", "UNKNOWN", "JUNK"]:
        no_news_warning = "⚠️ No confirmed catalyst — technical move only\n"

    alert_text = (
        f"{title}\n\n"
        f"{result['ticker']} | Score: {result['score']}/10\n\n"
        f"Price: ${result['price']:.4f}\n"
        f"Gain: {result['gain']:.1f}%\n"
        f"Float: {float_shares/1_000_000:.1f}M\n\n"
        f"Catalyst: {catalyst_type}\n"
        f"{catalyst_text}\n\n"
        f"{no_news_warning}"
        f"Status:\n{status}\n"
        f"Bias: {result.get('trap_runner', 'UNKNOWN')}\n"
        f"Entry: {result.get('entry_hint', 'N/A')}\n\n"
        f"Reasons:\n{reasons_text}\n\n"
        f"Risk:\n{risk_text}\n\n"
        f"📊 MARKET REGIME: {result.get('market_regime', 'UNKNOWN')}\n"
    )

    return alert_text
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

    price = result.get("price_float", float(result.get("price", 0) or 0))
    vwap = result.get("vwap_float", float(result.get("vwap", 0) or 0))
    
    has_vwap = result.get("has_vwap", False)
    above_vwap = result.get(
        "above_vwap",
        is_above_vwap(price, vwap)
    )
    volume_steady = result.get("recent_volume", 0) >= 75_000
    holding_gains = result.get("candle_session_gain", 0) >= 2
    higher_lows = higher_lows_forming(candles, count=4)
    no_bad_wick = bool(candles) and not is_big_upper_wick(candles[-1])

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
    now = time.time()

    if ticker in SEC_CACHE:
        cached = SEC_CACHE[ticker]

        if now - cached["time"] < CACHE_TTL_SECONDS:
            return cached["data"]
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

        risky_forms = {"S-1", "S-3", "424B5", "424B3", "F-1", "F-3"}

        hits = []
        for form, date in zip(forms[:20], dates[:20]):
            if form in risky_forms:
                hits.append(f"{form} filed {date}")
        
        if hits:
            data = (True, "; ".join(hits[:5]))
            SEC_CACHE[ticker] = {"time": now, "data": data}
            return data
        
        data = (False, "No recent offering-type SEC forms found")
        SEC_CACHE[ticker] = {"time": now, "data": data}
        return data

    except Exception as e:
        return False, f"SEC check error: {e}"


# --- NEWS QUALITY DETECTION ---
    
BAD_NEWS_KEYWORDS = [
    "top gainers",
    "stocks moving",
    "stocks are moving",
    "these stocks are moving",
    "moving in today's session",
    "market movers",
    "premarket session",
    "premarket movers",
    "moving before the opening bell",
    "here are",
    "why these stocks",
    "why shares are trading",
    "why is it moving",
    "market update",
    "roundup",
    "shares are trading higher",
    "driving market activity",
    "attracting the most attention",
    "gapping stocks",
    "most active stocks",
    "gap-ups and gap-downs",
    "top gainers and losers",
    "pre-market session",
    "premarket session",
    "market session",
    "gainers and losers",
    "stocks to watch today",
    "insights into",
    "get insights into",
    "shares are trading higher",
    "stocks moving premarket",
    "here are 20 stocks moving",
    ]

STRONG_KEYWORDS = [
    "fda", "approval", "approved", "clearance", "cleared", "510(k)",
    "clinical trial", "phase 1", "phase 2", "phase 3",
    "positive data", "topline", "endpoint", "orphan drug",
    "fast track", "breakthrough therapy",

    "contract", "agreement", "partnership", "collaboration",
    "deal", "order", "purchase order",
    "supply agreement", "distribution agreement",
    "license agreement", "strategic alliance",
    "definitive agreement", "letter of intent",

    "mou", "memorandum of understanding",
    "financing", "advance financing",
    "facility", "battery", "solid-state battery",
    "infrastructure", "validation initiative",

    "acquisition", "merger", "buyout", "takeover",

    "earnings", "revenue", "guidance",
    "raises guidance", "profitability", "record revenue",

    "bitcoin", "ethereum", "crypto", "blockchain",
    "artificial intelligence", "ai-powered", "nvidia",

    "primary endpoint", "met primary endpoint",
    "statistically significant", "pivotal trial",
    "new drug application", "nda", "bla",
    "510k", "de novo",
    "commercial launch",
]

def classify_news_quality(headline):
    if not headline:
        return "NONE"

    text = str(headline).lower().strip()

    if text in ["none", "no fresh catalyst found", "news check failed"]:
        return "NONE"

    if any(word in text for word in BAD_NEWS_KEYWORDS):
        return "JUNK"

    if any(word in text for word in STRONG_KEYWORDS):
        return "STRONG"

    return "WEAK"


def describe_news_quality(headline, news_quality=None):
    text = (headline or "").lower()

    if not headline or text in ["none", "no fresh catalyst found"]:
        return "❌ NO CLEAR NEWS"

    if news_quality == "JUNK":
        return "⚠️ WEAK NEWS / MOVER ROUNDUP"

    if news_quality == "NONE":
        return "❌ NO CLEAR NEWS"

    if news_quality == "STRONG":
        return "⚡ STRONG NEWS"

    if news_quality == "WEAK":
        return "🟡 WEAK NEWS"

    return "📰 NEWS FOUND"

def describe_volume_quality(candles):
    try:
        if not candles or len(candles) < 12:
            return "🟡 Volume data insufficient"

        recent_candles = candles[-3:]
        prior_candles = candles[-12:-3]

        recent_avg = sum(c.get("volume", 0) for c in recent_candles) / len(recent_candles)
        prior_avg = sum(c.get("volume", 0) for c in prior_candles) / len(prior_candles)

        if prior_avg <= 0:
            return "🟡 Volume unclear"

        volume_ratio = recent_avg / prior_avg

        if volume_ratio >= 2.0:
            return "🔥 Volume expanding — momentum increasing"

        if volume_ratio <= 0.7:
            return "⚠️ Volume fading — momentum weakening"

        return "🟡 Volume stable"

    except Exception:
        return "🟡 Volume analysis unavailable"
def describe_momentum_state(result):
    reasons = " ".join(result.get("reasons", [])).lower()
    risks = " ".join(result.get("risks", [])).lower()

    bad_signs = (
        "below vwap" in reasons
        or "below vwap" in risks
        or "upper wick" in reasons
        or "upper wick" in risks
        or "trap" in reasons
        or "trap" in risks
        or result.get("momentum_fading", False)
    )

    good_signs = (
        "price above vwap" in reasons
        or "higher lows" in reasons
        or result.get("volume_confirmed", False)
    )

    if bad_signs and not good_signs:
        return "⚠️ Momentum fading / trap risk"

    if bad_signs and good_signs:
        return "🟡 Mixed momentum — wait for confirmation"

    if good_signs:
        return "🔥 Momentum still active"

    return "🟡 Momentum unclear"
    
def extract_warrant_price(text):
    t = (text or "").lower()

    patterns = [
        r"exercise price of \$?(\d+(?:\.\d+)?)",
        r"exercise price equal to \$?(\d+(?:\.\d+)?)",
        r"exercisable at \$?(\d+(?:\.\d+)?)",
        r"exercise price is \$?(\d+(?:\.\d+)?)",
        r"warrants.*?exercise price.*?\$?(\d+(?:\.\d+)?)",
        r"warrants.*?\$?(\d+(?:\.\d+)?) per share",
    ]

    for pattern in patterns:
        match = re.search(pattern, t)
        if match:
            try:
                return float(match.group(1))
            except:
                return None

    return None
    
def describe_dilution_risk(risk_text):
    text = (risk_text or "").lower()

    strong_words = [
        "registered direct",
        "private placement",
        "securities purchase agreement",
        "atm offering",
        "at-the-market",
        "equity distribution agreement",
        "sales agreement",
        "warrant",
        "convertible",
        "equity line",
        "resale",
        "selling stockholder",
    ]

    medium_words = [
        "s-3",
        "f-3",
        "shelf",
        "prospectus",
        "424b5",
        "424b3",
    ]

    if any(w in text for w in strong_words):
        return "🚨 CONFIRMED DILUTION RISK: offering/warrants/financing language found → possible sell pressure on spikes"

    if any(w in text for w in medium_words):
        return "⚠️ DILUTION RISK BUILDING: shelf/prospectus filing found → company may be able to raise capital"

    if "8-k" in text or "6-k" in text:
        return "🟡 SEC FILINGS PRESENT: recent filings found, but no clear dilution terms confirmed"

    return ""
    
def detect_offering_risk(text, price=0):
    if not text:
        return []

    t = text.lower()
    risks = []

    if (
        "at-the-market" in t
        or "at the market" in t
        or "atm offering" in t
        or "equity distribution agreement" in t
        or "sales agreement" in t
    ):
        risks.append("🚨 ATM offering — company can sell shares anytime")

    if "registered direct offering" in t:
        risks.append("🚨 Registered direct offering — immediate dilution")

    if "private placement" in t:
        risks.append("🚨 Private placement — dilution risk")

    if "securities purchase agreement" in t or "purchase agreement" in t:
        risks.append("🚨 Securities purchase agreement — financing/dilution")

    if "shelf registration" in t or "form s-3" in t or "form f-3" in t:
        risks.append("⚠️ Shelf registration — future dilution possible")

    if "resale" in t or "resale prospectus" in t:
        risks.append("⚠️ Resale registration — shares may unlock for selling")

    if "convertible" in t:
        risks.append("🚨 Convertible financing — can convert into shares")

    if "warrant" in t:
        warrant_price = extract_warrant_price(t)

        if warrant_price:
            if price and price >= warrant_price:
                risks.append(f"🚨 Warrants in-the-money — ${warrant_price:.2f} vs current ${price:.2f}")
            else:
                risks.append(f"⚠️ Warrants detected — exercise ${warrant_price:.2f} not active yet")
        else:
            risks.append("⚠️ Warrants detected — exercise price unknown")

    return risks
    
def scrape_pr_headline(ticker):

    now = time.time()
    cache_key = ticker.upper()

    if cache_key in PR_CACHE:
        cached_time, cached_result = PR_CACHE[cache_key]
        if now - cached_time < PR_CACHE_TTL_SECONDS:
            return cached_result

    sources = [
        f"https://www.prnewswire.com/search/news/?keyword={ticker}",
    ]

    headers = {"User-Agent": "Mozilla/5.0"}
    for url in sources:
        try:
            r = requests.get(url, headers=headers, timeout=1)

            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "html.parser")

            for tag in soup.find_all(["a", "h1", "h2", "h3"]):
                text = tag.get_text(" ", strip=True)

                if not text or len(text) < 25:
                    continue

                if not re.search(rf"\b{re.escape(ticker)}\b", text, re.IGNORECASE):
                    continue

                quality = classify_news_quality(text)

                if quality == "STRONG":
                    PR_CACHE[cache_key] = (now, text)
                    return text

        except Exception as e:
            print(f"[PR SCRAPE ERROR] {ticker}: {e}", flush=True)

    PR_CACHE[cache_key] = (now, "")
    return ""

def find_real_news_headline(ticker, current_headline=""):
    """Find usable catalyst; rejects roundup junk and falls back to Yahoo/PR."""
    now = time.time()
    ticker = str(ticker or "").upper().strip()

    if ticker in NEWS_CACHE:
        cached = NEWS_CACHE[ticker]
        if now - cached["time"] < CACHE_TTL_SECONDS:
            return cached["data"]

    def clean_headline(text):
        text = str(text or "").strip()
        if not text:
            return ""
        lower = text.lower()
        junk_phrases = [
            "top gainers", "top gainers and losers", "stocks moving", "stocks are moving",
            "pre-market session", "premarket session", "market session", "gainers and losers",
            "stocks to watch today", "insights into", "get insights into", "market movers",
            "gap-ups and gap-downs", "most active stocks", "shares are trading higher",
            "why shares are trading", "why these stocks", "roundup",
        ]
        if any(x in lower for x in junk_phrases):
            return ""
        return text

    current_headline = clean_headline(current_headline)
    quality = classify_news_quality(current_headline)

    if quality in ["STRONG", "WEAK"]:
        data = (current_headline, quality)
        NEWS_CACHE[ticker] = {"time": now, "data": data}
        return data

    # Yahoo news fallback — strict ticker word matching to avoid false hits like GLE inside a word.
    try:
        url = f"https://finance.yahoo.com/quote/{ticker}/news/"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=2)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            for link in soup.find_all("a"):
                text = clean_headline(link.get_text(" ", strip=True))
                if not text or len(text) < 25:
                    continue
                if not re.search(rf"{re.escape(ticker)}", text, re.IGNORECASE):
                    continue
                scraped_quality = classify_news_quality(text)
                if scraped_quality in ["STRONG", "WEAK"]:
                    print(f"[NEWS SCRAPE] {ticker}: {text} ({scraped_quality})", flush=True)
                    data = (text, scraped_quality)
                    NEWS_CACHE[ticker] = {"time": now, "data": data}
                    return data
    except Exception as e:
        print(f"[YAHOO SCRAPE ERROR] {ticker}: {e}", flush=True)

    pr_headline = scrape_pr_headline(ticker)
    if pr_headline:
        pr_quality = classify_news_quality(pr_headline)
        data = (pr_headline, pr_quality)
        NEWS_CACHE[ticker] = {"time": now, "data": data}
        return data

    data = ("No fresh catalyst found", "NONE")
    NEWS_CACHE[ticker] = {"time": now, "data": data}
    return data


    now = time.time()

    if ticker in NEWS_CACHE:
        cached = NEWS_CACHE[ticker]

        if now - cached["time"] < CACHE_TTL_SECONDS:
            return cached["data"]

    # 🚫 Hard reject aggregator junk before anything else
    headline_lower = str(current_headline or "").lower()

    junk_phrases = [
        "top gainers and losers",
        "pre-market session",
        "premarket session",
        "market session",
        "gainers and losers",
        "stocks to watch today",
        "insights into",
        "get insights into",
    ]

    if any(x in headline_lower for x in junk_phrases):
        current_headline = ""

    quality = classify_news_quality(current_headline)

    quality = classify_news_quality(current_headline)

    # ✅ Keep good headline
    if quality in ["STRONG", "WEAK"]:

        data = (current_headline, quality)

        NEWS_CACHE[ticker] = {
            "time": now,
            "data": data
        }

        return data

    # 🔎 Try Yahoo scrape
    try:
        url = f"https://finance.yahoo.com/quote/{ticker}/news/"
        headers = {"User-Agent": "Mozilla/5.0"}

        r = requests.get(url, headers=headers, timeout=1)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            links = soup.find_all("a")

            for link in links:
                text = link.get_text(" ", strip=True)

                if not text or len(text) < 25:
                    continue

                if not re.search(rf"\b{re.escape(ticker)}\b", text, re.IGNORECASE):
                    continue

                if any(
                    x in text.lower()
                    for x in ["stocks moving", "top gainers", "market update"]
                ):
                    continue

                scraped_quality = classify_news_quality(text)

                if scraped_quality == "STRONG":
                    print(f"[NEWS SCRAPE] {ticker}: {text}", flush=True)

                    data = (text, scraped_quality)

                    NEWS_CACHE[ticker] = {
                        "time": now,
                        "data": data
                    }

                    return data

    except Exception as e:
        print(f"[YAHOO SCRAPE ERROR] {ticker}: {e}", flush=True)
        
       # 🔎 Try PR fallback only for higher-quality candidates
    should_pr_scrape = False

    # TEMP simple version: only scrape PR if no usable headline
    if quality in ["NONE", "JUNK"]:
        should_pr_scrape = True

    if should_pr_scrape:
        pr_headline = scrape_pr_headline(ticker)
    else:
        pr_headline = ""

    if pr_headline:
        pr_quality = classify_news_quality(pr_headline)

        data = (pr_headline, pr_quality)

        NEWS_CACHE[ticker] = {
            "time": now,
            "data": data
        }

        return data

    # ❌ Nothing found
    data = (current_headline, "NONE")

    NEWS_CACHE[ticker] = {
        "time": now,
        "data": data
    }

    return data
PROFILE_CACHE = {}
NEWS_CACHE = {}
SEC_CACHE = {}
CACHE_TTL_SECONDS = 60 * 30
PR_CACHE = {}
PR_CACHE_TTL_SECONDS = 60 * 30

# --- CONSOLIDATION / COIL DETECTION ---
def detect_consolidation(candles, lookback=6):
    if not candles or len(candles) < lookback:
        return False, 0

    recent = candles[-lookback:]
    highs = [c["high"] for c in recent]
    lows = [c["low"] for c in recent]

    range_pct = (max(highs) - min(lows)) / max(highs)

    tight = range_pct <= 0.15

    return tight, lookback
def update_vwap_state(result):
    price = float(result.get("price", 0) or 0)
    vwap = float(result.get("vwap", 0) or 0)

    has_vwap = vwap > 0
    above_vwap = price > vwap if has_vwap else True

    result["price_float"] = price
    result["vwap_float"] = vwap
    result["has_vwap"] = has_vwap
    result["above_vwap"] = above_vwap

    if has_vwap and price:
        result["vwap_distance"] = round(
            ((price - vwap) / vwap) * 100,
            2
        )
    else:
        result["vwap_distance"] = 0

    return result
def detect_bad_structure(structure_text):
    bad_keywords = [
        "below vwap",
        "upper wick",
        "trap",
        "failed",
        "lower highs",
        "rejection",
        "dead chop",
        "weak candle",
        "vwap rejection",
        
        "bad structure",
        "avoid chasing",
    ]

    return any(k in structure_text for k in bad_keywords)
def has_volume_confirmation(result, min_recent=75_000):
    recent_vol = result.get("recent_volume", 0)
    prev_vol = result.get("prev_volume", 0)

    return (
        recent_vol >= min_recent
        and recent_vol >= prev_vol
    )
def has_strong_news(result):
    headline = str(result.get("headline", "") or "").lower()
    catalyst = str(result.get("catalyst", "") or "").lower()
    catalyst_type = str(result.get("catalyst_type", "") or "").lower()
    news_quality = str(result.get("news_quality", "") or "").upper()

    strong_words = [
        "fda", "approval", "cleared", "clearance", "phase",
        "positive data", "topline", "contract", "agreement",
        "partnership", "purchase order", "merger", "acquisition",
        "earnings", "guidance", "patent", "mou", "financing"
    ]

    text = f"{headline} {catalyst} {catalyst_type}"

    return (
        news_quality == "STRONG"
        or catalyst_type in ["strong", "contract", "fda", "earnings", "biotech", "merger"]
        or any(word in text for word in strong_words)
    )
    
def has_momentum_structure(result):
    return (
        result.get("true_second_leg", False)
        or result.get("trend_builder_alert", False)
        or result.get("clean_trend_runner", False)
    )
def adjust_score(result, amount, reason=None, risk=None):
    result["score"] = max(0, min(10, result.get("score", 0) + amount))

    if reason:
        if reason not in result.setdefault("reasons", []):
            result["reasons"].append(reason)

    if risk:
        if risk not in result.setdefault("risks", []):
            result["risks"].append(risk)

    return result
    

# =====================================================================
# CLEAN REBUILD HELPERS — alert quality, dedupe, contradiction cleanup
# =====================================================================
BAD_TICKER_SUFFIXES = ("WS", "WT", "WQ", "WSA", "WSC", "IW", "WARRANT")


def is_bad_ticker(ticker):
    ticker = str(ticker or "").upper().strip()
    return (
        not ticker
        or "." in ticker
        or "-" in ticker
        or ticker.endswith(BAD_TICKER_SUFFIXES)
        or (ticker.endswith("W") and len(ticker) > 4)
    )


def dedupe_phrases(items):
    """Dedupes noisy repeated reasons like VWAP / higher lows / structure."""
    cleaned = []
    seen_keys = set()
    buckets = {
        "vwap": ["vwap", "above vwap", "price above vwap", "vwap hold"],
        "higher_lows": ["higher lows"],
        "clean_structure": ["clean structure", "clean trend runner structure", "structure confirmation"],
        "no_news": ["no confirmed catalyst", "technical momentum only"],
        "volume_fade": ["volume fading", "momentum weakening"],
        "dilution": ["dilution", "offering", "warrant", "shelf", "atm"],
    }

    for item in items or []:
        if not item:
            continue
        text = str(item).strip()
        if not text or text.lower() in ["none", "n/a", "null"]:
            continue
        lower = text.lower()
        key = lower
        for bucket, words in buckets.items():
            if any(w in lower for w in words):
                key = bucket
                break
        if key in seen_keys:
            continue
        seen_keys.add(key)
        cleaned.append(text)
    return cleaned


def compact_reasons(result):
    result["reasons"] = dedupe_phrases(result.get("reasons", []))[:7]
    result["risks"] = dedupe_phrases(result.get("risks", []))[:5]
    return result


def compute_momentum_flags(result):
    price = float(result.get("price_float", result.get("price", 0)) or 0)
    vwap = float(result.get("vwap_float", result.get("vwap", 0)) or 0)
    candles = result.get("candles", []) or []
    recent_vol = int(result.get("recent_volume", 0) or 0)
    prev_vol = int(result.get("prev_volume", 0) or 0)
    gain = float(result.get("gain", 0) or 0)

    has_vwap = vwap > 0
    above_vwap = (price >= vwap) if has_vwap else True
    result["has_vwap"] = has_vwap
    result["above_vwap"] = above_vwap
    result["vwap_distance"] = round(((price - vwap) / vwap) * 100, 2) if has_vwap and price else 0

    recent_high = price
    if candles:
        recent_high = max(float(c.get("high", 0) or 0) for c in candles[-10:]) or price
    result["recent_high"] = recent_high
    result["near_high"] = price >= recent_high * 0.97 if recent_high else False

    volume_expanding = recent_vol >= max(75_000, prev_vol)
    volume_fading = prev_vol > 0 and recent_vol < prev_vol * 0.60
    lost_vwap = has_vwap and price < vwap

    text = " ".join(result.get("reasons", []) + result.get("risks", [])).lower()
    bad_structure = detect_bad_structure(text)
    has_higher_lows = bool(result.get("has_higher_lows") or result.get("higher_lows") or "higher lows" in text)
    breakout = bool(result.get("breakout") or result.get("breakout_confirmed"))

    result["volume_expanding"] = volume_expanding
    result["volume_fading"] = volume_fading
    result["lost_vwap"] = lost_vwap
    result["bad_structure"] = bad_structure
    result["has_higher_lows"] = has_higher_lows
    result["breakout_confirmed"] = breakout

    result["momentum_decay"] = bool((volume_fading or lost_vwap or bad_structure) and not (above_vwap and has_higher_lows and result.get("near_high")))
    result["clean_trend_runner"] = bool(gain >= 20 and above_vwap and not bad_structure and recent_vol >= 100_000 and (has_higher_lows or breakout or result.get("near_high")))
    result["true_second_leg"] = bool(gain >= 25 and above_vwap and not bad_structure and recent_vol >= 150_000 and has_higher_lows and (breakout or result.get("near_high")))
    result["fresh_high_after_vwap_hold"] = bool(gain >= 20 and above_vwap and has_higher_lows and result.get("near_high") and recent_vol >= 100_000)
    result["massive_no_news_runner"] = bool(result.get("news_quality") in ["NONE", "UNKNOWN", "JUNK"] and gain >= 35 and result.get("volume", 0) >= 2_000_000 and recent_vol >= 150_000 and above_vwap and (has_higher_lows or breakout or result.get("near_high")))
    return result


def apply_clean_scoring(result):
    """Final score tune. Dilution/SEC risks are never score penalties — awareness only."""
    score = int(result.get("score", 0) or 0)

    if result.get("clean_trend_runner"):
        score += 2
        result.setdefault("reasons", []).append("📈 Clean trend runner")
    if result.get("true_second_leg"):
        score += 2
        result.setdefault("reasons", []).append("Second leg continuation")
    if result.get("fresh_high_after_vwap_hold"):
        score += 1
        result.setdefault("reasons", []).append("Fresh high after VWAP hold")
    if result.get("massive_no_news_runner"):
        score += 1
        result.setdefault("reasons", []).append("No-news volume runner")

    # Structure can reduce confidence; dilution never does.
    if result.get("momentum_decay"):
        score -= 1
        result.setdefault("risks", []).append("⚠️ Momentum decay / wait for reclaim")
    if result.get("above_vwap") is False:
        score -= 1
        result.setdefault("risks", []).append("Below VWAP")

    result["score"] = max(0, min(score, 10))
    return compact_reasons(result)



def classify_alert_tier(result, rank):
    """Elite alerts for trade-quality setups; watch tier for important top gainers."""
    gain = float(result.get("gain", 0) or 0)
    score = int(result.get("score", 0) or 0)
    recent_vol = int(result.get("recent_volume", 0) or 0)
    day_vol = int(result.get("volume", 0) or 0)
    no_news = result.get("news_quality") in ["NONE", "UNKNOWN", "JUNK"]

    if result.get("bad_structure") and not result.get("true_second_leg"):
        return "BLOCK"
    if result.get("momentum_decay") and not result.get("fresh_high_after_vwap_hold") and not result.get("true_second_leg"):
        return "BLOCK"

    if result.get("true_second_leg") or result.get("clean_trend_runner"):
        return "ELITE"
    if result.get("massive_no_news_runner"):
        return "ELITE"
    if score >= 8 and gain >= 20 and result.get("above_vwap") and recent_vol >= 100_000:
        return "ELITE"

    # Awareness tier prevents major fresh gainers from silently disappearing.
    if rank <= 12 and gain >= 25 and day_vol >= 1_000_000 and result.get("above_vwap") and recent_vol >= 60_000:
        return "WATCH"
    if score >= 7 and gain >= 20 and result.get("above_vwap") and not no_news:
        return "WATCH"

    return "BLOCK"


    if result.get("bad_structure") and not result.get("true_second_leg"):
        return "BLOCK"
    if result.get("momentum_decay") and not result.get("fresh_high_after_vwap_hold") and not result.get("true_second_leg"):
        return "BLOCK"
    if result.get("true_second_leg"):
        return "ELITE"
    if result.get("clean_trend_runner") and score >= 7:
        return "ELITE"
    if result.get("massive_no_news_runner"):
        return "ELITE"  # WOK-style: structure + volume can alert without news
    if score >= 8 and gain >= 20 and result.get("above_vwap") and recent_vol >= 100_000:
        return "ELITE"
    if rank <= 10 and gain >= 25 and result.get("volume", 0) >= 1_000_000 and result.get("above_vwap") and recent_vol >= 75_000:
        return "WATCH"  # awareness tier so top gainers don't vanish
    if no_news:
        return "BLOCK"
    if score >= 7 and gain >= 20 and result.get("above_vwap"):
        return "WATCH"
    return "BLOCK"


def title_for_tier(result, tier):
    if result.get("momentum_decay"):
        return "⚠️ MOMENTUM FADED"
    if result.get("true_second_leg"):
        return "🚨 SECOND LEG RUNNER"
    if result.get("fresh_high_after_vwap_hold"):
        return "🚀 FRESH HIGH VWAP HOLD"
    if result.get("massive_no_news_runner"):
        return "🔥 NO-NEWS VOLUME RUNNER"
    if result.get("clean_trend_runner"):
        return "📈 CLEAN TREND RUNNER"
    if tier == "WATCH":
        return "👁️ TOP GAINER WATCH"
    if result.get("score", 0) >= 9:
        return "🔥 MOMENTUM RUNNER"
    return "🚨 BUILDING MOMENTUM"


def setup_bias_and_entry(result):
    if result.get("momentum_decay"):
        result["trap_runner"] = "⚠️ Momentum faded — wait for reclaim"
        result["entry_hint"] = "Wait for VWAP reclaim + volume to return"
    elif result.get("bad_structure"):
        result["trap_runner"] = "⚠️ Trap risk"
        result["entry_hint"] = "Avoid chase — needs clean reclaim"
    elif result.get("true_second_leg"):
        result["trap_runner"] = "🟢 Runner watch"
        result["entry_hint"] = "Second leg — watch hold over VWAP/recent high"
    elif result.get("clean_trend_runner") or result.get("fresh_high_after_vwap_hold"):
        result["trap_runner"] = "🚀 Runner lean"
        result["entry_hint"] = "Clean trend — watch breakout/hold or VWAP dip"
    elif result.get("above_vwap"):
        result["trap_runner"] = "👁️ Watchlist only"
        result["entry_hint"] = "Needs stronger confirmation before entry"
    else:
        result["trap_runner"] = "🤔 Unclear"
        result["entry_hint"] = "Wait for setup confirmation"
    return result


def meaningful_realert(result, alert_history, runner_prices, alert_scores, alert_setups, now):
    ticker = result["ticker"]
    current_price = float(result.get("price", 0) or 0)
    current_score = int(result.get("score", 0) or 0)
    setup = result.get("title") or result.get("setup_tag") or ""

    last_time = alert_history.get(ticker, 0)
    cooldown_done = now - last_time >= ALERT_COOLDOWN_SECONDS
    last_price = runner_prices.get(ticker, 0)
    last_score = alert_scores.get(ticker, 0)
    last_setup = alert_setups.get(ticker, "")

    if last_time == 0:
        return True, "first alert"
    if not cooldown_done and not result.get("true_second_leg"):
        return False, "cooldown active"
    if last_price and current_price >= last_price * 1.03:
        return True, "new high +3%"
    if current_score > last_score:
        return True, "score improved"
    if setup and setup != last_setup and current_price >= last_price * 1.01:
        return True, "setup upgraded"
    if result.get("true_second_leg") and last_price and current_price >= last_price * 1.02:
        return True, "second leg new high"
    return False, "no meaningful change"


def build_compact_alert(result):
    result = compact_reasons(result)
    float_shares = float(result.get("float", 0) or 0)
    float_text = f"{float_shares/1_000_000:.1f}M" if float_shares else "Unknown"
    news_quality = result.get("news_quality", "UNKNOWN")
    catalyst_line = result.get("catalyst_text") or "No fresh catalyst found"

    reasons = "\n".join(f"• {x}" for x in result.get("reasons", [])[:5]) or "• Momentum watch"
    risks = "\n".join(f"• {x}" for x in result.get("risks", [])[:4]) or "• None obvious"

    if news_quality in ["NONE", "UNKNOWN", "JUNK"]:
        news_header = "❌ NO CONFIRMED NEWS"
    elif news_quality == "STRONG":
        news_header = "⚡ STRONG NEWS"
    else:
        news_header = "⚠️ WEAK/UNCLEAR NEWS"

    tier = result.get("alert_tier", "WATCH")
    return (
        f"{result.get('title', get_alert_title(result))}\n\n"
        f"{result['ticker']} | Score: {result['score']}/10 | {tier}\n"
        f"Price: ${float(result.get('price', 0) or 0):.4f}\n"
        f"Gain: {float(result.get('gain', 0) or 0):.1f}%\n"
        f"Float: {float_text}\n\n"
        f"Catalyst: {news_header}\n"
        f"{catalyst_line}\n\n"
        f"Bias: {result.get('trap_runner', 'UNKNOWN')}\n"
        f"Entry: {result.get('entry_hint', 'Wait for confirmation')}\n\n"
        f"Why:\n{reasons}\n\n"
        f"Risk / Awareness:\n{risks}"
    )

def run_scanner():
    print(f"[BOOT] Scanner started | {BOOT_MARKER}", flush=True)
    print(f"[BOOT] Scanning fresh {SCAN_MIN_GAIN}%+ gainers | elite + watch tiers", flush=True)

    alert_history = {}
    runner_prices = {}
    alert_scores = {}
    alert_setups = {}

    while True:
        sent_this_cycle = set()

        if not should_scan_now():
            print("[SLEEP] Market inactive — skipping scan", flush=True)
            time.sleep(60)
            continue

        print("[SCAN] Market active — refreshing top gainers", flush=True)
        session, session_notes = get_market_session()
        movers = get_percent_gainers()

        # Fresh top-gainer protection: always process the biggest movers first,
        # then rank by final score after news/candles/structure.
        movers = sorted(movers, key=lambda x: float(x.get("gain", 0) or 0), reverse=True)[:MAX_GAINERS]
        print("[FRESH GAINERS] " + ", ".join([f"{m['ticker']} {m['gain']:.1f}%" for m in movers[:15]]), flush=True)

        results = []
        seen_tickers = set()

        for raw_rank, mover in enumerate(movers, start=1):
            ticker = str(mover.get("ticker", "")).upper().strip()
            if ticker in seen_tickers or is_bad_ticker(ticker):
                continue
            seen_tickers.add(ticker)
            mover["ticker"] = ticker

            try:
                # Confirm quote, but don't let a temporary quote miss kill the candidate.
                quote = get_finnhub_quote(ticker)
                if quote:
                    mover["price"] = float(quote.get("price", mover.get("price", 0)) or 0)
                    mover["gain"] = float(quote.get("gain", mover.get("gain", 0)) or 0)
                    mover["gain_percent"] = mover["gain"]
                    print(f"[LIVE] {ticker} ${mover['price']:.4f} {mover['gain']:.1f}%", flush=True)
                else:
                    print(f"[LIVE] {ticker} quote unavailable — using screener values", flush=True)

                price = float(mover.get("price", 0) or 0)
                gain = float(mover.get("gain", 0) or 0)
                volume = int(float(mover.get("volume", 0) or 0))

                if price < 0.50 or price > MAX_PRICE:
                    print(f"[FILTER] {ticker} price ${price:.2f} outside range", flush=True)
                    continue
                if gain < 5:
                    print(f"[FILTER] {ticker} live gain too weak {gain:.1f}%", flush=True)
                    continue
                if volume <= 0:
                    mover["volume"] = 500_000

                catalyst_type, catalyst_text = get_news_catalyst(ticker)
                headline, news_quality = find_real_news_headline(ticker, catalyst_text)

                result = score_mover(mover, catalyst_type, headline)
                result["rank"] = raw_rank
                result["headline"] = headline
                result["catalyst_text"] = headline
                result["news_quality"] = news_quality
                result["strong_news"] = news_quality == "STRONG"

                if news_quality == "STRONG":
                    result["catalyst_type"] = "⚡ STRONG NEWS"
                elif news_quality == "WEAK":
                    result["catalyst_type"] = "⚠️ WEAK NEWS"
                elif news_quality == "JUNK":
                    result["catalyst_type"] = "🚫 JUNK NEWS"
                else:
                    result["catalyst_type"] = "❌ NO NEWS"

                market_cap, float_shares = get_finnhub_profile(ticker)
                result["market_cap"] = market_cap
                result["float"] = float_shares

                if market_cap and market_cap > MAX_MARKET_CAP:
                    print(f"[FILTER] {ticker} market cap over 1B", flush=True)
                    continue
                if float_shares and float_shares > 50_000_000:
                    print(f"[FILTER] {ticker} float too high {float_shares:,.0f}", flush=True)
                    continue
                if not float_shares:
                    result.setdefault("risks", []).append("⚠️ Float unknown")
                elif float_shares <= 10_000_000:
                    result["score"] = min(10, result["score"] + 1)
                    result.setdefault("reasons", []).append("Low float momentum potential")

                candles = get_alpaca_candles(ticker)
                if not candles:
                    print(f"[DATA] {ticker} Alpaca failed — using Yahoo candles", flush=True)
                    candles = get_yahoo_candles(ticker)
                result["candles"] = candles or []

                recent_volume = sum(int(c.get("volume", 0) or 0) for c in result["candles"][-5:]) if candles else 0
                prev_volume = sum(int(c.get("volume", 0) or 0) for c in result["candles"][-10:-5]) if candles and len(candles) >= 10 else 0
                total_candle_volume = sum(int(c.get("volume", 0) or 0) for c in result["candles"]) if candles else 0
                result["recent_volume"] = recent_volume
                result["prev_volume"] = prev_volume
                result["total_candle_volume"] = total_candle_volume

                if candles:
                    result["high"] = max(float(c.get("high", 0) or 0) for c in candles[-10:])
                    result["volume_status"] = describe_volume_quality(candles)
                    result.setdefault("reasons", []).append(result["volume_status"])

                structure = analyze_structure(ticker, candles or [])
                result["structure_score"] = int(structure.get("structure_score", 0) or 0)
                result["vwap"] = structure.get("vwap")
                result["above_vwap"] = structure.get("above_vwap", True)
                result["breakout"] = structure.get("breakout", False)
                result["breakout_confirmed"] = structure.get("breakout", False)
                result["breakout_level"] = structure.get("breakout_level")
                result["higher_lows"] = structure.get("higher_lows", False)
                result["has_higher_lows"] = structure.get("higher_lows", False)
                result["trend_builder"] = structure.get("trend_builder", False)

                result.setdefault("reasons", []).extend(structure.get("reasons", []) or [])
                result.setdefault("risks", []).extend(structure.get("risk_flags", []) or [])
                result = update_vwap_state(result)
                result = compute_momentum_flags(result)
                result = apply_clean_scoring(result)

                # SEC / dilution check only for real candidates so API is not hammered.
                # IMPORTANT: dilution is awareness-only. It never subtracts score or blocks alerts.
                sec_risk, sec_note = False, ""
                if result.get("score", 0) >= 6 or result.get("rank", 99) <= 12:
                    sec_risk, sec_note = check_sec_offering_risk(ticker)
                    result["sec_note"] = sec_note
                if sec_risk:
                    if any(form in sec_note for form in ["S-1", "S-3", "F-1", "F-3", "424B"]):
                        result.setdefault("risks", []).append(f"🚨 Active dilution filing: {sec_note}")
                    else:
                        result.setdefault("risks", []).append(f"⚠️ Filing detected: {sec_note}")

                filing_text = result.get("sec_note", "") + " " + result.get("catalyst_text", "")
                extra_risks = detect_offering_risk(filing_text, price=float(result.get("price", 0) or 0)) or []
                if isinstance(extra_risks, tuple):
                    found, msg = extra_risks
                    extra_risks = [f"🚨 DILUTION RISK: {msg}"] if found else []
                elif not isinstance(extra_risks, list):
                    extra_risks = [str(extra_risks)] if extra_risks else []
                result.setdefault("risks", []).extend(extra_risks)

                dilution_label = describe_dilution_risk(" ".join(result.get("risks", []) + [filing_text]))
                if dilution_label:
                    result.setdefault("risks", []).insert(0, dilution_label)

                result = compact_reasons(result)
                results.append(result)
                time.sleep(0.01)

            except Exception as e:
                print(f"[CANDIDATE ERROR] {ticker}: {e}", flush=True)
                continue

        if not results:
            print("[SCAN] No qualified gainers found", flush=True)
            time.sleep(SCAN_SLEEP)
            continue

        # Rank by score first, but keep raw top-gainer rank in the alert.
        results.sort(key=lambda r: (int(r.get("score", 0)), float(r.get("gain", 0)), int(r.get("recent_volume", 0))), reverse=True)

        regime, regime_notes = detect_market_regime(results)
        for r in results:
            r["market_regime"] = regime
            r["regime_notes"] = regime_notes

        top_line = " | ".join(f"#{r.get('rank')} {r['ticker']} {r['score']}/10 {r['gain']:.1f}%" for r in results[:10])
        print(f"[SCAN] Top ranked: {top_line}", flush=True)
        print(f"[REGIME] {regime} — {regime_notes}", flush=True)

        now = time.time()
        sent_count = 0

        for result in results[:MAX_GAINERS]:
            ticker = result["ticker"]
            if ticker in sent_this_cycle:
                continue
            if sent_count >= MAX_ALERTS_PER_CYCLE:
                break

            tier = classify_alert_tier(result, int(result.get("rank", 99)))
            if tier == "BLOCK":
                print(f"[NO ALERT] {ticker} blocked — tier filter", flush=True)
                continue

            result = setup_bias_and_entry(result)
            result["alert_tier"] = tier
            result["title"] = title_for_tier(result, tier)
            result["setup_tag"] = result["title"]

            ok, reason = meaningful_realert(result, alert_history, runner_prices, alert_scores, alert_setups, now)
            if not ok:
                print(f"[SKIP] {ticker} {reason}", flush=True)
                continue

            # Final no-news safety: allow ELITE technical runners and top-gainer WATCH awareness.
            if result.get("news_quality") in ["NONE", "UNKNOWN", "JUNK"] and tier not in ["ELITE", "WATCH"]:
                print(f"[NO ALERT] {ticker} no-news blocked — weak setup", flush=True)
                continue

            msg = build_compact_alert(result)
            print(f"[SEND] {ticker} tier={tier} score={result['score']} reason={reason}", flush=True)
            sent = send_alert(msg)
            print(f"[SEND RESULT] {ticker} sent={sent}", flush=True)

            if sent:
                sent_this_cycle.add(ticker)
                sent_count += 1
                alert_history[ticker] = now
                runner_prices[ticker] = float(result.get("price", 0) or 0)
                alert_scores[ticker] = int(result.get("score", 0) or 0)
                alert_setups[ticker] = result.get("title", "")
                print(f"[ALERT SENT] {ticker} {result.get('title')}", flush=True)

            time.sleep(0.1)

        print("[SCAN] Cycle complete — sleeping", flush=True)
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


   
