import os
import re
import time
import html
import json
import email.utils
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from threading import Thread
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from flask import Flask
from dotenv import load_dotenv

from structure_engine import analyze_structure
from alerts import send_alert

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

ET = ZoneInfo("America/New_York")

BOOT_MARKER = "elite scanner rebuild v23 — early leader + live action engine"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")

# Scanner universe
SCAN_MIN_GAIN = 12
SCAN_SLEEP = 90

# Alert philosophy:
# 1. Premarket = radar only by default.
# 2. Phone alerts are RUNNER / MAIN LEADER / LEADER only.
# 3. No WATCH wording.
# 4. Score 6 and below ignored.
# 5. Junk/news spam gate must pass before phone alert.
ALERT_MIN_GAIN = 25
EARLY_LEADER_MIN_GAIN = 18
MIN_ALERT_SCORE = 7
MIN_LIVE_ACTION_SCORE = 8
MIN_ALERT_RECENT_VOLUME = 75_000

PREMARKET_ALERTS_ENABLED = False
OPENING_5_MIN_PROTECTION = True

LEADER_MIN_GAIN = 40
LEADER_MIN_DAY_VOLUME = 2_000_000
LEADER_MIN_RECENT_VOLUME = 150_000

MAIN_LEADER_MIN_RECENT_VOLUME = 250_000

FRESH_IGNITION_MIN_GAIN = 25
RECLAIM_MAX_DISTANCE_PCT = 2.0
MIDDAY_CHOP_START = dtime(11, 0)
MIDDAY_CHOP_END = dtime(14, 0)

BLOCK_FADING_LEADER_ALERTS = False

ALERT_COOLDOWN_SECONDS = 900
MIN_RE_ALERT_SECONDS = 300
MAX_ALERTS_PER_CYCLE = 3

MAX_GAINERS = 70
MIN_VOLUME = 50_000
MAX_PRICE = 100
MIN_PRICE = 0.50
MAX_MARKET_CAP = 1_000_000_000
MAX_FLOAT_SHARES = 50_000_000

MAX_ALERT_REASONS = 4
MAX_ALERT_RISKS = 4

TREND_BUILDER_MIN_GAIN = 12

CACHE_TTL_SECONDS = 60 * 30
NEWS_CACHE_TTL_SECONDS = 60 * 20
PR_CACHE_TTL_SECONDS = 60 * 30
SEC_CACHE_TTL_SECONDS = 60 * 30
SEC_BODY_CACHE_TTL_SECONDS = 60 * 60

PROFILE_CACHE = {}
NEWS_CACHE = {}
SEC_CACHE = {}
SEC_BODY_CACHE = {}
PR_CACHE = {}
COMPANY_CACHE = {}

MARKET_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-19", "2026-02-16",
    "2026-04-03", "2026-05-25", "2026-06-19",
    "2026-07-03", "2026-09-07", "2026-11-26",
    "2026-12-25",
}

BAD_TICKER_SUFFIXES = ("WS", "WT", "WQ", "WSA", "WSC", "IW", "WARRANT", "U", "R")

BAD_NEWS_KEYWORDS = [
    "top gainers",
    "stocks moving",
    "stocks are moving",
    "these stocks are moving",
    "moving in today's session",
    "today's session",
    "what's going on",
    "shares are moving",
    "session movers",
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
    "gap-ups and gap-downs",
    "top gainers and losers",
    "pre-market session",
    "market session",
    "gainers and losers",
    "stocks to watch today",
    "insights into",
    "get insights into",
    "stocks moving premarket",
    "here are 20 stocks moving",
    "most active stocks",
]

BAD_PR_MATCH_PHRASES = [
    "market global forecast",
    "global forecast",
    "industry report",
    "research report",
    "market report",
    "market size",
    "featuring",
    "replacement vacuum",
    "introduces rvi",
    "clinical study on",
    "non- stim",
    "non-stim",
    "pr newswire",
    "globenewswire",
    "accesswire",
    "benzinga.com",
    "newsfile corp",
    "privacy policy",
    "terms of use",
    "cookie",
    "subscribe",
    "sign in",
]

AMBIGUOUS_TICKERS_REQUIRE_COMPANY = {
    "BESS": ["bess", "battery", "storage"],
    "GUTS": ["guts"],
    "STIM": ["stim", "non-stim", "stimulation"],
    "RVI": ["rvi", "vacuum interrupter"],
    "AI": ["artificial intelligence"],
    "CAN": ["can"],
    "ON": ["on"],
    "FOR": ["for"],
}

WEAK_NEWS_OVERRIDES = [
    "investor alert",
    "class action",
    "law firm",
    "rosen law",
    "pomerantz",
    "hagens berman",
    "deadline",
    "sued for securities",
    "investigates claims",
    "encourages investors",
    "secure counsel",
    "losses on their investment",
    "shareholder alert",
    "lead plaintiff",
]

SOFT_NEWS_PHRASES = [
    "begins trading",
    "ticker symbol change",
    "regains compliance",
    "announces stock ticker",
    "reports first quarter",
    "quarterly results",
    "annual meeting",
    "conference participation",
    "to present at",
]

STRONG_KEYWORDS = [
    "fda", "approval", "approved", "clearance", "cleared", "510(k)", "510k",
    "clinical trial", "phase 1", "phase 2", "phase 3",
    "positive data", "topline", "endpoint", "orphan drug",
    "fast track", "breakthrough therapy", "primary endpoint",
    "met primary endpoint", "statistically significant", "pivotal trial",
    "new drug application", "nda", "bla", "de novo", "commercial launch",
    "contract", "agreement", "partnership", "collaboration",
    "deal", "order", "purchase order", "supply agreement",
    "distribution agreement", "license agreement", "strategic alliance",
    "definitive agreement", "letter of intent", "mou", "memorandum of understanding",
    "financing", "advance financing", "facility", "battery",
    "solid-state battery", "infrastructure", "validation initiative",
    "acquisition", "merger", "buyout", "takeover",
    "earnings", "revenue", "guidance", "raises guidance",
    "profitability", "record revenue",
    "bitcoin", "ethereum", "crypto", "blockchain",
    "artificial intelligence", "ai-powered", "nvidia",
    "patent", "granted patent", "exclusive license", "licensing",
    "department of defense", "dod", "army", "navy", "air force",
    "launches", "commercialization", "strategic investment",
]

CATALYST_BUCKETS = {
    "FDA / biotech": [
        "fda", "approval", "approved", "clearance", "cleared", "510(k)", "510k",
        "clinical", "phase 1", "phase 2", "phase 3", "trial", "topline",
        "primary endpoint", "statistically significant", "orphan drug",
        "fast track", "breakthrough therapy", "nda", "bla", "de novo",
    ],
    "Contract / order": [
        "contract", "purchase order", "order", "supply agreement",
        "customer agreement", "award", "awarded", "procurement",
    ],
    "Partnership / MOU": [
        "partnership", "collaboration", "strategic alliance", "mou",
        "memorandum of understanding", "joint venture", "letter of intent",
    ],
    "AI / Nvidia": [
        "artificial intelligence", " ai ", "ai-powered", "machine learning",
        "nvidia", "gpu", "data center", "datacenter",
    ],
    "Battery / EV / energy": [
        "battery", "solid-state", "energy storage", "lithium", "ev",
        "charging", "solar", "grid", "bess",
    ],
    "Crypto / blockchain": [
        "bitcoin", "ethereum", "crypto", "blockchain", "mining", "digital asset",
    ],
    "Merger / acquisition": [
        "acquisition", "merger", "buyout", "takeover", "definitive agreement",
        "to acquire", "combination",
    ],
    "Earnings / guidance": [
        "earnings", "revenue", "guidance", "raises guidance", "record revenue",
        "profitability", "quarterly results",
    ],
    "Patent / IP": [
        "patent", "intellectual property", "exclusive license", "license agreement",
        "licensing agreement",
    ],
    "Financing / offering": [
        "offering", "registered direct", "private placement", "atm",
        "at-the-market", "warrant", "convertible", "securities purchase agreement",
    ],
    "Defense / government": [
        "department of defense", "dod", "army", "navy", "air force", "government",
        "federal", "nasa", "homeland security",
    ],
}

app = Flask(__name__)


# ============================================================
# WEB HEALTH
# ============================================================

@app.route("/")
def home():
    return "Bot alive"


@app.route("/health")
def health():
    return "OK"


# ============================================================
# MARKET CLOCK
# ============================================================

def should_scan_now():
    now = datetime.now(ET)
    print(f"[TIME] Market clock ET: {now.strftime('%Y-%m-%d %I:%M:%S %p %Z')}", flush=True)

    if now.weekday() >= 5:
        return False

    if now.date().isoformat() in MARKET_HOLIDAYS_2026:
        return False

    return dtime(7, 30) <= now.time() < dtime(16, 10)


def get_market_session():
    now = datetime.now(ET).time()

    if dtime(4, 0) <= now < dtime(9, 30):
        return "PREMARKET"
    if dtime(9, 30) <= now < dtime(11, 0):
        return "OPEN"
    if dtime(11, 0) <= now < dtime(14, 0):
        return "MIDDAY"
    if dtime(14, 0) <= now < dtime(16, 0):
        return "POWER HOUR"
    if dtime(16, 0) <= now <= dtime(20, 0):
        return "AFTERHOURS"

    return "CLOSED"


def get_bot_mode(regime, session):
    if session == "PREMARKET":
        return "PREMARKET"
    if regime == "CHOP":
        return "STRICT"
    if regime == "HOT":
        return "HOT"
    return "REGULAR"


# ============================================================
# BASIC HELPERS
# ============================================================

def is_bad_ticker(ticker):
    ticker = str(ticker or "").upper().strip()
    return (
        not ticker
        or "." in ticker
        or "-" in ticker
        or ticker.endswith(BAD_TICKER_SUFFIXES)
        or (ticker.endswith("W") and len(ticker) > 4)
    )


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def add_unique(items, text):
    if text and text not in items:
        items.append(text)


def is_above_vwap(price, vwap):
    if not vwap or not price:
        return True
    return price > (vwap * 0.995)


def request_get(url, headers=None, params=None, timeout=8):
    base_headers = {
        "User-Agent": "Mozilla/5.0 scanner-bot/1.0 contact@example.com",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    if headers:
        base_headers.update(headers)
    return requests.get(url, headers=base_headers, params=params, timeout=timeout)


def normalize_text(text):
    text = html.unescape(str(text or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def ticker_token_present(ticker, text):
    ticker = str(ticker or "").upper().strip()
    text = str(text or "")
    if not ticker or not text:
        return False
    return bool(re.search(rf"(?<![A-Z0-9]){re.escape(ticker)}(?![A-Z0-9])", text, re.IGNORECASE))


def detect_bad_structure(structure_text):
    hard_bad_keywords = [
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
        "clear below vwap",
    ]
    text = str(structure_text or "").lower()
    return any(k in text for k in hard_bad_keywords)


def detect_soft_structure_warning(structure_text):
    soft_keywords = ["below vwap", "reclaim watch", "slightly below vwap"]
    text = str(structure_text or "").lower()
    return any(k in text for k in soft_keywords)


def dedupe_phrases(items):
    cleaned = []
    seen_keys = set()

    buckets = {
        "vwap": ["vwap", "above vwap", "price above vwap", "vwap hold"],
        "higher_lows": ["higher lows"],
        "clean_structure": ["clean structure", "clean trend runner structure", "structure confirmation"],
        "no_news": ["no confirmed catalyst", "technical momentum only"],
        "volume_fade": ["volume fading", "momentum weakening"],
        "volume_expand": ["volume expanding", "momentum increasing"],
        "dilution": ["dilution", "offering", "warrant", "shelf", "atm"],
        "leader": ["market leader", "leaderboard"],
        "main_leader": ["main leader"],
        "momentum_moment": ["momentum moment", "ignition moment", "second leg moment"],
        "rvol": ["rvol"],
    }

    for item in items or []:
        if not item:
            continue

        text = str(item).strip()
        lower = text.lower()

        if not text or lower in ["none", "n/a", "null"]:
            continue

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
    reasons = []
    risks = []

    news_quality = result.get("news_quality", "UNKNOWN")

    banned_reason_bits = [
        "market cap",
        "fresh daily",
        "daily breakout",
        "session",
        "15%+ early mover",
        "watchlist",
    ]

    for r in result.get("reasons", []):
        low = str(r).lower().strip()
        if not low:
            continue
        if any(bit in low for bit in banned_reason_bits):
            continue
        if "fresh news" in low and news_quality in ["NONE", "UNKNOWN", "JUNK"]:
            continue
        reasons.append(str(r).strip())

    for r in result.get("risks", []):
        text = str(r).strip()
        low = text.lower()
        if low in ["none", "n/a", "", "null"]:
            continue
        if "sec filings present" in low and any("dilution" in str(x).lower() or "offering" in str(x).lower() for x in risks):
            continue
        risks.append(text)

    result["reasons"] = dedupe_phrases(reasons)[:MAX_ALERT_REASONS]
    result["risks"] = dedupe_phrases(risks)[:MAX_ALERT_RISKS]
    return result


def clean_alert_consistency(result):
    tier = result.get("alert_tier")
    title = str(result.get("title", ""))

    title = title.replace(" — PULLBACK WATCH", " — PULLBACK")
    title = title.replace(" — RECLAIM WATCH", " — RECLAIM NEEDED")
    title = title.replace("WATCH", "")
    title = " ".join(title.split()).strip()

    if tier == "RUNNER":
        if result.get("momentum_decay") or result.get("bad_structure") or result.get("deep_vwap_loss"):
            result["alert_tier"] = "LEADER" if result.get("market_leader") else "AVOID"
            tier = result["alert_tier"]

    if tier == "LEADER" and title.startswith("🟢 RUNNER"):
        title = "🔥 MARKET LEADER"
    elif tier == "MAIN_LEADER" and not title.startswith("👑"):
        title = "👑 MAIN LEADER"
    elif tier == "AVOID" and not title.startswith("🔴 AVOID"):
        title = "🔴 AVOID — STRUCTURE FAILED"

    result["title"] = title
    result = compact_reasons(result)
    return result


# ============================================================
# SCREENER SOURCES
# ============================================================

def get_nasdaq_gainers():
    url = "https://api.nasdaq.com/api/screener/stocks"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    }
    params = {
        "tableonly": "true",
        "limit": 200,
        "offset": 0,
        "download": "true",
    }

    movers = []

    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        data = r.json()
        rows = data.get("data", {}).get("rows", [])

        for row in rows:
            ticker = str(row.get("symbol", "")).upper().strip()

            if is_bad_ticker(ticker):
                continue

            price = safe_float(str(row.get("lastsale", "0")).replace("$", "").replace(",", ""))
            gain = safe_float(str(row.get("pctchange", "0")).replace("%", "").replace("+", ""))
            volume = safe_int(str(row.get("volume", "0")).replace(",", ""))

            if price <= 0 or gain < SCAN_MIN_GAIN or volume < MIN_VOLUME or price > MAX_PRICE:
                continue

            movers.append({
                "ticker": ticker,
                "price": price,
                "gain": gain,
                "gain_percent": gain,
                "volume": volume,
                "source": "NASDAQ",
            })

        print(f"[NASDAQ] Found {len(movers)} candidates over {SCAN_MIN_GAIN}%", flush=True)
        return movers

    except Exception as e:
        print(f"[NASDAQ ERROR] {e}", flush=True)
        return []


def dedupe_movers(movers):
    deduped = {}

    for mover in movers or []:
        ticker = str(mover.get("ticker", "")).upper().strip()

        if not ticker or is_bad_ticker(ticker):
            continue

        mover["ticker"] = ticker
        gain = safe_float(mover.get("gain", mover.get("gain_percent", 0)))
        old = deduped.get(ticker)

        if not old:
            deduped[ticker] = mover
            continue

        old_gain = safe_float(old.get("gain", old.get("gain_percent", 0)))
        old_volume = safe_int(old.get("volume"))
        volume = safe_int(mover.get("volume"))

        if gain > old_gain or (gain == old_gain and volume > old_volume):
            deduped[ticker] = mover

    cleaned = list(deduped.values())
    cleaned.sort(key=lambda x: safe_float(x.get("gain", x.get("gain_percent", 0))), reverse=True)
    return cleaned


def get_percent_gainers():
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    headers = {"User-Agent": "Mozilla/5.0"}

    screeners = [
        "day_gainers",
        "most_actives",
        "small_cap_gainers",
        "aggressive_small_caps",
        "undervalued_growth_stocks",
    ]

    all_movers = {}

    for screener in screeners:
        params = {"scrIds": screener, "count": 200}

        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            data = r.json()
            quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])

            for q in quotes:
                ticker = str(q.get("symbol", "")).upper().strip()

                if is_bad_ticker(ticker):
                    continue

                price = safe_float(q.get("regularMarketPrice"))
                gain = safe_float(q.get("regularMarketChangePercent"))
                volume = safe_int(q.get("regularMarketVolume"))
                company_name = normalize_text(q.get("shortName") or q.get("longName") or "")

                if price <= 0 or gain < SCAN_MIN_GAIN or volume < MIN_VOLUME or price > MAX_PRICE:
                    continue

                old = all_movers.get(ticker)
                if not old or gain > old.get("gain", 0):
                    all_movers[ticker] = {
                        "ticker": ticker,
                        "price": price,
                        "gain": gain,
                        "gain_percent": gain,
                        "volume": volume,
                        "source": f"YAHOO:{screener}",
                        "company_name": company_name,
                    }

        except Exception as e:
            print(f"[YAHOO {screener.upper()} ERROR] {e}", flush=True)

    for m in get_nasdaq_gainers():
        ticker = m["ticker"].upper()
        if is_bad_ticker(ticker):
            continue
        old = all_movers.get(ticker)
        if not old or m.get("gain", 0) > old.get("gain", 0):
            all_movers[ticker] = m

    movers = dedupe_movers(list(all_movers.values()))

    print(f"[GAINERS] Found {len(movers)} deduped scan candidates over {SCAN_MIN_GAIN}%", flush=True)
    print("[GAINERS] " + ", ".join([f"{m['ticker']} {m['gain']:.1f}%" for m in movers[:20]]), flush=True)

    return movers[:max(MAX_GAINERS, 100)]


# ============================================================
# MARKET DATA
# ============================================================

def get_finnhub_quote(ticker):
    if not FINNHUB_API_KEY:
        return None

    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": ticker, "token": FINNHUB_API_KEY}

    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        current = safe_float(data.get("c"))
        previous_close = safe_float(data.get("pc"))

        if current <= 0 or previous_close <= 0:
            return None

        gain = ((current - previous_close) / previous_close) * 100
        return {"price": current, "gain": gain}

    except Exception as e:
        print(f"[FINNHUB QUOTE ERROR] {ticker}: {e}", flush=True)
        return None


def get_finnhub_profile(ticker):
    now = time.time()

    if ticker in PROFILE_CACHE:
        cached = PROFILE_CACHE[ticker]
        if now - cached["time"] < CACHE_TTL_SECONDS:
            return cached["data"]

    if not FINNHUB_API_KEY:
        return 0, 0

    url = "https://finnhub.io/api/v1/stock/profile2"
    params = {"symbol": ticker, "token": FINNHUB_API_KEY}

    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        company_name = normalize_text(data.get("name", ""))
        if company_name:
            COMPANY_CACHE[ticker] = {"time": now, "data": company_name}

        market_cap = safe_float(data.get("marketCapitalization")) * 1_000_000
        float_shares = safe_float(data.get("shareOutstanding")) * 1_000_000

        PROFILE_CACHE[ticker] = {"time": now, "data": (market_cap, float_shares)}
        return market_cap, float_shares

    except Exception as e:
        print(f"[FINNHUB PROFILE ERROR] {ticker}: {e}", flush=True)
        return 0, 0


def get_company_name(ticker, fallback=""):
    ticker = str(ticker or "").upper().strip()
    now = time.time()

    if ticker in COMPANY_CACHE:
        cached = COMPANY_CACHE[ticker]
        if now - cached["time"] < CACHE_TTL_SECONDS:
            return cached["data"]

    if fallback:
        COMPANY_CACHE[ticker] = {"time": now, "data": normalize_text(fallback)}
        return normalize_text(fallback)

    if not FINNHUB_API_KEY:
        return ""

    try:
        url = "https://finnhub.io/api/v1/stock/profile2"
        params = {"symbol": ticker, "token": FINNHUB_API_KEY}
        r = requests.get(url, params=params, timeout=8)
        data = r.json()
        name = normalize_text(data.get("name", ""))
        COMPANY_CACHE[ticker] = {"time": now, "data": name}
        return name
    except Exception:
        return ""


def get_alpaca_candles(ticker):
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        return []

    url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }
    params = {"timeframe": "5Min", "limit": 50}

    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        data = r.json()
        bars = data.get("bars", [])

        candles = []
        for b in bars:
            candles.append({
                "open": safe_float(b.get("o")),
                "high": safe_float(b.get("h")),
                "low": safe_float(b.get("l")),
                "close": safe_float(b.get("c")),
                "volume": safe_int(b.get("v")),
            })

        return candles

    except Exception as e:
        print(f"[ALPACA ERROR] {ticker}: {e}", flush=True)
        return []


def get_yahoo_candles(ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "5m", "range": "1d"}
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json() if r.content else {}

        results = data.get("chart", {}).get("result") or []
        if not results:
            return []

        quote_list = results[0].get("indicators", {}).get("quote") or []
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

            candles.append({
                "open": safe_float(o),
                "high": safe_float(h),
                "low": safe_float(l),
                "close": safe_float(c),
                "volume": safe_int(v),
            })

        return candles

    except Exception as e:
        print(f"[CANDLE ERROR] {ticker}: {e}", flush=True)
        return []


# ============================================================
# NEWS / CATALYST ENGINE
# ============================================================

def classify_news_quality(headline):
    if not headline:
        return "NONE"

    text = normalize_text(headline).lower()

    if text in ["none", "no fresh catalyst found", "news check failed", "missing finnhub key", "technical momentum only"]:
        return "NONE"

    if any(word in text for word in WEAK_NEWS_OVERRIDES):
        return "WEAK"

    has_strong_keyword = any(word in text for word in STRONG_KEYWORDS)

    if any(word in text for word in BAD_NEWS_KEYWORDS) and not has_strong_keyword:
        return "JUNK"

    if any(word in text for word in SOFT_NEWS_PHRASES):
        return "WEAK"

    if has_strong_keyword:
        return "STRONG"

    return "WEAK"


def classify_catalyst_bucket(text):
    lower = f" {normalize_text(text).lower()} "
    for bucket, keywords in CATALYST_BUCKETS.items():
        for keyword in keywords:
            if keyword.strip().lower() in lower:
                return bucket
    return "General news"


def catalyst_display_label(bucket, quality):
    if quality == "STRONG":
        return f"⚡ {bucket}"
    if quality == "WEAK":
        return f"⚠️ {bucket}"
    if quality == "JUNK":
        return "🚫 Aggregator headline"
    return "❌ No confirmed news"


def clean_headline(text, allow_aggregator=False):
    text = normalize_text(text)

    if not text:
        return ""

    lower = text.lower()

    if any(x in lower for x in BAD_PR_MATCH_PHRASES):
        return ""

    has_real_keyword = any(k in lower for k in STRONG_KEYWORDS)
    if not allow_aggregator and any(x in lower for x in BAD_NEWS_KEYWORDS) and not has_real_keyword:
        return ""

    text = re.sub(r"^\s*(PR Newswire|GlobeNewswire|Accesswire)\s*[-:]\s*", "", text, flags=re.I)
    text = re.sub(r"\s+\|\s+(Yahoo Finance|Benzinga|StockTitan|MarketWatch).*$", "", text, flags=re.I)

    return text[:280].strip()


def parse_news_datetime(raw):
    if raw is None:
        return None

    try:
        if isinstance(raw, (int, float)) and raw > 0:
            return datetime.fromtimestamp(raw, ET)
    except Exception:
        pass

    try:
        dt = email.utils.parsedate_to_datetime(str(raw))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ET)
        return dt.astimezone(ET)
    except Exception:
        return None


def news_age_hours(dt):
    if not dt:
        return None
    try:
        return max(0, (datetime.now(ET) - dt).total_seconds() / 3600)
    except Exception:
        return None


def is_fresh_news(dt, max_hours=96):
    if not dt:
        return True
    return news_age_hours(dt) <= max_hours


def looks_like_stale_pr(text):
    lower = str(text or "").lower()
    this_year = str(datetime.now(ET).year)
    stale_years = [str(y) for y in range(2020, datetime.now(ET).year)]
    if this_year in lower:
        return False
    return any(year in lower for year in stale_years)


def company_tokens(company_name):
    name = normalize_text(company_name)
    if not name:
        return []

    remove_words = {
        "inc", "inc.", "corp", "corp.", "corporation", "company", "co",
        "ltd", "limited", "plc", "holdings", "holding", "group",
        "technologies", "technology", "therapeutics", "pharmaceuticals",
        "systems", "solutions", "international", "common", "stock",
    }

    tokens = []
    for token in re.findall(r"[A-Za-z0-9]+", name):
        if len(token) < 3:
            continue
        if token.lower() in remove_words:
            continue
        tokens.append(token.lower())

    return tokens[:4]


def headline_matches_ticker_or_company(ticker, text, company_name=""):
    ticker = str(ticker or "").upper().strip()
    text = normalize_text(text)
    lower = text.lower()

    if ticker_token_present(ticker, text):
        return True

    tokens = company_tokens(company_name)
    if tokens and any(tok in lower for tok in tokens[:3]):
        return True

    return False


def valid_scraped_headline(ticker, text, company_name="", require_symbol_or_company=True):
    ticker = str(ticker or "").upper().strip()
    text = clean_headline(text, allow_aggregator=True)

    if not text or len(text) < 25:
        return False

    lower = text.lower()

    if lower in {ticker.lower(), f"{ticker.lower()} stock", f"{ticker.lower()} news"}:
        return False

    if looks_like_stale_pr(text):
        return False

    if require_symbol_or_company and not headline_matches_ticker_or_company(ticker, text, company_name):
        return False

    if ticker in AMBIGUOUS_TICKERS_REQUIRE_COMPANY:
        words = AMBIGUOUS_TICKERS_REQUIRE_COMPANY[ticker]
        if any(w in lower for w in words):
            has_company_evidence = headline_matches_ticker_or_company(ticker, text, company_name)
            has_exchange_evidence = "nasdaq" in lower or "nyse" in lower or "inc" in lower or "corp" in lower or "ltd" in lower
            if not (has_company_evidence or has_exchange_evidence):
                return False

    return True


def extract_meta_candidates_from_html(html_text):
    candidates = []
    soup = BeautifulSoup(html_text, "html.parser")

    for tag in soup.find_all(["h1", "h2", "h3", "a"]):
        text = normalize_text(tag.get_text(" ", strip=True))
        if text:
            candidates.append(text)

    for meta_name in [
        {"name": "description"},
        {"property": "og:description"},
        {"property": "twitter:description"},
        {"property": "og:title"},
        {"name": "twitter:title"},
    ]:
        tag = soup.find("meta", attrs=meta_name)
        if tag and tag.get("content"):
            candidates.append(normalize_text(tag.get("content")))

    for p in soup.find_all("p")[:6]:
        text = normalize_text(p.get_text(" ", strip=True))
        if text:
            candidates.append(text)

    return candidates


def build_news_result(text, source, ticker, company_name="", published_at=None, confidence=0.70):
    clean = clean_headline(text, allow_aggregator=False)

    if not clean:
        return None

    quality = classify_news_quality(clean)
    if quality not in ["STRONG", "WEAK"]:
        return None

    if not valid_scraped_headline(ticker, clean, company_name, require_symbol_or_company=True):
        return None

    age = news_age_hours(published_at)
    bucket = classify_catalyst_bucket(clean)

    if age is not None and age > 96:
        return None

    if quality == "STRONG":
        confidence += 0.15
    if source in ["FINNHUB", "PR", "GLOBE", "GOOGLE_NEWS", "YAHOO"]:
        confidence += 0.05
    if published_at:
        confidence += 0.05

    return {
        "headline": clean,
        "quality": quality,
        "source": source,
        "bucket": bucket,
        "published_at": published_at,
        "age_hours": round(age, 1) if age is not None else None,
        "confidence": min(confidence, 0.98),
    }


def best_news_result(results):
    if not results:
        return None

    def score_item(item):
        quality_score = 2 if item.get("quality") == "STRONG" else 1
        source_score = {
            "FINNHUB": 4,
            "PR": 4,
            "GLOBE": 4,
            "GOOGLE_NEWS": 3,
            "YAHOO": 3,
            "SEC": 3,
        }.get(item.get("source"), 1)
        age = item.get("age_hours")
        freshness = 3
        if age is not None:
            if age <= 24:
                freshness = 4
            elif age <= 48:
                freshness = 3
            elif age <= 96:
                freshness = 1
        bucket_bonus = 1 if item.get("bucket") != "General news" else 0
        return (quality_score, source_score, freshness, bucket_bonus, item.get("confidence", 0))

    return sorted(results, key=score_item, reverse=True)[0]


def get_news_catalyst(ticker):
    if not FINNHUB_API_KEY:
        return "none", "Missing Finnhub key"

    today = datetime.now(ET).strftime("%Y-%m-%d")
    three_days_ago = (datetime.now(ET) - timedelta(days=3)).strftime("%Y-%m-%d")

    url = "https://finnhub.io/api/v1/company-news"
    params = {
        "symbol": ticker,
        "from": three_days_ago,
        "to": today,
        "token": FINNHUB_API_KEY,
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        news = r.json()

        if not isinstance(news, list) or not news:
            return "none", "No fresh catalyst found"

        candidates = []
        company_name = get_company_name(ticker)

        for item in news[:8]:
            headline = normalize_text(item.get("headline", ""))
            published_at = parse_news_datetime(item.get("datetime"))
            result = build_news_result(headline, "FINNHUB", ticker, company_name, published_at, confidence=0.78)
            if result:
                candidates.append(result)

        best = best_news_result(candidates)
        if best:
            return best.get("bucket", "news"), best.get("headline", "")

        return "none", "No fresh catalyst found"

    except Exception as e:
        print(f"[NEWS ERROR] {ticker}: {e}", flush=True)
        return "unknown", "News check failed"


def scrape_yahoo_news_deep(ticker, company_name=""):
    results = []

    urls = [
        f"https://finance.yahoo.com/quote/{ticker}/news/",
        f"https://finance.yahoo.com/quote/{ticker}",
    ]

    for url in urls:
        try:
            r = request_get(url, timeout=4)
            if r.status_code != 200:
                continue

            for text in extract_meta_candidates_from_html(r.text):
                result = build_news_result(text, "YAHOO", ticker, company_name, None, confidence=0.68)
                if result:
                    print(f"[YAHOO DEEP] {ticker}: {result['headline']} ({result['quality']})", flush=True)
                    results.append(result)

        except Exception as e:
            print(f"[YAHOO DEEP ERROR] {ticker}: {e}", flush=True)

    return results


def scrape_google_news_rss(ticker, company_name=""):
    results = []

    queries = [f"{ticker} stock"]
    if company_name:
        queries.append(f'"{company_name}" stock')

    for query in queries[:2]:
        url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"

        try:
            r = request_get(url, timeout=5)
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "xml")
            items = soup.find_all("item")[:8]

            for item in items:
                title = normalize_text(item.title.get_text(" ", strip=True) if item.title else "")
                pub_date = parse_news_datetime(item.pubDate.get_text(strip=True) if item.pubDate else None)

                if not is_fresh_news(pub_date, max_hours=96):
                    continue

                result = build_news_result(title, "GOOGLE_NEWS", ticker, company_name, pub_date, confidence=0.72)
                if result:
                    print(f"[GOOGLE NEWS] {ticker}: {result['headline']} ({result['quality']})", flush=True)
                    results.append(result)

        except Exception as e:
            print(f"[GOOGLE NEWS ERROR] {ticker}: {e}", flush=True)

    return results


def scrape_pr_headline(ticker, company_name=""):
    now = time.time()
    cache_key = f"{ticker.upper()}|{company_name}"

    if cache_key in PR_CACHE:
        cached_time, cached_result = PR_CACHE[cache_key]
        if now - cached_time < PR_CACHE_TTL_SECONDS:
            return cached_result

    query_terms = [ticker]
    if company_name:
        query_terms.append(company_name)

    source_urls = []
    for term in query_terms[:2]:
        q = quote_plus(term)
        source_urls.extend([
            ("PR", f"https://www.prnewswire.com/search/news/?keyword={q}"),
            ("GLOBE", f"https://www.globenewswire.com/search/keyword/{q}"),
        ])

    results = []

    for source, url in source_urls:
        try:
            r = request_get(url, timeout=4)
            if r.status_code != 200:
                continue

            candidates = extract_meta_candidates_from_html(r.text)

            for text in candidates:
                if not valid_scraped_headline(ticker, text, company_name, require_symbol_or_company=True):
                    continue

                result = build_news_result(text, source, ticker, company_name, None, confidence=0.76)
                if result:
                    results.append(result)
                    print(f"[PR/GLOBE SCRAPE] {ticker}: {result['headline']} ({result['quality']})", flush=True)

        except Exception as e:
            print(f"[PR/GLOBE SCRAPE ERROR] {ticker}: {e}", flush=True)

    best = best_news_result(results)
    PR_CACHE[cache_key] = (now, best)
    return best


def score_news_confidence(news_data, all_results=None):
    quality = news_data.get("quality", "NONE")
    source = news_data.get("source", "NONE")
    confidence = safe_float(news_data.get("confidence", 0))

    score = 0
    if quality == "STRONG":
        score += 4
    elif quality == "WEAK":
        score += 2
    elif quality == "JUNK":
        score -= 3

    if source in ["FINNHUB", "PR", "GLOBE", "SEC"]:
        score += 2
    elif source in ["GOOGLE_NEWS", "YAHOO"]:
        score += 1

    if confidence >= 0.85:
        score += 2
    elif confidence >= 0.70:
        score += 1

    return max(0, min(score, 10))


def find_real_news_headline(ticker, current_headline="", company_name=""):
    now = time.time()
    ticker = str(ticker or "").upper().strip()
    company_name = get_company_name(ticker, fallback=company_name)

    cache_key = f"{ticker}|{company_name}|{current_headline}"
    if cache_key in NEWS_CACHE:
        cached = NEWS_CACHE[cache_key]
        if now - cached["time"] < NEWS_CACHE_TTL_SECONDS:
            return cached["data"]

    results = []

    current_headline = clean_headline(current_headline, allow_aggregator=False)
    current_result = build_news_result(current_headline, "FINNHUB", ticker, company_name, None, confidence=0.80)
    if current_result:
        results.append(current_result)

    results.extend(scrape_yahoo_news_deep(ticker, company_name))
    results.extend(scrape_google_news_rss(ticker, company_name))

    pr_result = scrape_pr_headline(ticker, company_name)
    if pr_result:
        results.append(pr_result)

    sec_result = sec_filing_context_catalyst(ticker, company_name)
    if sec_result:
        print(f"[SEC CONTEXT] {ticker}: {sec_result['headline']} ({sec_result['quality']})", flush=True)
        results.append(sec_result)

    best = best_news_result(results)

    if best:
        headline = best["headline"]
        quality = best["quality"]
        bucket = best.get("bucket", "General news")
        source = best.get("source", "NEWS")
        age = best.get("age_hours")

        display = headline
        if age is not None and age <= 96:
            display = f"{headline} ({age:.0f}h old)"

        data = {
            "headline": display,
            "quality": quality,
            "bucket": bucket,
            "source": source,
            "confidence": best.get("confidence", 0),
            "news_confidence_score": score_news_confidence(best),
        }
        NEWS_CACHE[cache_key] = {"time": now, "data": data}
        print(f"[NEWS BEST] {ticker}: {display} | {quality} | {bucket} | {source}", flush=True)
        return data

    data = {
        "headline": "No fresh catalyst found",
        "quality": "NONE",
        "bucket": "No confirmed news",
        "source": "NONE",
        "confidence": 0,
        "news_confidence_score": 0,
    }
    NEWS_CACHE[cache_key] = {"time": now, "data": data}
    return data


# ============================================================
# SEC / DILUTION ENGINE
# ============================================================

def fetch_sec_company_record(ticker):
    headers = {"User-Agent": "scanner-bot contact@example.com"}

    tickers_url = "https://www.sec.gov/files/company_tickers.json"
    r = requests.get(tickers_url, headers=headers, timeout=10)
    companies = r.json()

    for item in companies.values():
        if item.get("ticker", "").upper() == ticker.upper():
            cik = str(item["cik_str"]).zfill(10)
            name = normalize_text(item.get("title", ""))
            return cik, name

    return None, ""


def get_recent_sec_filings(ticker):
    now = time.time()

    cache_key = f"FILINGS:{ticker}"
    if cache_key in SEC_CACHE:
        cached = SEC_CACHE[cache_key]
        if now - cached["time"] < SEC_CACHE_TTL_SECONDS:
            return cached["data"]

    try:
        headers = {"User-Agent": "scanner-bot contact@example.com"}
        cik, sec_name = fetch_sec_company_record(ticker)

        if not cik:
            data = {"cik": None, "company_name": "", "filings": [], "error": "SEC CIK not found"}
            SEC_CACHE[cache_key] = {"time": now, "data": data}
            return data

        filings_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = requests.get(filings_url, headers=headers, timeout=10)
        data = r.json()

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accession_numbers = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        descriptions = recent.get("primaryDocDescription", [])

        filings = []
        for form, date, acc, doc, desc in zip(forms[:30], dates[:30], accession_numbers[:30], primary_docs[:30], descriptions[:30]):
            filings.append({
                "form": form,
                "date": date,
                "accession": acc,
                "primary_doc": doc,
                "description": normalize_text(desc),
            })

        out = {
            "cik": cik,
            "company_name": sec_name,
            "filings": filings,
            "error": "",
        }
        SEC_CACHE[cache_key] = {"time": now, "data": out}
        return out

    except Exception as e:
        out = {"cik": None, "company_name": "", "filings": [], "error": f"SEC check error: {e}"}
        SEC_CACHE[cache_key] = {"time": now, "data": out}
        return out


def sec_document_url(cik, accession, primary_doc):
    if not cik or not accession or not primary_doc:
        return ""
    cik_num = str(int(cik))
    acc_clean = str(accession).replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/{primary_doc}"


def fetch_sec_filing_text(ticker, filing):
    data = get_recent_sec_filings(ticker)
    cik = data.get("cik")
    accession = filing.get("accession")
    primary_doc = filing.get("primary_doc")

    cache_key = f"BODY:{ticker}:{accession}:{primary_doc}"
    now = time.time()

    if cache_key in SEC_BODY_CACHE:
        cached = SEC_BODY_CACHE[cache_key]
        if now - cached["time"] < SEC_BODY_CACHE_TTL_SECONDS:
            return cached["data"]

    url = sec_document_url(cik, accession, primary_doc)
    if not url:
        return ""

    try:
        headers = {"User-Agent": "scanner-bot contact@example.com"}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return ""

        text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
        text = normalize_text(text)[:120_000]
        SEC_BODY_CACHE[cache_key] = {"time": now, "data": text}
        return text

    except Exception as e:
        print(f"[SEC BODY ERROR] {ticker}: {e}", flush=True)
        return ""


def check_sec_offering_risk(ticker):
    data = get_recent_sec_filings(ticker)

    if data.get("error"):
        return False, data.get("error")

    risky_forms = {"S-1", "S-3", "424B5", "424B3", "F-1", "F-3"}
    hits = []

    for filing in data.get("filings", [])[:20]:
        if filing.get("form") in risky_forms:
            hits.append(f"{filing.get('form')} filed {filing.get('date')}")

    if hits:
        return True, "; ".join(hits[:5])

    return False, "No recent offering-type SEC forms found"


def sec_filing_context_catalyst(ticker, company_name=""):
    data = get_recent_sec_filings(ticker)
    if data.get("error"):
        return None

    filings = data.get("filings", [])[:12]
    if not filings:
        return None

    today = datetime.now(ET).date()
    candidates = []

    catalyst_forms = {"8-K", "6-K", "10-Q", "10-K", "S-1", "S-3", "424B3", "424B5", "F-1", "F-3"}

    for filing in filings:
        form = filing.get("form", "")
        date_text = filing.get("date", "")
        desc = filing.get("description", "")
        if form not in catalyst_forms:
            continue

        try:
            filing_date = datetime.strptime(date_text, "%Y-%m-%d").date()
            age_days = (today - filing_date).days
        except Exception:
            age_days = 999

        if age_days > 5:
            continue

        raw = f"{form} filed {date_text}"
        if desc:
            raw += f" — {desc}"

        lower = raw.lower()
        quality = "WEAK"

        if any(k in lower for k in STRONG_KEYWORDS):
            quality = "STRONG"
        elif form in {"8-K", "6-K"}:
            quality = "WEAK"
        elif form in {"424B3", "424B5", "S-1", "S-3", "F-1", "F-3"}:
            quality = "WEAK"

        bucket = classify_catalyst_bucket(raw)
        candidates.append({
            "headline": raw,
            "quality": quality,
            "source": "SEC",
            "bucket": bucket if bucket != "General news" else "SEC filing",
            "published_at": None,
            "age_hours": age_days * 24,
            "confidence": 0.62 if quality == "WEAK" else 0.72,
        })

    return best_news_result(candidates)


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
            return safe_float(match.group(1), None)

    return None


def detect_offering_risk(text, price=0):
    if not text:
        return []

    t = text.lower()
    risks = []

    if "at-the-market" in t or "at the market" in t or "atm offering" in t:
        risks.append("🚨 ATM offering — company can sell shares anytime")

    if "equity distribution agreement" in t or "sales agreement" in t:
        risks.append("🚨 ATM/sales agreement — possible sell pressure")

    if "registered direct offering" in t:
        risks.append("🚨 Registered direct offering — immediate dilution")

    if "private placement" in t:
        risks.append("🚨 Private placement — dilution risk")

    if "securities purchase agreement" in t or "purchase agreement" in t:
        risks.append("🚨 Securities purchase agreement — financing/dilution")

    if "shelf registration" in t or "form s-3" in t or "form f-3" in t:
        risks.append("⚠️ Shelf registration — future dilution possible")

    if "resale" in t or "resale prospectus" in t or "selling stockholder" in t:
        risks.append("⚠️ Resale registration — shares may unlock for selling")

    if "equity line" in t:
        risks.append("🚨 Equity line financing — dilution risk")

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


def scan_recent_sec_bodies_for_dilution(ticker, price=0, max_filings=4):
    data = get_recent_sec_filings(ticker)
    if data.get("error"):
        return []

    today = datetime.now(ET).date()
    risks = []

    important_forms = {"8-K", "6-K", "S-1", "S-3", "F-1", "F-3", "424B3", "424B5"}

    for filing in data.get("filings", [])[:max_filings]:
        form = filing.get("form", "")
        date_text = filing.get("date", "")

        if form not in important_forms:
            continue

        try:
            filing_date = datetime.strptime(date_text, "%Y-%m-%d").date()
            age_days = (today - filing_date).days
        except Exception:
            age_days = 999

        if age_days > 10:
            continue

        text = fetch_sec_filing_text(ticker, filing)
        if not text:
            continue

        body_risks = detect_offering_risk(text, price=price)
        if body_risks:
            if age_days == 0:
                add_unique(risks, f"🚨 OFFERING LANGUAGE FILED TODAY: {form} {date_text}")
            elif age_days <= 2:
                add_unique(risks, f"🚨 Recent offering language: {form} {date_text}")

        for risk in body_risks[:3]:
            add_unique(risks, risk)

    return risks


def describe_dilution_risk(risk_text):
    text = (risk_text or "").lower()

    strong_words = [
        "registered direct", "private placement", "securities purchase agreement",
        "atm offering", "at-the-market", "equity distribution agreement",
        "sales agreement", "warrant", "convertible", "equity line",
        "resale", "selling stockholder", "offering language",
    ]

    medium_words = ["s-3", "f-3", "shelf", "prospectus", "424b5", "424b3"]

    if any(w in text for w in strong_words):
        return "🚨 CONFIRMED DILUTION RISK: offering/warrants/financing language found"

    if any(w in text for w in medium_words):
        return "⚠️ DILUTION RISK BUILDING: shelf/prospectus filing found"

    if "8-k" in text or "6-k" in text:
        return "🟡 SEC FILINGS PRESENT: recent filings found"

    return ""


# ============================================================
# SCORING / STRUCTURE
# ============================================================

def score_mover(mover, catalyst_type, catalyst_text):
    score = 0
    reasons = []
    risks = []

    ticker = str(mover.get("ticker", "")).upper()
    gain = safe_float(mover.get("gain", mover.get("gain_percent", 0)))
    price = safe_float(mover.get("price"))
    volume = safe_int(mover.get("volume"))

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
        reasons.append("27%+ momentum mover")
    elif gain >= 15:
        score += 1
        reasons.append("15%+ active mover")

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
        reasons.append("Confirmed catalyst")
    elif news_quality == "WEAK":
        score += 1
        reasons.append("Weak catalyst")
    elif news_quality == "JUNK":
        risks.append("⚠️ Aggregator headline only")
    else:
        risks.append("⚠️ No confirmed catalyst / technical momentum only")

    if catalyst_type in ["earnings", "patent", "contract", "legal", "biotech", "FDA / biotech", "Contract / order"]:
        score += 1
        reasons.append(f"Strong catalyst: {catalyst_type}")

    if gain > 30 and volume < 500_000:
        score -= 2
        risks.append("Thin-volume spike")

    if price < 1:
        risks.append("Sub-$1 stock")

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

    lows = [safe_float(c.get("low")) for c in candles[-count:]]
    return all(lows[i] >= lows[i - 1] for i in range(1, len(lows)))


def is_big_upper_wick(candle):
    high = safe_float(candle.get("high"))
    low = safe_float(candle.get("low"))
    close = safe_float(candle.get("close"))

    full_range = high - low
    upper_wick = high - close

    if full_range <= 0:
        return False

    return upper_wick / full_range >= 0.45


def is_trend_builder(result, candles):
    if len(candles) < 20:
        return False

    closes = [safe_float(c.get("close")) for c in candles]

    ema9 = ema(closes[-20:], 9)
    ema20 = ema(closes[-30:], 20) if len(closes) >= 30 else ema(closes, 20)
    ema50 = ema(closes[-50:], 50) if len(closes) >= 50 else None

    if ema9 is None or ema20 is None:
        return False

    price = safe_float(result.get("price_float", result.get("price")))
    vwap = safe_float(result.get("vwap_float", result.get("vwap")))

    above_vwap = result.get("above_vwap", is_above_vwap(price, vwap))
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


def update_vwap_state(result):
    price = safe_float(result.get("price"))
    vwap = safe_float(result.get("vwap"))

    has_vwap = vwap > 0
    above_vwap = price > vwap if has_vwap else True

    result["price_float"] = price
    result["vwap_float"] = vwap
    result["has_vwap"] = has_vwap
    result["above_vwap"] = above_vwap

    if has_vwap and price:
        result["vwap_distance"] = round(((price - vwap) / vwap) * 100, 2)
    else:
        result["vwap_distance"] = 0

    return result


def detect_quote_candle_mismatch(result):
    candles = result.get("candles", []) or []
    price = safe_float(result.get("price"))

    if candles and price > 0:
        last_close = safe_float(candles[-1].get("close"))
        if last_close and abs(price - last_close) / last_close > 0.12:
            add_unique(result.setdefault("risks", []), "⚠️ Quote/candle mismatch — possible bad print")
            result["bad_print_risk"] = True
        else:
            result["bad_print_risk"] = False
    else:
        result["bad_print_risk"] = False

    return result


def compute_momentum_flags(result):
    price = safe_float(result.get("price_float", result.get("price")))
    vwap = safe_float(result.get("vwap_float", result.get("vwap")))
    candles = result.get("candles", []) or []

    recent_vol = safe_int(result.get("recent_volume"))
    prev_vol = safe_int(result.get("prev_volume"))
    day_vol = safe_int(result.get("volume"))
    gain = safe_float(result.get("gain"))

    has_vwap = vwap > 0
    above_vwap = (price >= vwap) if has_vwap else True

    result["has_vwap"] = has_vwap
    result["above_vwap"] = above_vwap
    result["vwap_distance"] = round(((price - vwap) / vwap) * 100, 2) if has_vwap and price else 0

    recent_high = price
    if candles:
        recent_high = max(safe_float(c.get("high")) for c in candles[-10:]) or price

    result["recent_high"] = recent_high
    result["near_high"] = price >= recent_high * 0.97 if recent_high else False
    result["near_high_95"] = price >= recent_high * 0.95 if recent_high else False

    volume_expanding = recent_vol >= max(MIN_ALERT_RECENT_VOLUME, prev_vol)
    volume_fading = prev_vol > 0 and recent_vol < prev_vol * 0.60
    lost_vwap = has_vwap and price < vwap

    text = " ".join(result.get("reasons", []) + result.get("risks", [])).lower()
    bad_structure = detect_bad_structure(text)
    soft_structure_warning = detect_soft_structure_warning(text)

    has_higher_lows = bool(
        result.get("has_higher_lows")
        or result.get("higher_lows")
        or "higher lows" in text
    )

    breakout = bool(result.get("breakout") or result.get("breakout_confirmed"))

    result["volume_expanding"] = volume_expanding
    result["volume_fading"] = volume_fading
    result["lost_vwap"] = lost_vwap
    result["bad_structure"] = bad_structure
    result["soft_structure_warning"] = soft_structure_warning
    result["has_higher_lows"] = has_higher_lows
    result["breakout_confirmed"] = breakout

    shallow_vwap_slip = bool(lost_vwap and result.get("vwap_distance", 0) > -5.0)
    deep_vwap_loss = bool(lost_vwap and result.get("vwap_distance", 0) <= -5.0)

    result["shallow_vwap_slip"] = shallow_vwap_slip
    result["deep_vwap_loss"] = deep_vwap_loss

    healthy_pullback = bool(
        gain >= ALERT_MIN_GAIN
        and (above_vwap or shallow_vwap_slip)
        and (has_higher_lows or result.get("near_high") or breakout)
    )

    result["momentum_decay"] = bool(
        bad_structure
        or deep_vwap_loss
        or (volume_fading and lost_vwap and not healthy_pullback)
        or (volume_fading and not above_vwap and not has_higher_lows and not result.get("near_high"))
    )

    result["market_leader"] = bool(
        gain >= LEADER_MIN_GAIN
        and (
            day_vol >= LEADER_MIN_DAY_VOLUME
            or recent_vol >= LEADER_MIN_RECENT_VOLUME
        )
    )

    result["clean_trend_runner"] = bool(
        gain >= ALERT_MIN_GAIN
        and above_vwap
        and not bad_structure
        and recent_vol >= 100_000
        and (has_higher_lows or breakout or result.get("near_high"))
    )

    result["true_second_leg"] = bool(
        gain >= ALERT_MIN_GAIN
        and above_vwap
        and not bad_structure
        and recent_vol >= 150_000
        and has_higher_lows
        and (breakout or result.get("near_high"))
    )

    result["fresh_high_after_vwap_hold"] = bool(
        gain >= ALERT_MIN_GAIN
        and above_vwap
        and has_higher_lows
        and result.get("near_high")
        and recent_vol >= 100_000
    )

    # Stricter no-news runner: must be near high or breakout, not just drifting.
    result["massive_no_news_runner"] = bool(
        result.get("news_quality") in ["NONE", "UNKNOWN", "JUNK", "TECHNICAL"]
        and gain >= 35
        and day_vol >= 2_000_000
        and recent_vol >= 150_000
        and above_vwap
        and (breakout or result.get("near_high"))
    )

    result["fresh_leader_ignition"] = bool(
        gain >= FRESH_IGNITION_MIN_GAIN
        and (
            result.get("market_leader")
            or day_vol >= LEADER_MIN_DAY_VOLUME
            or recent_vol >= LEADER_MIN_RECENT_VOLUME
        )
        and above_vwap
        and result.get("near_high")
        and (volume_expanding or recent_vol >= 200_000)
        and not bad_structure
    )

    result["leader_reclaim"] = bool(
        gain >= ALERT_MIN_GAIN
        and (
            result.get("market_leader")
            or day_vol >= LEADER_MIN_DAY_VOLUME
        )
        and above_vwap
        and has_vwap
        and abs(result.get("vwap_distance", 0)) <= RECLAIM_MAX_DISTANCE_PCT
        and (has_higher_lows or volume_expanding)
        and not bad_structure
    )

    now_time = datetime.now(ET).time()
    result["midday_chop_risk"] = bool(
        MIDDAY_CHOP_START <= now_time < MIDDAY_CHOP_END
        and volume_fading
        and not result.get("fresh_leader_ignition")
        and not result.get("leader_reclaim")
        and not result.get("true_second_leg")
    )

    return result


def estimate_relative_volume(result):
    recent_vol = safe_int(result.get("recent_volume"))
    prev_vol = safe_int(result.get("prev_volume"))

    if prev_vol <= 0:
        result["rvol_estimate"] = 0
        result["rvol_label"] = "RVOL unclear"
        return result

    rvol = recent_vol / prev_vol if prev_vol else 0
    result["rvol_estimate"] = round(rvol, 2)

    if rvol >= 2.5:
        result["rvol_label"] = "🔥 Strong RVOL expansion"
    elif rvol >= 1.3:
        result["rvol_label"] = "🟢 RVOL building"
    elif rvol <= 0.6:
        result["rvol_label"] = "⚠️ RVOL fading"
    else:
        result["rvol_label"] = "🟡 RVOL stable"

    return result


def compute_leader_score(result):
    gain = safe_float(result.get("gain"))
    day_vol = safe_int(result.get("volume"))
    recent_vol = safe_int(result.get("recent_volume"))
    rvol = safe_float(result.get("rvol_estimate"))

    leader_score = 0

    if gain >= 100:
        leader_score += 5
    elif gain >= 75:
        leader_score += 4
    elif gain >= 50:
        leader_score += 3
    elif gain >= 40:
        leader_score += 2
    elif gain >= 25:
        leader_score += 1

    if day_vol >= 20_000_000:
        leader_score += 3
    elif day_vol >= 10_000_000:
        leader_score += 2
    elif day_vol >= 2_000_000:
        leader_score += 1

    if recent_vol >= 300_000:
        leader_score += 2
    elif recent_vol >= 150_000:
        leader_score += 1

    if rvol >= 2.0:
        leader_score += 1

    if result.get("market_leader"):
        leader_score += 1

    result["leader_score"] = max(0, min(leader_score, 10))

    if result["leader_score"] >= 8:
        add_unique(result.setdefault("reasons", []), "Leader score strong")

    return result


def compute_setup_score(result):
    score = 0

    if result.get("above_vwap"):
        score += 2
    if result.get("near_high"):
        score += 2
    elif result.get("near_high_95"):
        score += 1
    if result.get("volume_expanding"):
        score += 2
    if result.get("has_higher_lows"):
        score += 1
    if result.get("breakout_confirmed"):
        score += 1
    if result.get("true_second_leg"):
        score += 2
    if result.get("leader_reclaim") or result.get("fresh_leader_ignition"):
        score += 2
    if result.get("clean_trend_runner"):
        score += 1

    if result.get("momentum_decay"):
        score -= 3
    if result.get("bad_structure"):
        score -= 4
    if result.get("deep_vwap_loss"):
        score -= 4
    if result.get("midday_chop_risk"):
        score -= 2
    if result.get("bad_print_risk"):
        score -= 2

    result["setup_score"] = max(0, min(score, 10))
    return result


def compute_risk_score(result):
    risk = 0
    news_quality = result.get("news_quality", "NONE")
    risk_text = " ".join(result.get("risks", [])).lower()

    if news_quality == "JUNK":
        risk += 3
    if news_quality in ["NONE", "UNKNOWN"] and safe_float(result.get("gain")) < 40:
        risk += 2
    if result.get("volume_fading"):
        risk += 2
    if not result.get("above_vwap", True):
        risk += 3
    if result.get("bad_structure"):
        risk += 4
    if result.get("deep_vwap_loss"):
        risk += 4
    if result.get("midday_chop_risk"):
        risk += 2
    if result.get("bad_print_risk"):
        risk += 3
    if "dilution" in risk_text or "offering" in risk_text or "warrant" in risk_text:
        risk += 2

    result["risk_score"] = max(0, min(risk, 10))
    return result


def enforce_score_quality_boundaries(result):
    score = safe_int(result.get("score"))
    above_vwap = bool(result.get("above_vwap", True))

    clean_elite_structure = bool(
        above_vwap
        and not result.get("momentum_decay")
        and not result.get("bad_structure")
        and not result.get("deep_vwap_loss")
        and (
            result.get("true_second_leg")
            or result.get("fresh_leader_ignition")
            or result.get("leader_reclaim")
            or (
                result.get("clean_trend_runner")
                and result.get("near_high")
                and result.get("volume_expanding")
            )
        )
    )

    if score >= 10 and not clean_elite_structure:
        score = 9

    if result.get("momentum_decay") or result.get("bad_structure") or result.get("deep_vwap_loss"):
        score = min(score, 8)

    if not above_vwap:
        score = min(score, 7)

    if score >= 7 and safe_int(result.get("setup_score")) < 6 and not result.get("main_leader"):
        score = 6

    if safe_int(result.get("risk_score")) >= 7:
        score = min(score, 7)

    result["score"] = max(0, min(score, 10))
    return result


def apply_clean_scoring(result):
    score = int(result.get("score", 0) or 0)

    result = estimate_relative_volume(result)

    if result.get("market_leader"):
        score += 1
        add_unique(result.setdefault("reasons", []), "Market leader / heavy tape")

    if result.get("fresh_leader_ignition"):
        score += 2
        add_unique(result.setdefault("reasons", []), "Fresh leader ignition")

    if result.get("leader_reclaim"):
        score += 2
        add_unique(result.setdefault("reasons", []), "Leader VWAP reclaim")

    if result.get("rvol_label"):
        add_unique(result.setdefault("reasons", []), result.get("rvol_label"))

    if result.get("clean_trend_runner"):
        score += 2
        add_unique(result.setdefault("reasons", []), "Clean trend runner")

    if result.get("true_second_leg"):
        score += 2
        add_unique(result.setdefault("reasons", []), "Second leg continuation")

    if result.get("fresh_high_after_vwap_hold"):
        score += 1
        add_unique(result.setdefault("reasons", []), "Fresh high after VWAP hold")

    if result.get("massive_no_news_runner"):
        score += 1
        add_unique(result.setdefault("reasons", []), "No-news volume runner")

    if result.get("momentum_decay"):
        add_unique(result.setdefault("risks", []), "Momentum decay / wait for reclaim")

    if result.get("midday_chop_risk"):
        add_unique(result.setdefault("risks", []), "Midday chop / volume fade risk")

    if result.get("above_vwap") is False:
        if result.get("vwap_distance", 0) <= -5:
            add_unique(result.setdefault("risks", []), "Clear below VWAP")
        else:
            add_unique(result.setdefault("risks", []), "Below VWAP / reclaim needed")

    result["score"] = max(0, min(score, 10))
    result = compute_leader_score(result)
    result = compute_setup_score(result)
    result = compute_risk_score(result)
    result = enforce_score_quality_boundaries(result)
    result = compute_live_action_score(result)
    result = detect_early_leader(result)
    return compact_reasons(result)


def detect_sympathy_or_technical_context(result, all_results=None):
    gain = safe_float(result.get("gain"))
    volume = safe_int(result.get("volume"))
    recent_vol = safe_int(result.get("recent_volume"))
    above_vwap = bool(result.get("above_vwap", True))

    if result.get("news_quality") not in ["NONE", "UNKNOWN", "JUNK"]:
        return result

    if gain >= 35 and volume >= 2_000_000 and recent_vol >= 150_000 and above_vwap:
        result["catalyst_text"] = "Technical low-float / volume momentum"
        result["catalyst_type"] = "⚡ Technical momentum"
        result["news_quality"] = "TECHNICAL"
        add_unique(result.setdefault("reasons", []), "Technical volume momentum")
        return result

    result["catalyst_text"] = "Technical momentum only"
    return result


# ============================================================
# MAIN LEADER + MOMENTUM MOMENT ENGINE
# ============================================================

def classify_main_leader(result):
    gain = safe_float(result.get("gain"))
    volume = safe_int(result.get("volume"))
    recent_vol = safe_int(result.get("recent_volume"))
    rank = safe_int(result.get("rank", 99))

    above_vwap = bool(result.get("above_vwap", True))
    near_high = bool(result.get("near_high") or result.get("near_high_95"))
    bad_structure = bool(result.get("bad_structure"))
    deep_vwap_loss = bool(result.get("deep_vwap_loss"))
    momentum_decay = bool(result.get("momentum_decay"))

    tape_dominance = (
        (gain >= 50 and volume >= 5_000_000)
        or (gain >= 35 and volume >= 10_000_000)
        or (gain >= 75 and volume >= 2_000_000)
    )

    active_now = recent_vol >= MAIN_LEADER_MIN_RECENT_VOLUME
    top_of_tape = rank <= 5 or safe_int(result.get("leader_score")) >= 8

    result["main_leader"] = False
    result["leader_state"] = "NONE"

    if tape_dominance and active_now and top_of_tape and above_vwap and near_high and not bad_structure and not momentum_decay:
        result["main_leader"] = True
        result["leader_state"] = "👑 MAIN LEADER"
        add_unique(result.setdefault("reasons", []), "Main leader of tape")
        return result

    if tape_dominance and top_of_tape and above_vwap and not deep_vwap_loss:
        result["leader_state"] = "🔥 LEADER"
        return result

    if tape_dominance and not above_vwap:
        result["leader_state"] = "⚠️ FORMER LEADER — RECLAIM NEEDED"
        add_unique(result.setdefault("risks", []), "Former leader — reclaim needed")
        return result

    return result


def classify_momentum_moment(result):
    if result.get("momentum_decay") or result.get("bad_structure") or result.get("deep_vwap_loss"):
        result["momentum_moment"] = "NONE"
        return result

    if result.get("fresh_leader_ignition"):
        result["momentum_moment"] = "🚀 IGNITION MOMENT"
    elif result.get("true_second_leg"):
        result["momentum_moment"] = "🟢 SECOND LEG MOMENT"
    elif result.get("leader_reclaim"):
        result["momentum_moment"] = "🟢 VWAP RECLAIM MOMENT"
    elif result.get("fresh_high_after_vwap_hold"):
        result["momentum_moment"] = "📈 FRESH HIGH MOMENT"
    elif (
        result.get("above_vwap")
        and result.get("near_high")
        and result.get("volume_expanding")
        and safe_int(result.get("recent_volume")) >= 250_000
    ):
        result["momentum_moment"] = "🔥 LIVE MOMENTUM MOMENT"
    else:
        result["momentum_moment"] = "NONE"

    if result.get("momentum_moment") != "NONE":
        add_unique(result.setdefault("reasons", []), result["momentum_moment"])

    return result



def compute_live_action_score(result):
    """Ranks what matters most right now: fresh action + clean structure.
    Internal only. We do NOT print noisy sub-scores in the phone alert.
    """
    score = 0
    gain = safe_float(result.get("gain"))
    day_vol = safe_int(result.get("volume"))
    recent_vol = safe_int(result.get("recent_volume"))
    prev_vol = safe_int(result.get("prev_volume"))
    rvol = safe_float(result.get("rvol_estimate"))

    if gain >= 50:
        score += 2
    elif gain >= 30:
        score += 1
    elif gain >= EARLY_LEADER_MIN_GAIN:
        score += 1

    if recent_vol >= 300_000:
        score += 3
    elif recent_vol >= 150_000:
        score += 2
    elif recent_vol >= MIN_ALERT_RECENT_VOLUME:
        score += 1

    if prev_vol > 0 and recent_vol >= prev_vol * 1.75:
        score += 2
    elif prev_vol > 0 and recent_vol >= prev_vol * 1.15:
        score += 1
    elif rvol >= 2.0:
        score += 2
    elif rvol >= 1.25:
        score += 1

    if result.get("above_vwap"):
        score += 2
    if result.get("near_high"):
        score += 2
    elif result.get("near_high_95"):
        score += 1
    if result.get("has_higher_lows"):
        score += 1
    if result.get("breakout_confirmed"):
        score += 1
    if result.get("trend_builder_alert"):
        score += 1
    if result.get("clean_trend_runner"):
        score += 1
    if result.get("fresh_leader_ignition"):
        score += 2
    if result.get("true_second_leg"):
        score += 2
    if result.get("market_leader"):
        score += 1
    if day_vol >= 2_000_000:
        score += 1

    if result.get("news_quality") == "STRONG":
        score += 1

    if result.get("volume_fading"):
        score -= 2
    if result.get("momentum_decay"):
        score -= 3
    if result.get("bad_structure"):
        score -= 4
    if result.get("deep_vwap_loss"):
        score -= 4
    if result.get("midday_chop_risk"):
        score -= 2
    if result.get("bad_print_risk"):
        score -= 4

    result["live_action_score"] = max(0, min(score, 10))
    return result


def detect_early_leader(result):
    """Catches the PIII-at-$6 style phase: clean, active, near-high leadership before late confirmation."""
    gain = safe_float(result.get("gain"))
    recent_vol = safe_int(result.get("recent_volume"))
    day_vol = safe_int(result.get("volume"))
    prev_vol = safe_int(result.get("prev_volume"))

    volume_now = bool(
        recent_vol >= MIN_ALERT_RECENT_VOLUME
        and (
            prev_vol <= 0
            or recent_vol >= prev_vol * 1.10
            or result.get("volume_expanding")
            or safe_float(result.get("rvol_estimate")) >= 1.25
        )
    )

    clean_now = bool(
        result.get("above_vwap")
        and (result.get("near_high") or result.get("near_high_95"))
        and not result.get("bad_structure")
        and not result.get("deep_vwap_loss")
        and not result.get("momentum_decay")
        and not result.get("bad_print_risk")
    )

    structure_now = bool(
        result.get("has_higher_lows")
        or result.get("breakout_confirmed")
        or result.get("trend_builder_alert")
        or result.get("clean_trend_runner")
    )

    result["early_leader"] = bool(
        gain >= EARLY_LEADER_MIN_GAIN
        and clean_now
        and volume_now
        and structure_now
        and (day_vol >= 500_000 or recent_vol >= 150_000)
        and safe_int(result.get("live_action_score")) >= MIN_LIVE_ACTION_SCORE
    )

    if result.get("early_leader"):
        add_unique(result.setdefault("reasons", []), "Early leader: live action now")

    return result

# ============================================================
# ALERT GATES / TIERS
# ============================================================

def passes_master_alert_gate(result):
    gain = safe_float(result.get("gain"))
    score = safe_int(result.get("score"))
    live_action = safe_int(result.get("live_action_score"))
    recent_vol = safe_int(result.get("recent_volume"))
    day_vol = safe_int(result.get("volume"))

    early_override = bool(
        result.get("early_leader")
        and live_action >= MIN_LIVE_ACTION_SCORE
        and gain >= EARLY_LEADER_MIN_GAIN
    )

    leader_override = bool(
        score >= MIN_ALERT_SCORE
        and gain >= LEADER_MIN_GAIN
        and (
            day_vol >= LEADER_MIN_DAY_VOLUME
            or recent_vol >= LEADER_MIN_RECENT_VOLUME
        )
    )

    if score < MIN_ALERT_SCORE and not early_override:
        return False, f"score {score}/10 under hard {MIN_ALERT_SCORE}/10 floor"

    if live_action < MIN_LIVE_ACTION_SCORE and not leader_override:
        return False, f"live action {live_action}/10 under {MIN_LIVE_ACTION_SCORE}/10 floor"

    active_volume = recent_vol >= MIN_ALERT_RECENT_VOLUME or day_vol >= 500_000

    if gain < ALERT_MIN_GAIN and not early_override and not leader_override:
        return False, f"gain {gain:.1f}% under {ALERT_MIN_GAIN}% floor"

    if not active_volume and not leader_override:
        return False, "not enough active volume"

    return True, "passed"

def junk_spam_gate(result):
    confirmations = sum([
        bool(result.get("above_vwap")),
        bool(result.get("near_high")),
        bool(result.get("volume_expanding")),
        bool(result.get("has_higher_lows")),
    ])

    if result.get("bad_print_risk"):
        return False, "quote/candle mismatch"

    if result.get("bad_structure") or result.get("deep_vwap_loss"):
        if not result.get("main_leader"):
            return False, "bad structure"

    if result.get("news_quality") in ["JUNK"] and confirmations < 3:
        return False, "junk headline without strong structure"

    if result.get("news_quality") in ["NONE", "UNKNOWN"] and not (result.get("massive_no_news_runner") or result.get("early_leader") or result.get("main_leader")):
        return False, "no-news move not active enough"

    if confirmations < 2 and not result.get("main_leader"):
        return False, "not enough setup confirmation"

    if safe_int(result.get("setup_score")) < 6 and not (result.get("early_leader") or result.get("main_leader")):
        return False, "setup score too weak"

    if safe_int(result.get("risk_score")) >= 7 and not result.get("main_leader"):
        return False, "risk score too high"

    return True, "passed"


def opening_protection_gate(result, session):
    if not OPENING_5_MIN_PROTECTION:
        return True, "passed"

    now_time = datetime.now(ET).time()

    if session == "OPEN" and dtime(9, 30) <= now_time < dtime(9, 35):
        if safe_int(result.get("score")) < 9 and not result.get("main_leader"):
            return False, "waiting first 5 minutes"

    return True, "passed"


def classify_alert_tier(result, rank):
    gate_ok, _ = passes_master_alert_gate(result)
    if not gate_ok:
        return "AVOID"

    score = safe_int(result.get("score"))
    live_action = safe_int(result.get("live_action_score"))
    above_vwap = bool(result.get("above_vwap", True))
    deep_vwap_loss = bool(result.get("deep_vwap_loss"))
    bad_structure = bool(result.get("bad_structure"))

    if deep_vwap_loss or bad_structure or result.get("bad_print_risk"):
        if result.get("market_leader") and score >= MIN_ALERT_SCORE:
            return "LEADER"
        return "AVOID"

    if result.get("main_leader"):
        return "MAIN_LEADER"

    if result.get("momentum_decay"):
        if result.get("market_leader") and score >= MIN_ALERT_SCORE:
            return "LEADER"
        return "AVOID"

    if result.get("midday_chop_risk") and not (result.get("true_second_leg") or result.get("fresh_leader_ignition") or result.get("leader_reclaim") or result.get("early_leader")):
        if result.get("market_leader") and score >= MIN_ALERT_SCORE:
            return "LEADER"
        return "AVOID"

    if not above_vwap and not result.get("leader_reclaim"):
        if result.get("market_leader") and score >= MIN_ALERT_SCORE:
            return "LEADER"
        return "AVOID"

    if result.get("early_leader") and live_action >= MIN_LIVE_ACTION_SCORE:
        return "RUNNER"

    clean_runner_setup = bool(
        result.get("fresh_leader_ignition")
        or result.get("leader_reclaim")
        or result.get("true_second_leg")
        or result.get("clean_trend_runner")
        or result.get("fresh_high_after_vwap_hold")
        or result.get("massive_no_news_runner")
    )

    if score >= MIN_ALERT_SCORE and above_vwap and clean_runner_setup and live_action >= MIN_LIVE_ACTION_SCORE:
        return "RUNNER"

    if score >= 9 and safe_float(result.get("gain")) >= ALERT_MIN_GAIN and above_vwap and live_action >= MIN_LIVE_ACTION_SCORE:
        return "RUNNER"

    if result.get("market_leader") and score >= MIN_ALERT_SCORE:
        return "LEADER"

    return "AVOID"

def title_for_tier(result, tier):
    moment = result.get("momentum_moment", "NONE")

    if tier == "AVOID":
        if result.get("momentum_decay"):
            return "🔴 AVOID — MOMENTUM FADED"
        if result.get("bad_structure"):
            return "🔴 AVOID — TRAP RISK"
        return "🔴 AVOID"

    if result.get("main_leader"):
        if tier == "MAIN_LEADER" and moment != "NONE":
            return f"👑 MAIN LEADER — {moment.replace('🚀 ', '').replace('🟢 ', '').replace('📈 ', '').replace('🔥 ', '')}"
        return "👑 MAIN LEADER"

    if tier == "LEADER":
        if result.get("leader_state") == "⚠️ FORMER LEADER — RECLAIM NEEDED":
            return "🔥 MARKET LEADER — RECLAIM NEEDED"
        if result.get("momentum_decay"):
            return "🔥 MARKET LEADER — PULLBACK"
        if result.get("above_vwap"):
            return "🔥 MARKET LEADER"
        return "🔥 MARKET LEADER — RECLAIM NEEDED"

    if result.get("early_leader"):
        return "🚀 EARLY LEADER"

    if moment != "NONE":
        return moment

    if result.get("fresh_leader_ignition"):
        return "🟢 RUNNER — FRESH IGNITION"
    if result.get("leader_reclaim"):
        return "🟢 RUNNER — VWAP RECLAIM"
    if result.get("true_second_leg"):
        return "🟢 RUNNER — SECOND LEG"
    if result.get("fresh_high_after_vwap_hold"):
        return "🟢 RUNNER — VWAP HOLD"
    if result.get("massive_no_news_runner"):
        return "🟢 RUNNER — VOLUME RUNNER"
    if result.get("clean_trend_runner"):
        return "🟢 RUNNER — CLEAN TREND"

    return "🟢 RUNNER"


def setup_tier_context(result):
    tier = result.get("alert_tier", "AVOID")

    if tier == "MAIN_LEADER":
        result["tier_context"] = "Main leader of tape"
    elif tier == "RUNNER" and result.get("early_leader"):
        result["tier_context"] = "Early leader / live action now"
    elif tier == "RUNNER":
        result["tier_context"] = "Clean runner setup"
    elif tier == "LEADER":
        result["tier_context"] = "Market leader awareness"
    else:
        result["tier_context"] = "Avoid"

    return result


def meaningful_realert(result, alert_history, runner_prices, alert_scores, alert_setups, now):
    ticker = result["ticker"]
    current_price = safe_float(result.get("price"))
    current_score = safe_int(result.get("score"))
    current_live_action = safe_int(result.get("live_action_score"))
    setup = result.get("title") or result.get("setup_tag") or ""

    last_time = alert_history.get(ticker, 0)
    cooldown_done = now - last_time >= ALERT_COOLDOWN_SECONDS
    hard_cooldown_done = now - last_time >= MIN_RE_ALERT_SECONDS

    last_price = runner_prices.get(ticker, 0)
    last_score = alert_scores.get(ticker, 0)
    last_setup = alert_setups.get(ticker, "")

    if last_time == 0:
        return True, "first alert"

    if not hard_cooldown_done:
        return False, "hard cooldown active"

    if setup == last_setup and current_score <= last_score and current_price < last_price * 1.05:
        return False, "same setup / no improvement"

    major_upgrade = (
        ("LEADER" in last_setup and "RUNNER" in setup)
        or ("MAIN LEADER" in setup and "MAIN LEADER" not in last_setup)
        or ("RECLAIM" in setup and "RECLAIM" not in last_setup)
        or ("SECOND LEG" in setup and "SECOND LEG" not in last_setup)
        or ("IGNITION" in setup and "IGNITION" not in last_setup)
        or ("MOMENT" in setup and "MOMENT" not in last_setup)
        or ("EARLY LEADER" in setup and "EARLY LEADER" not in last_setup)
    )

    if major_upgrade and last_price and current_price >= last_price * 1.02:
        return True, "major setup upgrade"

    if result.get("true_second_leg") and last_price and current_price >= last_price * 1.04:
        return True, "second leg new high +4%"

    if result.get("early_leader") and last_price and current_price >= last_price * 1.025:
        return True, "early leader new high +2.5%"

    if result.get("leader_reclaim") or result.get("fresh_leader_ignition"):
        if last_price and current_price >= last_price * 1.03:
            return True, "confirmed ignition/reclaim"

    if not cooldown_done:
        return False, "cooldown active"

    if last_price and current_price >= last_price * 1.05:
        return True, "new high +5%"

    if current_live_action >= 9 and current_score >= last_score + 1:
        return True, "live action improved"

    if current_score >= last_score + 2:
        return True, "score improved +2"

    if result.get("main_leader") and last_price and current_price >= last_price * 1.04:
        return True, "main leader continuation +4%"

    if result.get("market_leader") and last_price and current_price >= last_price * 1.04:
        return True, "leader continuation +4%"

    return False, "no meaningful change"


def first_matching_reason(result):
    preferred = [
        "Main leader of tape",
        "Early leader: live action now",
        "Fresh leader ignition",
        "Leader VWAP reclaim",
        "Second leg continuation",
        "Fresh high after VWAP hold",
        "Clean trend runner",
        "No-news volume runner",
        "Market leader / heavy tape",
        "Volume expanding",
        "RVOL",
        "Technical volume momentum",
        "Price above VWAP",
        "Higher lows",
    ]

    reasons = result.get("reasons", []) or []
    for pref in preferred:
        for reason in reasons:
            if pref.lower() in str(reason).lower():
                return str(reason).replace("🔥 ", "").replace("🟢 ", "").replace("📈 ", "").replace("👑 ", "").strip()

    return str(reasons[0]).strip() if reasons else "Momentum setup"


def first_matching_risk(result):
    risks = result.get("risks", []) or []
    if not risks:
        return "None obvious"

    preferred = [
        "offering filed today",
        "dilution",
        "offering",
        "warrant",
        "quote/candle mismatch",
        "momentum decay",
        "midday chop",
        "upper wick",
        "below vwap",
        "no confirmed catalyst",
        "float unknown",
    ]

    for pref in preferred:
        for risk in risks:
            if pref in str(risk).lower():
                return str(risk).strip()

    return str(risks[0]).strip()


def build_compact_alert(result):
    result = clean_alert_consistency(result)

    float_shares = safe_float(result.get("float"))
    float_text = f"{float_shares/1_000_000:.1f}M" if float_shares else "Unknown"

    news_quality = result.get("news_quality", "UNKNOWN")
    catalyst_line = result.get("catalyst_text") or "No fresh catalyst found"
    catalyst_bucket = result.get("catalyst_bucket") or classify_catalyst_bucket(catalyst_line)
    catalyst_source = result.get("catalyst_source", "")

    if news_quality in ["NONE", "UNKNOWN", "JUNK"]:
        news_header = "❌ No confirmed news"
        catalyst_line = "Technical momentum only"
    elif news_quality == "TECHNICAL":
        news_header = "⚡ Technical momentum"
    elif news_quality == "STRONG":
        news_header = f"⚡ {catalyst_bucket}"
    else:
        news_header = f"⚠️ {catalyst_bucket}"

    source_note = f" [{catalyst_source}]" if catalyst_source and catalyst_source not in ["NONE", ""] else ""

    tier = result.get("alert_tier", "AVOID")
    title = result.get("title", title_for_tier(result, tier))

    setup_line = first_matching_reason(result)
    risk_line = first_matching_risk(result)

    return (
        f"{title}\n\n"
        f"{result['ticker']} | {tier} | ${safe_float(result.get('price')):.4f} | "
        f"+{safe_float(result.get('gain')):.1f}% | Float {float_text}\n"
        f"Catalyst: {news_header}{source_note} — {catalyst_line}\n\n"
        f"Setup: {setup_line}\n"
        f"Risk: {risk_line}"
    )

def detect_market_regime(results):
    if not results:
        return "UNKNOWN"

    top = results[:20]

    big_runners = sum(1 for r in top if safe_float(r.get("gain")) >= ALERT_MIN_GAIN)
    active_runners = sum(
        1 for r in top
        if safe_float(r.get("gain")) >= 15
        and (safe_int(r.get("volume")) >= 500_000 or safe_int(r.get("recent_volume")) >= 50_000)
    )
    quality_setups = sum(1 for r in top if safe_int(r.get("score")) >= 7)
    leaders = sum(1 for r in top if r.get("market_leader"))
    main_leaders = sum(1 for r in top if r.get("main_leader"))

    if main_leaders >= 1 or leaders >= 2 or big_runners >= 5 or (big_runners >= 3 and quality_setups >= 1):
        return "HOT"

    if leaders >= 1 or big_runners >= 2 or active_runners >= 4 or quality_setups >= 1:
        return "MIXED"

    return "CHOP"


# ============================================================
# MAIN SCANNER
# ============================================================

def run_scanner():
    print(f"[BOOT] Scanner started | {BOOT_MARKER}", flush=True)
    print(
        f"[BOOT] Scanning {SCAN_MIN_GAIN}%+ gainers internally | "
        f"premarket alerts={'ON' if PREMARKET_ALERTS_ENABLED else 'OFF'} | "
        f"phone alerts require {ALERT_MIN_GAIN}%+ and score {MIN_ALERT_SCORE}+ | "
        f"main leader / momentum moment / junk gate enabled",
        flush=True,
    )

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

        session = get_market_session()
        movers = dedupe_movers(get_percent_gainers())
        movers = sorted(movers, key=lambda x: safe_float(x.get("gain")), reverse=True)[:MAX_GAINERS]

        print(
            "[FRESH GAINERS] " + ", ".join([f"{m['ticker']} {m['gain']:.1f}%" for m in movers[:15]]),
            flush=True,
        )

        results = []
        seen_tickers = set()

        for raw_rank, mover in enumerate(movers, start=1):
            ticker = str(mover.get("ticker", "")).upper().strip()

            if ticker in seen_tickers or is_bad_ticker(ticker):
                continue

            seen_tickers.add(ticker)
            mover["ticker"] = ticker

            try:
                quote = get_finnhub_quote(ticker)
                if quote:
                    mover["price"] = safe_float(quote.get("price", mover.get("price")))
                    mover["gain"] = safe_float(quote.get("gain", mover.get("gain")))
                    mover["gain_percent"] = mover["gain"]
                    print(f"[LIVE] {ticker} ${mover['price']:.4f} {mover['gain']:.1f}%", flush=True)
                else:
                    print(f"[LIVE] {ticker} quote unavailable — using screener values", flush=True)

                price = safe_float(mover.get("price"))
                gain = safe_float(mover.get("gain"))
                volume = safe_int(mover.get("volume"))

                if price < MIN_PRICE or price > MAX_PRICE:
                    print(f"[FILTER] {ticker} price ${price:.2f} outside range", flush=True)
                    continue

                # Keep the scan wide enough to catch early leaders before the obvious 25%+ phase.
                if gain < EARLY_LEADER_MIN_GAIN:
                    print(f"[FILTER] {ticker} gain under early leader floor {gain:.1f}%", flush=True)
                    continue

                if volume <= 0:
                    mover["volume"] = 500_000

                market_cap, float_shares = get_finnhub_profile(ticker)
                company_name = get_company_name(ticker, fallback=mover.get("company_name", ""))

                if market_cap and market_cap > MAX_MARKET_CAP:
                    print(f"[FILTER] {ticker} market cap over 1B", flush=True)
                    continue

                if float_shares and float_shares > MAX_FLOAT_SHARES:
                    print(f"[FILTER] {ticker} float too high {float_shares:,.0f}", flush=True)
                    continue

                catalyst_type, catalyst_text = get_news_catalyst(ticker)
                news_data = find_real_news_headline(ticker, catalyst_text, company_name=company_name)

                headline = news_data.get("headline", "No fresh catalyst found")
                news_quality = news_data.get("quality", "NONE")
                catalyst_bucket = news_data.get("bucket", "No confirmed news")
                catalyst_source = news_data.get("source", "NONE")

                result = score_mover(mover, catalyst_bucket, headline)
                result["rank"] = raw_rank
                result["headline"] = headline
                result["catalyst_text"] = headline
                result["news_quality"] = news_quality
                result["catalyst_bucket"] = catalyst_bucket
                result["catalyst_source"] = catalyst_source
                result["news_confidence_score"] = safe_int(news_data.get("news_confidence_score"))
                result["session"] = session
                result["market_cap"] = market_cap
                result["float"] = float_shares
                result["company_name"] = company_name

                result["catalyst_type"] = catalyst_display_label(catalyst_bucket, news_quality)

                if not float_shares:
                    add_unique(result.setdefault("risks", []), "⚠️ Float unknown")
                elif float_shares <= 10_000_000:
                    result["score"] = min(10, result["score"] + 1)
                    add_unique(result.setdefault("reasons", []), "Low float momentum potential")

                candles = get_alpaca_candles(ticker)
                if not candles:
                    print(f"[DATA] {ticker} Alpaca failed — using Yahoo candles", flush=True)
                    candles = get_yahoo_candles(ticker)

                result["candles"] = candles or []

                recent_volume = sum(safe_int(c.get("volume")) for c in result["candles"][-5:]) if candles else 0
                prev_volume = sum(safe_int(c.get("volume")) for c in result["candles"][-10:-5]) if candles and len(candles) >= 10 else 0
                total_candle_volume = sum(safe_int(c.get("volume")) for c in result["candles"]) if candles else 0

                result["recent_volume"] = recent_volume
                result["prev_volume"] = prev_volume
                result["total_candle_volume"] = total_candle_volume

                if candles:
                    result["recent_high"] = max(safe_float(c.get("high")) for c in candles[-10:])
                    add_unique(result.setdefault("reasons", []), describe_volume_quality(candles))

                    first_close = safe_float(candles[0].get("close"))
                    last_close = safe_float(candles[-1].get("close"))
                    result["candle_session_gain"] = ((last_close - first_close) / first_close) * 100 if first_close else 0

                result = detect_quote_candle_mismatch(result)

                structure = analyze_structure(ticker, candles or [])

                result["structure_score"] = safe_int(structure.get("structure_score"))
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

                if is_trend_builder(result, candles or []):
                    result["trend_builder_alert"] = True
                    add_unique(result.setdefault("reasons", []), "Trend builder structure")
                else:
                    result["trend_builder_alert"] = False

                result = compute_momentum_flags(result)
                result = detect_sympathy_or_technical_context(result)
                result = apply_clean_scoring(result)
                result = classify_main_leader(result)
                result = classify_momentum_moment(result)
                result = compute_setup_score(result)
                result = compute_risk_score(result)
                result = enforce_score_quality_boundaries(result)

                sec_risk = False
                sec_note = ""

                if result.get("score", 0) >= MIN_ALERT_SCORE or result.get("rank", 99) <= 12 or result.get("market_leader"):
                    sec_risk, sec_note = check_sec_offering_risk(ticker)
                    result["sec_note"] = sec_note

                if sec_risk:
                    add_unique(result.setdefault("risks", []), f"🚨 Active dilution filing: {sec_note}")

                # Full SEC filing-body dilution scan only for priority names to keep bot fast.
                if result.get("score", 0) >= MIN_ALERT_SCORE or result.get("market_leader") or result.get("rank", 99) <= 8:
                    body_risks = scan_recent_sec_bodies_for_dilution(ticker, price=price, max_filings=4)
                    for risk in body_risks:
                        add_unique(result.setdefault("risks", []), risk)

                filing_text = f"{result.get('sec_note', '')} {result.get('catalyst_text', '')}"
                extra_risks = detect_offering_risk(filing_text, price=price) or []
                result.setdefault("risks", []).extend(extra_risks)

                dilution_label = describe_dilution_risk(" ".join(result.get("risks", []) + [filing_text]))
                if dilution_label:
                    result.setdefault("risks", []).insert(0, dilution_label)

                result = compute_risk_score(result)
                result = compute_live_action_score(result)
                result = detect_early_leader(result)
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

        # First sort for tape reading: hottest clean action NOW first.
        results.sort(
            key=lambda r: (
                bool(r.get("main_leader")),
                bool(r.get("early_leader")),
                safe_int(r.get("live_action_score")),
                safe_int(r.get("leader_score")),
                safe_int(r.get("score")),
                safe_float(r.get("gain")),
                safe_int(r.get("recent_volume")),
            ),
            reverse=True,
        )

        regime = detect_market_regime(results)
        bot_mode = get_bot_mode(regime, session)

        for r in results:
            r["market_regime"] = regime
            r["bot_mode"] = bot_mode

        top_line = " | ".join(
            f"#{r.get('rank')} {r['ticker']} {safe_int(r.get('live_action_score'))}/10 ACTION "
            f"{r['gain']:.1f}% {'EARLY' if r.get('early_leader') else ('MAIN' if r.get('main_leader') else ('LEADER' if r.get('market_leader') else ''))}"
            for r in results[:10]
        )

        print(f"[SCAN] Top ranked: {top_line}", flush=True)
        print(f"[REGIME] {regime} | [BOT MODE] {bot_mode} | [SESSION] {session}", flush=True)

        now = time.time()
        sent_count = 0

        alert_candidates = []
        tier_priority = {"MAIN_LEADER": 3, "RUNNER": 2, "LEADER": 1, "AVOID": 0}

        for result in results[:MAX_GAINERS]:
            ticker = result["ticker"]

            if session == "PREMARKET" and not PREMARKET_ALERTS_ENABLED:
                print(f"[PREMARKET] {ticker} radar only — no phone alert", flush=True)
                continue

            if bot_mode == "STRICT" and safe_int(result.get("live_action_score")) < 8 and not (result.get("early_leader") or result.get("main_leader")):
                print(f"[STRICT] {ticker} suppressed — live action under 8 in chop mode", flush=True)
                continue

            open_ok, open_reason = opening_protection_gate(result, session)
            if not open_ok:
                print(f"[OPEN FILTER] {ticker} {open_reason}", flush=True)
                continue

            gate_ok, gate_reason = passes_master_alert_gate(result)
            if not gate_ok:
                print(f"[NO ALERT] {ticker} {gate_reason}", flush=True)
                continue

            junk_ok, junk_reason = junk_spam_gate(result)
            if not junk_ok:
                print(f"[NO ALERT] {ticker} junk gate — {junk_reason}", flush=True)
                continue

            tier = classify_alert_tier(result, safe_int(result.get("rank", 99)))

            if tier == "AVOID":
                print(
                    f"[NO ALERT] {ticker} avoided — tier filter score={result.get('score')} "
                    f"gain={safe_float(result.get('gain')):.1f}% above_vwap={result.get('above_vwap')} "
                    f"decay={result.get('momentum_decay')}",
                    flush=True,
                )
                continue

            result["alert_tier"] = tier
            result = setup_tier_context(result)
            result["title"] = title_for_tier(result, tier)
            result["setup_tag"] = result["title"]
            result = clean_alert_consistency(result)

            if result.get("alert_tier") == "AVOID":
                print(f"[NO ALERT] {ticker} avoided after consistency cleanup", flush=True)
                continue

            alert_candidates.append(result)

        alert_candidates.sort(
            key=lambda r: (
                tier_priority.get(r.get("alert_tier", "AVOID"), 0),
                bool(r.get("early_leader")),
                safe_int(r.get("live_action_score")),
                safe_int(r.get("score")),
                safe_int(r.get("leader_score")),
                safe_int(r.get("recent_volume")),
                safe_float(r.get("gain")),
            ),
            reverse=True,
        )

        for result in alert_candidates:
            ticker = result["ticker"]

            if ticker in sent_this_cycle:
                continue

            if sent_count >= MAX_ALERTS_PER_CYCLE:
                break

            ok, reason = meaningful_realert(
                result,
                alert_history,
                runner_prices,
                alert_scores,
                alert_setups,
                now,
            )

            if not ok:
                print(f"[SKIP] {ticker} {reason}", flush=True)
                continue

            msg = build_compact_alert(result)

            print(
                f"[SEND] {ticker} tier={result.get('alert_tier')} action={safe_int(result.get('live_action_score'))}/10 "
                f"score={result['score']} gain={safe_float(result.get('gain')):.1f}% reason={reason}",
                flush=True,
            )

            sent = send_alert(msg)
            print(f"[SEND RESULT] {ticker} sent={sent}", flush=True)

            if sent:
                sent_this_cycle.add(ticker)
                sent_count += 1
                alert_history[ticker] = now
                runner_prices[ticker] = safe_float(result.get("price"))
                alert_scores[ticker] = safe_int(result.get("score"))
                alert_setups[ticker] = result.get("title", "")
                print(f"[ALERT SENT] {ticker} {result.get('title')}", flush=True)

            time.sleep(0.1)

        print("[SCAN] Cycle complete — sleeping", flush=True)
        print("[HEARTBEAT] alive", flush=True)
        time.sleep(SCAN_SLEEP)


# ============================================================
# STARTUP
# ============================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))

    print(f"[WEB] starting server on port {port}", flush=True)

    web_thread = Thread(
        target=lambda: app.run(
            host="0.0.0.0",
            port=port,
            debug=False,
            use_reloader=False,
        ),
        daemon=True,
    )

    web_thread.start()

    time.sleep(2)

    print("[BOOT] starting scanner", flush=True)
    run_scanner()
