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

from structure_engine import analyze_structure
from alerts import send_alert

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

ET = ZoneInfo("America/New_York")

BOOT_MARKER = "elite scanner rebuild v19 — runner/leader only + hard 7 floor"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")

# Scan universe can stay wider so bot still studies names internally.
SCAN_MIN_GAIN = 12
SCAN_SLEEP = 90

# V19 ALERT PHILOSOPHY:
# 1. Surface true market leaders.
# 2. Phone alerts are RUNNER / LEADER only.
# 3. No WATCH tier. Score 6 and below is ignored.
ALERT_MIN_GAIN = 25
MIN_ALERT_SCORE = 7
MIN_ALERT_RECENT_VOLUME = 75_000

LEADER_MIN_GAIN = 40
LEADER_MIN_DAY_VOLUME = 2_000_000
LEADER_MIN_RECENT_VOLUME = 150_000

# Fresh ignition / reclaim upgrades
FRESH_IGNITION_MIN_GAIN = 25
RECLAIM_MAX_DISTANCE_PCT = 2.0
MIDDAY_CHOP_START = dtime(11, 0)
MIDDAY_CHOP_END = dtime(14, 0)

# Do not hard-block fading leaders. Downgrade the title/bias instead.
BLOCK_FADING_WATCH_ALERTS = False

ALERT_COOLDOWN_SECONDS = 900
MIN_RE_ALERT_SECONDS = 300
MAX_ALERTS_PER_CYCLE = 4

MAX_GAINERS = 70
MIN_VOLUME = 50_000
MAX_PRICE = 100
MIN_PRICE = 0.50
MAX_MARKET_CAP = 1_000_000_000
MAX_FLOAT_SHARES = 50_000_000

TREND_BUILDER_MIN_GAIN = 12

CACHE_TTL_SECONDS = 60 * 30
PR_CACHE_TTL_SECONDS = 60 * 30

PROFILE_CACHE = {}
NEWS_CACHE = {}
SEC_CACHE = {}
PR_CACHE = {}

MARKET_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-19", "2026-02-16",
    "2026-04-03", "2026-05-25", "2026-06-19",
    "2026-07-03", "2026-09-07", "2026-11-26",
    "2026-12-25",
}

BAD_TICKER_SUFFIXES = ("WS", "WT", "WQ", "WSA", "WSC", "IW", "WARRANT")

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
]

AMBIGUOUS_TICKERS_REQUIRE_COMPANY = {
    # These symbols can also be common words/acronyms in unrelated PRs.
    "BESS": ["bess", "battery", "storage"],
    "GUTS": ["guts"],
    "STIM": ["stim", "non-stim", "stimulation"],
    "RVI": ["rvi", "vacuum interrupter"],
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
]

SOFT_NEWS_PHRASES = [
    "begins trading",
    "ticker symbol change",
    "regains compliance",
    "announces stock ticker",
    "reports first quarter",
    "quarterly results",
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
]

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


def detect_bad_structure(structure_text):
    """
    Hard bad structure only.
    Soft warnings like 'below VWAP / reclaim watch' should not automatically
    poison the ticker into AVOID.
    """
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
    soft_keywords = [
        "below vwap",
        "reclaim watch",
        "slightly below vwap",
    ]
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

    for r in result.get("reasons", []):
        low = str(r).lower()
        if "market cap" in low:
            continue
        if "fresh news" in low and news_quality in ["NONE", "UNKNOWN", "JUNK"]:
            continue
        if "15%+ early mover" in low:
            continue
        reasons.append(r)

    for r in result.get("risks", []):
        if str(r).strip().lower() in ["none", "n/a", ""]:
            continue
        risks.append(r)

    result["reasons"] = dedupe_phrases(reasons)[:6]
    result["risks"] = dedupe_phrases(risks)[:5]
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
    """Keep one row per ticker before any expensive quote/news/profile calls."""
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

        market_cap = safe_float(data.get("marketCapitalization")) * 1_000_000
        float_shares = safe_float(data.get("shareOutstanding")) * 1_000_000

        PROFILE_CACHE[ticker] = {"time": now, "data": (market_cap, float_shares)}
        return market_cap, float_shares

    except Exception as e:
        print(f"[FINNHUB PROFILE ERROR] {ticker}: {e}", flush=True)
        return 0, 0


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
# NEWS
# ============================================================

def classify_news_quality(headline):
    if not headline:
        return "NONE"

    text = str(headline).lower().strip()

    if text in ["none", "no fresh catalyst found", "news check failed", "missing finnhub key"]:
        return "NONE"

    if any(word in text for word in BAD_NEWS_KEYWORDS):
        return "JUNK"

    if any(word in text for word in WEAK_NEWS_OVERRIDES):
        return "WEAK"

    if any(word in text for word in SOFT_NEWS_PHRASES):
        return "WEAK"

    if any(word in text for word in STRONG_KEYWORDS):
        return "STRONG"

    return "WEAK"


def get_news_catalyst(ticker):
    if not FINNHUB_API_KEY:
        return "none", "Missing Finnhub key"

    today = time.strftime("%Y-%m-%d")
    url = "https://finnhub.io/api/v1/company-news"
    params = {
        "symbol": ticker,
        "from": today,
        "to": today,
        "token": FINNHUB_API_KEY,
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
        if "contract" in h or "agreement" in h or "partnership" in h:
            return "contract", headline
        if "fda" in h or "trial" in h or "phase" in h:
            return "biotech", headline
        if "lawsuit" in h or "jury" in h or "damages" in h:
            return "legal", headline
        if "offering" in h or "warrant" in h or "registered direct" in h:
            return "offering", headline

        return "news", headline

    except Exception as e:
        print(f"[NEWS ERROR] {ticker}: {e}", flush=True)
        return "unknown", "News check failed"


def clean_headline(text):
    text = re.sub(r"\s+", " ", str(text or "").strip())
    if not text:
        return ""

    lower = text.lower()
    if any(x in lower for x in BAD_NEWS_KEYWORDS):
        return ""

    if any(x in lower for x in BAD_PR_MATCH_PHRASES):
        return ""

    return text


def looks_like_stale_pr(text):
    """Reject old PR dates when scrape pages surface stale results."""
    lower = str(text or "").lower()
    stale_years = ["2025", "2024", "2023", "2022", "2021"]
    return any(year in lower for year in stale_years)


def valid_scraped_headline(ticker, text):
    ticker = str(ticker or "").upper().strip()
    text = clean_headline(text)

    if not text or len(text) < 35:
        return False

    lower = text.lower()

    # Reject company-name-only / symbol-only page fragments.
    if lower in {ticker.lower(), f"{ticker.lower()} stock", f"{ticker.lower()} news"}:
        return False

    # Reject stale PR pages like Oct 2025 results.
    if looks_like_stale_pr(text):
        return False

    # Ticker must appear as a clean standalone token.
    if not re.search(rf"\b{re.escape(ticker)}\b", text, re.IGNORECASE):
        return False

    # Symbols that are also generic words/acronyms need stronger evidence.
    if ticker in AMBIGUOUS_TICKERS_REQUIRE_COMPANY:
        words = AMBIGUOUS_TICKERS_REQUIRE_COMPANY[ticker]
        if any(w in lower for w in words) and "nasdaq" not in lower and "inc" not in lower and "corp" not in lower and "ltd" not in lower:
            return False

    return True


def scrape_pr_headline(ticker):
    now = time.time()
    cache_key = ticker.upper()

    if cache_key in PR_CACHE:
        cached_time, cached_result = PR_CACHE[cache_key]
        if now - cached_time < PR_CACHE_TTL_SECONDS:
            return cached_result

    sources = [
        f"https://www.prnewswire.com/search/news/?keyword={ticker}",
        f"https://www.globenewswire.com/search/keyword/{ticker}",
    ]

    headers = {"User-Agent": "Mozilla/5.0"}

    for url in sources:
        try:
            r = requests.get(url, headers=headers, timeout=3)
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "html.parser")

            for tag in soup.find_all(["a", "h1", "h2", "h3"]):
                text = tag.get_text(" ", strip=True)

                if not valid_scraped_headline(ticker, text):
                    continue

                text = clean_headline(text)
                quality = classify_news_quality(text)

                if quality in ["STRONG", "WEAK"]:
                    PR_CACHE[cache_key] = (now, text)
                    print(f"[PR SCRAPE] {ticker}: {text} ({quality})", flush=True)
                    return text

        except Exception as e:
            print(f"[PR SCRAPE ERROR] {ticker}: {e}", flush=True)

    PR_CACHE[cache_key] = (now, "")
    return ""

def find_real_news_headline(ticker, current_headline=""):
    now = time.time()
    ticker = str(ticker or "").upper().strip()

    if ticker in NEWS_CACHE:
        cached = NEWS_CACHE[ticker]
        if now - cached["time"] < CACHE_TTL_SECONDS:
            return cached["data"]

    current_headline = clean_headline(current_headline)
    quality = classify_news_quality(current_headline)

    if quality in ["STRONG", "WEAK"]:
        data = (current_headline, quality)
        NEWS_CACHE[ticker] = {"time": now, "data": data}
        return data

    try:
        url = f"https://finance.yahoo.com/quote/{ticker}/news/"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=2)

        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")

            for link in soup.find_all("a"):
                raw_text = link.get_text(" ", strip=True)

                if not valid_scraped_headline(ticker, raw_text):
                    continue

                text = clean_headline(raw_text)
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


# ============================================================
# SEC / DILUTION AWARENESS ONLY
# ============================================================

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
            data = (False, "SEC CIK not found")
            SEC_CACHE[ticker] = {"time": now, "data": data}
            return data

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
        data = (False, f"SEC check error: {e}")
        SEC_CACHE[ticker] = {"time": now, "data": data}
        return data


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


def describe_dilution_risk(risk_text):
    text = (risk_text or "").lower()

    strong_words = [
        "registered direct", "private placement", "securities purchase agreement",
        "atm offering", "at-the-market", "equity distribution agreement",
        "sales agreement", "warrant", "convertible", "equity line",
        "resale", "selling stockholder",
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

    if catalyst_type in ["earnings", "patent", "contract", "legal", "biotech"]:
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

    result["massive_no_news_runner"] = bool(
        result.get("news_quality") in ["NONE", "UNKNOWN", "JUNK"]
        and gain >= 35
        and day_vol >= 2_000_000
        and recent_vol >= 150_000
        and above_vwap
        and (has_higher_lows or breakout or result.get("near_high"))
    )

    # Fresh leader ignition: leader starts expanding again after consolidation.
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

    # Leader reclaim: important name gets back over VWAP after pullback.
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

    # Midday chop awareness: do not kill leaders, but warn on weak midday tape.
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
    """
    Lightweight RVOL-style estimate using 5-min candle volume.
    Not true historical RVOL, but useful for live tape quality.
    """
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
    """
    Leader score = how important the ticker is on today's tape.
    V19 removes entry_score and WATCH completely. Trade guidance comes from tier only:
    RUNNER / LEADER / WATCH / AVOID.
    """
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


def enforce_score_quality_boundaries(result):
    """
    V19 score discipline:
    - 10/10 must be rare and backed by clean structure.
    - Big gain alone cannot create a 10.
    - No entry_score exists anymore.
    """
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

    # Make 10s matter. If it is not a clean elite setup, it cannot stay 10.
    if score >= 10 and not clean_elite_structure:
        score = 9

    # Damaged structure can still be important, but it should not look elite.
    if result.get("momentum_decay") or result.get("bad_structure") or result.get("deep_vwap_loss"):
        score = min(score, 8)

    if not above_vwap:
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
            add_unique(result.setdefault("risks", []), "Below VWAP / reclaim watch")

    result["score"] = max(0, min(score, 10))
    result = compute_leader_score(result)
    result = enforce_score_quality_boundaries(result)
    return compact_reasons(result)


# ============================================================
# ALERT TIERS
# ============================================================

def passes_master_alert_gate(result):
    gain = safe_float(result.get("gain"))
    score = safe_int(result.get("score"))
    recent_vol = safe_int(result.get("recent_volume"))
    day_vol = safe_int(result.get("volume"))

    # V19 hard rule: no phone alerts under 7. Period.
    if score < MIN_ALERT_SCORE:
        return False, f"score {score}/10 under hard {MIN_ALERT_SCORE}/10 floor"

    leader_override = bool(
        score >= MIN_ALERT_SCORE
        and gain >= LEADER_MIN_GAIN
        and (
            day_vol >= LEADER_MIN_DAY_VOLUME
            or recent_vol >= LEADER_MIN_RECENT_VOLUME
        )
    )

    active_volume = recent_vol >= MIN_ALERT_RECENT_VOLUME or day_vol >= 500_000

    if gain < ALERT_MIN_GAIN and not leader_override:
        return False, f"gain {gain:.1f}% under {ALERT_MIN_GAIN}% floor"

    if not active_volume and not leader_override:
        return False, "not enough active volume"

    return True, "passed"


def classify_alert_tier(result, rank):
    """
    V19: no WATCH tier.
    Phone alerts are only:
    - RUNNER = clean continuation / actionable momentum
    - LEADER = major tape leader / awareness name
    Everything else is suppressed as AVOID.
    """
    gate_ok, _ = passes_master_alert_gate(result)
    if not gate_ok:
        return "AVOID"

    gain = safe_float(result.get("gain"))
    score = safe_int(result.get("score"))
    above_vwap = bool(result.get("above_vwap", True))
    deep_vwap_loss = bool(result.get("deep_vwap_loss"))
    bad_structure = bool(result.get("bad_structure"))

    if score < MIN_ALERT_SCORE:
        return "AVOID"

    # Damaged structure can still be a LEADER awareness alert if it is a major tape name,
    # but it cannot be a RUNNER.
    if deep_vwap_loss or bad_structure or result.get("momentum_decay"):
        if result.get("market_leader") and score >= MIN_ALERT_SCORE:
            return "LEADER"
        return "AVOID"

    if not above_vwap and not result.get("leader_reclaim"):
        if result.get("market_leader") and score >= MIN_ALERT_SCORE:
            return "LEADER"
        return "AVOID"

    clean_runner_setup = bool(
        result.get("fresh_leader_ignition")
        or result.get("leader_reclaim")
        or result.get("true_second_leg")
        or result.get("clean_trend_runner")
        or result.get("fresh_high_after_vwap_hold")
        or result.get("massive_no_news_runner")
    )

    # RUNNER = clean setup, not just a big mover.
    if score >= MIN_ALERT_SCORE and above_vwap and clean_runner_setup:
        return "RUNNER"

    # Strong 9+ names can become RUNNER if clean, even without a named setup flag.
    if score >= 9 and gain >= ALERT_MIN_GAIN and above_vwap:
        return "RUNNER"

    # LEADER = important tape name, even if not a clean continuation setup.
    if result.get("market_leader") and score >= MIN_ALERT_SCORE:
        return "LEADER"

    return "AVOID"

def title_for_tier(result, tier):
    if tier == "AVOID":
        if result.get("momentum_decay"):
            return "🔴 AVOID — MOMENTUM FADED"
        if result.get("bad_structure"):
            return "🔴 AVOID — TRAP RISK"
        return "🔴 AVOID"

    if tier == "LEADER":
        if result.get("momentum_decay"):
            return "🔥 MARKET LEADER — PULLBACK WATCH"
        if result.get("above_vwap"):
            return "🔥 MARKET LEADER"
        return "🔥 MARKET LEADER — RECLAIM WATCH"

    if result.get("fresh_leader_ignition"):
        return "🟢 RUNNER — FRESH IGNITION"
    if result.get("leader_reclaim"):
        return "🟢 RUNNER — VWAP RECLAIM"
    if result.get("true_second_leg"):
        return "🟢 RUNNER — SECOND LEG"
    if result.get("fresh_high_after_vwap_hold"):
        return "🟢 RUNNER — VWAP HOLD"
    if result.get("massive_no_news_runner"):
        return "🟢 RUNNER — NO-NEWS VOLUME"
    if result.get("clean_trend_runner"):
        return "🟢 RUNNER — CLEAN TREND"

    return "🟢 RUNNER"

def setup_tier_context(result):
    """V19: no WATCH/Entry. Only tier context for internal use."""
    tier = result.get("alert_tier", "AVOID")

    if tier == "RUNNER":
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
    setup = result.get("title") or result.get("setup_tag") or ""
    tier = result.get("alert_tier", "")

    last_time = alert_history.get(ticker, 0)
    cooldown_done = now - last_time >= ALERT_COOLDOWN_SECONDS
    hard_cooldown_done = now - last_time >= MIN_RE_ALERT_SECONDS

    last_price = runner_prices.get(ticker, 0)
    last_score = alert_scores.get(ticker, 0)
    last_setup = alert_setups.get(ticker, "")

    if last_time == 0:
        return True, "first alert"

    # Absolute anti-spam floor. Nothing re-alerts inside this window.
    if not hard_cooldown_done:
        return False, "hard cooldown active"

    # Major title/category upgrade only.
    major_upgrade = (
        ("LEADER" in last_setup and "RUNNER" in setup)
        or ("RECLAIM" in setup and "RECLAIM" not in last_setup)
        or ("SECOND LEG" in setup and "SECOND LEG" not in last_setup)
        or ("FRESH IGNITION" in setup and "FRESH IGNITION" not in last_setup)
    )

    if major_upgrade and last_price and current_price >= last_price * 1.02:
        return True, "major setup upgrade"

    # Strong second-leg continuation can re-alert before full cooldown,
    # but still needs real price improvement.
    if result.get("true_second_leg") and last_price and current_price >= last_price * 1.04:
        return True, "second leg new high +4%"

    # Reclaim / fresh ignition needs confirmation, not just title flip.
    if (result.get("leader_reclaim") or result.get("fresh_leader_ignition")):
        if last_price and current_price >= last_price * 1.03:
            return True, "confirmed ignition/reclaim"

    # Outside normal cooldown, allow meaningful continuation only.
    if not cooldown_done:
        return False, "cooldown active"

    if last_price and current_price >= last_price * 1.05:
        return True, "new high +5%"

    if current_score >= last_score + 2:
        return True, "score improved +2"

    if result.get("market_leader") and last_price and current_price >= last_price * 1.04:
        return True, "leader continuation +4%"

    return False, "no meaningful change"


def first_matching_reason(result):
    preferred = [
        "Fresh leader ignition",
        "Leader VWAP reclaim",
        "Second leg continuation",
        "Fresh high after VWAP hold",
        "Clean trend runner",
        "No-news volume runner",
        "Market leader / heavy tape",
        "Volume expanding",
        "RVOL",
        "Price above VWAP",
        "Higher lows",
    ]

    reasons = result.get("reasons", []) or []
    for pref in preferred:
        for reason in reasons:
            if pref.lower() in str(reason).lower():
                return str(reason).replace("🔥 ", "").replace("🟢 ", "").replace("📈 ", "").strip()

    return str(reasons[0]).strip() if reasons else "Momentum watch"


def first_matching_risk(result):
    risks = result.get("risks", []) or []
    if not risks:
        return "None obvious"

    preferred = [
        "dilution",
        "offering",
        "warrant",
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
    """
    V19 phone alert: RUNNER / LEADER only. No WATCH, no Entry, no Bias.
    Title + stats + catalyst + setup + risk only.
    """
    result = compact_reasons(result)

    float_shares = safe_float(result.get("float"))
    float_text = f"{float_shares/1_000_000:.1f}M" if float_shares else "Unknown"

    news_quality = result.get("news_quality", "UNKNOWN")
    catalyst_line = result.get("catalyst_text") or "No fresh catalyst found"

    if news_quality in ["NONE", "UNKNOWN", "JUNK"]:
        news_header = "❌ No confirmed news"
        catalyst_line = "Technical momentum only"
    elif news_quality == "STRONG":
        news_header = "⚡ Strong news"
    else:
        news_header = "⚠️ Weak/unclear news"

    tier = result.get("alert_tier", "AVOID")
    title = result.get("title", title_for_tier(result, tier))

    setup_line = first_matching_reason(result)
    risk_line = first_matching_risk(result)

    return (
        f"{title}\n\n"
        f"{result['ticker']} | {result['score']}/10 | {tier} | "
        f"${safe_float(result.get('price')):.4f} | +{safe_float(result.get('gain')):.1f}% | Float {float_text}\n"
        f"Catalyst: {news_header} — {catalyst_line}\n\n"
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

    if leaders >= 2 or big_runners >= 5 or (big_runners >= 3 and quality_setups >= 1):
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
        f"leaders require {LEADER_MIN_GAIN}%+ / heavy tape | "
        f"phone alerts require {ALERT_MIN_GAIN}%+ and score {MIN_ALERT_SCORE}+ | runner/leader only",
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

                # V18 speed/noise filter: do not spend profile/news/candle calls
                # on weak under-alert-floor names. The screener can stay wide,
                # but full processing is for 25%+ movers only.
                if gain < ALERT_MIN_GAIN:
                    print(f"[FILTER] {ticker} gain under alert floor {gain:.1f}%", flush=True)
                    continue

                if volume <= 0:
                    mover["volume"] = 500_000

                # Cheap profile filter BEFORE slow news/PR scraping.
                market_cap, float_shares = get_finnhub_profile(ticker)

                if market_cap and market_cap > MAX_MARKET_CAP:
                    print(f"[FILTER] {ticker} market cap over 1B", flush=True)
                    continue

                if float_shares and float_shares > MAX_FLOAT_SHARES:
                    print(f"[FILTER] {ticker} float too high {float_shares:,.0f}", flush=True)
                    continue

                catalyst_type, catalyst_text = get_news_catalyst(ticker)
                headline, news_quality = find_real_news_headline(ticker, catalyst_text)

                result = score_mover(mover, catalyst_type, headline)
                result["rank"] = raw_rank
                result["headline"] = headline
                result["catalyst_text"] = headline
                result["news_quality"] = news_quality
                result["session"] = session
                result["market_cap"] = market_cap
                result["float"] = float_shares

                if news_quality == "STRONG":
                    result["catalyst_type"] = "⚡ STRONG NEWS"
                elif news_quality == "WEAK":
                    result["catalyst_type"] = "⚠️ WEAK NEWS"
                elif news_quality == "JUNK":
                    result["catalyst_type"] = "🚫 JUNK NEWS"
                else:
                    result["catalyst_type"] = "❌ NO NEWS"

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
                result = apply_clean_scoring(result)

                sec_risk = False
                sec_note = ""

                if result.get("score", 0) >= MIN_ALERT_SCORE or result.get("rank", 99) <= 12 or result.get("market_leader"):
                    sec_risk, sec_note = check_sec_offering_risk(ticker)
                    result["sec_note"] = sec_note

                if sec_risk:
                    add_unique(result.setdefault("risks", []), f"🚨 Active dilution filing: {sec_note}")

                filing_text = f"{result.get('sec_note', '')} {result.get('catalyst_text', '')}"
                extra_risks = detect_offering_risk(filing_text, price=price) or []
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

        # V19: leaders ranked first internally; alerts prioritize RUNNER > LEADER only.
        results.sort(
            key=lambda r: (
                r.get("market_leader", False),
                safe_int(r.get("leader_score")),
                safe_float(r.get("gain")),
                safe_int(r.get("recent_volume")),
                safe_int(r.get("score")),
            ),
            reverse=True,
        )

        regime = detect_market_regime(results)

        for r in results:
            r["market_regime"] = regime

        top_line = " | ".join(
            f"#{r.get('rank')} {r['ticker']} S{r['score']}/10 L{safe_int(r.get('leader_score'))}/10 {r['gain']:.1f}% "
            f"{'LEADER' if r.get('market_leader') else ''}"
            for r in results[:10]
        )

        print(f"[SCAN] Top ranked: {top_line}", flush=True)
        print(f"[REGIME] {regime}", flush=True)

        now = time.time()
        sent_count = 0

        alert_candidates = []
        tier_priority = {"RUNNER": 2, "LEADER": 1, "AVOID": 0}

        for result in results[:MAX_GAINERS]:
            ticker = result["ticker"]

            gate_ok, gate_reason = passes_master_alert_gate(result)
            if not gate_ok:
                print(f"[NO ALERT] {ticker} {gate_reason}", flush=True)
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

            alert_candidates.append(result)

        alert_candidates.sort(
            key=lambda r: (
                tier_priority.get(r.get("alert_tier", "AVOID"), 0),
                safe_int(r.get("leader_score")),
                safe_float(r.get("gain")),
                safe_int(r.get("recent_volume")),
                safe_int(r.get("score")),
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
                f"[SEND] {ticker} tier={result.get('alert_tier')} score={result['score']} "
                f"gain={safe_float(result.get('gain')):.1f}% reason={reason}",
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
