import os
import re
import time
import html
import json
from datetime import datetime, timedelta, time as dtime, timezone
from zoneinfo import ZoneInfo
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from flask import Flask
from dotenv import load_dotenv

from structure_engine import analyze_structure
from alerts import send_alert

load_dotenv()

# ============================================================
# ELITE SCANNER REBUILD v33.14 FULL
# Fast Pass + Full Runner/Avoid Engine + News + SEC + Coil + Alert Safety
# ============================================================

ET = ZoneInfo("America/New_York")
BOOT_MARKER = "elite scanner v34.6 — fast parallel news + advanced dilution + clean alerts"

# ============================================================
# ENV
# ============================================================

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
BENZINGA_API_KEY = os.getenv("BENZINGA_API_KEY")
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://data.alpaca.markets")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")

# ============================================================
# CONFIG
# ============================================================

# Universe / fast pass
SCAN_MIN_GAIN = 8.0                  # scanner can view wider universe internally
PREMARKET_SCAN_MIN_GAIN = 8.0        # do not starve premarket candidate pool
OPEN_SCAN_MIN_GAIN = 8.0
HARD_MIN_GAIN = 8.0                 # regular-hours hard floor
PREMARKET_HARD_MIN_GAIN = 8.0       # fixed: do not kill 18-24% premarket leaders
ALERT_MIN_GAIN = 27.0                # alerts still prefer real 25%+ movers
PREMARKET_ALERT_MIN_GAIN = 27.0      # but allow elite premarket runners slightly early
MIN_PRICE = 0.50
MAX_PRICE = 80.0
MIN_FAST_VOLUME = 75_000             # fixed: premarket liquidity can be thinner
MIN_DEEP_VOLUME = 150_000
MAX_FLOAT = 80_000_000               # fixed: 40M was too nuclear in thin markets
MAX_MARKET_CAP = 1_200_000_000       # fixed: cap is awareness unless extreme
EXTREME_FLOAT_SKIP = 150_000_000     # only hard skip truly heavy floats
EXTREME_MARKET_CAP_SKIP = 3_000_000_000

# ============================================================
# v33 PURE LEADER UNIVERSE — NO WATCHLISTS
# ============================================================
DISCOVERY_MIN_GAIN = 8.0       # internal discovery only
RUNNER_MIN_GAIN = 27.0         # hard Telegram alert floor, no exceptions
SMALL_CAP_IDEAL = 300_000_000
SMALL_CAP_MAX = 1_500_000_000
BIG_CAP_SOURCE_SKIP = 3_000_000_000
LOW_PRICE_LEADER_MAX = 20.0
LEADER_MIN_VOLUME = 75_000

LOW_FLOAT_TINY = 5_000_000
LOW_FLOAT_ELITE = 10_000_000
LOW_FLOAT_GOOD = 20_000_000
LOW_FLOAT_ACCEPTABLE = 40_000_000

# Alerting
ALERT_MIN_SCORE = 7.0
MAX_GAINERS = 120
MAX_ALERTS_PER_CYCLE = 4
SCAN_SLEEP = 90

ALERT_COOLDOWN_SECONDS = 900
EARLY_ALERT_COOLDOWN_SECONDS = 600
RE_ALERT_NEW_HIGH_MULTIPLIER = 1.05
EARLY_RE_ALERT_NEW_HIGH_MULTIPLIER = 1.03

# Caches
CACHE_TTL_SECONDS = 1800
SHORT_CACHE_TTL_SECONDS = 120

PROFILE_CACHE = {}
QUOTE_CACHE = {}
LAST_GOOD_QUOTES = {}
NEWS_CACHE = {}
SEC_CACHE = {}
CANDLE_CACHE = {}
LAST_GOOD_CANDLES = {}
YAHOO_CANDLE_BLOCK_UNTIL = 0
YAHOO_CANDLE_429_COUNT = 0
MARKET_REGIME_CACHE = {}

LAST_ALERT = {}
LAST_EARLY_ALERT = {}
SENT_THIS_CYCLE = set()

# Output preferences
SHOW_FLOAT = True
SHOW_HEADLINE = False
SHOW_VERBOSE_DEBUG = True


# ============================================================
# v33.2 MULTI-SOURCE LEADER DISCOVERY
# ============================================================
DISCOVERY_MIN_GAIN = 8.0
RUNNER_MIN_GAIN = 27.0
ALERT_MIN_GAIN = 27.0

LEADER_SOURCE_LIMIT = 180
MAX_RAW_LEADER_POOL = 450

# Source-layer small-cap focus. Unknown cap is allowed because small-cap feeds
# often have incomplete data and the quote/profile step can verify later.
SOURCE_MAX_MARKET_CAP = 3_000_000_000
SOURCE_MAX_FLOAT = 150_000_000
SOURCE_MIN_PRICE = 0.20
SOURCE_MAX_PRICE = 80.00
SOURCE_MIN_VOLUME = 50_000


# ============================================================
# v33.2.1 MULTI-SOURCE LEADER DISCOVERY CONSTANTS
# ============================================================
DISCOVERY_MIN_GAIN = 8.0
RUNNER_MIN_GAIN = 27.0
ALERT_MIN_GAIN = 27.0
LEADER_SOURCE_LIMIT = 180
MAX_RAW_LEADER_POOL = 450
SOURCE_MAX_MARKET_CAP = 3_000_000_000
SOURCE_MAX_FLOAT = 150_000_000
SOURCE_MIN_PRICE = 0.20
SOURCE_MAX_PRICE = 80.00
SOURCE_MIN_VOLUME = 50_000

# ============================================================
# FLASK KEEPALIVE
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return "scanner alive — v34.1 quote fallback stack + RVOL phase/tier upgrade", 200


@app.route("/health")
def health():
    return {
        "status": "ok",
        "version": BOOT_MARKER,
        "time_et": now_et().isoformat(),
    }, 200


def start_web_server():
    port = int(os.getenv("PORT", "10000"))
    print(f"[WEB] starting server on port {port}")
    app.run(host="0.0.0.0", port=port)


# ============================================================
# UTILS
# ============================================================

def now_et():
    return datetime.now(ET)


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").replace("$", "").strip()
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").replace("$", "").strip()
        return int(float(value))
    except Exception:
        return default


def clean_text(text):
    if not text:
        return ""
    text = html.unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fmt_money(value):
    value = safe_float(value)
    if value >= 1:
        return f"${value:.2f}"
    return f"${value:.4f}"


def fmt_big_num(n):
    n = safe_float(n)
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def clamp(value, low=0.0, high=10.0):
    return max(low, min(high, safe_float(value)))


def dedupe(items):
    out = []
    seen = set()
    for item in items:
        item = clean_text(item)
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def cached_get(cache, key, ttl=CACHE_TTL_SECONDS):
    item = cache.get(key)
    if not item:
        return None
    ts, value = item
    if time.time() - ts > ttl:
        cache.pop(key, None)
        return None
    return value


def cached_set(cache, key, value):
    cache[key] = (time.time(), value)
    return value


def http_get(url, params=None, headers=None, timeout=6):
    default_headers = {
        "User-Agent": "Mozilla/5.0 scannerbot/1.0",
        "Accept": "text/html,application/json,*/*",
    }
    if headers:
        default_headers.update(headers)
    return requests.get(url, params=params, headers=default_headers, timeout=timeout)




def safe_json_response(response, label="HTTP"):
    """Return JSON safely without crashing when a site sends HTML, blank text, or rate-limit pages."""
    try:
        text = getattr(response, "text", "") or ""
        if not text.strip():
            print(f"[{label} ERROR] empty response")
            return None

        content_type = ""
        try:
            content_type = response.headers.get("Content-Type", "")
        except Exception:
            content_type = ""

        # Still allow JSON even if content-type is missing, but warn on obvious HTML.
        if "html" in content_type.lower() or text.lstrip().startswith("<"):
            print(f"[{label} ERROR] non-JSON response: {text[:120]}")
            return None

        return response.json()
    except Exception as e:
        text = getattr(response, "text", "") or ""
        print(f"[{label} ERROR] invalid JSON: {e} | raw={text[:120]}")
        return None

def http_post(url, payload=None, params=None, headers=None, timeout=8):
    default_headers = {
        "User-Agent": "Mozilla/5.0 scannerbot/1.0",
        "Accept": "application/json,text/plain,*/*",
        "Content-Type": "application/json",
        "Origin": "https://finance.yahoo.com",
        "Referer": "https://finance.yahoo.com/markets/stocks/gainers/",
    }
    if headers:
        default_headers.update(headers)
    return requests.post(url, params=params, json=payload or {}, headers=default_headers, timeout=timeout)


# ============================================================
# MARKET HOURS / REGIME
# ============================================================

def market_is_active():
    now = now_et()
    print(f"[TIME] Market clock ET: {now.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")

    if now.weekday() >= 5:
        print("[MARKET] Weekend — scanner sleeping")
        return False

    # Premarket to shortly after close
    if dtime(7, 30) <= now.time() <= dtime(16, 10):
        return True

    print(f"[MARKET] Alerts OFF — {now.strftime('%I:%M %p ET')}")
    return False


def get_market_session_label():
    t = now_et().time()
    if dtime(7, 30) <= t < dtime(9, 30):
        return "PREMARKET"
    if dtime(9, 30) <= t < dtime(11, 0):
        return "OPENING MOMENTUM"
    if dtime(11, 0) <= t < dtime(14, 30):
        return "MIDDAY"
    if dtime(14, 30) <= t <= dtime(16, 10):
        return "POWER HOUR"
    return "CLOSED"


def is_premarket_session():
    return get_market_session_label() == "PREMARKET"


def dynamic_scan_min_gain():
    return PREMARKET_SCAN_MIN_GAIN if is_premarket_session() else OPEN_SCAN_MIN_GAIN


def dynamic_hard_min_gain():
    return PREMARKET_HARD_MIN_GAIN if is_premarket_session() else HARD_MIN_GAIN


def dynamic_alert_min_gain(result=None):
    # Keep regular session strict, but don't miss clean early premarket runners.
    if is_premarket_session():
        return PREMARKET_ALERT_MIN_GAIN
    return ALERT_MIN_GAIN


def is_cold_regime(regime):
    return bool(regime and "COLD" in regime.get("label", ""))


def estimate_market_regime(candidates):
    cached = cached_get(MARKET_REGIME_CACHE, "regime", ttl=SHORT_CACHE_TTL_SECONDS)
    if cached:
        return cached

    strong = [c for c in candidates if safe_float(c.get("gain")) >= 30]
    hot = [c for c in candidates if safe_float(c.get("gain")) >= 50]
    liquid = [c for c in candidates if safe_int(c.get("volume")) >= 500_000 and safe_float(c.get("gain")) >= 25]

    if len(hot) >= 4 or len(strong) >= 10:
        regime = {
            "label": "🔥 HOT MOMENTUM MARKET",
            "score_adjust": 0.3,
            "description": "Many strong gainers active",
        }
    elif len(liquid) >= 5:
        regime = {
            "label": "⚠️ MIXED MOMENTUM MARKET",
            "score_adjust": 0.0,
            "description": "Some clean movers, stay selective",
        }
    else:
        regime = {
            "label": "❄️ COLD / THIN MOMENTUM MARKET",
            "score_adjust": -0.3,
            "description": "Fewer quality movers, tighten standards",
        }

    return cached_set(MARKET_REGIME_CACHE, "regime", regime)


# ============================================================
# HARD SKIPS / TICKER FILTER
# ============================================================

BAD_TICKER_PATTERNS = [
    r"\.W$", r"\.WS$", r"\.WT$", r"\.U$", r"\.R$",
    r"WS$", r"WT$", r"WQ$", r"RT$", r"R$", r"U$",
]

BAD_TICKER_WORDS = [
    "WARRANT", "RIGHT", "UNIT", "PREFERRED",
]


def is_bad_ticker(ticker):
    if not ticker:
        return True

    t = ticker.upper().strip()

    if any(word in t for word in BAD_TICKER_WORDS):
        return True

    # Avoid overblocking normal 4-letter tickers ending W.
    if len(t) > 4:
        for pat in BAD_TICKER_PATTERNS:
            if re.search(pat, t):
                return True

    if "-" in t or "/" in t:
        return True

    return False


def fast_pass_filter(ticker, price, gain, volume=0, market_cap=0, float_shares=0, regime=None):
    """
    V32.1 fix: only hard-skip things that are truly untradeable.
    Float/market-cap are mostly awareness/score penalties now, especially in cold markets,
    so Yahoo can feed real candidates into deep scan instead of collapsing to zero.
    """
    reasons = []
    warnings = []
    gain_floor = DISCOVERY_MIN_GAIN

    if is_bad_ticker(ticker):
        reasons.append("warrant/unit/right ticker")

    if gain is None or gain < gain_floor:
        reasons.append(f"gain under {gain_floor:.0f}%")

    if price is None or price < MIN_PRICE or price > MAX_PRICE:
        reasons.append(f"price outside {fmt_money(MIN_PRICE)}-${MAX_PRICE:.0f}")

    # Premarket liquidity is thinner; don't overblock names before the open.
    min_vol = 50_000 if is_premarket_session() else MIN_FAST_VOLUME
    if volume and volume < min_vol:
        reasons.append(f"volume under {fmt_big_num(min_vol)}")

    # Only hard-skip huge/heavy names. Normal float/cap over target becomes awareness.
    if float_shares and float_shares > EXTREME_FLOAT_SKIP:
        reasons.append(f"extreme float over {fmt_big_num(EXTREME_FLOAT_SKIP)}")
    elif float_shares and float_shares > MAX_FLOAT:
        warnings.append(f"float over ideal {fmt_big_num(MAX_FLOAT)}")

    if market_cap and market_cap > EXTREME_MARKET_CAP_SKIP:
        reasons.append(f"extreme market cap over {fmt_big_num(EXTREME_MARKET_CAP_SKIP)}")
    elif market_cap and market_cap > MAX_MARKET_CAP:
        warnings.append(f"market cap over ideal {fmt_big_num(MAX_MARKET_CAP)}")

    # In a cold tape, don't make float/cap nuclear unless truly extreme.
    if reasons:
        print(f"[FAST SKIP] {ticker}: " + " | ".join(reasons))
        return False, reasons, warnings

    warn_text = (" | " + " | ".join(warnings)) if warnings else ""
    print(f"[FAST PASS] {ticker}: {fmt_money(price)} +{gain:.1f}% vol={fmt_big_num(volume)}{warn_text}")
    return True, [], warnings


# ============================================================
# v33 SMALL-CAP / LOW-FLOAT LEADER HELPERS
# ============================================================

def classify_float(float_shares):
    f = safe_int(float_shares)
    if f <= 0:
        return {"tier": "UNKNOWN", "boost": 0.0, "label": "⚠️ Float unknown", "risk": "Float/profile data missing"}
    if f <= LOW_FLOAT_TINY:
        return {"tier": "TINY", "boost": 0.85, "label": f"🔥 TINY FLOAT {fmt_big_num(f)}", "risk": "Tiny float — explosive but halt/chop risk higher"}
    if f <= LOW_FLOAT_ELITE:
        return {"tier": "ELITE", "boost": 0.65, "label": f"🔥 LOW FLOAT {fmt_big_num(f)}", "risk": "Low float momentum name — size carefully"}
    if f <= LOW_FLOAT_GOOD:
        return {"tier": "GOOD", "boost": 0.40, "label": f"🟢 LOW FLOAT {fmt_big_num(f)}", "risk": "Low float can accelerate quickly"}
    if f <= LOW_FLOAT_ACCEPTABLE:
        return {"tier": "ACCEPTABLE", "boost": 0.15, "label": f"🟡 Float {fmt_big_num(f)}", "risk": ""}
    return {"tier": "HIGH", "boost": -0.10, "label": f"Float {fmt_big_num(f)}", "risk": "Higher float — needs stronger volume"}


def leader_gain_boost(gain):
    g = safe_float(gain)
    if g >= 100:
        return 1.25, "💯 100%+ day leader"
    if g >= 75:
        return 0.95, "🔥 75%+ day leader"
    if g >= 50:
        return 0.70, "🔥 50%+ day leader"
    if g >= RUNNER_MIN_GAIN:
        return 0.35, "🟢 27%+ momentum leader"
    return 0.0, ""


def source_universe_score(item):
    """Rank raw screener rows so small-cap high-percent leaders go first, not mega caps."""
    gain = safe_float(item.get("gain"))
    volume = safe_int(item.get("volume"))
    price = safe_float(item.get("price"))
    cap = safe_int(item.get("market_cap"))

    score = gain * 2.0
    if gain >= 50:
        score += 50
    elif gain >= 27:
        score += 25

    if price and price <= LOW_PRICE_LEADER_MAX:
        score += 15
    if volume >= 1_000_000:
        score += 12
    elif volume >= 250_000:
        score += 8
    elif volume >= LEADER_MIN_VOLUME:
        score += 4

    if cap:
        if cap <= SMALL_CAP_IDEAL:
            score += 25
        elif cap <= SMALL_CAP_MAX:
            score += 12
        elif cap >= BIG_CAP_SOURCE_SKIP:
            score -= 60
    return score


def source_big_cap_skip(item):
    """Stop Yahoo big-cap rows from using live quote/profile slots before real leaders."""
    gain = safe_float(item.get("gain"))
    cap = safe_int(item.get("market_cap"))
    price = safe_float(item.get("price"))
    volume = safe_int(item.get("volume"))

    # Always keep true high-percent leaders even if cap data is weird.
    if gain >= 50:
        return False

    # Hard reject obvious big-cap slow movers at the source layer.
    if cap and cap >= BIG_CAP_SOURCE_SKIP and gain < RUNNER_MIN_GAIN:
        return True

    # Slow expensive names are not this bot's game.
    if price > MAX_PRICE and gain < RUNNER_MIN_GAIN:
        return True

    # Illiquid sub-leaders do not need profile calls.
    if gain < DISCOVERY_MIN_GAIN:
        return True
    if volume and volume < LEADER_MIN_VOLUME and gain < RUNNER_MIN_GAIN:
        return True

    return False

# ============================================================
# GAINER SOURCES
# ============================================================

def parse_yahoo_quote_item(q, source="Yahoo Gainers"):
    # Yahoo returns slightly different field names depending on endpoint.
    price = (
        q.get("regularMarketPrice")
        or q.get("postMarketPrice")
        or q.get("preMarketPrice")
        or q.get("price")
    )
    gain = (
        q.get("regularMarketChangePercent")
        or q.get("postMarketChangePercent")
        or q.get("preMarketChangePercent")
        or q.get("percentChange")
        or q.get("changePercent")
    )
    volume = (
        q.get("regularMarketVolume")
        or q.get("postMarketVolume")
        or q.get("preMarketVolume")
        or q.get("volume")
    )
    return {
        "ticker": str(q.get("symbol", "")).upper().strip(),
        "price": safe_float(price),
        "gain": safe_float(gain),
        "volume": safe_int(volume),
        "market_cap": safe_int(q.get("marketCap")),
        "source": source,
    }


def merge_source_items(items):
    merged = {}
    for item in items:
        ticker = item.get("ticker", "").upper().strip()
        if not ticker or is_bad_ticker(ticker):
            continue

        gain = safe_float(item.get("gain"))
        volume = safe_int(item.get("volume"))
        price = safe_float(item.get("price"))

        # Discard empty/garbage rows, but keep low-gain rows for ranking safety net.
        if price <= 0 and gain <= 0:
            continue

        existing = merged.get(ticker)
        if not existing:
            merged[ticker] = item
            continue

        # Prefer the row with the best percent gain. Fill missing fields from the other row.
        if gain > safe_float(existing.get("gain")):
            old = existing
            merged[ticker] = item
            if not merged[ticker].get("volume"):
                merged[ticker]["volume"] = old.get("volume", 0)
            if not merged[ticker].get("market_cap"):
                merged[ticker]["market_cap"] = old.get("market_cap", 0)
        else:
            if not existing.get("volume") and volume:
                existing["volume"] = volume
            if not existing.get("market_cap") and item.get("market_cap"):
                existing["market_cap"] = item.get("market_cap")

    out = list(merged.values())
    out.sort(key=lambda x: (safe_float(x.get("gain")), safe_int(x.get("volume"))), reverse=True)
    return out


def get_yahoo_predefined_gainers():
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": "day_gainers", "count": 250, "formatted": "false"}

    try:
        r = http_get(url, params=params, timeout=8)
        data = safe_json_response(r, "GAINERS Yahoo predefined")
        if not data:
            return []
        result = data.get("finance", {}).get("result") or []
        if not result:
            return []
        quotes = result[0].get("quotes", [])
        results = [parse_yahoo_quote_item(q, source="Yahoo predefined day_gainers") for q in quotes]
        results = [x for x in results if x.get("ticker")]
        print(f"[GAINERS] Yahoo predefined returned {len(results)} names")
        return results
    except Exception as e:
        print(f"[GAINERS ERROR] Yahoo predefined: {e}")
        return []


def yahoo_screener_payload(min_gain=20.0, min_volume=0, size=250, max_market_cap=None, max_price=80.0):
    # v33: ask Yahoo for small-cap / low-price percent gainers first.
    operands = [
        {"operator": "EQ", "operands": ["region", "us"]},
        {"operator": "EQ", "operands": ["quoteType", "EQUITY"]},
        {"operator": "GT", "operands": ["regularMarketChangePercent", min_gain]},
        {"operator": "GT", "operands": ["regularMarketPrice", 0.30]},
        {"operator": "LT", "operands": ["regularMarketPrice", max_price]},
    ]
    if min_volume:
        operands.append({"operator": "GT", "operands": ["regularMarketVolume", int(min_volume)]})
    if max_market_cap:
        operands.append({"operator": "LT", "operands": ["marketCap", int(max_market_cap)]})

    return {
        "size": size,
        "offset": 0,
        "sortField": "regularMarketChangePercent",
        "sortType": "DESC",
        "quoteType": "EQUITY",
        "query": {"operator": "AND", "operands": operands},
        "userId": "",
        "userIdType": "guid",
    }


def get_yahoo_custom_percent_gainers():
    url = "https://query1.finance.yahoo.com/v1/finance/screener"
    all_results = []

    # v33: small-cap/high-percent scans first. If Yahoo returns none, predefined still works.
    scans = [
        (50.0, 0, SMALL_CAP_MAX, 80.0, "Yahoo smallcap 50pct"),
        (27.0, 50_000, SMALL_CAP_MAX, 80.0, "Yahoo smallcap 27pct liquid"),
        (15.0, 100_000, SMALL_CAP_MAX, 40.0, "Yahoo smallcap 15pct lowprice"),
        (8.0, 250_000, SMALL_CAP_MAX, 25.0, "Yahoo smallcap 8pct volume"),
        (50.0, 0, None, 500.0, "Yahoo anycap 50pct backup"),
    ]

    for min_gain, min_volume, max_cap, max_price, label in scans:
        try:
            payload = yahoo_screener_payload(
                min_gain=min_gain,
                min_volume=min_volume,
                size=250,
                max_market_cap=max_cap,
                max_price=max_price,
            )
            r = http_post(url, payload=payload, timeout=8)
            data = safe_json_response(r, f"GAINERS {label}")
            if not data:
                continue
            result = data.get("finance", {}).get("result") or []
            if not result:
                print(f"[GAINERS] {label} returned 0 names")
                continue
            quotes = result[0].get("quotes", [])
            results = [parse_yahoo_quote_item(q, source=label) for q in quotes]
            results = [x for x in results if x.get("ticker")]
            print(f"[GAINERS] {label} returned {len(results)} names")
            all_results.extend(results)
        except Exception as e:
            print(f"[GAINERS ERROR] {label}: {e}")

    return merge_source_items(all_results)


def get_yahoo_gainers():
    results = []
    results.extend(get_yahoo_predefined_gainers())
    results.extend(get_yahoo_custom_percent_gainers())

    merged = merge_source_items(results)

    # Diagnostic that will immediately show if the bot sees the same leaders you see.
    top50 = [x for x in merged if safe_float(x.get("gain")) >= 50.0]
    if top50:
        print("[GAINERS] Yahoo +50% leaders: " + " | ".join(
            f"{x['ticker']} +{safe_float(x.get('gain')):.1f}%" for x in top50[:12]
        ))
    else:
        print("[GAINERS] Yahoo +50% leaders: none detected from Yahoo endpoints")

    print(f"[GAINERS] Yahoo merged total {len(merged)} names")
    return merged[:250]


def get_nasdaq_gainers():
    # Fallback source; Nasdaq endpoints can change often, so failure is acceptable.
    url = "https://api.nasdaq.com/api/marketmovers"
    params = {"assetclass": "stocks", "direction": "gainers"}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    }

    try:
        r = http_get(url, params=params, headers=headers, timeout=8)
        data = r.json()
        rows = data.get("data", {}).get("marketMovers", {}).get("rows", [])

        results = []
        for row in rows:
            ticker = str(row.get("symbol", "")).upper()
            price = safe_float(row.get("lastsale"))
            pct = str(row.get("pctchange", "")).replace("+", "")
            gain = safe_float(pct)
            volume = safe_int(row.get("volume"))

            if ticker:
                results.append({
                    "ticker": ticker,
                    "price": price,
                    "gain": gain,
                    "volume": volume,
                    "source": "Nasdaq Gainers",
                })

        print(f"[GAINERS] Nasdaq returned {len(results)} names")
        return results

    except Exception as e:
        print(f"[GAINERS ERROR] Nasdaq: {e}")
        return []




# ============================================================
# MULTI-SOURCE SMALL-CAP LEADER SOURCES — v33.2.1
# ============================================================

def source_pass_item(item):
    ticker = str(item.get("ticker", "")).upper().strip()
    if not ticker or is_bad_ticker(ticker):
        return False

    gain = safe_float(item.get("gain"))
    price = safe_float(item.get("price"))
    volume = safe_int(item.get("volume"))
    market_cap = safe_int(item.get("market_cap"))
    float_shares = safe_int(item.get("float"))

    if gain < DISCOVERY_MIN_GAIN:
        return False

    if price and (price < SOURCE_MIN_PRICE or price > SOURCE_MAX_PRICE):
        return False

    if volume and volume < SOURCE_MIN_VOLUME:
        return False

    if market_cap and market_cap > SOURCE_MAX_MARKET_CAP:
        return False

    if float_shares and float_shares > SOURCE_MAX_FLOAT:
        return False

    return True


def normalize_leader_item(ticker, price=0, gain=0, volume=0, market_cap=0, float_shares=0, source="unknown"):
    return {
        "ticker": str(ticker or "").upper().strip(),
        "price": safe_float(price),
        "gain": safe_float(gain),
        "volume": safe_int(volume),
        "market_cap": safe_int(market_cap),
        "float": safe_int(float_shares),
        "source": source,
        "sources": [source],
    }


def parse_percent_text(value):
    return safe_float(str(value or "").replace("+", "").replace("%", ""))


def parse_money_text(value):
    return safe_float(str(value or "").replace("$", "").replace(",", ""))


def parse_big_number_text(value):
    if value is None:
        return 0
    s = str(value).replace(",", "").replace("$", "").strip().upper()
    mult = 1
    if s.endswith("T"):
        mult = 1_000_000_000_000
        s = s[:-1]
    elif s.endswith("B"):
        mult = 1_000_000_000
        s = s[:-1]
    elif s.endswith("M"):
        mult = 1_000_000
        s = s[:-1]
    elif s.endswith("K"):
        mult = 1_000
        s = s[:-1]
    return int(safe_float(s) * mult)


def get_stockanalysis_gainers():
    url = "https://stockanalysis.com/markets/gainers/"
    results = []
    try:
        r = http_get(url, timeout=8)
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table tbody tr")

        for row in rows[:LEADER_SOURCE_LIMIT]:
            cells = [clean_text(c.get_text(" ")) for c in row.find_all("td")]
            if len(cells) < 5:
                continue

            ticker = cells[1]
            gain = parse_percent_text(cells[3] if len(cells) > 3 else 0)
            price = parse_money_text(cells[4] if len(cells) > 4 else 0)
            volume = parse_big_number_text(cells[5] if len(cells) > 5 else 0)
            market_cap = parse_big_number_text(cells[6] if len(cells) > 6 else 0)

            item = normalize_leader_item(
                ticker=ticker,
                price=price,
                gain=gain,
                volume=volume,
                market_cap=market_cap,
                source="StockAnalysis",
            )
            if source_pass_item(item):
                results.append(item)

        print(f"[GAINERS] StockAnalysis returned {len(results)} filtered leaders")
    except Exception as e:
        print(f"[GAINERS ERROR] StockAnalysis: {e}")

    return results


def get_finviz_smallcap_gainers():
    url = "https://finviz.com/screener.ashx"
    params = {
        "v": "111",
        "f": "cap_smallover,sh_avgvol_o100,sh_price_u80,ta_change_u5",
        "o": "-change",
    }
    results = []

    try:
        r = http_get(url, params=params, timeout=8)
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("tr[valign='top']")

        for row in rows[:LEADER_SOURCE_LIMIT]:
            cells = [clean_text(c.get_text(" ")) for c in row.find_all("td")]
            if len(cells) < 10:
                continue

            ticker = cells[1]
            market_cap = parse_big_number_text(cells[6] if len(cells) > 6 else 0)
            price = parse_money_text(cells[8] if len(cells) > 8 else 0)
            gain = parse_percent_text(cells[9] if len(cells) > 9 else 0)
            volume = parse_big_number_text(cells[10] if len(cells) > 10 else 0)

            item = normalize_leader_item(
                ticker=ticker,
                price=price,
                gain=gain,
                volume=volume,
                market_cap=market_cap,
                source="Finviz",
            )
            if source_pass_item(item):
                results.append(item)

        print(f"[GAINERS] Finviz small-cap returned {len(results)} filtered leaders")
    except Exception as e:
        print(f"[GAINERS ERROR] Finviz: {e}")

    return results


def get_marketwatch_gainers():
    url = "https://www.marketwatch.com/tools/screener/stock"
    params = {"exchange": "All", "skip": "0", "sort": "percentchange", "order": "desc"}
    results = []

    try:
        r = http_get(url, params=params, timeout=8)
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table tbody tr")

        for row in rows[:LEADER_SOURCE_LIMIT]:
            text = clean_text(row.get_text(" "))
            ticker_match = re.search(r"\b[A-Z]{1,5}\b", text)
            if not ticker_match:
                continue

            ticker = ticker_match.group(0)
            nums = re.findall(r"[-+]?\d+(?:\.\d+)?%", text)
            gain = parse_percent_text(nums[0]) if nums else 0
            money = re.findall(r"\$\s*\d+(?:\.\d+)?", text)
            price = parse_money_text(money[0]) if money else 0

            item = normalize_leader_item(
                ticker=ticker,
                price=price,
                gain=gain,
                volume=0,
                market_cap=0,
                source="MarketWatch",
            )
            if source_pass_item(item):
                results.append(item)

        print(f"[GAINERS] MarketWatch returned {len(results)} filtered leaders")
    except Exception as e:
        print(f"[GAINERS ERROR] MarketWatch: {e}")

    return results


def merge_leader_sources(items):
    merged = {}

    for item in items:
        ticker = str(item.get("ticker", "")).upper().strip()
        if not ticker or is_bad_ticker(ticker):
            continue

        existing = merged.get(ticker)
        if not existing:
            item["sources"] = list(dict.fromkeys(item.get("sources", [item.get("source", "unknown")])))
            merged[ticker] = item
            continue

        existing["gain"] = max(safe_float(existing.get("gain")), safe_float(item.get("gain")))
        existing["price"] = safe_float(item.get("price")) or safe_float(existing.get("price"))
        existing["volume"] = max(safe_int(existing.get("volume")), safe_int(item.get("volume")))
        existing["market_cap"] = safe_int(existing.get("market_cap")) or safe_int(item.get("market_cap"))
        existing["float"] = safe_int(existing.get("float")) or safe_int(item.get("float"))

        sources = existing.get("sources", [])
        src = item.get("source", "unknown")
        if src not in sources:
            sources.append(src)
        existing["sources"] = sources
        existing["source"] = "+".join(sources)

    out = list(merged.values())

    def sort_key(x):
        gain = safe_float(x.get("gain"))
        volume = safe_int(x.get("volume"))
        cap = safe_int(x.get("market_cap"))
        source_count = len(x.get("sources", []))
        small_cap_bonus = 1 if (cap and cap <= SOURCE_MAX_MARKET_CAP) or not cap else 0
        leader_bonus = 3 if gain >= 50 else 2 if gain >= 27 else 1 if gain >= 15 else 0
        return (leader_bonus, gain, source_count, small_cap_bonus, volume)

    out.sort(key=sort_key, reverse=True)

    top50 = [x for x in out if safe_float(x.get("gain")) >= 50]
    if top50:
        print("[LEADERS] +50% detected: " + " | ".join(
            f"{x['ticker']} +{safe_float(x.get('gain')):.1f}% src={','.join(x.get('sources', []))}"
            for x in top50[:12]
        ))

    return out[:MAX_RAW_LEADER_POOL]


def get_multi_source_leaders():
    sources = []

    try:
        sources.extend(get_yahoo_gainers())
    except Exception as e:
        print(f"[GAINERS ERROR] Yahoo source stack: {e}")

    sources.extend(get_stockanalysis_gainers())
    sources.extend(get_finviz_smallcap_gainers())
    sources.extend(get_marketwatch_gainers())

    merged = merge_leader_sources(sources)
    print(f"[LEADERS] multi-source merged {len(merged)} candidates from {len(sources)} raw filtered names")
    return merged



def get_candidates():
    raw = get_multi_source_leaders()

    seen = {}
    for item in raw:
        ticker = str(item.get("ticker", "")).upper().strip()
        if not ticker or is_bad_ticker(ticker):
            continue

        gain = safe_float(item.get("gain"))
        if gain < DISCOVERY_MIN_GAIN:
            continue

        existing = seen.get(ticker)
        if not existing:
            seen[ticker] = item
            continue

        if gain > safe_float(existing.get("gain")):
            existing.update(item)

        sources = list(dict.fromkeys(
            (existing.get("sources") or []) + (item.get("sources") or [item.get("source", "unknown")])
        ))
        existing["sources"] = sources
        existing["source"] = "+".join(sources)

    candidates = list(seen.values())
    candidates.sort(
        key=lambda x: (
            safe_float(x.get("gain")) >= 50,
            safe_float(x.get("gain")) >= 27,
            safe_float(x.get("gain")),
            len(x.get("sources", [])),
            safe_int(x.get("volume")),
        ),
        reverse=True,
    )

    print(f"[CANDIDATES] merged {len(candidates)} leader candidates from {len(raw)} multi-source names")
    return candidates[:MAX_GAINERS]


# ============================================================
# LIVE QUOTE / PROFILE
# ============================================================

def normalize_quote(price=0, gain=0, volume=0, source="none", stale=False, reason=""):
    return {
        "price": safe_float(price),
        "gain": safe_float(gain),
        "volume": safe_int(volume),
        "source": source,
        "stale": bool(stale),
        "reason": reason,
    }


def quote_is_valid(quote, ticker=""):
    """Reject bad API prints before they can break scoring."""
    if not isinstance(quote, dict):
        return False

    price = safe_float(quote.get("price"))
    gain = safe_float(quote.get("gain"))

    if price <= 0:
        return False
    if price < 0.01 or price > 10000:
        return False
    # API glitch protection. True runners can be wild, but +/-900% is usually bad data.
    if gain <= -95 or gain >= 900:
        return False
    return True


def remember_good_quote(ticker, quote):
    if quote_is_valid(quote, ticker):
        LAST_GOOD_QUOTES[ticker] = (time.time(), quote)
    return quote


def get_last_good_quote(ticker, max_age=600):
    item = LAST_GOOD_QUOTES.get(ticker)
    if not item:
        return None
    ts, quote = item
    age = time.time() - ts
    if age > max_age:
        return None
    q = dict(quote)
    q["stale"] = True
    q["source"] = f"last-good/{quote.get('source', 'unknown')}"
    q["reason"] = f"all quote sources failed; using {int(age)}s old quote"
    return q


def get_finnhub_quote_raw(ticker):
    if not FINNHUB_API_KEY:
        return normalize_quote(source="Finnhub", reason="missing FINNHUB_API_KEY")

    try:
        url = "https://finnhub.io/api/v1/quote"
        r = http_get(url, params={"symbol": ticker, "token": FINNHUB_API_KEY}, timeout=4)
        data = safe_json_response(r, f"QUOTE Finnhub {ticker}")
        if not data:
            return normalize_quote(source="Finnhub", reason="empty/non-json response")

        price = safe_float(data.get("c"))
        prev_close = safe_float(data.get("pc"))
        gain = ((price - prev_close) / prev_close) * 100 if price > 0 and prev_close > 0 else 0.0
        return normalize_quote(price=price, gain=gain, volume=0, source="Finnhub")
    except Exception as e:
        print(f"[QUOTE ERROR] {ticker}: Finnhub {e}")
        return normalize_quote(source="Finnhub", reason=str(e))


def get_yahoo_quote_raw(ticker):
    try:
        url = "https://query1.finance.yahoo.com/v7/finance/quote"
        r = http_get(url, params={"symbols": ticker, "formatted": "false"}, timeout=5)
        data = safe_json_response(r, f"QUOTE Yahoo {ticker}")
        if not data:
            return normalize_quote(source="Yahoo", reason="empty/non-json response")

        rows = data.get("quoteResponse", {}).get("result", [])
        if not rows:
            return normalize_quote(source="Yahoo", reason="symbol not returned")

        q = rows[0]
        price = (
            q.get("regularMarketPrice")
            or q.get("postMarketPrice")
            or q.get("preMarketPrice")
        )
        gain = (
            q.get("regularMarketChangePercent")
            or q.get("postMarketChangePercent")
            or q.get("preMarketChangePercent")
            or 0
        )
        volume = (
            q.get("regularMarketVolume")
            or q.get("postMarketVolume")
            or q.get("preMarketVolume")
            or 0
        )
        return normalize_quote(price=price, gain=gain, volume=volume, source="Yahoo")
    except Exception as e:
        print(f"[QUOTE ERROR] {ticker}: Yahoo {e}")
        return normalize_quote(source="Yahoo", reason=str(e))


def get_twelvedata_quote_raw(ticker):
    if not TWELVEDATA_API_KEY:
        return normalize_quote(source="TwelveData", reason="missing TWELVEDATA_API_KEY")

    try:
        url = "https://api.twelvedata.com/quote"
        r = http_get(url, params={"symbol": ticker, "apikey": TWELVEDATA_API_KEY}, timeout=5)
        data = safe_json_response(r, f"QUOTE TwelveData {ticker}")
        if not data or data.get("status") == "error":
            return normalize_quote(source="TwelveData", reason=clean_text(data.get("message", "error")) if isinstance(data, dict) else "empty response")

        price = data.get("close") or data.get("price")
        percent_change = data.get("percent_change") or data.get("percentChange") or 0
        volume = data.get("volume") or 0
        return normalize_quote(price=price, gain=percent_change, volume=volume, source="TwelveData")
    except Exception as e:
        print(f"[QUOTE ERROR] {ticker}: TwelveData {e}")
        return normalize_quote(source="TwelveData", reason=str(e))


def get_live_quote(ticker):
    """
    v34.1 quote stack:
    Finnhub primary -> Yahoo fallback -> TwelveData optional fallback -> last-good quote.
    This prevents price=0 / gain=0 API failures from collapsing the scan.
    """
    cached = cached_get(QUOTE_CACHE, ticker, ttl=SHORT_CACHE_TTL_SECONDS)
    if cached:
        return cached

    sources = [
        ("Finnhub", get_finnhub_quote_raw),
        ("Yahoo", get_yahoo_quote_raw),
        ("TwelveData", get_twelvedata_quote_raw),
    ]

    errors = []
    for label, fn in sources:
        quote = fn(ticker)
        if quote_is_valid(quote, ticker):
            if label != "Finnhub":
                print(f"[QUOTE FALLBACK] {ticker}: using {label}")
            print(f"[LIVE] {ticker} {fmt_money(quote.get('price'))} {safe_float(quote.get('gain')):.1f}% src={quote.get('source')}")
            remember_good_quote(ticker, quote)
            return cached_set(QUOTE_CACHE, ticker, quote)
        errors.append(f"{label}:{quote.get('reason', 'invalid quote')}")
        print(f"[QUOTE BAD] {ticker}: {label} invalid ({quote.get('reason', 'bad data')})")

    last_good = get_last_good_quote(ticker)
    if last_good and quote_is_valid(last_good, ticker):
        print(f"[QUOTE FALLBACK] {ticker}: using stale last-good quote ({last_good.get('reason')})")
        return cached_set(QUOTE_CACHE, ticker, last_good)

    print(f"[QUOTE FAIL] {ticker}: " + " | ".join(errors))
    return normalize_quote(source="none", reason="all quote sources failed")


# Backward-compatible name so older code paths still work.
def get_finnhub_quote(ticker):
    return get_live_quote(ticker)

def get_profile(ticker):
    cached = cached_get(PROFILE_CACHE, ticker)
    if cached:
        return cached

    profile = {
        "float": 0,
        "market_cap": 0,
        "shares_outstanding": 0,
        "source": "none",
    }

    if not FINNHUB_API_KEY:
        return profile

    try:
        url = "https://finnhub.io/api/v1/stock/profile2"
        r = http_get(url, params={"symbol": ticker, "token": FINNHUB_API_KEY}, timeout=4)
        data = r.json()

        market_cap = safe_float(data.get("marketCapitalization")) * 1_000_000
        shares_out = safe_float(data.get("shareOutstanding")) * 1_000_000

        profile = {
            "float": safe_int(shares_out),  # Finnhub usually gives shares out, used as float proxy if true float unavailable.
            "market_cap": safe_int(market_cap),
            "shares_outstanding": safe_int(shares_out),
            "source": "Finnhub",
        }

        print(f"[PROFILE] {ticker}: float~{fmt_big_num(profile['float'])} cap={fmt_big_num(profile['market_cap'])}")
        return cached_set(PROFILE_CACHE, ticker, profile)

    except Exception as e:
        print(f"[PROFILE ERROR] {ticker}: {e}")
        return profile


# ============================================================
# CANDLES
# ============================================================

def normalize_candle(o, h, l, c, v, ts=None):
    return {
        "time": ts,
        "open": safe_float(o),
        "high": safe_float(h),
        "low": safe_float(l),
        "close": safe_float(c),
        "volume": safe_int(v),
    }


def valid_candle_list(candles, min_count=3):
    if not candles or not isinstance(candles, list):
        return False
    good = 0
    for c in candles:
        if isinstance(c, dict) and candle_close(c) > 0 and candle_high(c) > 0 and candle_low(c) > 0:
            good += 1
    return good >= min_count


def remember_good_candles(ticker, candles, source="unknown"):
    if valid_candle_list(candles, min_count=5):
        LAST_GOOD_CANDLES[ticker] = {
            "ts": time.time(),
            "candles": candles,
            "source": source,
        }
    return candles


def get_last_good_candles(ticker, max_age_seconds=900):
    item = LAST_GOOD_CANDLES.get(ticker)
    if not item:
        return []
    age = time.time() - item.get("ts", 0)
    candles = item.get("candles") or []
    if age <= max_age_seconds and valid_candle_list(candles, min_count=5):
        print(f"[CANDLES FALLBACK] {ticker}: using last good {len(candles)} candles from {item.get('source', 'cache')} age={int(age)}s")
        return candles
    LAST_GOOD_CANDLES.pop(ticker, None)
    return []


def get_alpaca_candles(ticker):
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        return []

    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=9)

        url = f"{ALPACA_BASE_URL}/v2/stocks/{quote_plus(ticker)}/bars"
        params = {
            "timeframe": "1Min",
            "start": start.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "end": end.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "limit": 500,
            "adjustment": "raw",
            "feed": "iex",
        }
        headers = {
            "APCA-API-KEY-ID": ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        }

        r = http_get(url, params=params, headers=headers, timeout=6)
        data = safe_json_response(r, f"CANDLES Alpaca {ticker}")
        if not isinstance(data, dict):
            return []

        bars = data.get("bars") or []
        if not isinstance(bars, list):
            print(f"[ALPACA ERROR] {ticker}: bars not list")
            return []

        candles = []
        for b in bars:
            if not isinstance(b, dict):
                continue
            c = normalize_candle(b.get("o"), b.get("h"), b.get("l"), b.get("c"), b.get("v"), b.get("t"))
            if candle_close(c) > 0 and candle_high(c) > 0 and candle_low(c) > 0:
                candles.append(c)

        if candles:
            print(f"[CANDLES] {ticker}: Alpaca {len(candles)}")
            remember_good_candles(ticker, candles, "Alpaca")
        return candles

    except Exception as e:
        print(f"[ALPACA ERROR] {ticker}: {e}")
        return []


def get_yahoo_candles(ticker):
    global YAHOO_CANDLE_BLOCK_UNTIL, YAHOO_CANDLE_429_COUNT

    now = time.time()
    if now < YAHOO_CANDLE_BLOCK_UNTIL:
        wait_left = int(YAHOO_CANDLE_BLOCK_UNTIL - now)
        print(f"[YAHOO CANDLES BLOCKED] {ticker}: cooling down {wait_left}s after rate limit")
        return []

    try:
        # Tiny throttle keeps Render from hammering Yahoo across many leaders in one cycle.
        time.sleep(0.20)

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote_plus(ticker)}"
        params = {"interval": "1m", "range": "1d", "includePrePost": "true"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Referer": f"https://finance.yahoo.com/quote/{quote_plus(ticker)}/chart",
        }
        r = http_get(url, params=params, headers=headers, timeout=6)

        text = getattr(r, "text", "") or ""
        if getattr(r, "status_code", 0) == 429 or "Too Many Requests" in text:
            YAHOO_CANDLE_429_COUNT += 1
            cooldown = min(300, 45 * YAHOO_CANDLE_429_COUNT)
            YAHOO_CANDLE_BLOCK_UNTIL = time.time() + cooldown
            print(f"[YAHOO CANDLES 429] {ticker}: blocking Yahoo candle calls for {cooldown}s")
            return []

        data = safe_json_response(r, f"CANDLES Yahoo {ticker}")
        if not isinstance(data, dict):
            return []

        chart = data.get("chart") or {}
        error = chart.get("error")
        if error:
            print(f"[YAHOO CANDLES ERROR] {ticker}: {error}")
            return []

        result = chart.get("result") or []
        if not result:
            return []

        node = result[0] or {}
        timestamps = node.get("timestamp") or []
        indicators = node.get("indicators") or {}
        quote_list = indicators.get("quote") or []
        quote = quote_list[0] if quote_list and isinstance(quote_list[0], dict) else {}

        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []

        candles = []
        max_len = min(len(timestamps), len(opens), len(highs), len(lows), len(closes))
        for i in range(max_len):
            try:
                o, h, l, c = opens[i], highs[i], lows[i], closes[i]
                v = volumes[i] if i < len(volumes) else 0
                if None in [o, h, l, c]:
                    continue
                candle = normalize_candle(o, h, l, c, v, timestamps[i])
                if candle_close(candle) > 0 and candle_high(candle) > 0 and candle_low(candle) > 0:
                    candles.append(candle)
            except Exception:
                continue

        if candles:
            YAHOO_CANDLE_429_COUNT = 0
            print(f"[CANDLES] {ticker}: Yahoo {len(candles)}")
            remember_good_candles(ticker, candles, "Yahoo")
        return candles

    except Exception as e:
        print(f"[CANDLE ERROR] {ticker}: {e}")
        return []


def get_candles(ticker):
    cached = cached_get(CANDLE_CACHE, ticker, ttl=SHORT_CACHE_TTL_SECONDS)
    if cached:
        return cached

    candles = get_alpaca_candles(ticker)
    if not valid_candle_list(candles, min_count=5):
        print(f"[DATA FALLBACK] {ticker}: Alpaca failed/weak — using Yahoo")
        candles = get_yahoo_candles(ticker)

    if not valid_candle_list(candles, min_count=5):
        candles = get_last_good_candles(ticker)

    if candles:
        cached_set(CANDLE_CACHE, ticker, candles)
    return candles or []


# ============================================================
# INTERNAL STRUCTURE HELPERS
# ============================================================

def candle_close(c):
    return safe_float(c.get("close"))


def candle_high(c):
    return safe_float(c.get("high"))


def candle_low(c):
    return safe_float(c.get("low"))


def candle_volume(c):
    return safe_int(c.get("volume"))


def calc_vwap(candles):
    total_pv = 0.0
    total_v = 0.0
    for c in candles:
        h, l, close, vol = candle_high(c), candle_low(c), candle_close(c), candle_volume(c)
        if vol <= 0:
            continue
        typical = (h + l + close) / 3
        total_pv += typical * vol
        total_v += vol
    if total_v <= 0:
        return 0.0
    return total_pv / total_v


def finalized_candles(candles, min_keep=8):
    """
    V32.4 stability fix.
    Alpaca/Yahoo 1-minute data includes the currently forming candle. That
    unfinished candle can flip wick/VWAP/breakout/decay between scans and caused
    scores like STR4 -> STR1 seconds apart. All structure-style calculations use
    finalized candles only.
    """
    if not candles:
        return []
    cleaned = [c for c in candles if candle_close(c) > 0 and candle_high(c) > 0 and candle_low(c) > 0]
    if len(cleaned) > min_keep:
        return cleaned[:-1]
    return cleaned


def fallback_structure_analysis(candles):
    candles = finalized_candles(candles)
    if not candles or len(candles) < 8:
        return {
            "above_vwap": False,
            "higher_lows": False,
            "breakout": False,
            "near_high": False,
            "bad_structure": True,
            "big_upper_wick": False,
            "momentum_decay": False,
            "coil": False,
        }

    recent = candles[-8:]
    last = candles[-1]
    price = candle_close(last)
    vwap = calc_vwap(candles)
    day_high = max(candle_high(c) for c in candles)
    recent_high = max(candle_high(c) for c in recent)
    recent_low = min(candle_low(c) for c in recent)
    prior_high = max(candle_high(c) for c in candles[-20:-8]) if len(candles) >= 20 else recent_high

    lows = [candle_low(c) for c in recent[-5:]]
    higher_lows = len(lows) >= 3 and lows[-1] >= min(lows[:-1]) and lows[-1] > lows[0] * 0.98

    above_vwap = vwap > 0 and price >= vwap
    breakout = price >= prior_high * 1.005 if prior_high else False
    near_high = day_high > 0 and price >= day_high * 0.96

    body = abs(candle_close(last) - safe_float(last.get("open")))
    upper_wick = candle_high(last) - max(candle_close(last), safe_float(last.get("open")))
    candle_range = max(0.0001, candle_high(last) - candle_low(last))
    big_upper_wick = upper_wick / candle_range > 0.45

    recent_vol = sum(candle_volume(c) for c in candles[-3:])
    prev_vol = sum(candle_volume(c) for c in candles[-6:-3])
    momentum_decay = prev_vol > 0 and recent_vol < prev_vol * 0.60

    range_now = max(candle_high(c) for c in candles[-5:]) - min(candle_low(c) for c in candles[-5:])
    range_prev = max(candle_high(c) for c in candles[-10:-5]) - min(candle_low(c) for c in candles[-10:-5])
    tightening = range_prev > 0 and range_now < range_prev * 0.75

    bad_structure = (not above_vwap and not breakout) or big_upper_wick

    return {
        "above_vwap": above_vwap,
        "higher_lows": higher_lows,
        "breakout": breakout,
        "breakout_level": prior_high,
        "near_high": near_high,
        "bad_structure": bad_structure,
        "big_upper_wick": big_upper_wick,
        "momentum_decay": momentum_decay,
        "coil": tightening and above_vwap and higher_lows,
        "tightening_range": tightening,
        "recent_volume": recent_vol,
        "previous_volume": prev_vol,
        "vwap": vwap,
        "day_high": day_high,
        "recent_high": recent_high,
        "recent_low": recent_low,
    }


def merge_structure(external, fallback):
    """
    Stable fallback wins for core live-trading booleans. External structure_engine
    can add extra context, but it should not flip VWAP/coil/decay states from one
    scan to the next because of a forming candle or different parser assumptions.
    """
    if not isinstance(external, dict):
        return fallback

    merged = dict(external)
    stable_core_keys = {
        "above_vwap", "higher_lows", "breakout", "breakout_level",
        "near_high", "bad_structure", "big_upper_wick", "momentum_decay",
        "coil", "tightening_range", "recent_volume", "previous_volume",
        "vwap", "day_high", "recent_high", "recent_low",
    }
    for k, v in fallback.items():
        if k in stable_core_keys or merged.get(k) is None:
            merged[k] = v
    return merged


def get_structure(candles, ticker):
    candles = finalized_candles(candles)
    fallback = fallback_structure_analysis(candles)
    external = {}

    # V32.2 fix:
    # Older structure_engine versions used analyze_structure(ticker, candles).
    # Some newer drafts used analyze_structure(candles).
    # Try the production signature first so Render does not spam:
    # [STRUCTURE ERROR] analyze_structure() missing 1 required positional argument: 'candles'
    try:
        external = analyze_structure(ticker, candles)
    except TypeError:
        try:
            external = analyze_structure(candles)
        except Exception as e:
            print(f"[STRUCTURE ERROR] {ticker}: {e}")
            external = {}
    except Exception as e:
        print(f"[STRUCTURE ERROR] {ticker}: {e}")
        external = {}

    return merge_structure(external, fallback)


# ============================================================
# NEWS ENGINE
# ============================================================

JUNK_HEADLINE_PHRASES = [
    # Aggregator / mover-roundup junk
    "stocks moving", "stock is moving", "why shares", "why is",
    "top gainers", "market movers", "most active", "gap-ups and gap-downs",
    "driving market activity", "shares are trading higher",
    "benzinga examines", "what's going on", "today's session",
    "why it matters", "stocks to watch", "midday movers", "pre-market movers",

    # Law-firm / investigation junk
    "deadline", "law firm", "investigation", "shareholder alert",
    "class action", "reminds investors", "notice to investors",
    "johnson fistel", "levi & korsinsky", "pomerantz", "rosen law",

    # v33.15: quote-card / webpage boilerplate junk seen in live logs
    "get top stock picks", "trading disclosure", "coinbase",
    "benchmark is", "s&p 500", "^gspc", "ytd", "1-year", "3-year",
    "all news earnings", "earnings calls press releases sec filings",
    "items per page", "25 per page", "50 per page", "75 per page", "100 per page",
    "as of ", "trade ", "return ",
]

STALE_NEWS_MARKERS = [
    "mo ago", "yr ago", "year ago", "years ago",
    "sep ", "sept ", "oct ", "nov ", "dec 2025", "2025", "2024", "2023",
]

QUOTE_CARD_RE = re.compile(
    r"\b[A-Z]{1,5}\b.*?[-+]?\d+(?:\.\d+)?%.*?(as of|ytd|1-year|3-year|get top stock picks)",
    re.IGNORECASE,
)

NEGATIVE_HEADLINE_PHRASES = [
    "public offering", "registered direct", "private placement",
    "prices offering", "announces pricing", "atm offering",
    "reverse split", "delisting", "bankruptcy", "going concern",
]

STRONG_NEWS_PATTERNS = {
    "FDA / Clinical": [
        "fda approval", "fda clearance", "approved by the fda", "fast track",
        "breakthrough therapy", "phase 2", "phase ii", "phase 3", "phase iii",
        "clinical data", "positive topline", "meets primary endpoint",
    ],
    "Contract / Order": [
        "contract", "purchase order", "supply agreement", "distribution agreement", "licensing agreement", "government contract",
        "multi-year agreement", "master services agreement", "award",
    ],
    "AI / Nvidia": [
        "nvidia", "artificial intelligence", " ai ", "gpu", "data center",
        "hyperscale", "semiconductor", "machine learning",
    ],
    "M&A": [
        "acquisition", "merger", "buyout", "takeover", "to be acquired",
        "strategic transaction",
    ],
    "Financial Beat": [
        "record revenue", "revenue growth", "raises guidance", "earnings beat",
        "profitability", "q1 results", "q2 results", "q3 results", "q4 results",
    ],
    "Partnership": [
        "partnership", "collaboration", "mou", "memorandum of understanding",
        "strategic alliance", "joint venture", "letter of intent",
    ],
    "Infrastructure / Facility": [
        "facility", "battery", "manufacturing", "buildout", "production capacity",
    ],
}

WEAK_NEWS_PATTERNS = {
    "Compliance": ["regains compliance", "nasdaq compliance", "bid price compliance"],
    "Presentation": ["conference", "presentation", "webcast", "fireside chat"],
    "Generic Update": ["corporate update", "business update", "announces update"],
    "Product": ["launches", "unveils", "introduces"],
}

# Some tickers are normal English words. Do not treat lowercase prose like
# "ramp up revenue" or "fly higher" as ticker-specific news.
COMMON_WORD_TICKERS = {
    "RAMP", "FLY", "OPEN", "ROOT", "REAL", "PLAY", "LOVE", "LIFE",
    "EYES", "BODY", "TREE", "TRUE", "HUGE", "VERY", "NICE", "GOOD",
}


def is_junk_news_text(headline, ticker=None):
    """
    v33.15 news-only cleanup.
    Blocks quote cards, stale snippets, law-firm pages, ETF/page boilerplate,
    and scraped webpage fragments before they can become catalysts.
    """
    raw = clean_text(headline)
    if not raw:
        return True

    h = f" {raw.lower()} "

    if any(p in h for p in JUNK_HEADLINE_PHRASES):
        return True

    # Old news should not be treated as a fresh catalyst for a live scanner.
    if any(p in h for p in STALE_NEWS_MARKERS):
        return True

    # Yahoo/PR pages often expose quote widgets that look like headlines.
    if QUOTE_CARD_RE.search(raw):
        return True

    # Reject obvious performance-stat snippets.
    if re.search(r"\b(YTD|1-Year|3-Year|S&P 500|\^GSPC)\b", raw, flags=re.IGNORECASE):
        return True

    # Reject sentences that are mostly navigation/boilerplate.
    boiler_count = sum(1 for p in ["news", "earnings", "press releases", "sec filings", "quote", "chart", "watchlist"] if p in h)
    if boiler_count >= 3:
        return True

    # False positive seen live: medical plural GCTs treated as ticker GCTS.
    if ticker and ticker.upper() == "GCTS" and "gcts können" in h:
        return True

    return False


def strict_ticker_in_text(ticker, text):
    if not ticker or not text:
        return False

    t = ticker.upper().strip()
    raw = str(text)

    # Strong ticker formats first.
    strong_patterns = [
        rf"\${re.escape(t)}\b",
        rf"\({re.escape(t)}\)",
        rf"\bNASDAQ[:/ ]+{re.escape(t)}\b",
        rf"\bNYSE[:/ ]+{re.escape(t)}\b",
        rf"\bAMEX[:/ ]+{re.escape(t)}\b",
        rf"\bNYSEAMERICAN[:/ ]+{re.escape(t)}\b",
    ]
    if any(re.search(p, raw) for p in strong_patterns):
        return True

    # For common-word tickers, require exact uppercase symbol in the original text.
    if t in COMMON_WORD_TICKERS:
        return re.search(rf"\b{re.escape(t)}\b", raw) is not None

    # For normal tickers, case-insensitive word match is acceptable.
    return re.search(rf"\b{re.escape(t)}\b", raw, flags=re.IGNORECASE) is not None


def is_stale_news_text(headline):
    """Block old/stale headlines from being scored as live catalysts."""
    raw = clean_text(headline)
    if not raw:
        return False
    h = f" {raw.lower()} "

    stale_patterns = [
        r"\b2024\b", r"\b2023\b", r"\b2022\b",
        r"\bjan(?:uary)?\s+\d{1,2},\s*2025\b",
        r"\bfeb(?:ruary)?\s+\d{1,2},\s*2025\b",
        r"\bmar(?:ch)?\s+\d{1,2},\s*2025\b",
        r"\bapr(?:il)?\s+\d{1,2},\s*2025\b",
        r"\bmay\s+\d{1,2},\s*2025\b",
        r"\bjun(?:e)?\s+\d{1,2},\s*2025\b",
        r"\bjul(?:y)?\s+\d{1,2},\s*2025\b",
        r"\baug(?:ust)?\s+\d{1,2},\s*2025\b",
        r"\bsep(?:t|tember)?\s+\d{1,2},\s*2025\b",
        r"\boct(?:ober)?\s+\d{1,2},\s*2025\b",
        r"\bnov(?:ember)?\s+\d{1,2},\s*2025\b",
        r"\bdec(?:ember)?\s+\d{1,2},\s*2025\b",
        r"\b\d+\s+(?:month|months|mo|mos|year|years|yr|yrs)\s+ago\b",
    ]
    return any(re.search(pat, h, flags=re.IGNORECASE) for pat in stale_patterns)


def classify_news(headline, ticker=None):
    h_raw = clean_text(headline)
    h = f" {h_raw.lower()} "

    if not h_raw:
        return {
            "score": 0,
            "quality": "NONE",
            "category": "No News",
            "label": "❌ NO CONFIRMED NEWS",
            "explain": "No fresh catalyst found",
            "headline": "",
        }

    if is_stale_news_text(h_raw):
        return {
            "score": 1,
            "quality": "STALE",
            "category": "Stale / Old News",
            "label": "🕒 STALE NEWS",
            "explain": "Old headline/date detected — not a live catalyst",
            "headline": h_raw,
        }

    if is_junk_news_text(h_raw, ticker):
        return {
            "score": 0,
            "quality": "JUNK",
            "category": "Junk / Webpage Snippet",
            "label": "🚫 JUNK NEWS",
            "explain": "Webpage/quote-card/old-news snippet — not a real catalyst",
            "headline": h_raw,
        }

    if any(p in h for p in NEGATIVE_HEADLINE_PHRASES):
        return {
            "score": 1,
            "quality": "NEGATIVE",
            "category": "Offering / Negative",
            "label": "🚨 NEGATIVE / OFFERING NEWS",
            "explain": "Offering or negative financing language detected",
            "headline": h_raw,
        }

    if any(p in h for p in JUNK_HEADLINE_PHRASES):
        return {
            "score": 2,
            "quality": "JUNK",
            "category": "Junk / Aggregator",
            "label": "🚫 JUNK NEWS",
            "explain": "Aggregator/law-firm/mover headline — not a real catalyst",
            "headline": h_raw,
        }

    for category, words in STRONG_NEWS_PATTERNS.items():
        if any(f" {w} " in h or w in h for w in words):
            score = 9 if category in ["FDA / Clinical", "Contract / Order", "M&A", "AI / Nvidia"] else 8
            return {
                "score": score,
                "quality": "STRONG",
                "category": category,
                "label": "⚡ STRONG NEWS",
                "explain": f"Real catalyst: {category}",
                "headline": h_raw,
            }

    for category, words in WEAK_NEWS_PATTERNS.items():
        if any(w in h for w in words):
            return {
                "score": 5,
                "quality": "WEAK",
                "category": category,
                "label": "⚠️ WEAK / UNCLEAR NEWS",
                "explain": f"Weaker catalyst: {category}",
                "headline": h_raw,
            }

    return {
        "score": 4,
        "quality": "UNCLEAR",
        "category": "Unclear",
        "label": "📰 NEWS FOUND",
        "explain": "Headline found but catalyst strength unclear",
        "headline": h_raw,
    }


def extract_headlines_from_soup(soup, ticker):
    text = clean_text(soup.get_text(" "))
    chunks = re.split(r"(?<=[.!?])\s+", text)
    headlines = []

    for chunk in chunks:
        chunk = clean_text(chunk)
        if not (25 <= len(chunk) <= 240):
            continue

        # V32.2 fix: do NOT accept random strong-keyword chunks unless the ticker
        # is actually present. This prevents false catalysts like RAMP receiving
        # unrelated GCTS / Dust / Denarius headlines scraped from generic pages.
        if ticker and not strict_ticker_in_text(ticker, chunk):
            continue

        # v33.15: filter quote cards, stale snippets, law-firm pages, and boilerplate before ranking.
        if is_junk_news_text(chunk, ticker):
            continue

        headlines.append(chunk)

    return dedupe(headlines)[:10]


def scrape_yahoo_news(ticker):
    try:
        url = f"https://finance.yahoo.com/quote/{quote_plus(ticker)}/news"
        r = http_get(url, timeout=5)
        soup = BeautifulSoup(r.text, "html.parser")
        headlines = extract_headlines_from_soup(soup, ticker)
        if headlines:
            return headlines
    except Exception as e:
        print(f"[YAHOO NEWS ERROR] {ticker}: {e}")
    return []


def scrape_prnewswire(ticker):
    try:
        url = "https://www.prnewswire.com/search/news/"
        params = {"keyword": ticker}
        r = http_get(url, params=params, timeout=5)
        soup = BeautifulSoup(r.text, "html.parser")
        headlines = extract_headlines_from_soup(soup, ticker)
        if headlines:
            print(f"[PR SCRAPE] {ticker}: {headlines[0][:120]}")
            return headlines
    except Exception as e:
        print(f"[PR SCRAPE ERROR] {ticker}: {e}")
    return []


def scrape_globenewswire(ticker):
    try:
        url = f"https://www.globenewswire.com/search/keyword/{quote_plus(ticker)}"
        r = http_get(url, timeout=5)
        soup = BeautifulSoup(r.text, "html.parser")
        headlines = extract_headlines_from_soup(soup, ticker)
        if headlines:
            print(f"[GLOBE SCRAPE] {ticker}: {headlines[0][:120]}")
            return headlines
    except Exception as e:
        print(f"[GLOBE SCRAPE ERROR] {ticker}: {e}")
    return []


def normalize_headline_key(headline):
    h = clean_text(headline).lower()
    h = re.sub(r"[^a-z0-9 ]+", " ", h)
    h = re.sub(r"\b(nasdaq|nyse|amex|inc|ltd|corp|company|plc|llc)\b", " ", h)
    h = re.sub(r"\s+", " ", h).strip()
    return h[:160]


def dedupe_headlines_fast(headlines):
    out = []
    seen = set()
    for h in headlines or []:
        h = clean_text(h)
        if not h:
            continue
        key = normalize_headline_key(h)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(h)
    return out


def fetch_finnhub_company_news(ticker):
    """Fast API news source. Uses only last 3 calendar days so old stories do not outrank fresh catalysts."""
    if not FINNHUB_API_KEY:
        return []
    try:
        today = now_et().date()
        start = today - timedelta(days=3)
        url = "https://finnhub.io/api/v1/company-news"
        params = {
            "symbol": ticker,
            "from": start.isoformat(),
            "to": today.isoformat(),
            "token": FINNHUB_API_KEY,
        }
        r = http_get(url, params=params, timeout=3)
        data = safe_json_response(r, f"NEWS Finnhub {ticker}")
        if not isinstance(data, list):
            return []
        headlines = []
        for item in data[:12]:
            headline = clean_text(item.get("headline") or item.get("summary") or "")
            if headline and strict_ticker_in_text(ticker, headline) and not is_junk_news_text(headline, ticker):
                headlines.append(headline)
        if headlines:
            print(f"[NEWS FAST] Finnhub {ticker}: {headlines[0][:100]}")
        return dedupe_headlines_fast(headlines)[:8]
    except Exception as e:
        print(f"[NEWS ERROR] Finnhub {ticker}: {e}")
        return []


def fetch_benzinga_news(ticker):
    """Optional premium source. Only runs when BENZINGA_API_KEY is set."""
    if not BENZINGA_API_KEY:
        return []
    try:
        url = "https://api.benzinga.com/api/v2/news"
        params = {
            "token": BENZINGA_API_KEY,
            "tickers": ticker,
            "items": 10,
            "displayOutput": "full",
        }
        r = http_get(url, params=params, timeout=3)
        data = safe_json_response(r, f"NEWS Benzinga {ticker}")
        if not isinstance(data, list):
            return []
        headlines = []
        for item in data[:10]:
            headline = clean_text(item.get("title") or "")
            if headline and not is_junk_news_text(headline, ticker):
                headlines.append(headline)
        if headlines:
            print(f"[NEWS FAST] Benzinga {ticker}: {headlines[0][:100]}")
        return dedupe_headlines_fast(headlines)[:8]
    except Exception as e:
        print(f"[NEWS ERROR] Benzinga {ticker}: {e}")
        return []


def rank_news_candidates(headlines, ticker):
    ranked = []
    for h in dedupe_headlines_fast(headlines):
        c = classify_news(h, ticker)
        # Fresh, real catalysts should beat generic snippets. Stale/junk cannot win.
        if c.get("quality") in {"JUNK", "STALE"}:
            c["score"] = min(safe_float(c.get("score", 0)), 1)
        if c.get("quality") == "STRONG":
            c["score"] = min(10, safe_float(c.get("score", 0)) + 0.5)
        ranked.append(c)
    ranked.sort(key=lambda x: safe_float(x.get("score", 0)), reverse=True)
    return ranked


def get_best_news(ticker):
    cached = cached_get(NEWS_CACHE, ticker)
    if cached:
        return cached

    all_headlines = []

    # v34.6: API/PR sources run in parallel first. Yahoo is last-resort only
    # because it rate-limits and often returns stale quote-card junk.
    fast_sources = [fetch_finnhub_company_news, fetch_benzinga_news, scrape_prnewswire, scrape_globenewswire]
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fn, ticker): fn.__name__ for fn in fast_sources}
        for fut in as_completed(futures, timeout=7):
            name = futures[fut]
            try:
                all_headlines.extend(fut.result() or [])
            except Exception as e:
                print(f"[NEWS ERROR] {ticker} {name}: {e}")

    ranked = rank_news_candidates(all_headlines, ticker)
    if ranked and safe_float(ranked[0].get("score", 0)) >= 7:
        best = ranked[0]
        print(f"[NEWS] {ticker}: {best.get('headline','')[:120]} ({best['quality']} {best['score']}/10 FAST)")
        return cached_set(NEWS_CACHE, ticker, best)

    # Fallback only if fast sources did not find a real catalyst.
    yahoo_headlines = scrape_yahoo_news(ticker)
    if yahoo_headlines:
        all_headlines.extend(yahoo_headlines)
        ranked = rank_news_candidates(all_headlines, ticker)

    if not ranked:
        news = classify_news("", ticker)
        print(f"[NEWS] {ticker}: NO NEWS")
        return cached_set(NEWS_CACHE, ticker, news)

    best = ranked[0]
    print(f"[NEWS] {ticker}: {best.get('headline','')[:120]} ({best['quality']} {best['score']}/10)")
    return cached_set(NEWS_CACHE, ticker, best)


# ============================================================
# SEC / DILUTION ENGINE — v34.4 ADVANCED TIERS
# ============================================================

# Active dilution = can create near-term sell pressure into spikes.
ACTIVE_DILUTION_TERMS = [
    "registered direct", "public offering", "private placement", "best efforts offering",
    "securities purchase agreement", "priced an offering", "pricing of its offering",
    "announces pricing", "institutional investors", "pre-funded warrant",
    "common warrants", "placement agent warrants", "convertible note", "convertible notes",
]

ATM_TERMS = [
    "at-the-market", "atm offering", "equity distribution agreement", "sales agreement",
    "may sell shares", "from time to time", "sales agent", "offer and sell shares",
]

WARRANT_TERMS = [
    "warrant", "warrants", "exercise price", "exercisable", "pre-funded warrants",
    "common warrants", "placement agent warrants", "warrant shares",
]

SHELF_TERMS = [
    "shelf registration", "resale prospectus", "prospectus supplement", "may offer from time to time",
    "selling stockholders", "registration statement", "form s-3", "form f-3", "form s-1", "form f-1",
]

DILUTION_TERMS = list(dict.fromkeys(ACTIVE_DILUTION_TERMS + ATM_TERMS + WARRANT_TERMS + SHELF_TERMS))

ACTIVE_FORMS = ["424B5", "424B4", "FWP"]
SHELF_FORMS = ["S-1", "S-3", "F-1", "F-3", "424B3", "424B5", "POS AM"]
SEC_FORMS_TO_DETECT = ["424B3", "424B4", "424B5", "S-1", "S-3", "F-1", "F-3", "8-K", "6-K", "POS AM", "FWP"]


def sec_filing_dates_from_text(text):
    """Extract visible EDGAR dates from browse page text and return newest-first dates."""
    dates = []
    for m in re.finditer(r"(20\d{2}-\d{2}-\d{2})", text or ""):
        try:
            d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
            dates.append(d)
        except Exception:
            continue
    return sorted(set(dates), reverse=True)


def age_bucket_for_sec(days):
    if days is None:
        return "unknown age", 0.0
    if days <= 1:
        return "today/1d", 1.2
    if days <= 7:
        return "≤7d", 1.0
    if days <= 30:
        return "≤30d", 0.7
    if days <= 90:
        return "≤90d", 0.35
    if days <= 180:
        return "≤180d", 0.1
    return ">180d/stale", -0.25


def matched_terms(lower_text, terms):
    return [term for term in terms if term.lower() in lower_text]


def build_dilution_label(severity, category, forms, terms, age_bucket):
    form_txt = "/".join(forms[:3]) if forms else "filing"
    if severity == "HIGH":
        return f"🚨 ACTIVE DILUTION RISK — {category} ({form_txt}, {age_bucket})"
    if severity == "MEDIUM":
        return f"⚠️ SHELF/ATM RISK — {category} ({form_txt}, {age_bucket})"
    if severity == "LOW":
        return f"🟡 SEC AWARENESS — {category} ({form_txt}, {age_bucket})"
    return ""


def check_sec_filings(ticker):
    cached = cached_get(SEC_CACHE, ticker)
    if cached:
        return cached

    risk = {
        "has_risk": False,
        "severity": "NONE",
        "category": "Clean / no obvious dilution",
        "label": "",
        "forms": [],
        "terms": [],
        "filing_age_days": None,
        "freshness": "unknown age",
        "risk_score": 0.0,
        "atm_active": False,
        "warrant_overhang": False,
    }

    try:
        url = "https://www.sec.gov/cgi-bin/browse-edgar"
        params = {
            "action": "getcompany",
            "CIK": ticker,
            "owner": "exclude",
            "count": "40",
        }
        headers = {
            "User-Agent": "scannerbot robert@example.com",
            "Accept-Encoding": "gzip, deflate",
        }

        r = http_get(url, params=params, headers=headers, timeout=6)
        text = clean_text(r.text)
        lower = text.lower()

        filing_dates = sec_filing_dates_from_text(text)
        age_days = None
        if filing_dates:
            age_days = (now_et().date() - filing_dates[0]).days
        age_bucket, freshness_boost = age_bucket_for_sec(age_days)

        forms = []
        for form in SEC_FORMS_TO_DETECT:
            if form.lower() in lower:
                forms.append(form)
        forms = dedupe(forms)

        active_terms = matched_terms(lower, ACTIVE_DILUTION_TERMS)
        atm_terms = matched_terms(lower, ATM_TERMS)
        warrant_terms = matched_terms(lower, WARRANT_TERMS)
        shelf_terms = matched_terms(lower, SHELF_TERMS)
        all_terms = dedupe(active_terms + atm_terms + warrant_terms + shelf_terms)

        has_active_form = any(f in ACTIVE_FORMS for f in forms)
        has_shelf_form = any(f in SHELF_FORMS for f in forms)
        atm_active = bool(atm_terms)
        warrant_overhang = bool(warrant_terms and any(x in lower for x in ["exercise price", "exercisable", "warrant shares", "pre-funded"]))

        base_score = 0.0
        category = "Clean / no obvious dilution"
        severity = "NONE"

        if active_terms or has_active_form:
            category = "offering/financing language"
            severity = "HIGH"
            base_score = 3.0
        elif atm_active:
            category = "ATM/sales agreement available"
            severity = "HIGH" if age_days is not None and age_days <= 30 else "MEDIUM"
            base_score = 2.4
        elif warrant_overhang:
            category = "warrant overhang"
            severity = "MEDIUM"
            base_score = 2.0
        elif shelf_terms or has_shelf_form:
            category = "shelf/resale capacity"
            severity = "MEDIUM"
            base_score = 1.5
        elif forms:
            category = "recent SEC filings only"
            severity = "LOW"
            base_score = 0.6

        # Freshness matters. Old shelf filings become awareness, not panic.
        risk_score = max(0.0, base_score + freshness_boost)
        if age_days is not None and age_days > 180 and severity in ["HIGH", "MEDIUM"]:
            severity = "LOW"
            category = f"older {category}"
            risk_score = min(risk_score, 0.8)

        if severity != "NONE":
            risk.update({
                "has_risk": True,
                "severity": severity,
                "category": category,
                "label": build_dilution_label(severity, category, forms, all_terms, age_bucket),
                "forms": forms[:6],
                "terms": all_terms[:8],
                "filing_age_days": age_days,
                "freshness": age_bucket,
                "risk_score": round(risk_score, 2),
                "atm_active": atm_active,
                "warrant_overhang": warrant_overhang,
            })

        print(f"[SEC] {ticker}: {risk['label'] or 'clean/no obvious filing risk'}")
        return cached_set(SEC_CACHE, ticker, risk)

    except Exception as e:
        print(f"[SEC ERROR] {ticker}: {e}")
        return cached_set(SEC_CACHE, ticker, risk)


# ============================================================
# COIL / SECOND LEG / DECAY / HALT ENGINES
# ============================================================

def get_struct(structure, key, default=None):
    if not isinstance(structure, dict):
        return default
    return structure.get(key, default)


def detect_coil(candles, structure):
    candles = finalized_candles(candles)
    if not candles or len(candles) < 15:
        return {
            "detected": False,
            "score": 0,
            "label": "",
            "reason": "",
        }

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    near_high = bool(get_struct(structure, "near_high", False))

    recent = candles[-6:]
    prior = candles[-12:-6]

    recent_range = max(candle_high(c) for c in recent) - min(candle_low(c) for c in recent)
    prior_range = max(candle_high(c) for c in prior) - min(candle_low(c) for c in prior)

    recent_vol = sum(candle_volume(c) for c in recent)
    prior_vol = sum(candle_volume(c) for c in prior)

    tightening = prior_range > 0 and recent_range < prior_range * 0.80
    volume_contracting = prior_vol > 0 and recent_vol < prior_vol * 0.90
    volume_not_dead = recent_vol >= MIN_DEEP_VOLUME
    raw_coil = bool(get_struct(structure, "coil", False) or get_struct(structure, "tightening_range", False))

    score = 0
    reasons = []

    if above_vwap:
        score += 2
        reasons.append("above VWAP")
    if higher_lows:
        score += 2
        reasons.append("higher lows")
    if near_high:
        score += 1.5
        reasons.append("near highs")
    if tightening or raw_coil:
        score += 2
        reasons.append("tightening range")
    if volume_contracting:
        score += 1
        reasons.append("volume contraction")
    if volume_not_dead:
        score += 1
        reasons.append("still liquid")

    detected = score >= 6

    label = "🌀 TIGHT COIL" if detected else ""
    reason = " / ".join(reasons)

    return {
        "detected": detected,
        "score": clamp(score),
        "label": label,
        "reason": reason,
    }


def detect_second_leg(candles, structure, coil):
    above_vwap = bool(get_struct(structure, "above_vwap", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    breakout = bool(get_struct(structure, "breakout", False))
    near_high = bool(get_struct(structure, "near_high", False))

    recent_vol = safe_int(get_struct(structure, "recent_volume", 0))
    previous_vol = safe_int(get_struct(structure, "previous_volume", 0))
    volume_expanding = recent_vol > previous_vol and recent_vol >= MIN_DEEP_VOLUME

    detected = above_vwap and higher_lows and near_high and (breakout or coil["detected"] or volume_expanding)

    score = 0
    if above_vwap:
        score += 2
    if higher_lows:
        score += 2
    if near_high:
        score += 2
    if breakout:
        score += 2
    if volume_expanding:
        score += 2

    return {
        "detected": detected,
        "score": clamp(score),
        "label": "🔥 SECOND LEG" if detected else "",
        "volume_expanding": volume_expanding,
    }


def detect_momentum_decay(candles, structure):
    risks = []
    score_penalty = 0.0

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    bad_structure = bool(get_struct(structure, "bad_structure", False))
    big_upper_wick = bool(get_struct(structure, "big_upper_wick", False))
    raw_decay = bool(get_struct(structure, "momentum_decay", False))
    near_high = bool(get_struct(structure, "near_high", False))
    breakout = bool(get_struct(structure, "breakout", False))

    recent_vol = safe_int(get_struct(structure, "recent_volume", 0))
    previous_vol = safe_int(get_struct(structure, "previous_volume", 0))

    premarket = is_premarket_session()
    holding_continuation = above_vwap and (near_high or breakout)

    if not above_vwap:
        risks.append("Lost VWAP / reclaim needed")
        score_penalty += 1.2 if premarket else 1.5

    if previous_vol > 0 and recent_vol < previous_vol * 0.60:
        risks.append("Momentum decay / volume fading")
        # Premarket often has uneven 1-min volume; do not over-punish leaders still holding highs.
        if premarket and holding_continuation:
            score_penalty += 0.35
        elif premarket:
            score_penalty += 0.60
        else:
            score_penalty += 1.0

    if bad_structure:
        risks.append("Bad structure / failed momentum")
        score_penalty += 0.85 if (premarket and holding_continuation) else 1.5

    if big_upper_wick:
        risks.append("Big upper wick / possible trap")
        score_penalty += 0.50 if premarket else 1.0

    if raw_decay and "Momentum decay / volume fading" not in risks:
        risks.append("Momentum decay / wait for reclaim")
        score_penalty += 0.35 if premarket else 1.0

    # Do not mark a premarket leader as fully fading if it is still above VWAP and near highs.
    detected = bool(risks)
    if premarket and holding_continuation and score_penalty <= 0.85:
        detected = False

    return {
        "detected": detected,
        "penalty": score_penalty,
        "risks": risks,
    }

def detect_exhaustion(candles, structure, price):
    candles = finalized_candles(candles)
    if not candles:
        return {
            "detected": False,
            "risk": "",
            "penalty": 0,
        }

    vwap = safe_float(get_struct(structure, "vwap", calc_vwap(candles)))
    day_high = safe_float(get_struct(structure, "day_high", max(candle_high(c) for c in candles)))

    distance_from_vwap = ((price - vwap) / vwap * 100) if vwap else 0
    off_high = ((day_high - price) / day_high * 100) if day_high else 0

    recent = candles[-5:]
    upper_wick_count = 0
    for c in recent:
        open_ = safe_float(c.get("open"))
        close = candle_close(c)
        high = candle_high(c)
        low = candle_low(c)
        rng = max(0.0001, high - low)
        upper = high - max(open_, close)
        if upper / rng > 0.45:
            upper_wick_count += 1

    premarket = is_premarket_session()

    # Premarket leaders are often naturally extended. Treat extension as awareness unless
    # the chart is also rejecting/far off highs.
    if distance_from_vwap >= (28 if premarket else 18):
        return {
            "detected": True,
            "risk": "Very extended from VWAP / chase risk",
            "penalty": 0.65 if premarket else 1.0,
        }

    if distance_from_vwap >= (18 if premarket else 12) and upper_wick_count >= 2:
        return {
            "detected": True,
            "risk": "Extended + repeated upper wicks",
            "penalty": 0.85 if premarket else 1.25,
        }

    if off_high >= (18 if premarket else 12):
        return {
            "detected": True,
            "risk": "Fading far off highs",
            "penalty": 0.75 if premarket else 1.0,
        }

    return {
        "detected": False,
        "risk": "",
        "penalty": 0,
    }


def detect_halt_risk(price, gain, float_shares, candles):
    candles = finalized_candles(candles)
    risk = "LOW"
    reasons = []
    score = 0

    if gain >= 80:
        score += 2
        reasons.append("huge % gain")
    elif gain >= 50:
        score += 1

    if float_shares and float_shares <= 10_000_000:
        score += 2
        reasons.append("low float")
    elif float_shares and float_shares <= 20_000_000:
        score += 1

    if candles and len(candles) >= 3:
        last3 = candles[-3:]
        ranges = []
        for c in last3:
            low = candle_low(c)
            high = candle_high(c)
            if low > 0:
                ranges.append((high - low) / low * 100)
        if ranges and max(ranges) >= 10:
            score += 2
            reasons.append("violent candle range")
        elif ranges and max(ranges) >= 6:
            score += 1

    if score >= 5:
        risk = "HIGH"
    elif score >= 3:
        risk = "MEDIUM"

    return {
        "risk": risk,
        "score": score,
        "reasons": reasons,
        "label": f"⚠️ {risk} HALT RISK" if risk in ["MEDIUM", "HIGH"] else "",
    }


# ============================================================
# v34 PARTICIPATION / FAKEOUT / TIER ENGINE
# Adds relative-volume style participation without needing a paid avg-volume feed.
# ============================================================

def calc_participation_score_v34(candles, total_volume=0):
    candles = finalized_candles(candles)
    reasons = []
    if not candles or len(candles) < 10:
        return {"score": 0.0, "rvol_proxy": 0.0, "label": "", "reasons": []}

    recent = candles[-5:]
    baseline = candles[:-5]
    recent_avg = sum(candle_volume(c) for c in recent) / max(1, len(recent))
    base_avg = sum(candle_volume(c) for c in baseline) / max(1, len(baseline))
    rvol_proxy = recent_avg / base_avg if base_avg > 0 else 0.0

    score = 0.0
    label = ""

    if rvol_proxy >= 5.0:
        score += 2.0
        label = f"🔥 RVOL proxy {rvol_proxy:.1f}x"
        reasons.append(label)
    elif rvol_proxy >= 3.0:
        score += 1.5
        label = f"🔥 RVOL proxy {rvol_proxy:.1f}x"
        reasons.append(label)
    elif rvol_proxy >= 1.8:
        score += 1.0
        label = f"🟢 Volume expanding {rvol_proxy:.1f}x"
        reasons.append(label)
    elif rvol_proxy < 0.65 and len(candles) >= 20:
        score -= 0.75
        label = f"⚠️ Volume fading {rvol_proxy:.1f}x"
        reasons.append(label)

    total_volume = safe_int(total_volume)
    if total_volume >= 50_000_000:
        score += 0.6
    elif total_volume >= 10_000_000:
        score += 0.35

    return {
        "score": clamp(score, -1.0, 2.5),
        "rvol_proxy": rvol_proxy,
        "label": label,
        "reasons": dedupe(reasons),
    }


def detect_fakeout_v34(candles, structure):
    candles = finalized_candles(candles)
    if not candles or len(candles) < 12:
        return {"detected": False, "penalty": 0.0, "risk": "", "label": ""}

    breakout_level = safe_float(get_struct(structure, "breakout_level", 0))
    last = candles[-1]
    prev = candles[-2]
    price = candle_close(last)
    above_vwap = bool(get_struct(structure, "above_vwap", False))
    breakout = bool(get_struct(structure, "breakout", False))

    open_ = safe_float(last.get("open"))
    high = candle_high(last)
    low = candle_low(last)
    rng = max(0.0001, high - low)
    upper_wick = high - max(open_, price)
    red_close = price < open_

    failed_breakout = bool(breakout_level and price < breakout_level * 0.995 and high > breakout_level * 1.005)
    wick_rejection = upper_wick / rng >= 0.50 and red_close
    lost_vwap_after_breakout = breakout and not above_vwap
    lower_close = candle_close(last) < candle_close(prev) * 0.985

    if failed_breakout or (wick_rejection and lower_close) or lost_vwap_after_breakout:
        risk = "Fakeout/stuff candle risk — wait for reclaim/hold"
        return {"detected": True, "penalty": 1.25, "risk": risk, "label": "⚠️ FAKEOUT RISK"}

    return {"detected": False, "penalty": 0.0, "risk": "", "label": ""}


def build_phase_v34(structure, coil, second_leg, exhaustion, decay, fakeout=None, participation=None):
    fakeout = fakeout or {}
    participation = participation or {}

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    near_high = bool(get_struct(structure, "near_high", False))
    breakout = bool(get_struct(structure, "breakout", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    strong_participation = safe_float(participation.get("score", 0)) >= 0.75
    continuation = above_vwap and (near_high or breakout) and (higher_lows or strong_participation)

    if fakeout.get("detected"):
        return "⚠️ FAKEOUT / RECLAIM NEEDED"
    if second_leg and second_leg.get("detected"):
        return "🔥 SECOND LEG"
    if coil and coil.get("detected"):
        return "🌀 COIL / PRESSURE BUILDING"
    if continuation:
        return "🟢 RUNNER CONTINUATION"
    if exhaustion and exhaustion.get("detected"):
        return "⚠️ EXTENDED / RESET NEEDED"
    if decay and decay.get("detected"):
        return "⚠️ FADING"
    if breakout and participation.get("score", 0) > 0:
        return "🚀 BREAKOUT HOLD"
    if above_vwap and near_high:
        return "🟢 IGNITION / HOLDING"
    if above_vwap:
        return "🟢 ABOVE VWAP"
    return "👀 RECLAIM WATCH"

def build_trade_tier_v34(score, bias, phase, exhaustion=None, decay=None, fakeout=None, entry_score=0, structure_score=0):
    bias = bias or ""
    phase = phase or ""
    exhausted = bool(exhaustion.get("detected")) if isinstance(exhaustion, dict) else False
    fading = bool(decay.get("detected")) if isinstance(decay, dict) else False
    stuffed = bool(fakeout.get("detected")) if isinstance(fakeout, dict) else False
    score = safe_float(score)
    entry_score = safe_float(entry_score)
    structure_score = safe_float(structure_score)

    continuation_phase = "RUNNER CONTINUATION" in phase or "SECOND LEG" in phase or "COIL" in phase or "BREAKOUT HOLD" in phase

    if stuffed or "AVOID" in bias:
        return "⚠️ AVOID / WAIT"
    if fading and not continuation_phase and score < 7.0:
        return "⚠️ AVOID / WAIT"
    if score >= 7.5 and entry_score >= 4.5 and structure_score >= 4.0 and ("RUNNER" in bias or continuation_phase):
        return "🔥 TRADEABLE RUNNER"
    if score >= 6.7 and ("RUNNER" in bias or "WATCH" in bias or continuation_phase):
        return "🟢 RUNNER WATCH"
    if exhausted or "EXTENDED" in bias or "EXTENDED" in phase:
        return "👀 AWARENESS — EXTENDED"
    return "👀 MARKET WATCH"



# ============================================================
# LEADERSHIP ENGINE — v33.3
# Separates "who owns the day" from "is this a clean entry right now?"
# ============================================================


    gain = safe_float(gain)
    volume = safe_int(volume)
    float_shares = safe_int(float_shares)
    news_score = safe_float(news_score)
    sources = sources or []

    # Percent gain is king for this bot.
    if gain >= 200:
        score += 4.0
        reasons.append("200%+ day leader")
    elif gain >= 100:
        score += 3.4
        reasons.append("100%+ day leader")
    elif gain >= 75:
        score += 2.8
        reasons.append("75%+ day leader")
    elif gain >= 50:
        score += 2.3
        reasons.append("50%+ day leader")
    elif gain >= 27:
        score += 1.7
        reasons.append("27%+ momentum leader")

    # Liquidity/attention.
    if volume >= 200_000_000:
        score += 2.0
        reasons.append("200M+ volume")
    elif volume >= 100_000_000:
        score += 1.7
        reasons.append("100M+ volume")
    elif volume >= 50_000_000:
        score += 1.4
        reasons.append("50M+ volume")
    elif volume >= 20_000_000:
        score += 1.1
        reasons.append("20M+ volume")
    elif volume >= 5_000_000:
        score += 0.7
        reasons.append("5M+ volume")

    # Low float = leadership accelerator.
    if 0 < float_shares <= 5_000_000:
        score += 1.6
        reasons.append("tiny float")
    elif 0 < float_shares <= 10_000_000:
        score += 1.3
        reasons.append("elite low float")
    elif 0 < float_shares <= 20_000_000:
        score += 0.9
        reasons.append("low float")
    elif 0 < float_shares <= 40_000_000:
        score += 0.5
        reasons.append("decent float")

    # Catalyst helps, but no-news squeezes can still lead.
    if news_score >= 8:
        score += 1.0
        reasons.append("strong catalyst")
    elif news_score >= 5:
        score += 0.45
        reasons.append("some catalyst")

    # Multiple sources confirming the leader is valuable.
    source_count = len(sources)
    if source_count >= 3:
        score += 0.6
        reasons.append("confirmed by multiple sources")
    elif source_count == 2:
        score += 0.35
        reasons.append("confirmed by 2 sources")

    return clamp(score), dedupe(reasons)




def simple_leader_reasons(gain, float_info, volume, news_score):
    reasons = []
    gain = safe_float(gain)
    volume = safe_int(volume)
    news_score = safe_float(news_score)

    if gain >= 300:
        reasons.append("300%+ day leader")
    elif gain >= 200:
        reasons.append("200%+ day leader")
    elif gain >= 100:
        reasons.append("100%+ day leader")
    elif gain >= 75:
        reasons.append("75%+ day leader")
    elif gain >= 50:
        reasons.append("50%+ day leader")
    elif gain >= RUNNER_MIN_GAIN:
        reasons.append("27%+ momentum leader")

    if float_info and float_info.get("label"):
        reasons.append(float_info.get("label"))

    if volume >= 100_000_000:
        reasons.append("100M+ volume")
    elif volume >= 50_000_000:
        reasons.append("50M+ volume")
    elif volume >= 20_000_000:
        reasons.append("20M+ volume")
    elif volume >= 5_000_000:
        reasons.append("5M+ volume")

    if news_score >= 8:
        reasons.append("strong catalyst")
    elif news_score >= 5:
        reasons.append("some catalyst")

    return dedupe(reasons)



def simple_market_label(gain, float_shares, volume, score, entry_score, structure_score, exhaustion, decay):
    gain = safe_float(gain)
    float_shares = safe_int(float_shares)
    volume = safe_int(volume)
    score = safe_float(score)
    entry_score = safe_float(entry_score)
    structure_score = safe_float(structure_score)

    exhausted = bool(exhaustion.get("detected")) if isinstance(exhaustion, dict) else False
    fading = bool(decay.get("detected")) if isinstance(decay, dict) else False

    is_low_float = 0 < float_shares <= 20_000_000
    huge_volume = volume >= 20_000_000
    premarket = is_premarket_session()

    # In premarket, true leaders can be extended but still valid continuation watches.
    if gain >= 50 and (is_low_float or huge_volume):
        if score >= 7.0 or (premarket and entry_score >= 4.5 and structure_score >= 3.5):
            return "🟢 RUNNER"
        if exhausted or fading or entry_score < 3.5:
            return "🔥 MARKET LEADER — EXTENDED"
        return "🔥 MARKET LEADER"

    if gain >= RUNNER_MIN_GAIN:
        if score >= 7.0 or (premarket and entry_score >= 4.5 and structure_score >= 3.5):
            return "🟢 RUNNER"
        if exhausted or fading or entry_score < 3.5:
            return "🔥 MARKET LEADER — EXTENDED"
        if score >= 5:
            return "👀 WATCH"

    return "⚠️ AVOID"

def simple_leader_reasons(gain, float_info, volume, news_score):
    reasons = []
    gain = safe_float(gain)
    volume = safe_int(volume)
    news_score = safe_float(news_score)

    if gain >= 300:
        reasons.append("300%+ day leader")
    elif gain >= 200:
        reasons.append("200%+ day leader")
    elif gain >= 100:
        reasons.append("100%+ day leader")
    elif gain >= 75:
        reasons.append("75%+ day leader")
    elif gain >= 50:
        reasons.append("50%+ day leader")
    elif gain >= RUNNER_MIN_GAIN:
        reasons.append("27%+ momentum leader")

    if float_info and float_info.get("label"):
        reasons.append(float_info.get("label"))

    if volume >= 100_000_000:
        reasons.append("100M+ volume")
    elif volume >= 50_000_000:
        reasons.append("50M+ volume")
    elif volume >= 20_000_000:
        reasons.append("20M+ volume")
    elif volume >= 5_000_000:
        reasons.append("5M+ volume")

    if news_score >= 8:
        reasons.append("strong catalyst")
    elif news_score >= 5:
        reasons.append("some catalyst")

    return dedupe(reasons)


# ============================================================
# SCORING ENGINE
# ============================================================

def score_structure(structure):
    score = 0
    reasons = []
    risks = []

    reasons.extend(simple_leader_reasons(
        gain=gain,
        float_info=float_info,
        volume=volume,
        news_score=news_score,
    ))

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    breakout = bool(get_struct(structure, "breakout", False))
    near_high = bool(get_struct(structure, "near_high", False))
    bad_structure = bool(get_struct(structure, "bad_structure", False))
    big_upper_wick = bool(get_struct(structure, "big_upper_wick", False))
    raw_coil = bool(get_struct(structure, "coil", False) or get_struct(structure, "tightening_range", False))

    # V32.3 calibration: structure should not be nuked to 1-2 when a name is
    # liquid, coiling, and not clearly failing. The old version was too bearish.
    if above_vwap:
        score += 3.0
        reasons.append("Above VWAP")
    else:
        risks.append("Below VWAP / reclaim needed")

    if higher_lows:
        score += 2.0
        reasons.append("Higher lows")

    if breakout:
        score += 2.0
        reasons.append("Breakout / expansion attempt")

    if near_high:
        score += 1.5
        reasons.append("Holding near highs")

    if raw_coil:
        score += 1.0
        reasons.append("Tightening / coil structure")

    # Only punish bad_structure hard if price is also below VWAP. This prevents
    # contradictory "coil + strong entry" names from being scored as dead fades.
    if bad_structure and not above_vwap:
        score -= 2.0
        risks.append("Bad structure / failed momentum")
    elif bad_structure:
        score -= 0.75
        risks.append("Structure warning / needs confirmation")

    if big_upper_wick:
        score -= 0.75
        risks.append("Big upper wick / possible trap")

    return clamp(score), reasons, risks


def score_volume(volume, structure):
    score = 0
    reasons = []

    if volume >= 5_000_000:
        score += 5
        reasons.append("5M+ volume")
    elif volume >= 2_000_000:
        score += 4
        reasons.append("2M+ volume")
    elif volume >= 1_000_000:
        score += 3
        reasons.append("1M+ volume")
    elif volume >= 500_000:
        score += 2
        reasons.append("500K+ volume")
    elif volume >= 100_000:
        score += 1
        reasons.append("100K+ volume")

    recent_vol = safe_int(get_struct(structure, "recent_volume", 0))
    previous_vol = safe_int(get_struct(structure, "previous_volume", 0))

    if recent_vol and previous_vol and recent_vol > previous_vol:
        score += 3
        reasons.append("Volume expanding")

    if recent_vol >= 300_000:
        score += 1
        reasons.append("Strong recent candle volume")

    return clamp(score), reasons


def score_entry_quality(structure, coil, second_leg, exhaustion):
    score = 5.0
    reasons = []
    risks = []

    if bool(get_struct(structure, "above_vwap", False)):
        score += 1.5
        reasons.append("clean VWAP location")
    else:
        score -= 2
        risks.append("entry weak below VWAP")

    if coil["detected"]:
        score += 1
        reasons.append("coil gives tighter risk")

    if second_leg["detected"]:
        score += 1
        reasons.append("second-leg continuation")

    if bool(get_struct(structure, "near_high", False)):
        score += 0.5
        reasons.append("near high pressure")

    if exhaustion["detected"]:
        score -= exhaustion["penalty"]
        risks.append(exhaustion["risk"])

    return clamp(score), reasons, risks


def build_bias(score, structure_score, entry_score, news, risks, second_leg, decay, exhaustion, coil=None):
    risk_text = " ".join(risks).lower()
    coil = coil or {"detected": False}
    news_score = safe_float(news.get("score", 0))

    if coil.get("detected"):
        score += 0.75

    if second_leg.get("detected"):
        score += 0.85

    # Penalties are awareness nudges, not nuclear score killers.
    score -= decay.get("penalty", 0) * 0.15
    score -= exhaustion.get("penalty", 0) * 0.15

    if sec.get("severity") == "HIGH":
        score -= 0.20
    elif sec.get("severity") in ["MEDIUM", "LOW"]:
        score -= 0.05

    # Float/cap are quality awareness only unless extreme. Large-cap movers can
    # still be useful market leaders, but they should not be treated like low-float rockets.
    if float_shares and float_shares > MAX_FLOAT:
        score -= 0.20 if is_cold_regime(regime) else 0.35
    if market_cap and market_cap > MAX_MARKET_CAP:
        score -= 0.15 if is_cold_regime(regime) else 0.30

    # Cold market should tighten a little, not bury every setup.
    if is_cold_regime(regime):
        score -= 0.10
    else:
        score += regime.get("score_adjust", 0)

    # Safety floor: liquid 25%+ gainer + strong entry + coil/second-leg + real
    # catalyst deserves at least WATCH range unless it is truly broken.
    if (
        gain >= 25 and
        entry_score >= 7.0 and
        volume_score >= 4.0 and
        news_score >= 7.0 and
        (coil.get("detected") or second_leg.get("detected")) and
        not (decay.get("detected") and exhaustion.get("detected"))
    ):
        score = max(score, 7.05)

    # Liquid coil with clean entry should not be buried just because the headline
    # parser returns UNCLEAR instead of STRONG. Keep it as WATCH, not RUNNER.
    if (
        gain >= 25 and
        entry_score >= 7.5 and
        volume_score >= 6.0 and
        structure_score >= 4.5 and
        coil.get("detected") and
        news_score >= 4.0 and
        not (decay.get("detected") and exhaustion.get("detected"))
    ):
        score = max(score, 7.0)

    # No-news but elite structure/volume can still be a technical runner watch.
    if (
        gain >= 30 and
        entry_score >= 7.0 and
        volume_score >= 6.0 and
        structure_score >= 5.5 and
        news_score <= 2 and
        not exhaustion.get("detected")
    ):
        score = max(score, 7.0)

    return clamp(score)


# ============================================================
# ALERT MEMORY / COOLDOWN
# ============================================================

def meaningful_change_since_alert(ticker, price, score, bias):
    item = LAST_ALERT.get(ticker)
    if not item:
        return True, "first alert"

    elapsed = time.time() - item.get("time", 0)
    last_price = safe_float(item.get("price"))
    last_score = safe_float(item.get("score"))

    new_high = last_price > 0 and price >= last_price * RE_ALERT_NEW_HIGH_MULTIPLIER
    score_improved = score >= last_score + 0.8
    upgraded = item.get("bias") != bias and "RUNNER" in bias

    if elapsed >= ALERT_COOLDOWN_SECONDS and (new_high or score_improved or upgraded):
        reason = []
        if new_high:
            reason.append("new high")
        if score_improved:
            reason.append("score improvement")
        if upgraded:
            reason.append("bias upgraded")
        return True, " / ".join(reason)

    return False, "cooldown/no meaningful change"


def should_alert(result):
    ticker = result["ticker"]

    if ticker in SENT_THIS_CYCLE:
        print(f"[NO ALERT] {ticker}: already sent this cycle")
        return False

    # v33 hard rule: never alert any name under 27% on the day.
    alert_gain_floor = RUNNER_MIN_GAIN
    if result["gain"] < alert_gain_floor:
        print(f"[NO ALERT] {ticker}: gain {result['gain']:.1f}% below hard alert floor {alert_gain_floor:.0f}%")
        return False

    if result["score"] < ALERT_MIN_SCORE:
        print(f"[NO ALERT] {ticker}: score {result['score']:.1f} below floor")
        return False

    tier = result.get("trade_tier", "")
    if result["bias"] == "⚠️ AVOID" or "AVOID" in tier:
        print(f"[NO ALERT] {ticker}: avoid/wait tier")
        return False

    # v34: extended market leaders stay visible in ranking but only alert if truly exceptional.
    if "AWARENESS" in tier and result["score"] < 8.5:
        print(f"[NO ALERT] {ticker}: awareness/extended tier under 8.5")
        return False

    ok, reason = meaningful_change_since_alert(ticker, result["price"], result["score"], result["bias"])
    if not ok:
        print(f"[COOLDOWN] {ticker}: {reason}")
        return False

    print(f"[ALERT OK] {ticker}: {reason}")
    LAST_ALERT[ticker] = {
        "time": time.time(),
        "price": result["price"],
        "score": result["score"],
        "bias": result["bias"],
    }
    SENT_THIS_CYCLE.add(ticker)
    return True


# ============================================================
# ALERT BUILDER — CLEAN HUD OUTPUT
# ============================================================

def alert_title(result):
    tier = result.get("trade_tier", "")
    if "TRADEABLE RUNNER" in tier:
        if result.get("second_leg", {}).get("detected"):
            return "🔥 TRADEABLE RUNNER — SECOND LEG"
        if result.get("coil", {}).get("detected"):
            return "🔥 TRADEABLE RUNNER — COIL"
        return "🔥 TRADEABLE RUNNER"
    if "AWARENESS" in tier:
        return "👀 MARKET LEADER — EXTENDED"
    if "AVOID" in tier:
        return "⚠️ AVOID / WAIT"
    if "MARKET LEADER — EXTENDED" in result["bias"]:
        return "🔥 MARKET LEADER — EXTENDED"
    if "MARKET LEADER" in result["bias"]:
        return "🔥 MARKET LEADER"
    if "LEADER / EXTENDED" in result["bias"]:
        return "🔥 MARKET LEADER — EXTENDED"
    if "MARKET LEADER" in result["bias"]:
        return "🔥 MARKET LEADER"
    if "RUNNER" in result["bias"]:
        if result["second_leg"]["detected"]:
            return "🔥 RUNNER — SECOND LEG"
        if result["coil"]["detected"]:
            return "🔥 RUNNER — COIL BREAKOUT"
        return "🔥 RUNNER"

    if "WATCH" in result["bias"]:
        if result["coil"]["detected"]:
            return "👀 WATCH — COIL"
        return "👀 WATCH"

    return "⚠️ AVOID"


def main_risk_sentence(result):
    risks = result.get("risks", [])
    if not risks:
        return "No major structural risk detected yet."

    # Compress into one high-value warning first.
    priority = [
        "Lost VWAP", "Below VWAP", "Bad structure", "Momentum decay",
        "Very extended", "Extended", "Big upper wick", "DILUTION",
        "offering", "No fresh catalyst", "Aggregator",
    ]

    for p in priority:
        for r in risks:
            if p.lower() in r.lower():
                return r

    return risks[0]



def normalize_alert_fields(result):
    """v33.14: one missing field should never crash Telegram alerts."""
    if not isinstance(result, dict):
        return {}

    news = result.get("news") or {}
    regime = result.get("regime") or {}

    if isinstance(regime, str):
        regime = {"label": regime, "description": "", "score_adjust": 0}
    elif not isinstance(regime, dict):
        regime = {}

    result["regime"] = {
        "label": regime.get("label") or "⚪ NORMAL",
        "description": regime.get("description") or "Normal momentum tape",
        "score_adjust": safe_float(regime.get("score_adjust", 0)),
    }

    result["ticker"] = result.get("ticker", "UNKNOWN")
    result["score"] = safe_float(result.get("score", 0))
    result["price"] = safe_float(result.get("price", 0))
    result["gain"] = safe_float(result.get("gain", 0))
    result["bias"] = result.get("bias") or "🤔 UNCLEAR"
    result["trade_tier"] = result.get("trade_tier") or "👀 MARKET WATCH"
    result["phase"] = result.get("phase") or "⚪ NEUTRAL"
    result["entry"] = result.get("entry") or "👀 WATCH"
    result["reasons"] = result.get("reasons") or []
    result["risks"] = result.get("risks") or []
    result["float_info"] = result.get("float_info") or {}
    result["halt_risk"] = result.get("halt_risk") or {"label": ""}
    result["sec"] = result.get("sec") or {"has_risk": False, "label": ""}
    result["coil"] = result.get("coil") or {"detected": False}
    result["second_leg"] = result.get("second_leg") or {"detected": False}
    result["decay"] = result.get("decay") or {"detected": False, "risks": []}
    result["exhaustion"] = result.get("exhaustion") or {"detected": False, "risk": ""}
    result["fakeout"] = result.get("fakeout") or {"detected": False, "risk": "", "label": ""}
    result["participation"] = result.get("participation") or {"score": 0, "rvol_proxy": 0, "label": "", "reasons": []}

    news_score = safe_float(result.get("news_score", news.get("score", 0)))
    news_label = result.get("news_label") or news.get("label") or "📰 NEWS"
    news_headline = result.get("news_headline") or news.get("headline") or ""
    news_explain = (
        result.get("news_explain")
        or news.get("explain")
        or news_headline
        or "No catalyst details available"
    )
    if news_explain == news_label:
        news_explain = news_headline or "No catalyst details available"

    result["news_score"] = news_score
    result["news_label"] = news_label
    result["news_headline"] = news_headline
    result["news_explain"] = news_explain
    result["catalyst_line"] = f"Catalyst: {news_score:.0f}/10 {news_label} — {news_explain}"

    return result

def build_alert(result):
    result = normalize_alert_fields(result)
    title = alert_title(result)

    # v33.14 hotfix: never let a missing news_explain/news field crash alerts.
    news = result.get("news") or {}
    news_score = safe_float(result.get("news_score", news.get("score", 0)))
    news_label = result.get("news_label") or news.get("label") or "📰 NEWS"
    news_headline = result.get("news_headline") or news.get("headline") or ""
    news_explain = (
        result.get("news_explain")
        or news.get("explain")
        or news_headline
        or "No catalyst details available"
    )
    if news_explain == news_label:
        news_explain = news_headline or "No catalyst details available"

    # Store normalized safe values back into result for downstream code/logging.
    result["news_score"] = news_score
    result["news_label"] = news_label
    result["news_headline"] = news_headline
    result["news_explain"] = news_explain

    header = f"{result['ticker']} | {result['score']:.1f}/10 | {fmt_money(result['price'])} | +{result['gain']:.1f}%"
    if SHOW_FLOAT and result.get("float"):
        header += f" | {result.get('float_info', {}).get('label') or ('Float ' + fmt_big_num(result['float']))}"

    lines = [
        title,
        "",
        header,
        "",
        f"Catalyst: {news_score:.0f}/10 {news_label} — {news_explain}",
        f"Tier: {result.get('trade_tier', '👀 MARKET WATCH')}",
        f"State: {result['bias']}",
        f"Phase: {result['phase']}",
        "",
        "Why:",
    ]

    for reason in result["reasons"][:5]:
        lines.append(f"• {reason}")

    lines.extend([
        "",
        f"Entry: {result['entry']}",
        f"Risk: {main_risk_sentence(result)}",
        f"Bias: {result['bias']}",
    ])

    # Add only high-value awareness, not clutter.
    awareness = []
    if result.get("halt_risk", {}).get("label"):
        awareness.append(result.get("halt_risk", {}).get("label"))

    if result.get("float_info", {}).get("risk"):
        awareness.append(result.get("float_info", {}).get("risk"))

    if result.get("sec", {}).get("has_risk"):
        awareness.append(result.get("sec", {}).get("label"))

    if result.get("fakeout", {}).get("label"):
        awareness.append(result.get("fakeout", {}).get("label"))

    if result.get("participation", {}).get("label") and "fading" in result.get("participation", {}).get("label", "").lower():
        awareness.append(result.get("participation", {}).get("label"))

    if result.get("regime", {}).get("label") == "❄️ COLD / THIN MOMENTUM MARKET":
        awareness.append("Cold market — be extra selective")

    if awareness:
        lines.append("")
        lines.append("Awareness:")
        for item in dedupe(awareness)[:3]:
            lines.append(f"• {item}")

    if SHOW_HEADLINE and result.get("headline"):
        lines.append("")
        lines.append(f"Headline: {result['headline'][:220]}")

    return "\n".join(lines)


# ============================================================
# CANDIDATE ANALYSIS PIPELINE
# ============================================================



# ============================================================
# SELF-CONTAINED SCORING HELPERS — v33.10
# No dependency on old score_entry / score_risk / score_candidate / build_phase.
# ============================================================

def calc_structure_score_v3310(structure, coil, second_leg):
    score = 0.0
    reasons = []

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    breakout = bool(get_struct(structure, "breakout", False))
    near_high = bool(get_struct(structure, "near_high", False))
    bad_structure = bool(get_struct(structure, "bad_structure", False))
    big_upper_wick = bool(get_struct(structure, "big_upper_wick", False))

    if above_vwap:
        score += 2.0
        reasons.append("above VWAP")
    if higher_lows:
        score += 1.5
        reasons.append("higher lows")
    if breakout:
        score += 1.5
        reasons.append("breakout/holding high")
    if near_high:
        score += 1.0
        reasons.append("near highs")
    if coil and coil.get("detected"):
        score += 1.5
        reasons.append(coil.get("label") or "coil")
    if second_leg and second_leg.get("detected"):
        score += 1.5
        reasons.append(second_leg.get("label") or "second leg")

    if bad_structure:
        score -= 1.5
    if big_upper_wick:
        score -= 0.75

    return clamp(score), dedupe(reasons)


def calc_volume_score_v3310(volume):
    volume = safe_int(volume)
    reasons = []
    score = 0.0

    if volume >= 250_000_000:
        score = 10
        reasons.append("250M+ volume")
    elif volume >= 100_000_000:
        score = 9
        reasons.append("100M+ volume")
    elif volume >= 50_000_000:
        score = 8
        reasons.append("50M+ volume")
    elif volume >= 20_000_000:
        score = 7
        reasons.append("20M+ volume")
    elif volume >= 5_000_000:
        score = 6
        reasons.append("5M+ volume")
    elif volume >= 1_000_000:
        score = 5
        reasons.append("1M+ volume")
    elif volume >= MIN_DEEP_VOLUME:
        score = 3.5
        reasons.append("liquid enough")
    else:
        score = 1.5

    return clamp(score), reasons


def calc_entry_score_v3310(structure, coil, second_leg, exhaustion, decay):
    score = 3.0
    reasons = []

    if bool(get_struct(structure, "above_vwap", False)):
        score += 1.5
    if bool(get_struct(structure, "higher_lows", False)):
        score += 1.25
    if bool(get_struct(structure, "near_high", False)):
        score += 0.75
    if coil and coil.get("detected"):
        score += 1.25
        reasons.append("coil setup")
    if second_leg and second_leg.get("detected"):
        score += 1.25
        reasons.append("second-leg setup")

    if exhaustion and exhaustion.get("detected"):
        score -= 1.0 if is_premarket_session() else 2.0
        if exhaustion.get("risk"):
            reasons.append(exhaustion.get("risk"))
    if decay and decay.get("detected"):
        score -= 0.75 if is_premarket_session() else 1.5

    return clamp(score), dedupe(reasons)


def calc_risk_penalty_v3310(decay, exhaustion, sec, halt_risk, fast_warnings=None):
    penalty = 0.0
    reasons = []

    fast_warnings = fast_warnings or []

    if decay and decay.get("detected"):
        penalty += safe_float(decay.get("penalty", 1.0))
        reasons.extend(decay.get("risks", []))

    if exhaustion and exhaustion.get("detected"):
        penalty += safe_float(exhaustion.get("penalty", 1.0))
        if exhaustion.get("risk"):
            reasons.append(exhaustion.get("risk"))

    sec_severity = sec.get("severity", "") if isinstance(sec, dict) else ""
    sec_risk_score = safe_float(sec.get("risk_score", 0.0)) if isinstance(sec, dict) else 0.0
    # Dilution is awareness first. Only same-day/active terms should noticeably drag score.
    if sec_severity == "HIGH":
        penalty += min(0.9, 0.25 + sec_risk_score * 0.18)
    elif sec_severity == "MEDIUM":
        penalty += min(0.45, 0.15 + sec_risk_score * 0.12)
    elif sec_severity == "LOW":
        penalty += 0.05

    halt_label = halt_risk.get("label", "") if isinstance(halt_risk, dict) else ""
    if halt_label:
        penalty += 0.35
        reasons.append(halt_label)

    reasons.extend(fast_warnings)
    return penalty, dedupe(reasons)


def calc_total_score_v3310(structure_score, volume_score, entry_score, news_score, risk_penalty, gain, float_info, regime=None):
    score = (
        safe_float(structure_score) * 0.35
        + safe_float(volume_score) * 0.25
        + safe_float(entry_score) * 0.25
        + safe_float(news_score) * 0.15
    )

    # Percent leader boost.
    gain_boost, _ = leader_gain_boost(gain)
    score += gain_boost

    # Low float boost only if the chart is not totally broken.
    if safe_float(structure_score) >= 3.0 and safe_float(entry_score) >= 3.0:
        score += safe_float((float_info or {}).get("boost", 0))

    score -= safe_float(risk_penalty) * (0.12 if is_premarket_session() else 0.20)

    # Continuation-friendly floor: high-gain, low-float, liquid leaders should not be buried
    # purely because they are extended premarket. Alerts still require tier/score/cooldown.
    if is_premarket_session() and safe_float(gain) >= 50:
        score += 0.45

    if regime:
        score += safe_float(regime.get("score_adjust", 0))

    return clamp(score)


def build_phase_v3310(structure, coil, second_leg, exhaustion, decay, fakeout=None, participation=None):
    # v34 wrapper keeps old function name stable while using upgraded phase logic.
    return build_phase_v34(structure, coil, second_leg, exhaustion, decay, fakeout=fakeout, participation=participation)


def build_entry_v3310(bias, structure, coil, second_leg, entry_score):
    if "EXTENDED" in bias:
        return "Wait for reset / VWAP hold"
    if second_leg and second_leg.get("detected"):
        return "Second-leg continuation only if holding highs"
    if coil and coil.get("detected"):
        return "Coil breakout / VWAP hold"
    if bool(get_struct(structure, "above_vwap", False)):
        return "VWAP hold / higher-low entry"
    return "No clean entry yet"



def analyze_candidate(candidate, regime):
    ticker = str(candidate.get("ticker", "")).upper().strip()
    if not ticker or is_bad_ticker(ticker):
        return None

    price = safe_float(candidate.get("price"))
    gain = safe_float(candidate.get("gain"))
    volume = safe_int(candidate.get("volume"))
    source = candidate.get("source", "unknown")

    if gain <= 0:
        return None

    live = get_live_quote(ticker)
    live_price = safe_float(live.get("price"))
    live_gain = safe_float(live.get("gain"))
    if live_price > 0:
        price = live_price
    if live_gain > 0:
        gain = live_gain
    live_volume = safe_int(live.get("volume"))
    if live_volume > 0:
        volume = max(volume, live_volume)
    quote_stale = bool(live.get("stale"))

    profile = get_profile(ticker)
    float_shares = safe_int(profile.get("float"))
    market_cap = safe_int(profile.get("market_cap"))

    ok, fast_reasons, fast_warnings = fast_pass_filter(
        ticker=ticker,
        price=price,
        gain=gain,
        volume=volume,
        market_cap=market_cap,
        float_shares=float_shares,
        regime=regime,
    )
    if not ok:
        return None
    if quote_stale:
        fast_warnings.append("stale quote fallback — confirm price before entry")

    print(f"[PIPELINE] {ticker}: passed fast filter — running deep scan")

    candles_raw = get_candles(ticker)
    candles = finalized_candles(candles_raw)
    if candles:
        print(f"[CANDLES] {ticker}: using {len(candles)} finalized bars (ignored active bar)")

    if not candles or len(candles) < 8:
        print(f"[DEEP SKIP] {ticker}: insufficient finalized candles")
        return None

    structure = get_structure(candles, ticker)
    coil = detect_coil(candles, structure)
    second_leg = detect_second_leg(candles, structure, coil)
    decay = detect_momentum_decay(candles, structure)
    exhaustion = detect_exhaustion(candles, structure, price)
    fakeout = detect_fakeout_v34(candles, structure)
    participation = calc_participation_score_v34(candles, volume)
    halt_risk = detect_halt_risk(price, gain, float_shares, candles)

    float_info = classify_float(float_shares)
    news = get_best_news(ticker)
    news_score = safe_float(news.get("score", 0))
    sec = check_sec_filings(ticker)

    structure_score, structure_reasons = calc_structure_score_v3310(structure, coil, second_leg)
    volume_score, volume_reasons = calc_volume_score_v3310(volume)
    entry_score, entry_reasons = calc_entry_score_v3310(structure, coil, second_leg, exhaustion, decay)
    risk_penalty, risk_reasons = calc_risk_penalty_v3310(decay, exhaustion, sec, halt_risk, fast_warnings)
    risk_penalty += safe_float(fakeout.get("penalty", 0))

    score = calc_total_score_v3310(
        structure_score=structure_score,
        volume_score=volume_score,
        entry_score=entry_score,
        news_score=news_score,
        risk_penalty=risk_penalty,
        gain=gain,
        float_info=float_info,
        regime=regime,
    )

    # v34 participation boost/penalty after base score.
    score += safe_float(participation.get("score", 0))

    # v33.12 ranking visibility floor:
    # True tiny/low-float leaders stay visible even when extended.
    # This does NOT force alerts; should_alert still controls alerts.
    if gain >= 50 and 0 < float_shares <= 25_000_000:
        score = max(score, 6.6 if is_premarket_session() and entry_score >= 4.0 else 5.5)

    bias = simple_market_label(
        gain=gain,
        float_shares=float_shares,
        volume=volume,
        score=score,
        entry_score=entry_score,
        structure_score=structure_score,
        exhaustion=exhaustion,
        decay=decay,
    )

    phase = build_phase_v3310(structure, coil, second_leg, exhaustion, decay, fakeout=fakeout, participation=participation)
    trade_tier = build_trade_tier_v34(score, bias, phase, exhaustion, decay, fakeout, entry_score, structure_score)
    entry = build_entry_v3310(bias, structure, coil, second_leg, entry_score)

    reasons = []
    risks = []

    reasons.extend(simple_leader_reasons(gain, float_info, volume, news_score))
    reasons.extend(structure_reasons)
    reasons.extend(volume_reasons)
    reasons.extend(participation.get("reasons", []))
    reasons.extend(entry_reasons)

    risks.extend(risk_reasons)
    if fakeout.get("risk"):
        risks.append(fakeout.get("risk"))
    if sec.get("label"):
        risks.append(sec.get("label"))
    if float_info.get("risk") and float_info.get("tier") in ["TINY", "ELITE", "UNKNOWN"]:
        risks.append(float_info.get("risk"))
    if halt_risk.get("label"):
        risks.append(halt_risk.get("label"))

    reasons = dedupe(reasons)
    risks = dedupe(risks)

    print(
        f"[RANK] {ticker} {score:.1f}/10 {trade_tier} {bias} "
        f"+{gain:.1f}% {float_info.get('label', '')} Phase={phase}"
    )

    return {
        "ticker": ticker,
        "price": price,
        "gain": gain,
        "volume": volume,
        "float": float_shares,
        "float_info": float_info,
        "market_cap": market_cap,
        "regime": regime or {"label": "⚪ NORMAL", "description": "Normal momentum tape", "score_adjust": 0},
        "score": score,
        "bias": bias,
        "trade_tier": trade_tier,
        "phase": phase,
        "entry": entry,
        "reasons": reasons,
        "risks": risks,
        "news": news,
        "news_label": news.get("label", "📰 NEWS"),
        "news_headline": news.get("headline", ""),
        "news_explain": news.get("explain") or news.get("headline") or "No catalyst details available",
        "news_category": news.get("category", ""),
        "news_quality": news.get("quality", ""),
        "news_score": news_score,
        "sec": sec,
        "structure": structure,
        "coil": coil,
        "second_leg": second_leg,
        "decay": decay,
        "exhaustion": exhaustion,
        "fakeout": fakeout,
        "participation": participation,
        "halt_risk": halt_risk,
        "source": source,
    }


def sort_results(results):
    return sorted(
        results,
        key=lambda r: (
            safe_float(r.get("score", 0)),
            safe_float(r.get("gain", 0)),
            safe_int(r.get("volume", 0)),
        ),
        reverse=True,
    )

def print_top_ranked(results):
    if not results:
        print("[SCAN] No qualified deep-scan results")
        return

    top = " | ".join(
        f"{r['ticker']} {r['score']:.1f}/10 {r['bias'].replace('🟢 ', '').replace('👀 ', '').replace('⚠️ ', '')} +{r['gain']:.1f}% {r.get('float_info', {}).get('label', '')}"
        for r in results[:5]
    )
    print(f"[SCAN] Top ranked: {top}")

def run_scanner():
    print(f"[BOOT] {BOOT_MARKER}")

    while True:
        try:
            if not market_is_active():
                now = datetime.now(ET)

                if now.time() >= dtime(16, 10) or now.time() < dtime(7, 30):
                    time.sleep(300)
                else:
                    time.sleep(60)

                continue

            SENT_THIS_CYCLE.clear()

            print(f"[SCAN] Market active — running scan ({get_market_session_label()})")

            candidates = get_candidates()
            regime = estimate_market_regime(candidates)

            print(f"[REGIME] {regime['label']} — {regime['description']}")

            results = []
            for candidate in candidates:
                try:
                    result = analyze_candidate(candidate, regime)
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"[CANDIDATE ERROR] {candidate.get('ticker')}: {e}")
                    continue

            results = sort_results(results)
            print_top_ranked(results)

            sent = 0
            for result in results:
                if sent >= MAX_ALERTS_PER_CYCLE:
                    break

                if should_alert(result):
                    msg = build_alert(result)
                    send_alert(msg)
                    print(f"[ALERT SENT] {result['ticker']}")
                    sent += 1

            print("[SCAN] Cycle complete")
            time.sleep(SCAN_SLEEP)

        except Exception as e:
            print(f"[SCANNER ERROR] {e}")
            time.sleep(30)


if __name__ == "__main__":
    Thread(target=start_web_server, daemon=True).start()
    run_scanner()
