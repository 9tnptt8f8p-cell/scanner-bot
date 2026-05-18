import os
import re
import time
import html
import json
from datetime import datetime, timedelta, time as dtime, timezone
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
# ELITE SCANNER REBUILD v32 FULL
# Fast Pass + Full Runner/Avoid Engine + News + SEC + Coil
# ============================================================

ET = ZoneInfo("America/New_York")
BOOT_MARKER = "elite scanner rebuild v32.2 FIXED — structure signature + clean news + UTC"

# ============================================================
# ENV
# ============================================================

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
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
OPEN_SCAN_MIN_GAIN = 12.0
HARD_MIN_GAIN = 25.0                 # regular-hours hard floor
PREMARKET_HARD_MIN_GAIN = 18.0       # fixed: do not kill 18-24% premarket leaders
ALERT_MIN_GAIN = 25.0                # alerts still prefer real 25%+ movers
PREMARKET_ALERT_MIN_GAIN = 20.0      # but allow elite premarket runners slightly early
MIN_PRICE = 0.50
MAX_PRICE = 80.0
MIN_FAST_VOLUME = 75_000             # fixed: premarket liquidity can be thinner
MIN_DEEP_VOLUME = 150_000
MAX_FLOAT = 80_000_000               # fixed: 40M was too nuclear in thin markets
MAX_MARKET_CAP = 1_200_000_000       # fixed: cap is awareness unless extreme
EXTREME_FLOAT_SKIP = 150_000_000     # only hard skip truly heavy floats
EXTREME_MARKET_CAP_SKIP = 3_000_000_000

# Alerting
ALERT_MIN_SCORE = 7.0
MAX_GAINERS = 80
MAX_ALERTS_PER_CYCLE = 3
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
NEWS_CACHE = {}
SEC_CACHE = {}
CANDLE_CACHE = {}
MARKET_REGIME_CACHE = {}

LAST_ALERT = {}
LAST_EARLY_ALERT = {}
SENT_THIS_CYCLE = set()

# Output preferences
SHOW_FLOAT = True
SHOW_HEADLINE = False
SHOW_VERBOSE_DEBUG = True

# ============================================================
# FLASK KEEPALIVE
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return "scanner alive — v32 FULL fast pass runner/avoid engine", 200


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
    gain_floor = dynamic_hard_min_gain()

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
# GAINER SOURCES
# ============================================================

def parse_yahoo_quote_item(q):
    return {
        "ticker": str(q.get("symbol", "")).upper(),
        "price": safe_float(q.get("regularMarketPrice")),
        "gain": safe_float(q.get("regularMarketChangePercent")),
        "volume": safe_int(q.get("regularMarketVolume")),
        "market_cap": safe_int(q.get("marketCap")),
        "source": "Yahoo Gainers",
    }


def get_yahoo_gainers():
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": "day_gainers", "count": MAX_GAINERS, "formatted": "false"}

    try:
        r = http_get(url, params=params, timeout=8)
        data = r.json()
        quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])

        results = []
        for q in quotes:
            item = parse_yahoo_quote_item(q)
            if item["ticker"]:
                results.append(item)

        print(f"[GAINERS] Yahoo returned {len(results)} names")
        return results

    except Exception as e:
        print(f"[GAINERS ERROR] Yahoo: {e}")
        return []


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


def get_candidates():
    sources = []
    sources.extend(get_yahoo_gainers())

    # Fallback/merge only if Yahoo is thin.
    if len(sources) < 20:
        sources.extend(get_nasdaq_gainers())

    seen = {}
    candidate_floor = dynamic_scan_min_gain()

    for item in sources:
        ticker = item.get("ticker", "").upper()
        if not ticker or is_bad_ticker(ticker):
            continue

        gain = safe_float(item.get("gain"))
        if gain < candidate_floor:
            continue

        existing = seen.get(ticker)
        if not existing or gain > safe_float(existing.get("gain")):
            seen[ticker] = item

    candidates = list(seen.values())
    candidates.sort(key=lambda x: safe_float(x.get("gain")), reverse=True)

    print(f"[CANDIDATES] merged {len(candidates)} candidates")
    return candidates[:MAX_GAINERS]


# ============================================================
# LIVE QUOTE / PROFILE
# ============================================================

def get_finnhub_quote(ticker):
    cached = cached_get(QUOTE_CACHE, ticker, ttl=SHORT_CACHE_TTL_SECONDS)
    if cached:
        return cached

    quote = {"price": 0.0, "gain": 0.0, "volume": 0, "source": "none"}

    if not FINNHUB_API_KEY:
        return quote

    try:
        url = "https://finnhub.io/api/v1/quote"
        r = http_get(url, params={"symbol": ticker, "token": FINNHUB_API_KEY}, timeout=4)
        data = r.json()

        price = safe_float(data.get("c"))
        prev_close = safe_float(data.get("pc"))
        gain = 0.0

        if price > 0 and prev_close > 0:
            gain = ((price - prev_close) / prev_close) * 100

        quote = {
            "price": price,
            "gain": gain,
            "volume": 0,
            "source": "Finnhub",
        }

        print(f"[LIVE] {ticker} {fmt_money(price)} {gain:.1f}%")
        return cached_set(QUOTE_CACHE, ticker, quote)

    except Exception as e:
        print(f"[QUOTE ERROR] {ticker}: {e}")
        return quote


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
        data = r.json()
        bars = data.get("bars", [])

        candles = []
        for b in bars:
            candles.append(normalize_candle(
                b.get("o"), b.get("h"), b.get("l"), b.get("c"), b.get("v"), b.get("t")
            ))

        if candles:
            print(f"[CANDLES] {ticker}: Alpaca {len(candles)}")
        return candles

    except Exception as e:
        print(f"[ALPACA ERROR] {ticker}: {e}")
        return []


def get_yahoo_candles(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote_plus(ticker)}"
        params = {"interval": "1m", "range": "1d"}
        r = http_get(url, params=params, timeout=6)
        data = r.json()

        result = data.get("chart", {}).get("result", [])
        if not result:
            return []

        node = result[0]
        timestamps = node.get("timestamp", [])
        quote = node.get("indicators", {}).get("quote", [{}])[0]

        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])

        candles = []
        for i, ts in enumerate(timestamps):
            try:
                o = opens[i] if i < len(opens) else None
                h = highs[i] if i < len(highs) else None
                l = lows[i] if i < len(lows) else None
                c = closes[i] if i < len(closes) else None
                v = volumes[i] if i < len(volumes) else 0

                if None in [o, h, l, c]:
                    continue

                candles.append(normalize_candle(o, h, l, c, v, ts))
            except Exception:
                continue

        if candles:
            print(f"[CANDLES] {ticker}: Yahoo {len(candles)}")
        return candles

    except Exception as e:
        print(f"[CANDLE ERROR] {ticker}: {e}")
        return []


def get_candles(ticker):
    cached = cached_get(CANDLE_CACHE, ticker, ttl=SHORT_CACHE_TTL_SECONDS)
    if cached:
        return cached

    candles = get_alpaca_candles(ticker)
    if not candles:
        print(f"[DATA FALLBACK] {ticker}: Alpaca failed — using Yahoo")
        candles = get_yahoo_candles(ticker)

    return cached_set(CANDLE_CACHE, ticker, candles)


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


def fallback_structure_analysis(candles):
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
    if not isinstance(external, dict):
        return fallback

    merged = dict(fallback)
    for k, v in external.items():
        if v is not None:
            merged[k] = v
    return merged


def get_structure(candles, ticker):
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
    "stocks moving", "stock is moving", "why shares", "why is",
    "top gainers", "market movers", "most active", "gap-ups and gap-downs",
    "driving market activity", "shares are trading higher",
    "deadline", "law firm", "investigation", "shareholder alert",
    "class action", "reminds investors", "notice to investors",
    "benzinga examines", "what's going on", "today's session",
]

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
        "contract", "purchase order", "supply agreement", "government contract",
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
        "strategic alliance",
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


def strict_ticker_in_text(ticker, text):
    if not ticker or not text:
        return False
    return re.search(rf"\b{re.escape(ticker.upper())}\b", text.upper()) is not None


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


def get_best_news(ticker):
    cached = cached_get(NEWS_CACHE, ticker)
    if cached:
        return cached

    all_headlines = []

    # Yahoo first, then PR sources.
    for fn in [scrape_yahoo_news, scrape_prnewswire, scrape_globenewswire]:
        headlines = fn(ticker)
        for h in headlines:
            if h not in all_headlines:
                all_headlines.append(h)

    if not all_headlines:
        news = classify_news("", ticker)
        print(f"[NEWS] {ticker}: NO NEWS")
        return cached_set(NEWS_CACHE, ticker, news)

    ranked = []
    for h in all_headlines:
        c = classify_news(h, ticker)
        ranked.append(c)

    ranked.sort(key=lambda x: x["score"], reverse=True)
    best = ranked[0]

    print(f"[NEWS] {ticker}: {best.get('headline','')[:120]} ({best['quality']} {best['score']}/10)")
    return cached_set(NEWS_CACHE, ticker, best)


# ============================================================
# SEC / DILUTION ENGINE
# ============================================================

DILUTION_TERMS = [
    "at-the-market", "atm offering", "equity distribution agreement",
    "sales agreement", "registered direct", "public offering",
    "private placement", "securities purchase agreement",
    "warrant", "warrants", "convertible", "convertible note",
    "shelf registration", "resale prospectus", "form s-1", "form s-3",
    "424b3", "424b5", "f-1", "f-3",
]

SHELF_FORMS = ["S-1", "S-3", "F-1", "F-3", "424B3", "424B5"]
SEC_FORMS_TO_DETECT = ["424B3", "424B5", "S-1", "S-3", "F-1", "F-3", "8-K", "6-K"]


def check_sec_filings(ticker):
    cached = cached_get(SEC_CACHE, ticker)
    if cached:
        return cached

    risk = {
        "has_risk": False,
        "severity": "NONE",
        "label": "",
        "forms": [],
        "terms": [],
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

        forms = []
        for form in SEC_FORMS_TO_DETECT:
            if form.lower() in lower:
                forms.append(form)

        terms = []
        for term in DILUTION_TERMS:
            if term in lower:
                terms.append(term)

        if terms and any(term in terms for term in [
            "registered direct", "public offering", "private placement",
            "securities purchase agreement", "warrant", "warrants",
            "convertible", "atm offering", "at-the-market",
        ]):
            risk.update({
                "has_risk": True,
                "severity": "HIGH",
                "label": "🚨 CONFIRMED DILUTION RISK: offering/warrants/financing language found",
                "forms": forms[:6],
                "terms": terms[:6],
            })
        elif forms and any(f in SHELF_FORMS for f in forms):
            risk.update({
                "has_risk": True,
                "severity": "MEDIUM",
                "label": "⚠️ DILUTION RISK BUILDING: shelf/prospectus filing found",
                "forms": forms[:6],
                "terms": terms[:6],
            })
        elif forms:
            risk.update({
                "has_risk": True,
                "severity": "LOW",
                "label": "🟡 SEC FILINGS PRESENT: recent filings found",
                "forms": forms[:6],
                "terms": terms[:6],
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
    score_penalty = 0

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    bad_structure = bool(get_struct(structure, "bad_structure", False))
    big_upper_wick = bool(get_struct(structure, "big_upper_wick", False))
    raw_decay = bool(get_struct(structure, "momentum_decay", False))

    recent_vol = safe_int(get_struct(structure, "recent_volume", 0))
    previous_vol = safe_int(get_struct(structure, "previous_volume", 0))

    if not above_vwap:
        risks.append("Lost VWAP / reclaim needed")
        score_penalty += 1.5

    if previous_vol > 0 and recent_vol < previous_vol * 0.60:
        risks.append("Momentum decay / volume fading")
        score_penalty += 1.0

    if bad_structure:
        risks.append("Bad structure / failed momentum")
        score_penalty += 1.5

    if big_upper_wick:
        risks.append("Big upper wick / possible trap")
        score_penalty += 1.0

    if raw_decay and "Momentum decay / volume fading" not in risks:
        risks.append("Momentum decay / wait for reclaim")
        score_penalty += 1.0

    return {
        "detected": bool(risks),
        "penalty": score_penalty,
        "risks": risks,
    }


def detect_exhaustion(candles, structure, price):
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

    if distance_from_vwap >= 18:
        return {
            "detected": True,
            "risk": "Very extended from VWAP / chase risk",
            "penalty": 1.0,
        }

    if distance_from_vwap >= 12 and upper_wick_count >= 2:
        return {
            "detected": True,
            "risk": "Extended + repeated upper wicks",
            "penalty": 1.25,
        }

    if off_high >= 12:
        return {
            "detected": True,
            "risk": "Fading far off highs",
            "penalty": 1.0,
        }

    return {
        "detected": False,
        "risk": "",
        "penalty": 0,
    }


def detect_halt_risk(price, gain, float_shares, candles):
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
# SCORING ENGINE
# ============================================================

def score_structure(structure):
    score = 0
    reasons = []
    risks = []

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    breakout = bool(get_struct(structure, "breakout", False))
    near_high = bool(get_struct(structure, "near_high", False))
    bad_structure = bool(get_struct(structure, "bad_structure", False))
    big_upper_wick = bool(get_struct(structure, "big_upper_wick", False))

    if above_vwap:
        score += 2.5
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

    if bad_structure:
        score -= 2.0
        risks.append("Bad structure / failed momentum")

    if big_upper_wick:
        score -= 1.0
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


def build_bias(score, structure_score, entry_score, news, risks, second_leg, decay, exhaustion):
    risk_text = " ".join(risks).lower()

    if "offering" in risk_text and score < 8.5:
        return "⚠️ AVOID"

    if ("below vwap" in risk_text or "lost vwap" in risk_text) and not second_leg["detected"]:
        if score < 8:
            return "⚠️ AVOID"

    if decay["detected"] and exhaustion["detected"] and score < 8.2:
        return "⚠️ AVOID"

    if score >= 8.2 and structure_score >= 6 and entry_score >= 6:
        return "🟢 RUNNER"

    if score >= 7.0 and structure_score >= 5:
        return "👀 WATCH"

    return "⚠️ AVOID"


def build_phase(structure, coil, second_leg, exhaustion, decay):
    if exhaustion["detected"]:
        return "⚠️ EXHAUSTION"
    if second_leg["detected"]:
        return "🌀 COIL → 🔥 EXPANSION"
    if coil["detected"]:
        return "🌀 COIL"
    if bool(get_struct(structure, "breakout", False)):
        return "🔥 EXPANSION"
    if bool(get_struct(structure, "above_vwap", False)):
        return "🟢 IGNITION / HOLDING"
    if decay["detected"]:
        return "⚠️ FADING"
    return "👀 WATCH"


def build_entry(bias, structure, coil, second_leg, entry_score):
    if "RUNNER" in bias:
        if second_leg["detected"]:
            return "First clean pullback into VWAP hold after second-leg breakout"
        if coil["detected"]:
            return "VWAP hold or coil breakout hold"
        return "VWAP hold or breakout hold only"

    if "WATCH" in bias:
        return "Wait for VWAP hold + higher low + breakout confirmation"

    return "No trade unless VWAP reclaim + clean reset"


def score_candidate(gain, structure_score, volume_score, news_score, entry_score, coil, second_leg, decay, exhaustion, sec, regime, float_shares=0, market_cap=0):
    # Weighted for user's style: structure > volume > entry > news.
    score = (
        structure_score * 0.40 +
        volume_score * 0.20 +
        entry_score * 0.20 +
        news_score * 0.10 +
        clamp(gain / 10) * 0.10
    )

    if coil["detected"]:
        score += 0.25

    if second_leg["detected"]:
        score += 0.55

    score -= decay["penalty"] * 0.35
    score -= exhaustion["penalty"] * 0.45

    # SEC is awareness mostly, but heavy confirmed offering should nudge down.
    if sec.get("severity") == "HIGH":
        score -= 0.45

    # Float/cap are no longer hard-kills unless extreme; they become small quality penalties.
    if float_shares and float_shares > MAX_FLOAT:
        score -= 0.35 if is_cold_regime(regime) else 0.55
    if market_cap and market_cap > MAX_MARKET_CAP:
        score -= 0.25 if is_cold_regime(regime) else 0.45

    score += regime.get("score_adjust", 0)

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

    alert_gain_floor = dynamic_alert_min_gain(result)
    elite_exception = result["score"] >= 8.2 and "RUNNER" in result["bias"]
    if result["gain"] < alert_gain_floor and not elite_exception:
        print(f"[NO ALERT] {ticker}: gain {result['gain']:.1f}% below alert floor {alert_gain_floor:.0f}%")
        return False

    if result["score"] < ALERT_MIN_SCORE:
        print(f"[NO ALERT] {ticker}: score {result['score']:.1f} below floor")
        return False

    if "AVOID" in result["bias"]:
        print(f"[NO ALERT] {ticker}: avoid bias")
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


def build_alert(result):
    title = alert_title(result)

    header = f"{result['ticker']} | {result['score']:.1f}/10 | {fmt_money(result['price'])} | +{result['gain']:.1f}%"
    if SHOW_FLOAT and result.get("float"):
        header += f" | Float {fmt_big_num(result['float'])}"

    lines = [
        title,
        "",
        header,
        "",
        f"Catalyst: {result['news_score']}/10 {result['news_label']} — {result['news_explain']}",
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
    if result["halt_risk"]["label"]:
        awareness.append(result["halt_risk"]["label"])

    if result["sec"].get("has_risk"):
        awareness.append(result["sec"].get("label"))

    if result["regime"]["label"] == "❄️ COLD / THIN MOMENTUM MARKET":
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

def analyze_candidate(candidate, regime):
    ticker = candidate["ticker"].upper()

    if is_bad_ticker(ticker):
        print(f"[FAST SKIP] {ticker}: bad ticker")
        return None

    # Live quote first, screener fallback.
    quote = get_finnhub_quote(ticker)
    price = safe_float(quote.get("price")) or safe_float(candidate.get("price"))
    gain = safe_float(quote.get("gain")) or safe_float(candidate.get("gain"))
    volume = safe_int(candidate.get("volume")) or safe_int(quote.get("volume"))

    # Lightweight profile before expensive deep scan.
    profile = get_profile(ticker)
    market_cap = profile.get("market_cap", 0) or candidate.get("market_cap", 0)
    float_shares = profile.get("float", 0)

    # HARD FAST PASS — no candles/news/sec before this.
    passed, skip_reasons, fast_warnings = fast_pass_filter(
        ticker=ticker,
        price=price,
        gain=gain,
        volume=volume,
        market_cap=market_cap,
        float_shares=float_shares,
        regime=regime,
    )

    if not passed:
        return None

    print(f"[PIPELINE] {ticker}: passed fast filter — running deep scan")

    candles = get_candles(ticker)
    if not candles or len(candles) < 8:
        print(f"[DEEP SKIP] {ticker}: insufficient candles")
        return None

    structure = get_structure(candles, ticker)

    # Deeper checks only after fast pass.
    news = get_best_news(ticker)
    sec = check_sec_filings(ticker)

    coil = detect_coil(candles, structure)
    second_leg = detect_second_leg(candles, structure, coil)
    decay = detect_momentum_decay(candles, structure)
    exhaustion = detect_exhaustion(candles, structure, price)
    halt_risk = detect_halt_risk(price, gain, float_shares, candles)

    structure_score, structure_reasons, structure_risks = score_structure(structure)
    volume_score, volume_reasons = score_volume(volume, structure)
    entry_score, entry_reasons, entry_risks = score_entry_quality(structure, coil, second_leg, exhaustion)

    news_score = safe_float(news.get("score", 0))

    reasons = []
    risks = []

    reasons.extend(structure_reasons)
    reasons.extend(volume_reasons)

    if coil["detected"]:
        reasons.append("Tight coil / second-leg pressure")

    if second_leg["detected"]:
        reasons.append("Second-leg continuation")

    if news_score >= 8:
        reasons.append(news.get("explain", "Real catalyst"))

    reasons.extend(entry_reasons)

    # Soft fast-pass warnings become awareness, not hard skips.
    risks.extend(fast_warnings)

    # Risk stack
    if news_score <= 2:
        risks.append(news.get("explain", "Weak/no catalyst"))

    risks.extend(structure_risks)
    risks.extend(decay["risks"])
    risks.extend(entry_risks)

    if exhaustion["detected"]:
        risks.append(exhaustion["risk"])

    if sec.get("has_risk"):
        risks.append(sec.get("label"))

    if halt_risk["risk"] == "HIGH":
        risks.append("High halt risk / size carefully")

    score = score_candidate(
        gain=gain,
        structure_score=structure_score,
        volume_score=volume_score,
        news_score=news_score,
        entry_score=entry_score,
        coil=coil,
        second_leg=second_leg,
        decay=decay,
        exhaustion=exhaustion,
        sec=sec,
        regime=regime,
        float_shares=float_shares,
        market_cap=market_cap,
    )

    bias = build_bias(
        score=score,
        structure_score=structure_score,
        entry_score=entry_score,
        news=news,
        risks=risks,
        second_leg=second_leg,
        decay=decay,
        exhaustion=exhaustion,
    )

    phase = build_phase(structure, coil, second_leg, exhaustion, decay)
    entry = build_entry(bias, structure, coil, second_leg, entry_score)

    result = {
        "ticker": ticker,
        "price": price,
        "gain": gain,
        "volume": volume,
        "float": float_shares,
        "market_cap": market_cap,
        "score": score,
        "structure_score": structure_score,
        "volume_score": volume_score,
        "entry_score": entry_score,
        "news_score": news_score,
        "news_label": news.get("label", ""),
        "news_explain": news.get("explain", ""),
        "headline": news.get("headline", ""),
        "news_quality": news.get("quality", ""),
        "reasons": dedupe(reasons),
        "risks": dedupe(risks),
        "bias": bias,
        "entry": entry,
        "phase": phase,
        "structure": structure,
        "coil": coil,
        "second_leg": second_leg,
        "decay": decay,
        "exhaustion": exhaustion,
        "halt_risk": halt_risk,
        "sec": sec,
        "regime": regime,
    }

    print(
        f"[RANK] {ticker} {score:.1f}/10 {bias} "
        f"STR{structure_score:.1f} VOL{volume_score:.1f} ENT{entry_score:.1f} NEWS{news_score:.0f} "
        f"+{gain:.1f}% Phase={phase}"
    )

    return result


# ============================================================
# SCANNER LOOP
# ============================================================

def sort_results(results):
    # Prefer true runner quality over raw gain.
    return sorted(
        results,
        key=lambda r: (
            r["score"],
            1 if "RUNNER" in r["bias"] else 0,
            1 if r["second_leg"]["detected"] else 0,
            r["gain"],
        ),
        reverse=True,
    )


def print_top_ranked(results):
    if not results:
        print("[SCAN] No qualified deep-scan results")
        return

    top = " | ".join(
        f"{r['ticker']} {r['score']:.1f}/10 {r['bias'].replace('🟢 ', '').replace('👀 ', '').replace('⚠️ ', '')} +{r['gain']:.1f}%"
        for r in results[:5]
    )
    print(f"[SCAN] Top ranked: {top}")


def run_scanner():
    print(f"[BOOT] {BOOT_MARKER}")

    while True:
        try:
            if not market_is_active():
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
