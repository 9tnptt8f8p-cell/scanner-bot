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
BOOT_MARKER = "elite scanner v37.10 — strict 25% alert floor + Yahoo cooldown + elite cooldown bypass"

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
SCAN_MIN_GAIN = 5.0                  # scanner can view wider universe internally
PREMARKET_SCAN_MIN_GAIN = 5.0        # legacy constant; premarket scanning disabled
OPEN_SCAN_MIN_GAIN = 5.0
HARD_MIN_GAIN = 8.0                 # regular-hours hard floor
PREMARKET_HARD_MIN_GAIN = 8.0       # legacy constant; premarket scanning disabled
ALERT_MIN_GAIN = 25.0                # v36.6: Telegram alerts require 25%+ gain
PREMARKET_ALERT_MIN_GAIN = 25.0      # legacy constant; premarket scanning disabled
MIN_PRICE = 0.50
MAX_PRICE = 80.0
MIN_FAST_VOLUME = 25_000             # v36.11: discovery-only lowered so BRAI-style ignitions enter early
MIN_DEEP_VOLUME = 150_000
ALERT_MIN_VOLUME = 100_000            # elite phone alerts need confirmation; discovery can be looser
LOW_FLOAT_ALERT_MIN_VOLUME = 50_000    # lets BUUU-style low-float leaders surface earlier
AWARENESS_MIN_SCORE = 6.5              # v37: lower awareness lane in hot tape, still structure-gated
AWARENESS_MIN_VOLUME = 50_000          # awareness volume floor for low-float/top-gainer movers
MIN_FINALIZED_CANDLES = 6              # fresh ignitions no longer die at 7 bars

# v36.40 right-time chart gates — phone alerts must be fresh, not just high % gain.
CHART_MAX_OFF_HIGH_ALERT = 5.5          # best alerts fire very close to HOD
CHART_STALE_OFF_HIGH = 10.0             # hard stale/fade zone for most runners
CHART_MIN_VOLUME_RATIO = 0.95           # recent 3 bars vs prior 3 bars
CHART_FRESH_VOLUME_RATIO = 1.20         # breakout alerts need expanding current volume
CHART_RE_ALERT_HIGH_MULTIPLIER = 1.012  # require a true new HOD area for repeats
CHART_HOD_RECENT_BARS = 5               # HOD/breakout must be recent, not an old morning spike
CHART_STALE_HOD_BARS = 25               # old HOD with no reclaim/new push is stale
CHART_BUILDING_SCORE_CAP = 7.4          # never elite-alert a chart labeled not ready
CHART_NOT_FRESH_SCORE_CAP = 7.6         # no fresh trigger = visibility only
MAX_FLOAT = 80_000_000               # fixed: 40M was too nuclear in thin markets
MAX_MARKET_CAP = 1_200_000_000       # fixed: cap is awareness unless extreme
EXTREME_FLOAT_SKIP = 150_000_000     # only hard skip truly heavy floats
EXTREME_MARKET_CAP_SKIP = 3_000_000_000

# ============================================================
# v33 PURE LEADER UNIVERSE — NO WATCHLISTS
# ============================================================
DISCOVERY_MIN_GAIN = 5.0       # internal discovery only
RUNNER_MIN_GAIN = 25.0         # v36.6: hard Telegram floor = 25%+
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
ALERT_MIN_SCORE = 8.0                 # base elite floor; v37 dynamic floor adjusts by market regime
WATCH_ALERT_MIN_SCORE = 7.0           # internal rank/watch labels only; NOT a Telegram lane
WATCH_ALERT_MIN_ENTRY_SCORE = 3.5     # internal display compatibility only
WATCH_ALERT_MIN_STRUCTURE_SCORE = 3.0 # internal display compatibility only
TRADE_ALERT_MIN_ENTRY_SCORE = 3.5
TRADE_ALERT_MIN_STRUCTURE_SCORE = 3.0
ALERT_HARD_MIN_GAIN = 25.0            # Telegram hard floor: no alert under +25%, ever
CONTINUATION_MIN_GAIN = 25.0          # strict mode: continuation lane may rank, but never alerts under 25%
CONTINUATION_MIN_SCORE = 7.5          # internal display/ranking only; phone alerts still require 25%+
SUPER_MOMO_MIN_SCORE = 8.0            # no score bypass below 8.0
SUPER_MOMO_MIN_GAIN = 60.0
SUPER_MOMO_MIN_VOLUME = 100_000_000
EARLY_OPEN_DRIVE_GAIN = 25.0          # v36.6: alerts require 25%+ gain, even open-drive
MAX_GAINERS = 180                 # v36.8: wider fresh-mover pool so BRAI-style ignitions are not missed
MAX_ALERTS_PER_CYCLE = 5
SCAN_SLEEP = 45

# Render/free-tier protection:
# When the market is closed, do not keep polling APIs every 60-300 seconds.
# This will not magically stop Render from counting a live service instance,
# but it greatly reduces API burn, logs, CPU churn, and restart noise.
CLOSED_MARKET_SLEEP_SECONDS = 1800      # 30 min after-hours / before 7:30 ET
WEEKEND_SLEEP_SECONDS = 3600            # 60 min weekends
CLOSED_ERROR_SLEEP_SECONDS = 300        # slower retry if errors happen off-hours

FRESH_MOVER_MIN_GAIN = 25.0
FRESH_MOVER_ACCEL_GAIN = 10.0
FRESH_MOVER_SCAN_BOOST = 2.5

ALERT_COOLDOWN_SECONDS = 900
EARLY_ALERT_COOLDOWN_SECONDS = 600
RE_ALERT_NEW_HIGH_MULTIPLIER = 1.05
EARLY_RE_ALERT_NEW_HIGH_MULTIPLIER = 1.03

# Caches
CACHE_TTL_SECONDS = 1800
SHORT_CACHE_TTL_SECONDS = 60

PROFILE_CACHE = {}
QUOTE_CACHE = {}
LAST_GOOD_QUOTES = {}
NEWS_CACHE = {}
SEC_CACHE = {}
CANDLE_CACHE = {}
LAST_GOOD_CANDLES = {}
YAHOO_CANDLE_BLOCK_UNTIL = 0
YAHOO_CANDLE_429_COUNT = 0
YAHOO_GAINERS_BLOCK_UNTIL = 0
YAHOO_GAINERS_429_COUNT = 0
MARKET_REGIME_CACHE = {}
DAILY_CONTEXT_CACHE = {}
FRESHNESS_STATE = {}
FRESH_LEADER_STATE = {}

LAST_ALERT = {}
LAST_EARLY_ALERT = {}
SENT_THIS_CYCLE = set()

# Output preferences
SHOW_FLOAT = True
SHOW_HEADLINE = False
SHOW_VERBOSE_DEBUG = True

# ============================================================
# v36.12 LIVE SPEED MODE
# ============================================================
# Keep discovery wide, but do not let slow news/SEC/daily calls block every ticker.
LIVE_SPEED_MODE = True
MAX_DEEP_SCAN_NAMES = 60
MAX_NEWS_NAMES_PER_CYCLE = 22
MAX_PRIORITY_NEWS_NAMES_PER_CYCLE = 36  # v36.17: let high-priority runners bypass normal speed cap
MAX_SEC_NAMES_PER_CYCLE = 10
ENABLE_PRNEWSWIRE_INTRADAY = True
NEWS_FAST_TIMEOUT = 2.5
NEWS_PARALLEL_TIMEOUT = 3.8
GLOBE_TIMEOUT = 2.5
YAHOO_NEWS_TIMEOUT = 2.5
SEC_FAST_TIMEOUT = 2.0
DAILY_CONTEXT_MIN_GAIN = 18.0

NEWS_CALLS_THIS_CYCLE = 0
SEC_CALLS_THIS_CYCLE = 0


# ============================================================
# v33.2 MULTI-SOURCE LEADER DISCOVERY
# ============================================================
DISCOVERY_MIN_GAIN = 5.0
RUNNER_MIN_GAIN = 20.0
ALERT_MIN_GAIN = 25.0

LEADER_SOURCE_LIMIT = 250
MAX_RAW_LEADER_POOL = 700

# Source-layer small-cap focus. Unknown cap is allowed because small-cap feeds
# often have incomplete data and the quote/profile step can verify later.
SOURCE_MAX_MARKET_CAP = 3_000_000_000
SOURCE_MAX_FLOAT = 150_000_000
SOURCE_MIN_PRICE = 0.20
SOURCE_MAX_PRICE = 80.00
SOURCE_MIN_VOLUME = 25_000


# ============================================================
# v33.2.1 MULTI-SOURCE LEADER DISCOVERY CONSTANTS
# ============================================================
DISCOVERY_MIN_GAIN = 5.0
RUNNER_MIN_GAIN = 25.0
ALERT_MIN_GAIN = 25.0
LEADER_SOURCE_LIMIT = 250
MAX_RAW_LEADER_POOL = 700
SOURCE_MAX_MARKET_CAP = 3_000_000_000
SOURCE_MAX_FLOAT = 150_000_000
SOURCE_MIN_PRICE = 0.20
SOURCE_MAX_PRICE = 80.00
SOURCE_MIN_VOLUME = 25_000

# ============================================================
# FLASK KEEPALIVE
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return "scanner alive — v37.10 strict 25% alert floor + Yahoo cooldown + elite cooldown bypass", 200


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

    # Regular-hours only: no premarket scan because source/candle percent data is unreliable.
    if dtime(9, 20) <= now.time() <= dtime(16, 10):
        return True

    print(f"[MARKET] Alerts OFF — {now.strftime('%I:%M %p ET')}")
    return False




def seconds_until_next_scan_window(now=None):
    """
    Render-safe idle timer.

    Scanner window is 9:20 AM ET -> 4:10 PM ET on weekdays.
    Outside that window, sleep close to the next useful scan time, capped so
    logs/health checks still show life occasionally.
    """
    now = now or now_et()

    # Weekend: sleep in long blocks until Monday regular-hours scanner window.
    if now.weekday() >= 5:
        return WEEKEND_SLEEP_SECONDS

    today_start = datetime.combine(now.date(), dtime(9, 20), tzinfo=ET)
    today_end = datetime.combine(now.date(), dtime(16, 10), tzinfo=ET)

    if now < today_start:
        return max(300, min(CLOSED_MARKET_SLEEP_SECONDS, int((today_start - now).total_seconds())))

    if now > today_end:
        # Next weekday 7:30 AM ET.
        next_day = now.date() + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        next_start = datetime.combine(next_day, dtime(9, 20), tzinfo=ET)
        return max(300, min(CLOSED_MARKET_SLEEP_SECONDS, int((next_start - now).total_seconds())))

    return dynamic_scan_sleep() if "dynamic_scan_sleep" in globals() else SCAN_SLEEP


def get_market_session_label():
    t = now_et().time()
    if dtime(9, 20) <= t < dtime(9, 30):
        return "PRE-OPEN WARMUP"
    if dtime(9, 30) <= t < dtime(11, 0):
        return "OPENING MOMENTUM"
    if dtime(11, 0) <= t < dtime(14, 30):
        return "MIDDAY"
    if dtime(14, 30) <= t <= dtime(16, 10):
        return "POWER HOUR"
    return "CLOSED"


def is_premarket_session():
    # v36.15: premarket scanning disabled; keep function for backward compatibility.
    return False


def dynamic_scan_min_gain():
    return PREMARKET_SCAN_MIN_GAIN if is_premarket_session() else OPEN_SCAN_MIN_GAIN


def dynamic_hard_min_gain():
    return PREMARKET_HARD_MIN_GAIN if is_premarket_session() else HARD_MIN_GAIN


def dynamic_alert_min_gain(result=None):
    return ALERT_MIN_GAIN


def dynamic_scan_sleep():
    """
    v36.1: faster open scans so CPSH-style ignition moves are not missed
    between 90-second cycles.
    """
    session = get_market_session_label()
    if session == "OPENING MOMENTUM":
        return 20
    if session == "MIDDAY":
        return 25
    if session == "POWER HOUR":
        return 20
    return SCAN_SLEEP


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
    min_vol = 25_000 if is_premarket_session() else MIN_FAST_VOLUME
    if volume and volume < min_vol:
        reasons.append(f"volume under {fmt_big_num(min_vol)}")

    # v36.6: float is awareness only. Never skip or penalize for float.
    if float_shares and float_shares > MAX_FLOAT:
        warnings.append(f"large float {fmt_big_num(float_shares)}")

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
    """v36.6: float is display/awareness only. No boost, no penalty, no skip."""
    f = safe_int(float_shares)
    if f <= 0:
        return {"tier": "UNKNOWN", "boost": 0.0, "label": "⚠️ Float unknown", "risk": ""}
    if f <= LOW_FLOAT_TINY:
        return {"tier": "TINY", "boost": 0.0, "label": f"🔥 TINY FLOAT {fmt_big_num(f)}", "risk": ""}
    if f <= LOW_FLOAT_ELITE:
        return {"tier": "ELITE", "boost": 0.0, "label": f"🔥 LOW FLOAT {fmt_big_num(f)}", "risk": ""}
    if f <= LOW_FLOAT_GOOD:
        return {"tier": "GOOD", "boost": 0.0, "label": f"🟢 LOW FLOAT {fmt_big_num(f)}", "risk": ""}
    if f <= LOW_FLOAT_ACCEPTABLE:
        return {"tier": "ACCEPTABLE", "boost": 0.0, "label": f"🟡 Float {fmt_big_num(f)}", "risk": ""}
    return {"tier": "HIGH", "boost": 0.0, "label": f"Float {fmt_big_num(f)}", "risk": ""}


def leader_gain_boost(gain):
    g = safe_float(gain)
    if g >= 100:
        return 1.25, "💯 100%+ day leader"
    if g >= 75:
        return 0.95, "🔥 75%+ day leader"
    if g >= 50:
        return 0.70, "🔥 50%+ day leader"
    if g >= RUNNER_MIN_GAIN:
        return 0.35, "🟢 25%+ momentum leader"
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



def yahoo_gainers_is_rate_limited(response):
    """Detect Yahoo screener 429/Too Many Requests and cool down source calls."""
    text = getattr(response, "text", "") or ""
    status = getattr(response, "status_code", 0)
    return status == 429 or "Too Many Requests" in text


def set_yahoo_gainers_cooldown(label="Yahoo"):
    global YAHOO_GAINERS_BLOCK_UNTIL, YAHOO_GAINERS_429_COUNT
    YAHOO_GAINERS_429_COUNT += 1
    cooldown = min(600, 300 + (YAHOO_GAINERS_429_COUNT - 1) * 60)
    YAHOO_GAINERS_BLOCK_UNTIL = time.time() + cooldown
    print(f"[YAHOO GAINERS 429] {label}: blocking Yahoo gainers for {cooldown}s")


def yahoo_gainers_cooling_down():
    if time.time() < YAHOO_GAINERS_BLOCK_UNTIL:
        left = int(YAHOO_GAINERS_BLOCK_UNTIL - time.time())
        print(f"[YAHOO GAINERS BLOCKED] cooling down {left}s after rate limit")
        return True
    return False

def get_yahoo_predefined_gainers():
    if yahoo_gainers_cooling_down():
        return []
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": "day_gainers", "count": 250, "formatted": "false"}

    try:
        r = http_get(url, params=params, timeout=8)
        if yahoo_gainers_is_rate_limited(r):
            set_yahoo_gainers_cooldown("Yahoo predefined")
            return []
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
    if yahoo_gainers_cooling_down():
        return []
    # Yahoo's screener endpoint often rejects one host while the other still works.
    # Try query1 first, then query2 with the exact same payload before giving up.
    urls = [
        "https://query1.finance.yahoo.com/v1/finance/screener",
        "https://query2.finance.yahoo.com/v1/finance/screener",
    ]
    all_results = []

    # v33: small-cap/high-percent scans first. If Yahoo returns none, predefined still works.
    scans = [
        (50.0, 0, SMALL_CAP_MAX, 80.0, "Yahoo smallcap 50pct"),
        (27.0, 50_000, SMALL_CAP_MAX, 80.0, "Yahoo smallcap 27pct liquid"),
        (15.0, 100_000, SMALL_CAP_MAX, 40.0, "Yahoo smallcap 15pct lowprice"),
        (8.0, 250_000, SMALL_CAP_MAX, 25.0, "Yahoo smallcap 8pct volume"),
        (25.0, 50_000, None, 80.0, "Yahoo anycap 25pct fresh backup"),
        (15.0, 250_000, None, 80.0, "Yahoo anycap 15pct liquid backup"),
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
            data = None
            for url in urls:
                r = http_post(url, payload=payload, timeout=8)
                if yahoo_gainers_is_rate_limited(r):
                    set_yahoo_gainers_cooldown(label)
                    return merge_source_items(all_results)
                data = safe_json_response(r, f"GAINERS {label}")
                if data:
                    break
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

    if not all_results:
        print("[YAHOO FALLBACK] custom screener empty — using predefined leaders + external sources")

    return merge_source_items(all_results)


def get_yahoo_gainers():
    if yahoo_gainers_cooling_down():
        return []
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

    if market_cap and market_cap > SOURCE_MAX_MARKET_CAP and gain < RUNNER_MIN_GAIN:
        return False

    # v36.6: do not source-filter by float. Let volume + price action decide.
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
        "f": "sh_avgvol_o100,sh_price_u80,ta_change_u5",
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




def get_tradingview_gainers():
    """
    v36.9 extra discovery source.
    TradingView's scanner often catches fresh small-cap movers before Yahoo's
    custom screener wakes up. Discovery only; alerts still require 8+/25%/VWAP.
    """
    url = "https://scanner.tradingview.com/america/scan"
    payload = {
        "filter": [
            {"left": "type", "operation": "equal", "right": "stock"},
            {"left": "subtype", "operation": "in_range", "right": ["common", "foreign-issuer", "preferred"]},
            {"left": "exchange", "operation": "in_range", "right": ["NASDAQ", "NYSE", "AMEX"]},
            {"left": "change", "operation": "greater", "right": DISCOVERY_MIN_GAIN},
            {"left": "close", "operation": "greater", "right": SOURCE_MIN_PRICE},
            {"left": "close", "operation": "less", "right": SOURCE_MAX_PRICE},
            {"left": "volume", "operation": "greater", "right": SOURCE_MIN_VOLUME},
        ],
        "options": {"lang": "en"},
        "markets": ["america"],
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change", "volume", "market_cap_basic"],
        "sort": {"sortBy": "change", "sortOrder": "desc"},
        "range": [0, LEADER_SOURCE_LIMIT],
    }

    results = []
    try:
        r = http_post(url, payload=payload, timeout=8)
        data = safe_json_response(r, "GAINERS TradingView")
        rows = (data or {}).get("data") or []
        for row in rows:
            d = row.get("d") or []
            if len(d) < 4:
                continue
            ticker = str(d[0] or "").upper().strip()
            item = normalize_leader_item(
                ticker=ticker,
                price=safe_float(d[1] if len(d) > 1 else 0),
                gain=safe_float(d[2] if len(d) > 2 else 0),
                volume=safe_int(d[3] if len(d) > 3 else 0),
                market_cap=safe_int(d[4] if len(d) > 4 else 0),
                source="TradingView",
            )
            if source_pass_item(item):
                results.append(item)
        print(f"[GAINERS] TradingView returned {len(results)} filtered leaders")
    except Exception as e:
        print(f"[GAINERS ERROR] TradingView: {e}")
    return results



def get_webull_gainers():
    """
    v36.11 stable Webull discovery source.

    Webull is optional discovery only:
    - no duplicate endpoint retries
    - no scanner slowdown if Webull rejects the request
    - no hard dependency on Webull response shape
    - alerts still require the normal deep-scan validation
    """
    url = "https://quotes-gw.webullfintech.com/api/wlas/ranking/region/6/page/1/list"
    params = {
        "deviceId": "scannerbot",
        "sortType": 3,
        "pageSize": 100,
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://app.webull.com",
        "Referer": "https://app.webull.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    def _pick(obj, keys, default=0):
        for key in keys:
            if isinstance(obj, dict) and obj.get(key) not in [None, ""]:
                return obj.get(key)
        return default

    def _extract_rows(data):
        if not isinstance(data, dict):
            return []

        node = data.get("data")

        if isinstance(node, dict):
            rows = (
                node.get("list")
                or node.get("rankList")
                or node.get("items")
                or node.get("data")
                or []
            )
            if isinstance(rows, list):
                return rows

        if isinstance(node, list):
            return node

        rows = data.get("list") or data.get("rankList") or data.get("items") or []
        return rows if isinstance(rows, list) else []

    results = []
    seen = set()

    try:
        r = http_get(url, params=params, headers=headers, timeout=4)

        if getattr(r, "status_code", 0) != 200:
            # Webull commonly rejects cloud requests with 417.
            # Keep this quiet and let Yahoo/TradingView/StockAnalysis carry discovery.
            print(f"[WEBULL] unavailable status={getattr(r, 'status_code', 'NA')} — skipped")
            return []

        data = safe_json_response(r, "GAINERS Webull")
        rows = _extract_rows(data)

        if not rows:
            print("[WEBULL] empty leaderboard — skipped")
            return []

        for row in rows:
            if not isinstance(row, dict):
                continue

            ticker = str(_pick(row, ["symbol", "disSymbol"], "")).upper().strip()

            raw_ticker = row.get("ticker")
            if not ticker and isinstance(raw_ticker, dict):
                ticker = str(_pick(raw_ticker, ["symbol", "disSymbol"], "")).upper().strip()
            elif not ticker and isinstance(raw_ticker, str):
                ticker = raw_ticker.upper().strip()

            if not ticker or ticker in seen or is_bad_ticker(ticker):
                continue

            raw_gain = _pick(
                row,
                ["changeRatio", "changeRate", "changePercent", "pctChange", "change"],
                0,
            )
            gain = safe_float(raw_gain)

            # Webull often returns ratio form like 0.312 for +31.2%.
            if 0 < abs(gain) <= 3:
                gain *= 100

            price = safe_float(_pick(row, ["close", "price", "lastPrice", "pPrice", "tradePrice"], 0))
            volume = safe_int(_pick(row, ["volume", "vol", "turnoverVolume"], 0))
            market_cap = safe_int(_pick(row, ["marketValue", "marketCap", "totalMarketValue"], 0))

            # Discovery only. Do not let bad/missing Webull fields create junk.
            if gain < DISCOVERY_MIN_GAIN:
                continue
            if price and (price < SOURCE_MIN_PRICE or price > SOURCE_MAX_PRICE):
                continue
            if volume and volume < SOURCE_MIN_VOLUME:
                continue

            item = normalize_leader_item(
                ticker=ticker,
                price=price,
                gain=gain,
                volume=volume,
                market_cap=market_cap,
                source="Webull",
            )

            seen.add(ticker)
            if source_pass_item(item) or gain >= dynamic_hard_min_gain():
                results.append(item)

        print(f"[GAINERS] Webull returned {len(results)} filtered leaders")
        return results

    except Exception as e:
        print(f"[WEBULL ERROR] {e}")
        return []


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
        sources = x.get("sources", []) or []
        source_count = len(sources)
        stockanalysis_bonus = 1 if "StockAnalysis" in sources else 0
        small_cap_bonus = 1 if (cap and cap <= SOURCE_MAX_MARKET_CAP) or not cap else 0
        leader_bonus = 3 if gain >= 50 else 2 if gain >= 27 else 1 if gain >= 15 else 0
        return (leader_bonus, stockanalysis_bonus, gain, source_count, small_cap_bonus, volume)

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

    # v37: primary discovery is StockAnalysis + TradingView because Yahoo custom
    # screeners often return zero during the open. Yahoo still fills quote fields,
    # but it no longer controls who gets into the deep-scan queue.
    sources.extend(get_stockanalysis_gainers())
    sources.extend(get_tradingview_gainers())

    try:
        sources.extend(get_yahoo_gainers())
    except Exception as e:
        print(f"[GAINERS ERROR] Yahoo source stack: {e}")

    sources.extend(get_webull_gainers())
    sources.extend(get_nasdaq_gainers())
    sources.extend(get_finviz_smallcap_gainers())
    sources.extend(get_marketwatch_gainers())

    merged = merge_leader_sources(sources)
    print(f"[LEADERS] multi-source merged {len(merged)} candidates from {len(sources)} raw filtered names")
    return merged



def calc_fresh_leader_scan_boost(item):
    """
    v36.8 source-layer fresh mover injector.
    This does not send an alert by itself. It only pushes brand-new or
    rapidly accelerating leaders higher into the deep-scan queue so names
    like BRAI are less likely to be missed between refreshes.
    """
    ticker = str(item.get("ticker", "")).upper().strip()
    if not ticker:
        return 0.0

    key = _fresh_key(ticker) if "_fresh_key" in globals() else f"{trading_day_key()}:{ticker}"
    gain = safe_float(item.get("gain"))
    price = safe_float(item.get("price"))
    volume = safe_int(item.get("volume"))
    prev = FRESH_LEADER_STATE.get(key)

    boost = 0.0
    if not prev:
        if gain >= 50:
            boost += 3.0
        elif gain >= FRESH_MOVER_MIN_GAIN:
            boost += FRESH_MOVER_SCAN_BOOST
    else:
        prev_gain = safe_float(prev.get("gain"))
        prev_price = safe_float(prev.get("price"))
        prev_volume = safe_int(prev.get("volume"))

        gain_delta = gain - prev_gain
        if gain_delta >= 20:
            boost += 3.0
        elif gain_delta >= FRESH_MOVER_ACCEL_GAIN:
            boost += 2.0
        elif gain_delta >= 5:
            boost += 1.0

        if prev_price > 0 and price >= prev_price * 1.05:
            boost += 1.0
        if prev_volume > 0 and volume >= prev_volume * 1.75:
            boost += 0.75

    FRESH_LEADER_STATE[key] = {"time": time.time(), "gain": gain, "price": price, "volume": volume}
    if boost >= 2.0:
        print(f"[FRESH INJECT] {ticker}: source boost {boost:.1f} gain={gain:.1f}% vol={fmt_big_num(volume)}")
    return boost


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
    for c in candidates:
        c["fresh_scan_boost"] = calc_fresh_leader_scan_boost(c)

    candidates.sort(
        key=lambda x: (
            safe_float(x.get("fresh_scan_boost", 0)),
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

        r = http_get(url, params=params, headers=headers, timeout=SEC_FAST_TIMEOUT)
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
        r = http_get(url, params=params, headers=headers, timeout=SEC_FAST_TIMEOUT)

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


def finalized_candles(candles, min_keep=MIN_FINALIZED_CANDLES):
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
    if not candles or len(candles) < MIN_FINALIZED_CANDLES:
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

    recent = candles[-min(8, len(candles)):]
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

    recent_range_candles = candles[-min(5, len(candles)): ]
    prev_range_candles = candles[-10:-5] if len(candles) >= 10 else candles[:max(1, len(candles)//2)]
    range_now = max(candle_high(c) for c in recent_range_candles) - min(candle_low(c) for c in recent_range_candles)
    range_prev = max(candle_high(c) for c in prev_range_candles) - min(candle_low(c) for c in prev_range_candles)
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
        "multi-year agreement", "master services agreement", "award", "purchase agreement", "commercial agreement", "customer win",
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
        "strategic alliance", "joint venture", "letter of intent", "strategic relationship", "teaming agreement",
    ],
    "Infrastructure / Facility": [
        "facility", "battery", "manufacturing", "buildout", "production capacity", "capacity expansion", "new plant", "pilot production",
    ],
}

WEAK_NEWS_PATTERNS = {
    "Compliance": ["regains compliance", "nasdaq compliance", "bid price compliance"],
    "Presentation": ["conference", "presentation", "webcast", "fireside chat"],
    "Generic Update": ["corporate update", "business update", "announces update"],
    "Product": ["launches", "unveils", "introduces"],
}


def news_rank_from_score_quality(score, quality, category=""):
    """Simple trader-facing news rank: A+ matters, C is weak/speculative, D/F is junk/trap."""
    score = safe_float(score)
    q = (quality or "").upper()
    cat = (category or "").lower()

    if q in ["NEGATIVE"] or "offering" in cat:
        return "F", "Dilution/news trap"
    if q in ["JUNK", "STALE"]:
        return "D", "Junk/stale news"
    if q == "NONE" or score <= 0:
        return "D", "No confirmed catalyst"
    if score >= 9:
        return "A+", "Elite catalyst"
    if score >= 8:
        return "A", "Strong catalyst"
    if score >= 5:
        return "B", "Decent catalyst"
    if score >= 3:
        return "C", "Weak/speculative"
    return "D", "Low-quality news"


def compact_news_line(news):
    news = news or {}
    rank = news.get("rank") or news_rank_from_score_quality(news.get("score", 0), news.get("quality", ""), news.get("category", ""))[0]
    meaning = news.get("rank_meaning") or news_rank_from_score_quality(news.get("score", 0), news.get("quality", ""), news.get("category", ""))[1]
    category = news.get("category") or "Catalyst"
    if rank in ["A+", "A", "B"]:
        return f"NEWS: {rank} — {category}"
    if rank == "C":
        return f"NEWS: C — {meaning}"
    return f"NEWS: {rank} — {meaning}"


def compact_dilution_label(sec):
    """Dilution is awareness only. Only call out same-day/very fresh offering as urgent."""
    if not isinstance(sec, dict) or not sec.get("has_risk"):
        return ""

    severity = sec.get("severity", "")
    forms = "/".join((sec.get("forms") or [])[:3])
    age = sec.get("filing_age_days")
    category = (sec.get("category") or "").lower()
    atm = bool(sec.get("atm_active"))

    if severity == "HIGH" and age is not None and age <= 1:
        return f"Offering risk TODAY ({forms or 'filing'})"
    if severity == "HIGH" and (atm or "atm" in category or "sales agreement" in category):
        return "Company can sell shares anytime"
    if severity in ["HIGH", "MEDIUM"]:
        return "Financing ability on file"
    if severity == "LOW":
        return "SEC filings on file"
    return ""

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


def classify_news_raw(headline, ticker=None):
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


def classify_news(headline, ticker=None):
    news = classify_news_raw(headline, ticker)
    rank, meaning = news_rank_from_score_quality(news.get("score", 0), news.get("quality", ""), news.get("category", ""))
    news["rank"] = rank
    news["rank_meaning"] = meaning
    news["compact_line"] = compact_news_line(news)
    return news

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
        r = http_get(url, timeout=YAHOO_NEWS_TIMEOUT)
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
        r = http_get(url, params=params, timeout=NEWS_FAST_TIMEOUT)
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
        r = http_get(url, timeout=GLOBE_TIMEOUT)
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
        r = http_get(url, params=params, timeout=NEWS_FAST_TIMEOUT)
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
        r = http_get(url, params=params, timeout=NEWS_FAST_TIMEOUT)
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


def get_best_news(ticker, priority=False):
    """
    v37 news stack: do not let Finnhub timeouts decide catalyst quality.

    Priority order:
    1) GlobeNewswire
    2) PR Newswire
    3) Yahoo News
    4) Benzinga, if key exists
    5) Finnhub last, because it timed out heavily in live logs
    """
    global NEWS_CALLS_THIS_CYCLE

    cached = cached_get(NEWS_CACHE, ticker)
    # If a previous low-priority pass cached a speed-mode skip/no-news, a priority
    # runner gets one real refresh so names like NAMM/AIIO do not stay "NO NEWS".
    if cached and not (priority and str(cached.get("quality", "")).upper() in {"NONE", "JUNK", "STALE"}):
        return cached

    normal_cap_hit = LIVE_SPEED_MODE and NEWS_CALLS_THIS_CYCLE >= MAX_NEWS_NAMES_PER_CYCLE
    priority_cap_hit = LIVE_SPEED_MODE and NEWS_CALLS_THIS_CYCLE >= MAX_PRIORITY_NEWS_NAMES_PER_CYCLE

    if normal_cap_hit and (not priority or priority_cap_hit):
        news = classify_news("", ticker)
        news["explain"] = "Skipped news lookup in speed mode"
        tag = "priority cap reached" if priority else "speed-mode news cap reached"
        print(f"[NEWS SKIP] {ticker}: {tag}")
        return cached_set(NEWS_CACHE, ticker, news)

    if normal_cap_hit and priority:
        print(f"[NEWS PRIORITY] {ticker}: bypassing normal speed cap")

    NEWS_CALLS_THIS_CYCLE += 1
    all_headlines = []

    # Run publisher/PR sources first. These are more likely to contain the real
    # catalyst than generic quote pages, and they avoid Finnhub timeout traps.
    primary_sources = [scrape_globenewswire, scrape_prnewswire, scrape_yahoo_news]
    if BENZINGA_API_KEY:
        primary_sources.append(fetch_benzinga_news)

    try:
        with ThreadPoolExecutor(max_workers=min(4, len(primary_sources))) as executor:
            futures = {executor.submit(fn, ticker): fn.__name__ for fn in primary_sources}
            for fut in as_completed(futures, timeout=NEWS_PARALLEL_TIMEOUT):
                name = futures[fut]
                try:
                    all_headlines.extend(fut.result(timeout=0.1) or [])
                except Exception as e:
                    print(f"[NEWS ERROR] {ticker} {name}: {e}")
    except Exception as e:
        print(f"[NEWS TIMEBOX] {ticker}: primary news timebox hit ({e})")

    ranked = rank_news_candidates(all_headlines, ticker)
    if ranked and safe_float(ranked[0].get("score", 0)) >= 7:
        best = ranked[0]
        print(f"[NEWS] {ticker}: {best.get('headline','')[:120]} ({best['quality']} {best['score']}/10 PRIMARY)")
        return cached_set(NEWS_CACHE, ticker, best)

    # Finnhub is last. If it works, great. If it times out, the scanner already
    # had the best public PR/news sources first and will not freeze on it.
    fh = fetch_finnhub_company_news(ticker)
    if fh:
        all_headlines.extend(fh)
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
    # Trader-focused: dilution is awareness only, not a score killer.
    form_txt = "/".join(forms[:3]) if forms else "filing"
    cat = (category or "").lower()

    if severity == "HIGH" and "today" in str(age_bucket).lower():
        return f"Offering risk TODAY ({form_txt})"
    if severity == "HIGH" and ("atm" in cat or "sales agreement" in cat):
        return "Company can sell shares anytime"
    if severity in ["HIGH", "MEDIUM"]:
        return "Financing ability on file"
    if severity == "LOW":
        return "SEC filings on file"
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

        r = http_get(url, params=params, headers=headers, timeout=SEC_FAST_TIMEOUT)
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
        score_penalty += 0.8 if premarket else 1.0

    if previous_vol > 0 and recent_vol < previous_vol * 0.60:
        risks.append("Momentum decay / volume fading")
        # Premarket often has uneven 1-min volume; do not over-punish leaders still holding highs.
        if premarket and holding_continuation:
            score_penalty += 0.35
        elif premarket:
            score_penalty += 0.45
        else:
            score_penalty += 0.65

    if bad_structure:
        risks.append("Bad structure / failed momentum")
        score_penalty += 0.55 if (premarket and holding_continuation) else 0.85

    if big_upper_wick:
        risks.append("Big upper wick / possible trap")
        score_penalty += 0.35 if premarket else 0.65

    if raw_decay and "Momentum decay / volume fading" not in risks:
        risks.append("Momentum decay / wait for reclaim")
        score_penalty += 0.25 if premarket else 0.65

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

    # v35.0: high score means quality. Do not label 8–10 scores as AVOID just because
    # entry is extended/stuffed; that is entry-risk, not ticker-quality.
    if score >= 8.0 and ("RUNNER" in bias or continuation_phase or entry_score >= 3.5):
        return "🔥 TRADEABLE" if entry_score >= 4.0 and structure_score >= 3.5 else "🟢 RUNNER WATCH"
    if score >= 7.0 and ("RUNNER" in bias or "WATCH" in bias or continuation_phase):
        return "🟢 RUNNER WATCH"

    # Only true broken/low-score names get AVOID.
    if (stuffed or "AVOID" in bias) and score < 6.5:
        return "⚠️ AVOID / WAIT"
    if fading and not continuation_phase and score < 6.5:
        return "⚠️ AVOID / WAIT"
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
# FRESHNESS + DAILY CONTEXT ENGINE v34.8
# Keeps alerts clean while ranking fresh QTEX-style names higher.
# ============================================================

def trading_day_key():
    return now_et().strftime("%Y-%m-%d")


def _fresh_key(ticker):
    return f"{trading_day_key()}:{ticker.upper().strip()}"


def calc_freshness_boost(ticker, price, gain, volume, structure):
    """
    Rank fresh names higher without creating new alert categories.
    This favors first-discovery leaders, gain acceleration, volume expansion,
    and fresh highs. It penalizes recycled/re-alerted names lightly.
    """
    key = _fresh_key(ticker)
    prev = FRESHNESS_STATE.get(key)
    score = 0.0
    reasons = []

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    near_high = bool(get_struct(structure, "near_high", False))
    recent_vol = safe_int(get_struct(structure, "recent_volume", 0))
    previous_vol = safe_int(get_struct(structure, "previous_volume", 0))

    if not prev:
        if gain >= 50:
            score += 1.00
            reasons.append("fresh high-percent leader")
        elif gain >= RUNNER_MIN_GAIN:
            score += 0.65
            reasons.append("fresh runner candidate")
    else:
        prev_gain = safe_float(prev.get("gain"))
        prev_price = safe_float(prev.get("price"))
        prev_volume = safe_int(prev.get("volume"))

        gain_delta = gain - prev_gain
        if gain_delta >= 20:
            score += 1.20
            reasons.append("rapid gain acceleration")
        elif gain_delta >= 10:
            score += 0.85
            reasons.append("gain accelerating")
        elif gain_delta >= 5:
            score += 0.35

        if prev_price > 0 and price >= prev_price * 1.05:
            score += 0.70
            reasons.append("fresh price expansion")
        elif prev_price > 0 and price >= prev_price * 1.03:
            score += 0.35

        if prev_volume > 0 and volume >= prev_volume * 1.75:
            score += 0.50
            reasons.append("volume expanding vs prior scan")

    if recent_vol and previous_vol and recent_vol >= previous_vol * 1.50:
        score += 0.45
        reasons.append("recent volume ignition")

    if above_vwap and near_high and gain >= RUNNER_MIN_GAIN:
        score += 0.35
        reasons.append("fresh high/VWAP pressure")

    # Recycled names can still run, but new names should outrank stale repeaters.
    if ticker in LAST_ALERT:
        score -= 0.35

    FRESHNESS_STATE[key] = {
        "time": time.time(),
        "price": price,
        "gain": gain,
        "volume": volume,
    }

    return {"score": clamp(score, -1.0, 2.75), "reasons": dedupe(reasons)}


def get_yahoo_daily_candles(ticker):
    cached = cached_get(DAILY_CONTEXT_CACHE, ticker, ttl=900)
    if cached:
        return cached

    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote_plus(ticker)}"
        params = {"interval": "1d", "range": "1y", "includePrePost": "false"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Referer": f"https://finance.yahoo.com/quote/{quote_plus(ticker)}/chart",
        }
        r = http_get(url, params=params, headers=headers, timeout=SEC_FAST_TIMEOUT)
        data = safe_json_response(r, f"DAILY Yahoo {ticker}")
        if not isinstance(data, dict):
            return []

        result = ((data.get("chart") or {}).get("result") or [])
        if not result:
            return []
        node = result[0] or {}
        timestamps = node.get("timestamp") or []
        quote = (((node.get("indicators") or {}).get("quote") or [{}])[0]) or {}
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []

        candles = []
        max_len = min(len(timestamps), len(highs), len(lows), len(closes))
        for i in range(max_len):
            h, l, c = highs[i], lows[i], closes[i]
            if None in [h, l, c]:
                continue
            candles.append({
                "time": timestamps[i],
                "high": safe_float(h),
                "low": safe_float(l),
                "close": safe_float(c),
                "volume": safe_int(volumes[i] if i < len(volumes) else 0),
            })

        if candles:
            print(f"[DAILY] {ticker}: Yahoo {len(candles)} daily bars")
            return cached_set(DAILY_CONTEXT_CACHE, ticker, candles)
    except Exception as e:
        print(f"[DAILY ERROR] {ticker}: {e}")

    return []


def calc_daily_context(ticker, price):
    """
    Lightweight daily-chart context. No new categories; only score/ranking support
    plus one short alert line when there is a true daily breakout/52W high.
    """
    candles = get_yahoo_daily_candles(ticker)
    if not candles or len(candles) < 20 or price <= 0:
        return {"score": 0.0, "label": "", "reasons": [], "risk": ""}

    # Exclude the current/last daily bar so intraday price can be compared to prior resistance.
    prior = candles[:-1] if len(candles) > 1 else candles
    highs = [safe_float(c.get("high")) for c in prior if safe_float(c.get("high")) > 0]
    if not highs:
        return {"score": 0.0, "label": "", "reasons": [], "risk": ""}

    high_52w = max(highs[-252:]) if len(highs) >= 60 else max(highs)
    high_6m = max(highs[-126:]) if len(highs) >= 126 else max(highs)
    high_3m = max(highs[-63:]) if len(highs) >= 63 else max(highs)
    high_20d = max(highs[-20:])

    score = 0.0
    label = ""
    reasons = []
    risk = ""

    if high_52w > 0 and price >= high_52w * 1.002:
        score += 1.50
        label = "52W high breakout"
        reasons.append("52W high breakout")
    elif high_6m > 0 and price >= high_6m * 1.002:
        score += 1.05
        label = "6M daily breakout"
        reasons.append("6M daily breakout")
    elif high_3m > 0 and price >= high_3m * 1.002:
        score += 0.70
        label = "3M daily breakout"
        reasons.append("3M daily breakout")
    elif high_20d > 0 and price >= high_20d * 1.002:
        score += 0.40
        label = "20D daily breakout"
        reasons.append("20D daily breakout")

    # Early daily breakouts get a small reward; very extended breakouts get awareness only.
    breakout_level = high_52w if "52W" in label else high_6m if "6M" in label else high_3m if "3M" in label else high_20d
    if label and breakout_level > 0:
        extension = (price / breakout_level) - 1.0
        if extension <= 0.08:
            score += 0.35
            reasons.append("near daily breakout level")
        elif extension >= 0.35:
            risk = "Extended above daily breakout"

    # If price is under nearby resistance, don't punish hard; just withhold breakout boost.
    return {"score": clamp(score, 0.0, 2.0), "label": label, "reasons": dedupe(reasons), "risk": risk}

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

    # v36.9 calibration:
    # - Tiny/low-float second-leg runners deserve a small quality nudge.
    # - Huge-float / multi-billion cap movers can still pass, but should not outrank
    #   clean small-cap runners unless the setup is truly exceptional.
    if float_shares and float_shares <= LOW_FLOAT_ELITE and (second_leg.get("detected") or coil.get("detected")):
        score += 0.35
    elif float_shares and float_shares <= LOW_FLOAT_GOOD and second_leg.get("detected"):
        score += 0.20

    if float_shares and float_shares > 300_000_000:
        score -= 1.00
    elif float_shares and float_shares > 150_000_000:
        score -= 0.60

    if market_cap and market_cap > 2_000_000_000:
        score -= 0.80
    elif market_cap and market_cap > MAX_MARKET_CAP:
        score -= 0.35

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
# v36.30 CHART TIMING ENGINE
# ============================================================

def pct_change(a, b):
    a = safe_float(a)
    b = safe_float(b)
    if b <= 0:
        return 0.0
    return ((a - b) / b) * 100.0


def detect_vwap_reclaim(candles, vwap):
    candles = finalized_candles(candles)
    vwap = safe_float(vwap)
    if not candles or vwap <= 0 or len(candles) < 4:
        return False
    recent = candles[-5:]
    was_below = any(candle_close(c) < vwap for c in recent[:-1])
    now_above = candle_close(recent[-1]) >= vwap
    return bool(was_below and now_above)


def analyze_chart_timing(candles, structure, price):
    """
    Right-time chart engine.

    This is intentionally stricter than a percent-gainer ranker. It tries to
    mimic a trader asking: "Is this actionable right now?"

    Alertable triggers:
    - fresh HOD breakout/new high in the last few finalized bars
    - fresh VWAP reclaim with volume expansion
    - second-leg coil near HOD with higher lows and active volume

    Non-alertable states:
    - BUILDING / NOT READY: decent structure but no live trigger yet
    - POST-PEAK FADE / WAIT: old high, fading volume, too far off HOD, or below VWAP
    """
    candles = finalized_candles(candles)
    structure = structure or {}
    price = safe_float(price)

    base = {
        "label": "UNKNOWN",
        "alert_ok": False,
        "fresh_breakout": False,
        "vwap_reclaim": False,
        "second_leg_ready": False,
        "coiling": False,
        "stale": True,
        "building": False,
        "reason": "not enough chart data",
        "off_high_pct": 999.0,
        "volume_ratio": 0.0,
        "last5_change_pct": 0.0,
        "last10_change_pct": 0.0,
        "day_high": 0.0,
        "hod_age_bars": 999,
        "recent_new_high": False,
        "recent_range_pct": 999.0,
    }

    if not candles or len(candles) < MIN_FINALIZED_CANDLES:
        return base

    highs = [candle_high(c) for c in candles]
    lows = [candle_low(c) for c in candles]
    day_high = safe_float(get_struct(structure, "day_high", 0)) or max(highs)
    # Use the most recent occurrence of HOD so old morning spikes are punished.
    hod_index = 0
    if day_high > 0:
        for i, h in enumerate(highs):
            if h >= day_high * 0.995:
                hod_index = i
    hod_age_bars = max(0, len(candles) - 1 - hod_index)

    recent_window = min(10, len(candles))
    recent_high = safe_float(get_struct(structure, "recent_high", 0)) or max(highs[-recent_window:])
    recent_low = safe_float(get_struct(structure, "recent_low", 0)) or min(lows[-recent_window:])
    vwap = safe_float(get_struct(structure, "vwap", calc_vwap(candles)))

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    breakout = bool(get_struct(structure, "breakout", False))
    near_high = bool(get_struct(structure, "near_high", False))

    recent3_vol = sum(candle_volume(c) for c in candles[-3:]) if len(candles) >= 3 else 0
    prior3_vol = sum(candle_volume(c) for c in candles[-6:-3]) if len(candles) >= 6 else 0
    prior10_vol = sum(candle_volume(c) for c in candles[-13:-3]) if len(candles) >= 13 else 0
    avg_prior3 = prior3_vol / 3 if prior3_vol > 0 else 0
    avg_prior10 = prior10_vol / 10 if prior10_vol > 0 else 0
    avg_recent3 = recent3_vol / 3 if recent3_vol > 0 else 0
    volume_ratio = (avg_recent3 / avg_prior3) if avg_prior3 > 0 else (1.0 if avg_recent3 > 0 else 0.0)
    volume_ratio_10 = (avg_recent3 / avg_prior10) if avg_prior10 > 0 else volume_ratio
    active_volume = bool(volume_ratio >= CHART_MIN_VOLUME_RATIO or volume_ratio_10 >= 1.05)
    expanding_volume = bool(volume_ratio >= CHART_FRESH_VOLUME_RATIO or volume_ratio_10 >= 1.20)

    close_now = candle_close(candles[-1])
    close_3 = candle_close(candles[-4]) if len(candles) >= 4 else candle_close(candles[0])
    close_5 = candle_close(candles[-6]) if len(candles) >= 6 else candle_close(candles[0])
    close_10 = candle_close(candles[-11]) if len(candles) >= 11 else candle_close(candles[0])
    last3_change = pct_change(close_now, close_3)
    last5_change = pct_change(close_now, close_5)
    last10_change = pct_change(close_now, close_10)

    off_high = ((day_high - price) / day_high * 100.0) if day_high > 0 else 999.0
    recent_new_high = bool(hod_age_bars <= CHART_HOD_RECENT_BARS)
    very_near_high = bool(day_high > 0 and price >= day_high * (1 - CHART_MAX_OFF_HIGH_ALERT / 100.0))
    tight_to_high = bool(day_high > 0 and price >= day_high * 0.975)

    recent_range_pct = ((recent_high - recent_low) / recent_low * 100.0) if recent_low > 0 else 999.0
    coiling = bool(above_vwap and higher_lows and recent_range_pct <= 7.5 and off_high <= CHART_MAX_OFF_HIGH_ALERT)
    vwap_reclaim = bool(detect_vwap_reclaim(candles, vwap) and active_volume and last3_change >= -0.75)

    fresh_breakout = bool(
        above_vwap
        and (breakout or recent_new_high)
        and recent_new_high
        and very_near_high
        and expanding_volume
        and last5_change >= -0.75
    )

    second_leg_ready = bool(
        above_vwap
        and higher_lows
        and tight_to_high
        and off_high <= CHART_MAX_OFF_HIGH_ALERT
        and hod_age_bars <= CHART_STALE_HOD_BARS
        and (coiling or breakout or recent_new_high or near_high)
        and active_volume
        and last10_change >= -2.0
    )

    old_hod_no_push = bool(hod_age_bars > CHART_STALE_HOD_BARS and not vwap_reclaim and not fresh_breakout)
    fading_from_peak = bool(off_high >= CHART_STALE_OFF_HIGH)
    weak_recent_momo = bool(last10_change <= -3.5 and not recent_new_high)
    volume_fading = bool(volume_ratio < 0.65 and volume_ratio_10 < 0.80 and not recent_new_high and not vwap_reclaim)
    below_or_lost_vwap = bool(not above_vwap)
    stale = bool(fading_from_peak or old_hod_no_push or weak_recent_momo or volume_fading or below_or_lost_vwap)

    alert_ok = bool((fresh_breakout or vwap_reclaim or second_leg_ready) and not stale and off_high <= CHART_MAX_OFF_HIGH_ALERT)

    if fresh_breakout:
        label = "🔥 FRESH HOD BREAKOUT"
        reason = f"fresh HOD within {hod_age_bars} bars with expanding volume"
    elif vwap_reclaim:
        label = "🟢 VWAP RECLAIM"
        reason = "fresh VWAP reclaim with active volume"
    elif second_leg_ready:
        label = "🌀 SECOND LEG READY"
        reason = f"near HOD with higher lows/coil; HOD age {hod_age_bars} bars"
    elif stale:
        label = "⚠️ POST-PEAK FADE / WAIT"
        reason = f"stale chart: HOD age {hod_age_bars} bars, off high {off_high:.1f}%, vol ratio {volume_ratio:.2f}, last10 {last10_change:.1f}%"
    else:
        label = "👀 BUILDING / NOT READY"
        reason = f"building only: HOD age {hod_age_bars} bars, no live trigger"

    return {
        "label": label,
        "alert_ok": alert_ok,
        "fresh_breakout": fresh_breakout,
        "vwap_reclaim": vwap_reclaim,
        "second_leg_ready": second_leg_ready,
        "coiling": coiling,
        "stale": stale,
        "building": bool(label.startswith("👀")),
        "reason": reason,
        "off_high_pct": off_high,
        "volume_ratio": volume_ratio,
        "volume_ratio_10": volume_ratio_10,
        "active_volume": active_volume,
        "expanding_volume": expanding_volume,
        "last3_change_pct": last3_change,
        "last5_change_pct": last5_change,
        "last10_change_pct": last10_change,
        "day_high": day_high,
        "hod_age_bars": hod_age_bars,
        "recent_new_high": recent_new_high,
        "recent_range_pct": recent_range_pct,
    }

def apply_chart_timing_score(score, chart_timing):
    """Final score layer: reward fresh timing, cap non-actionable charts."""
    score = safe_float(score)
    chart_timing = chart_timing or {}

    if chart_timing.get("fresh_breakout"):
        score += 1.10
    elif chart_timing.get("second_leg_ready"):
        score += 0.85
    elif chart_timing.get("vwap_reclaim"):
        score += 0.70

    if chart_timing.get("stale"):
        off_high = safe_float(chart_timing.get("off_high_pct"))
        hod_age = safe_int(chart_timing.get("hod_age_bars"), 999)
        if off_high >= 18 or hod_age >= 45:
            score -= 3.00
        elif off_high >= CHART_STALE_OFF_HIGH or hod_age >= CHART_STALE_HOD_BARS:
            score -= 2.00
        else:
            score -= 1.10
        score = min(score, 6.2)
    elif chart_timing.get("building") or not chart_timing.get("alert_ok"):
        # This is the key stale-alert fix: good stats can stay visible, but
        # no elite phone alert unless the live chart has a trigger now.
        score = min(score, CHART_BUILDING_SCORE_CAP if chart_timing.get("building") else CHART_NOT_FRESH_SCORE_CAP)

    return clamp(score)


# ============================================================
# ALERT MEMORY / COOLDOWN
# ============================================================

def meaningful_change_since_alert(ticker, result):
    """Smart cooldown: repeat alerts only when the chart makes a real new high or materially upgrades while fresh."""
    item = LAST_ALERT.get(ticker)
    price = safe_float(result.get("price"))
    score = safe_float(result.get("score"))
    bias = result.get("bias")
    chart_timing = result.get("chart_timing") or {}
    current_day_high = safe_float(chart_timing.get("day_high")) or safe_float(get_struct(result.get("structure") or {}, "day_high", 0))

    if not item:
        return True, "first alert"

    elapsed = time.time() - item.get("time", 0)
    last_price = safe_float(item.get("price"))
    last_score = safe_float(item.get("score"))
    last_day_high = safe_float(item.get("day_high"))

    price_push = last_price > 0 and price >= last_price * 1.03
    true_new_high = current_day_high > 0 and (last_day_high <= 0 or current_day_high >= last_day_high * CHART_RE_ALERT_HIGH_MULTIPLIER)
    fresh_again = bool(chart_timing.get("fresh_breakout") or chart_timing.get("second_leg_ready") or chart_timing.get("vwap_reclaim"))
    score_improved = score >= last_score + 1.0 and fresh_again
    upgraded = item.get("bias") != bias and "RUNNER" in str(bias).upper() and fresh_again

    # v37: do not bury the best names of the day. A 9/10+ runner can re-alert
    # on a fresh trigger much sooner than the normal 15-minute cooldown, but it
    # still needs a small price push/new-HOD style confirmation to avoid spam.
    if score >= 9.0 and elapsed >= 120 and fresh_again and (true_new_high or price >= last_price * 1.005):
        return True, "elite runner cooldown bypass"

    if elapsed >= ALERT_COOLDOWN_SECONDS and ((price_push and true_new_high and fresh_again) or score_improved or upgraded):
        reason = []
        if price_push and true_new_high:
            reason.append("fresh new high")
        if score_improved:
            reason.append("score improvement with fresh setup")
        if upgraded:
            reason.append("bias upgraded with fresh setup")
        return True, " / ".join(reason)

    return False, "cooldown/no fresh new high"




def is_elite_market_leader(result):
    """
    v34.9: separates runner quality from entry quality.
    A true day leader should not be blocked as AVOID just because the current
    entry is extended, fakeout-sensitive, or needs a reset.
    """
    if not isinstance(result, dict):
        return False

    gain = safe_float(result.get("gain"))
    score = safe_float(result.get("score"))
    volume = safe_int(result.get("volume"))
    float_shares = safe_int(result.get("float"))
    structure = result.get("structure") or {}
    news_score = safe_float(result.get("news_score", (result.get("news") or {}).get("score", 0)))
    daily_score = safe_float((result.get("daily_context") or {}).get("score", 0))
    fresh_score = safe_float((result.get("freshness") or {}).get("score", 0))

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    near_high = bool(get_struct(structure, "near_high", False))
    breakout = bool(get_struct(structure, "breakout", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))

    low_or_unknown_float = float_shares <= 0 or float_shares <= 40_000_000
    massive_volume = volume >= 10_000_000
    extreme_leader = gain >= 75 and massive_volume and low_or_unknown_float
    strong_leader = gain >= 50 and score >= 8.0 and massive_volume and low_or_unknown_float
    catalyst_leader = gain >= RUNNER_MIN_GAIN and score >= 8.5 and news_score >= 7.5 and volume >= 1_000_000
    daily_fresh_leader = gain >= RUNNER_MIN_GAIN and score >= 8.5 and (daily_score >= 1.0 or fresh_score >= 1.0) and volume >= 1_000_000

    # Needs at least one sign it is still structurally alive. This prevents true broken fades
    # from getting promoted just because they had a giant earlier spike.
    still_alive = above_vwap or near_high or breakout or higher_lows

    return bool(still_alive and (extreme_leader or strong_leader or catalyst_leader or daily_fresh_leader))


def apply_elite_leader_gate_fix(result):
    """v34.9: never call elite leaders AVOID; mark them as extended/watch instead."""
    if not isinstance(result, dict) or not is_elite_market_leader(result):
        return result

    tier = result.get("trade_tier", "") or ""
    phase = result.get("phase", "") or ""
    bias = result.get("bias", "") or ""

    if "AVOID" in tier:
        result["trade_tier"] = "👀 MARKET LEADER — EXTENDED"
    elif not tier or "MARKET WATCH" in tier:
        result["trade_tier"] = "👀 MARKET LEADER — EXTENDED"

    if bias == "⚠️ AVOID" or "AVOID" in bias:
        result["bias"] = "🔥 MARKET LEADER — EXTENDED"

    if "FAKEOUT" in phase:
        result["phase"] = "⚠️ EXTENDED / RESET NEEDED"

    result["elite_leader"] = True
    return result



def apply_elite_score_floor_v35(score, gain, volume, float_shares, structure, freshness=None, daily_context=None):
    """
    v35.0: keep QTEX-style day leaders visible even after normal pullbacks.
    This does not blindly mark them as clean entries; it prevents the score from
    collapsing below alert range while they still show elite leader-quality traits.
    """
    score = safe_float(score)
    gain = safe_float(gain)
    volume = safe_int(volume)
    float_shares = safe_int(float_shares)
    structure = structure or {}
    freshness = freshness or {}
    daily_context = daily_context or {}

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    near_high = bool(get_struct(structure, "near_high", False))
    breakout = bool(get_struct(structure, "breakout", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    still_alive = above_vwap or near_high or breakout or higher_lows
    low_or_unknown_float = float_shares <= 0 or float_shares <= 40_000_000

    fresh_or_daily = safe_float(freshness.get("score", 0)) >= 0.5 or safe_float(daily_context.get("score", 0)) >= 1.0

    # Explosive day leader floor: QTEX-type move.
    if still_alive and low_or_unknown_float and gain >= 75 and volume >= 10_000_000:
        score = max(score, 8.0 if fresh_or_daily else 7.5)

    # Strong low-float continuation floor: GOVX/KIDZ/MTVA-type move.
    if still_alive and low_or_unknown_float and gain >= 40 and volume >= 10_000_000:
        score = max(score, 7.0)

    # Real second-leg/daily breakout with alertable gain should not say AVOID.
    if still_alive and gain >= RUNNER_MIN_GAIN and volume >= 1_000_000 and fresh_or_daily:
        score = max(score, 7.0)

    return clamp(score)

def detect_open_drive_runner(gain, volume, structure, candles, float_shares=0, market_cap=0, decay=None, exhaustion=None, fakeout=None):
    """
    v36.1 CPSH fix: catch opening-drive ignition runners before the normal
    second-leg/coil logic fully forms. This is still safety-gated: above VWAP,
    near highs/breakout, real volume expansion, and no fakeout/exhaustion.
    """
    structure = structure or {}
    decay = decay or {}
    exhaustion = exhaustion or {}
    fakeout = fakeout or {}
    candles = candles or []

    gain = safe_float(gain)
    volume = safe_int(volume)
    float_shares = safe_int(float_shares)
    market_cap = safe_int(market_cap)

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    breakout = bool(get_struct(structure, "breakout", False))
    near_high = bool(get_struct(structure, "near_high", False))
    bad_structure = bool(get_struct(structure, "bad_structure", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))

    recent_vol = safe_int(get_struct(structure, "recent_volume", 0))
    if not recent_vol and candles:
        recent_vol = sum(candle_volume(c) for c in candles[-3:])

    prev_vol = safe_int(get_struct(structure, "previous_volume", 0))
    if not prev_vol and len(candles) >= 6:
        prev_vol = sum(candle_volume(c) for c in candles[-6:-3])

    volume_expanding = recent_vol >= 250_000 and (prev_vol <= 0 or recent_vol >= prev_vol * 1.15)
    liquid_enough = volume >= 750_000 or recent_vol >= 250_000
    small_enough = (not market_cap or market_cap <= 500_000_000) and (not float_shares or float_shares <= 80_000_000)

    clean_open_drive_structure = bool(
        liquid_enough
        and volume_expanding
        and small_enough
        and above_vwap
        and (breakout or near_high)
        and not bad_structure
        and not fakeout.get("detected")
        and not exhaustion.get("detected")
        and not decay.get("detected")
    )

    detected = bool(gain >= 20 and clean_open_drive_structure)
    early_detected = bool(gain >= EARLY_OPEN_DRIVE_GAIN and clean_open_drive_structure)

    return {
        "detected": detected,
        "early_detected": early_detected,
        "label": "🚨 OPEN DRIVE RUNNER" if detected else ("⚡ EARLY OPEN DRIVE" if early_detected else ""),
        "recent_volume": recent_vol,
        "previous_volume": prev_vol,
        "volume_expanding": volume_expanding,
        "reason": "open drive: gain + volume expansion + VWAP + highs" if detected else ("early open drive: volume expansion + VWAP + highs" if early_detected else ""),
        "higher_lows": higher_lows,
    }



def dynamic_alert_volume_floor(result):
    """Lower confirmation volume only for the exact names we kept missing: low-float or explosive top-gainer leaders."""
    gain = safe_float(result.get("gain"))
    volume = safe_int(result.get("volume"))
    float_shares = safe_int(result.get("float"))
    score = safe_float(result.get("score"))
    structure = result.get("structure") or {}
    news_score = safe_float(result.get("news_score", (result.get("news") or {}).get("score", 0)))
    above_vwap = bool(get_struct(structure, "above_vwap", False))
    near_high = bool(get_struct(structure, "near_high", False))
    breakout = bool(get_struct(structure, "breakout", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    low_float = float_shares <= 0 or float_shares <= LOW_FLOAT_GOOD
    tiny_float = 0 < float_shares <= LOW_FLOAT_TINY
    strong_leader = gain >= 35 and (near_high or breakout or higher_lows or above_vwap)
    if tiny_float and gain >= ALERT_HARD_MIN_GAIN:
        return LOW_FLOAT_ALERT_MIN_VOLUME
    if low_float and strong_leader and score >= AWARENESS_MIN_SCORE:
        return LOW_FLOAT_ALERT_MIN_VOLUME
    if gain >= 50 and score >= AWARENESS_MIN_SCORE:
        return AWARENESS_MIN_VOLUME
    if news_score >= 8 and gain >= ALERT_HARD_MIN_GAIN:
        return AWARENESS_MIN_VOLUME
    return ALERT_MIN_VOLUME


def dynamic_elite_score_floor(result):
    """v37: elite floor adapts to market regime instead of hard-blocking every 7.x hot-tape runner."""
    regime = result.get("regime") or {}
    label = str(regime.get("label", regime)).upper() if regime else ""
    if "HOT" in label:
        return 7.5
    if "COLD" in label or "THIN" in label:
        return 8.5
    return ALERT_MIN_SCORE


def has_continuation_trigger(result):
    """Strict mode: continuation setups may rank/watch, but Telegram alerts never fire below +25%."""
    return False

def dynamic_required_alert_gain(result):
    return ALERT_HARD_MIN_GAIN

def is_awareness_lane(result):
    """Phone-visible potential runner lane. Still requires 25%+ gain, real structure, and no decay/fakeout."""
    structure = result.get("structure") or {}
    score = safe_float(result.get("score"))
    gain = safe_float(result.get("gain"))
    volume = safe_int(result.get("volume"))
    float_shares = safe_int(result.get("float"))
    news_score = safe_float(result.get("news_score", (result.get("news") or {}).get("score", 0)))
    coil = result.get("coil") or {}
    second_leg = result.get("second_leg") or {}
    decay = result.get("decay") or {}
    exhaustion = result.get("exhaustion") or {}
    fakeout = result.get("fakeout") or {}

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    breakout = bool(get_struct(structure, "breakout", False))
    near_high = bool(get_struct(structure, "near_high", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))
    low_or_unknown_float = float_shares <= 0 or float_shares <= LOW_FLOAT_GOOD
    chart_timing = result.get("chart_timing") or {}
    technical_trigger = bool(chart_timing.get("alert_ok") or chart_timing.get("fresh_breakout") or chart_timing.get("vwap_reclaim") or chart_timing.get("second_leg_ready"))
    catalyst_or_leader = news_score >= 8 or gain >= 35 or (low_or_unknown_float and gain >= ALERT_HARD_MIN_GAIN)

    return bool(
        gain >= ALERT_HARD_MIN_GAIN
        and score >= AWARENESS_MIN_SCORE
        and volume >= dynamic_alert_volume_floor(result)
        and above_vwap
        and technical_trigger
        and catalyst_or_leader
        and not chart_timing.get("stale")
        and not decay.get("detected")
        and not exhaustion.get("detected")
        and not fakeout.get("detected")
    )

def is_super_momo_override(result):
    """
    v36.18: no Telegram score bypass.

    Keep the function for compatibility, but strict mode means every
    phone alert must still be score >= ALERT_MIN_SCORE.
    """
    score = safe_float(result.get("score"))
    return bool(score >= dynamic_elite_score_floor(result))

def is_in_play_alert(result):
    """
    v36.20 phone-alert filter.

    Two lanes:
    - Elite: score >= 8.0, normal volume confirmation.
    - Awareness/Potential Runner: score >= 7.0, 25%+ gain, low-float/top-gainer or strong-news context, above VWAP, and clean trigger.
    """
    ticker = result.get("ticker", "UNKNOWN")
    structure = result.get("structure") or {}
    entry = str(result.get("entry", "")).lower()
    phase = str(result.get("phase", "")).lower()
    tier = str(result.get("trade_tier", "")).lower()
    bias = str(result.get("bias", "")).lower()
    risk_text = " ".join(result.get("risks", []) or []).lower()

    score = safe_float(result.get("score"))
    gain = safe_float(result.get("gain"))
    news_score = safe_float(result.get("news_score", (result.get("news") or {}).get("score", 0)))
    entry_score = safe_float(result.get("entry_score", 0))
    structure_score = safe_float(result.get("structure_score", 0))
    volume = safe_int(result.get("volume"))

    above_vwap = bool(get_struct(structure, "above_vwap", False))
    breakout = bool(get_struct(structure, "breakout", False))
    near_high = bool(get_struct(structure, "near_high", False))
    higher_lows = bool(get_struct(structure, "higher_lows", False))

    coil = result.get("coil") or {}
    second_leg = result.get("second_leg") or {}
    decay = result.get("decay") or {}
    exhaustion = result.get("exhaustion") or {}
    fakeout = result.get("fakeout") or {}
    open_drive = bool(result.get("open_drive_runner"))
    early_open_drive = bool(result.get("early_open_drive"))
    chart_timing = result.get("chart_timing") or {}

    elite_floor = dynamic_elite_score_floor(result)
    elite_alert_lane = bool(score >= elite_floor)
    continuation_alert_lane = bool(has_continuation_trigger(result))
    awareness_alert_lane = bool(is_awareness_lane(result))

    # Absolute phone-alert gates first. These stop watchlist/rank names from
    # slipping through via old text overrides.
    required_gain = dynamic_required_alert_gain(result)
    if gain < ALERT_HARD_MIN_GAIN:
        return False, f"gain {gain:.1f}% below alert floor 25%"

    min_alert_volume = dynamic_alert_volume_floor(result)
    if volume < min_alert_volume:
        return False, f"volume {fmt_big_num(volume)} below alert confirmation floor {fmt_big_num(min_alert_volume)}"

    if not (elite_alert_lane or awareness_alert_lane):
        return False, f"score {score:.1f} below elite {elite_floor:.1f} and awareness {AWARENESS_MIN_SCORE:.1f} lanes"

    if not above_vwap:
        return False, "not in play — below/lost VWAP"

    # v36.30 chart-timing hard gate. A percent leader is not alertable unless
    # the live chart is fresh: HOD breakout, VWAP reclaim, or second-leg ready.
    if chart_timing.get("stale"):
        return False, f"not in play — stale chart ({chart_timing.get('reason', 'post-peak fade')})"

    if not chart_timing.get("alert_ok"):
        return False, f"not in play — no fresh chart trigger ({chart_timing.get('label', 'not ready')})"

    if safe_float(chart_timing.get("off_high_pct")) > CHART_MAX_OFF_HIGH_ALERT:
        return False, f"not in play — {safe_float(chart_timing.get('off_high_pct')):.1f}% off high"

    # v36.16: these labels are always watch-only. Do this BEFORE any elite
    # setup override so ASTC/BRTX-style extended names cannot alert off score.
    bad_phase_words = [
        "fading", "extended", "reset needed", "reclaim needed",
        "fakeout", "avoid", "lost vwap", "below vwap",
    ]
    if any(w in phase for w in bad_phase_words):
        return False, f"not in play — phase is {result.get('phase', '')}"

    bad_risk_words = [
        "lost vwap", "below vwap", "reclaim", "fakeout", "exhaustion",
        "avoid chase", "do not chase", "extended",
    ]
    if any(w in risk_text for w in bad_risk_words):
        return False, "not in play — risk says wait/reclaim/avoid"

    if fakeout.get("detected"):
        return False, "not in play — fakeout/stuff risk"

    if exhaustion.get("detected"):
        return False, "not in play — extended/exhaustion risk"

    if decay.get("detected"):
        return False, "not in play — momentum decay"

    blocked_entry_words = [
        "no clean entry", "wait for reset", "wait for reclaim", "reclaim needed",
        "not an entry", "do not chase", "avoid chase", "watchlist only",
    ]
    if any(w in entry for w in blocked_entry_words):
        return False, "not in play — no clean entry"

    # Only these phases/setups are allowed to hit the phone.
    clean_phase = any(w in phase for w in [
        "second leg", "runner continuation", "ignition", "breakout hold", "coil", "above vwap"
    ])

    high_quality_above_vwap = bool(
        above_vwap
        and (score >= elite_floor or continuation_alert_lane or awareness_alert_lane)
        and entry_score >= TRADE_ALERT_MIN_ENTRY_SCORE
        and structure_score >= TRADE_ALERT_MIN_STRUCTURE_SCORE
        and (near_high or breakout or higher_lows or coil.get("detected"))
    )

    clean_trigger = bool(
        chart_timing.get("fresh_breakout")
        or chart_timing.get("vwap_reclaim")
        or chart_timing.get("second_leg_ready")
        or second_leg.get("detected")
        or open_drive
        or early_open_drive
        or (breakout and near_high and higher_lows)
        or (coil.get("detected") and (near_high or higher_lows) and entry_score >= TRADE_ALERT_MIN_ENTRY_SCORE)
        or high_quality_above_vwap
        or (clean_phase and (elite_alert_lane or continuation_alert_lane) and entry_score >= TRADE_ALERT_MIN_ENTRY_SCORE)
    )

    if not clean_trigger:
        return False, "not in play — no clean second-leg/continuation/ignition trigger"

    # Do not let stale RUNNER WATCH / MARKET LEADER / AVOID wording fire alerts.
    # TRADEABLE or RUNNER is okay only if the clean trigger above already passed.
    if "avoid" in tier:
        return False, f"not in play — tier is {result.get('trade_tier', '')}"
    if ("market leader" in tier or "awareness" in tier) and not (high_quality_above_vwap or awareness_alert_lane):
        return False, f"not in play — tier is {result.get('trade_tier', '')}"

    if any(w in bias for w in ["avoid", "extended", "reclaim watch"]):
        return False, f"not in play — bias is {result.get('bias', '')}"
    if "market leader" in bias and not (high_quality_above_vwap or awareness_alert_lane):
        return False, f"not in play — bias is {result.get('bias', '')}"

    min_entry = TRADE_ALERT_MIN_ENTRY_SCORE
    min_structure = TRADE_ALERT_MIN_STRUCTURE_SCORE

    if entry_score and entry_score < min_entry:
        return False, f"not in play — entry score {entry_score:.1f} too low"

    if structure_score and structure_score < min_structure:
        return False, f"not in play — structure score {structure_score:.1f} too low"

    # Weak/no-news setups must have a real technical trigger.
    if news_score < 5.0 and not (second_leg.get("detected") or breakout or clean_phase or awareness_alert_lane):
        return False, "not in play — weak news without breakout/second leg"

    if awareness_alert_lane and not elite_alert_lane:
        result["awareness_alert"] = True
        return True, "potential runner awareness lane"

    return True, "clean active setup (elite lane)"


def should_alert(result):
    ticker = result["ticker"]

    if ticker in SENT_THIS_CYCLE:
        print(f"[NO ALERT] {ticker}: already sent this cycle")
        return False

    in_play, play_reason = is_in_play_alert(result)
    if not in_play:
        print(f"[BLOCK] {ticker}: {play_reason}")
        return False

    ok, reason = meaningful_change_since_alert(ticker, result)
    if not ok:
        print(f"[COOLDOWN] {ticker}: {reason}")
        return False

    print(f"[ALERT OK] {ticker}: IN PLAY — {reason}")
    LAST_ALERT[ticker] = {
        "time": time.time(),
        "price": result["price"],
        "score": result["score"],
        "bias": result["bias"],
        "day_high": safe_float((result.get("chart_timing") or {}).get("day_high")),
        "chart_label": (result.get("chart_timing") or {}).get("label", ""),
    }
    SENT_THIS_CYCLE.add(ticker)
    return True


# ============================================================
# ALERT BUILDER — CLEAN HUD OUTPUT
# ============================================================

def alert_title(result):
    if result.get("awareness_alert"):
        return "👀 POTENTIAL RUNNER — EARLY"
    # v36.1: open-drive ignition gets its own clean title.
    if result.get("open_drive_runner"):
        return "🚨 OPEN DRIVE RUNNER"
    if result.get("early_open_drive"):
        return "⚡ EARLY OPEN DRIVE"
    # v36: if it hits Telegram, the name is IN PLAY. No more MARKET LEADER / WATCH titles.
    if result.get("second_leg", {}).get("detected"):
        return "🔥 IN PLAY — SECOND LEG"
    if result.get("coil", {}).get("detected"):
        return "🌀 IN PLAY — COIL BREAKOUT"
    if bool(get_struct(result.get("structure", {}), "breakout", False)):
        return "🚀 IN PLAY — BREAKOUT"
    entry = str(result.get("entry", "")).lower()
    if "vwap hold" in entry:
        return "🟢 IN PLAY — VWAP HOLD"
    if "higher-low" in entry or "higher low" in entry:
        return "📈 IN PLAY — HIGHER LOW"
    return "🔥 IN PLAY"


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
    result["open_drive"] = result.get("open_drive") or {"detected": False, "early_detected": False, "label": "", "reason": ""}
    result["open_drive_runner"] = bool(result.get("open_drive_runner") or result["open_drive"].get("detected"))
    result["early_open_drive"] = bool(result.get("early_open_drive") or result["open_drive"].get("early_detected"))

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

    news = result.get("news") or {}
    news_line = compact_news_line(news)

    header = f"{result['ticker']} | {result['score']:.1f} | {fmt_money(result['price'])} | +{result['gain']:.1f}%"

    stat_bits = []
    if SHOW_FLOAT and result.get("float"):
        label = result.get("float_info", {}).get("label") or f"Float {fmt_big_num(result['float'])}"
        # Strip repeated noisy words but keep tiny/low-float edge.
        stat_bits.append(label.replace("🔥 ", "🔥 ").replace("🟢 ", "").replace("🟡 ", ""))
    if result.get("volume"):
        stat_bits.append(f"{fmt_big_num(result.get('volume'))} vol")

    setup_bits = []
    if result.get("open_drive_runner"):
        setup_bits.append("open drive")
    if result.get("second_leg", {}).get("detected"):
        setup_bits.append("second leg")
    if result.get("coil", {}).get("detected"):
        setup_bits.append("coil")
    if bool(get_struct(result.get("structure", {}), "above_vwap", False)):
        setup_bits.append("above VWAP")
    if bool(get_struct(result.get("structure", {}), "higher_lows", False)):
        setup_bits.append("higher lows")
    daily_label = (result.get("daily_context") or {}).get("label", "")
    if daily_label:
        setup_bits.append(daily_label)

    risk_items = []
    sec_short = compact_dilution_label(result.get("sec"))
    if sec_short:
        risk_items.append(sec_short)

    main_risk = main_risk_sentence(result)
    if main_risk and "no major" not in main_risk.lower():
        # Avoid duplicating the same dilution line.
        if not sec_short or main_risk.lower() != sec_short.lower():
            risk_items.append(main_risk)

    lines = [title, "", header]

    if stat_bits:
        lines.append(" | ".join(dedupe(stat_bits)[:3]))

    lines.extend(["", news_line])

    if setup_bits:
        lines.append("✅ " + " + ".join(dedupe(setup_bits)[:4]))

    lines.append("")
    lines.append(f"Entry: {result['entry']}")

    if risk_items:
        lines.append(f"Risk: {' | '.join(dedupe(risk_items)[:2])}")
    else:
        lines.append("Risk: clean for now")

    if SHOW_HEADLINE and (news.get("headline") or result.get("headline")):
        headline = news.get("headline") or result.get("headline")
        lines.append("")
        lines.append(f"Headline: {headline[:220]}")

    return "\n".join(lines)




def blank_news(ticker=""):
    news = classify_news("", ticker)
    news["explain"] = "Skipped in speed mode"
    return news


def blank_sec_result():
    return {
        "has_risk": False,
        "severity": "NONE",
        "category": "Skipped / speed mode",
        "label": "",
        "forms": [],
        "terms": [],
        "filing_age_days": None,
        "freshness": "skipped",
        "risk_score": 0.0,
        "atm_active": False,
        "warrant_overhang": False,
    }


def needs_full_research(gain, volume, float_shares, second_leg=None, structure=None, freshness=None):
    """Only spend slow news/SEC/daily calls on names that can realistically matter."""
    gain = safe_float(gain)
    volume = safe_int(volume)
    float_shares = safe_int(float_shares)
    second_leg_detected = bool((second_leg or {}).get("detected"))
    above_vwap = bool(get_struct(structure or {}, "above_vwap", False))
    fresh_score = safe_float((freshness or {}).get("score", 0))

    return bool(
        gain >= 25
        or volume >= 5_000_000
        or fresh_score >= 1.0
        or (0 < float_shares <= 20_000_000 and gain >= 15 and above_vwap)
        or (second_leg_detected and gain >= 18)
    )


def should_check_sec(gain, news_score, float_shares, full_research):
    if not full_research:
        return False
    return bool(gain >= 25 or news_score >= 8 or (0 < safe_int(float_shares) <= 20_000_000))


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
        # v36.11: exhaustion is an entry caution, not a nuclear score killer.
        score -= 0.65 if is_premarket_session() else 1.0
        if exhaustion.get("risk"):
            reasons.append(exhaustion.get("risk"))
    if decay and decay.get("detected"):
        # v36.11: reduce fade penalty; strong context is handled after full scoring.
        score -= 0.35 if is_premarket_session() else 0.75

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

    # Dilution / SEC filings are awareness only.
    # Do NOT subtract score for dilution; many real runners have S-1/424B/ATM/warrants on file.
    # The alert will still show whether the company can offer/sell shares.

    halt_label = halt_risk.get("label", "") if isinstance(halt_risk, dict) else ""
    if halt_label:
        penalty += 0.35
        reasons.append(halt_label)

    reasons.extend(fast_warnings)
    # v36.11: cap stacked decay/exhaustion/fakeout-style risk penalties so a
    # real low-float/strong-news runner is not buried from one bad label.
    return min(penalty, 2.0), dedupe(reasons)


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

    # v36.6: no float boost/penalty. Volume + price action decide.

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

    # Source/screener values are discovery only. They can be stale or wrong.
    # Live quote is the truth for price and percent gain before fast-pass, score, rank, or alert.
    source_price = safe_float(candidate.get("price"))
    source_gain = safe_float(candidate.get("gain"))
    source_volume = safe_int(candidate.get("volume"))
    source = candidate.get("source", "unknown")

    if source_gain < DISCOVERY_MIN_GAIN:
        return None

    # v37.10 source-layer hard skip before quote/profile calls.
    # Saves API time by rejecting obvious mega-cap/too-expensive rows from TradingView/Yahoo first.
    source_market_cap = safe_int(candidate.get("market_cap"))
    if source_market_cap and source_market_cap > EXTREME_MARKET_CAP_SKIP and source_gain < 50:
        print(f"[SOURCE SKIP] {ticker}: source market cap over {fmt_big_num(EXTREME_MARKET_CAP_SKIP)} before quote/profile")
        return None
    if source_price and source_price > MAX_PRICE and source_gain < RUNNER_MIN_GAIN:
        print(f"[SOURCE SKIP] {ticker}: source price {fmt_money(source_price)} outside max before quote/profile")
        return None

    live = get_live_quote(ticker)
    if not quote_is_valid(live, ticker):
        print(f"[LIVE SKIP] {ticker}: invalid live quote — source gain ignored ({source_gain:.1f}% from {source})")
        return None

    price = safe_float(live.get("price")) or source_price
    gain = safe_float(live.get("gain"))
    live_volume = safe_int(live.get("volume"))
    volume = live_volume or source_volume
    quote_stale = bool(live.get("stale"))

    if abs(gain - source_gain) >= 8.0:
        print(f"[LIVE OVERRIDE] {ticker}: source {source_gain:.1f}% -> live {gain:.1f}% ({live.get('source', 'unknown')})")

    if gain < dynamic_hard_min_gain():
        print(f"[LIVE SKIP] {ticker}: live gain {gain:.1f}% under {dynamic_hard_min_gain():.0f}% floor; source {source_gain:.1f}% ignored")
        return None

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

    if not candles or len(candles) < MIN_FINALIZED_CANDLES:
        print(f"[DEEP SKIP] {ticker}: insufficient finalized candles ({len(candles) if candles else 0}/{MIN_FINALIZED_CANDLES})")
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
    freshness = calc_freshness_boost(ticker, price, gain, volume, structure)

    full_research = needs_full_research(
        gain=gain,
        volume=volume,
        float_shares=float_shares,
        second_leg=second_leg,
        structure=structure,
        freshness=freshness,
    )

    priority_news_candidate = bool(
        gain >= 25
        or volume >= 1_000_000
        or second_leg.get("detected")
        or safe_float(freshness.get("score", 0)) >= 0.7
        or (bool(get_struct(structure, "above_vwap", False)) and (coil.get("detected") or get_struct(structure, "breakout", False) or get_struct(structure, "near_high", False)))
    )

    if full_research or priority_news_candidate:
        news = get_best_news(ticker, priority=priority_news_candidate)
    else:
        news = blank_news(ticker)
        print(f"[NEWS SKIP] {ticker}: weak candidate speed skip")

    news_score = safe_float(news.get("score", 0))

    global SEC_CALLS_THIS_CYCLE
    if should_check_sec(gain, news_score, float_shares, full_research) and SEC_CALLS_THIS_CYCLE < MAX_SEC_NAMES_PER_CYCLE:
        SEC_CALLS_THIS_CYCLE += 1
        sec = check_sec_filings(ticker)
    else:
        sec = blank_sec_result()
        print(f"[SEC SKIP] {ticker}: speed-mode SEC skip")

    if full_research or gain >= DAILY_CONTEXT_MIN_GAIN:
        daily_context = calc_daily_context(ticker, price)
    else:
        daily_context = {"score": 0.0, "label": "", "reason": "skipped speed mode"}

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

    # v34 participation + v34.8 fresh-name/daily-context boosts after base score.
    score += safe_float(participation.get("score", 0))
    score += safe_float(freshness.get("score", 0))
    score += safe_float(daily_context.get("score", 0))

    # ============================================================
    # v36.11 BALANCED CONTEXT REPAIR
    # Fixes EVTV/IPWR/CMND/BGDE-style cases where one FADING/EXTENDED label
    # overpowered strong catalyst + low float + VWAP/second-leg context.
    # Alerts still require hard 8+/25% + volume + VWAP safety below.
    # ============================================================
    above_vwap_ctx = bool(get_struct(structure, "above_vwap", False))
    low_float_ctx = bool(float_shares and float_shares <= 20_000_000)
    tiny_float_ctx = bool(float_shares and float_shares <= 5_000_000)
    strong_news_ctx = news_score >= 8.0
    second_leg_ctx = bool(second_leg.get("detected"))
    continuation_ctx = bool(coil.get("detected") or second_leg_ctx or above_vwap_ctx)

    if low_float_ctx:
        score += 0.75
    if tiny_float_ctx:
        score += 0.75
    if strong_news_ctx:
        score += 0.75
    if second_leg_ctx:
        score += 0.75
    if above_vwap_ctx:
        score += 0.35

    # Discovery can rank 8%+ names, but true alert-quality momentum should be
    # gain-weighted so UPST/EOSE-style single-digit grinders do not crowd the top.
    if gain < 10:
        score -= 1.5
    elif gain < 15:
        score -= 0.75

    # Big-float dampening: awareness, not a hard block.
    if float_shares > 250_000_000:
        score -= 1.0
    elif float_shares > 150_000_000:
        score -= 0.5

    # If a strong runner context exists, add back part of stacked decay/exhaustion
    # penalties. Do not protect below-VWAP/fakeout names.
    strong_runner_context = bool(
        (strong_news_ctx or low_float_ctx or tiny_float_ctx or second_leg_ctx or continuation_ctx)
        and above_vwap_ctx
        and not fakeout.get("detected")
    )
    if strong_runner_context:
        protected = min(1.0, (safe_float(decay.get("penalty", 0)) + safe_float(exhaustion.get("penalty", 0))) * 0.35)
        score += protected

    # ============================================================
    # v36.13 CATALYST CONFIDENCE CAP
    # Speed mode fixed latency, but it made some no-news / skipped-news
    # tiny-float second legs inflate to 9.5-10. Keep them visible, but
    # reserve elite 9.5-10 territory for confirmed catalysts or true
    # monster leaders.
    # ============================================================
    monster_technical_leader = bool(
        gain >= 50
        and volume >= 10_000_000
        and above_vwap_ctx
        and (second_leg_ctx or bool(get_struct(structure, "near_high", False)) or bool(get_struct(structure, "breakout", False)))
        and not fakeout.get("detected")
    )

    if news_score < 8.0 and not monster_technical_leader:
        score = min(score, 8.8)

    if news_score <= 4.0 and gain < 35:
        score -= 0.50

    if news_score <= 0 and gain < 30:
        score -= 0.25

    open_drive = detect_open_drive_runner(
        gain=gain,
        volume=volume,
        structure=structure,
        candles=candles,
        float_shares=float_shares,
        market_cap=market_cap,
        decay=decay,
        exhaustion=exhaustion,
        fakeout=fakeout,
    )
    if open_drive.get("detected"):
        score += 1.5
        score = max(score, 7.0)
    elif open_drive.get("early_detected"):
        score += 1.0
        score = max(score, 7.0)

    # Re-apply the non-catalyst cap after open-drive bonuses so speed-mode
    # technical movers do not become automatic 10/10 alerts.
    if news_score < 8.0 and not monster_technical_leader:
        score = min(score, 8.8)

    score = apply_elite_score_floor_v35(score, gain, volume, float_shares, structure, freshness, daily_context)

    # v36.30: chart timing is the final quality layer. This makes the bot behave
    # more like a live chart reader: reward fresh HOD/VWAP/second-leg timing and
    # punish post-peak fades before they can become stale phone alerts.
    chart_timing = analyze_chart_timing(candles, structure, price)

    # v37.10 elite structure bonus: keep true +25% fresh-HOD/second-leg/VWAP runners
    # from collapsing below alert floor due to temporary news/decay noise.
    elite_structure_trigger = bool(
        gain >= ALERT_HARD_MIN_GAIN
        and above_vwap_ctx
        and (chart_timing.get("fresh_breakout") or chart_timing.get("second_leg_ready") or chart_timing.get("vwap_reclaim") or second_leg_ctx)
        and not fakeout.get("detected")
        and not exhaustion.get("detected")
    )
    if elite_structure_trigger:
        score += 1.25

    score = apply_chart_timing_score(score, chart_timing)
    score = clamp(score)

    if chart_timing.get("stale") and gain >= RUNNER_MIN_GAIN:
        phase = "⚠️ POST-PEAK FADE / WAIT"
        if score < 8.5:
            bias = "⚠️ AVOID / WAIT"

    # v33.12 ranking visibility floor:
    # True tiny/low-float leaders stay visible even when extended.
    # This does NOT force alerts; should_alert still controls alerts.
    if gain >= 50 and 0 < float_shares <= 25_000_000:
        score = max(score, 6.6 if is_premarket_session() and entry_score >= 4.0 else 5.5)

    # Re-apply right-time caps after visibility floors. A huge % mover can stay
    # visible, but BUILDING/STALE cannot become elite just because it is up big.
    if chart_timing.get("stale"):
        score = min(score, 6.2)
    elif chart_timing.get("building") or not chart_timing.get("alert_ok"):
        score = min(score, CHART_BUILDING_SCORE_CAP if chart_timing.get("building") else CHART_NOT_FRESH_SCORE_CAP)
    score = clamp(score)

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

    # v35.0: score is quality; avoid is reserved for genuinely broken/low-quality names.
    if score >= 8.0 and (bias == "⚠️ AVOID" or "AVOID" in bias):
        bias = "🟢 RUNNER"
    elif score >= 7.0 and (bias == "⚠️ AVOID" or "AVOID" in bias):
        bias = "🟢 RUNNER WATCH"

    phase = build_phase_v3310(structure, coil, second_leg, exhaustion, decay, fakeout=fakeout, participation=participation)
    trade_tier = build_trade_tier_v34(score, bias, phase, exhaustion, decay, fakeout, entry_score, structure_score)
    entry = build_entry_v3310(bias, structure, coil, second_leg, entry_score)

    reasons = []
    risks = []

    reasons.extend(simple_leader_reasons(gain, float_info, volume, news_score))
    if open_drive.get("detected") or open_drive.get("early_detected"):
        reasons.append(open_drive.get("reason") or "open drive momentum")
    reasons.extend(structure_reasons)
    reasons.extend(volume_reasons)
    reasons.extend(participation.get("reasons", []))
    reasons.extend(freshness.get("reasons", []))
    reasons.extend(daily_context.get("reasons", []))
    reasons.extend(entry_reasons)

    if chart_timing.get("reason"):
        reasons.append(chart_timing.get("reason"))
    if chart_timing.get("stale"):
        risks.append(chart_timing.get("reason"))

    risks.extend(risk_reasons)
    if fakeout.get("risk"):
        risks.append(fakeout.get("risk"))
    if sec.get("label"):
        risks.append(sec.get("label"))
    if float_info.get("risk") and float_info.get("tier") in ["TINY", "ELITE", "UNKNOWN"]:
        risks.append(float_info.get("risk"))
    if halt_risk.get("label"):
        risks.append(halt_risk.get("label"))
    if daily_context.get("risk"):
        risks.append(daily_context.get("risk"))

    reasons = dedupe(reasons)
    risks = dedupe(risks)

    # v36.17 contradiction cleaner: do not show SECOND LEG / CONTINUATION
    # with generic avoid/wait wording unless there is an actual broken state.
    phase_text_for_clean = str(phase or "").upper()
    hard_broken_context = bool(
        fakeout.get("detected")
        or exhaustion.get("detected")
        or not bool(get_struct(structure, "above_vwap", False))
    )
    if not hard_broken_context and score >= 6.5 and ("SECOND LEG" in phase_text_for_clean or "RUNNER CONTINUATION" in phase_text_for_clean):
        risks = [r for r in risks if "avoid" not in str(r).lower() and "do not chase" not in str(r).lower()]
        if "AVOID" in str(bias).upper():
            bias = "🟢 RUNNER WATCH"
        if "AVOID" in str(trade_tier).upper():
            trade_tier = "🟢 RUNNER WATCH"

    preview_result = {
        "ticker": ticker, "score": score, "price": price, "gain": gain,
        "volume": volume, "float": float_shares, "bias": bias,
        "trade_tier": trade_tier, "phase": phase, "structure": structure,
        "news_score": news_score, "news": news,
        "daily_context": daily_context, "freshness": freshness,
        "chart_timing": chart_timing,
        "open_drive": open_drive, "open_drive_runner": bool(open_drive.get("detected")), "early_open_drive": bool(open_drive.get("early_detected")),
    }
    preview_result = apply_elite_leader_gate_fix(preview_result)
    trade_tier = preview_result.get("trade_tier", trade_tier)
    bias = preview_result.get("bias", bias)
    phase = preview_result.get("phase", phase)
    elite_leader = bool(preview_result.get("elite_leader"))

    # v36.17 final display sanity pass after elite gate.
    phase_text_for_clean = str(phase or "").upper()
    if score >= 6.5 and ("SECOND LEG" in phase_text_for_clean or "RUNNER CONTINUATION" in phase_text_for_clean):
        if "AVOID" in str(bias).upper() and not fakeout.get("detected") and not exhaustion.get("detected"):
            bias = "🟢 RUNNER WATCH"
        if "AVOID" in str(trade_tier).upper() and not fakeout.get("detected") and not exhaustion.get("detected"):
            trade_tier = "🟢 RUNNER WATCH"

    print(
        f"[LIVE RANK] {ticker} +{gain:.1f}% {fmt_money(price)} "
        f"vol={fmt_big_num(volume)} float={fmt_big_num(float_shares) if float_shares else 'unknown'} "
        f"news={news_score:.1f}/10 now={live_facts_line({'structure': structure, 'chart_timing': chart_timing, 'second_leg': second_leg})}"
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
        "entry_score": entry_score,
        "structure_score": structure_score,
        "volume_score": volume_score,
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
        "freshness": freshness,
        "daily_context": daily_context,
        "chart_timing": chart_timing,
        "elite_leader": elite_leader,
        "open_drive": open_drive,
        "open_drive_runner": bool(open_drive.get("detected")),
        "early_open_drive": bool(open_drive.get("early_detected")),
        "source": source,
    }


def live_momentum_sort_key(r):
    """Pure live feed sorting: what is moving now, not prediction labels."""
    gain = safe_float(r.get("gain", 0))
    volume = safe_int(r.get("volume", 0))
    float_shares = safe_int(r.get("float", 0))
    news_score = safe_float(r.get("news_score", 0))
    chart = r.get("chart_timing") or {}
    above_vwap = bool(get_struct(r.get("structure", {}), "above_vwap", False))
    fresh = bool(chart.get("fresh_breakout") or chart.get("vwap_reclaim") or chart.get("second_leg_ready"))
    low_float_rank = 999_999_999 if float_shares <= 0 else float_shares
    return (
        gain,
        safe_int(volume),
        news_score,
        fresh,
        above_vwap,
        -low_float_rank,
    )


def sort_results(results):
    # v38: pure live momentum feed. Biggest real live movers first.
    return sorted(results, key=live_momentum_sort_key, reverse=True)


def live_facts_line(r):
    chart = r.get("chart_timing") or {}
    facts = []
    if bool(get_struct(r.get("structure", {}), "above_vwap", False)):
        facts.append("above VWAP")
    if chart.get("fresh_breakout"):
        facts.append("fresh HOD")
    if chart.get("vwap_reclaim"):
        facts.append("VWAP reclaim")
    if chart.get("second_leg_ready") or (r.get("second_leg") or {}).get("detected"):
        facts.append("second-leg push")
    vr = safe_float(chart.get("volume_ratio", 0))
    if vr > 0:
        facts.append(f"vol ratio {vr:.1f}x")
    return " | ".join(dedupe(facts)[:4])


def print_top_ranked(results):
    if not results:
        print("[SCAN] No live movers found")
        return

    top = " | ".join(
        f"{r['ticker']} +{r['gain']:.1f}% {fmt_money(r['price'])} vol={fmt_big_num(r.get('volume'))} float={fmt_big_num(r.get('float')) if r.get('float') else 'unknown'}"
        for r in results[:8]
    )
    print(f"[LIVE FEED] Top movers: {top}")


def should_alert(result):
    """v38 pure live data alert rule. No runner/avoid/building prediction gates.

    Telegram rule: never alert below +25%. Above +25%, alert live movers,
    using cooldown only to prevent spam unless the move meaningfully expands.
    """
    ticker = result["ticker"]
    gain = safe_float(result.get("gain", 0))

    if ticker in SENT_THIS_CYCLE:
        print(f"[NO ALERT] {ticker}: already sent this cycle")
        return False

    if gain < ALERT_HARD_MIN_GAIN:
        print(f"[BLOCK] {ticker}: live gain {gain:.1f}% below 25% alert floor")
        return False

    ok, reason = meaningful_change_since_alert(ticker, result)
    if not ok:
        print(f"[COOLDOWN] {ticker}: {reason}")
        return False

    print(f"[ALERT OK] {ticker}: LIVE MOVER — {reason}")
    LAST_ALERT[ticker] = {
        "time": time.time(),
        "price": result["price"],
        "score": result.get("score", 0),
        "bias": "LIVE",
        "day_high": safe_float((result.get("chart_timing") or {}).get("day_high")),
        "chart_label": "LIVE FEED",
    }
    SENT_THIS_CYCLE.add(ticker)
    return True


# ============================================================
# ALERT BUILDER — v38 PURE LIVE MOMENTUM FEED
# ============================================================

def build_alert(result):
    result = normalize_alert_fields(result)
    news = result.get("news") or {}
    news_line = compact_news_line(news)

    float_text = "unknown"
    if result.get("float"):
        float_text = fmt_big_num(result.get("float"))

    lines = [
        f"🔥 LIVE MOVER — {result['ticker']}",
        "",
        f"{fmt_money(result['price'])} | +{result['gain']:.1f}%",
        f"Vol: {fmt_big_num(result.get('volume'))} | Float: {float_text}",
    ]

    facts = live_facts_line(result)
    if facts:
        lines.append(f"Now: {facts}")

    if news_line:
        lines.extend(["", news_line])

    sec_short = compact_dilution_label(result.get("sec"))
    if sec_short:
        lines.append(f"Filing: {sec_short}")

    return "\n".join(lines)


def run_scanner():
    print(f"[BOOT] {BOOT_MARKER}")

    while True:
        try:
            if not market_is_active():
                sleep_for = seconds_until_next_scan_window()
                print(f"[SCANNER OFF] Market closed — sleeping {sleep_for // 60} min; manually stop Render after close to save instance hours")
                time.sleep(sleep_for)
                continue

            SENT_THIS_CYCLE.clear()
            global NEWS_CALLS_THIS_CYCLE, SEC_CALLS_THIS_CYCLE
            NEWS_CALLS_THIS_CYCLE = 0
            SEC_CALLS_THIS_CYCLE = 0

            print(f"[SCAN] Market active — running scan ({get_market_session_label()})")

            candidates = get_candidates()
            if LIVE_SPEED_MODE and len(candidates) > MAX_DEEP_SCAN_NAMES:
                print(f"[SPEED] limiting deep scan {len(candidates)} -> {MAX_DEEP_SCAN_NAMES} names")
                candidates = candidates[:MAX_DEEP_SCAN_NAMES]
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
            time.sleep(dynamic_scan_sleep())

        except Exception as e:
            print(f"[SCANNER ERROR] {e}")
            # If the scanner errors while the market is closed, do not hammer Render/logs.
            time.sleep(30 if market_is_active() else CLOSED_ERROR_SLEEP_SECONDS)


if __name__ == "__main__":
    Thread(target=start_web_server, daemon=True).start()
    run_scanner()
