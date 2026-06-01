import os
import re
import time
import html
import json
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from flask import Flask
from dotenv import load_dotenv

from alerts import send_alert

load_dotenv()

# ============================================================
# CLEAN LEADER-ONLY SCANNER v42.0
# Built fresh: no stacked v39/v40/v41 overrides.
#
# Mission:
#   Track only true market leaders and alert only when they are spiking,
#   breaking HOD, reclaiming VWAP, or setting up a clean second leg.
#
# Live sources:
#   1) StockAnalysis leader table
#   2) TradingView scanner
#   3) Alpaca 1-minute candles
#   4) Finnhub quote fallback
#
# Background news:
#   Finnhub news + GlobeNewswire scrape + PRNewswire scrape
# ============================================================

ET = ZoneInfo("America/New_York")
BOOT_MARKER = "elite scanner v42.1 — CLEAN LEADER ONLY + SCORE/DILUTION"

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

# Market window
SCAN_START = dtime(9, 20)
SCAN_END = dtime(16, 10)
SCAN_SLEEP_OPEN = 15
SCAN_SLEEP_NORMAL = 20
SCAN_SLEEP_CLOSED = 900
SCAN_SLEEP_WEEKEND = 1800

# Universe
MIN_PRICE = 0.50
MAX_PRICE = 80.0
DISCOVERY_MIN_GAIN = 25.0
TRUE_LEADER_MIN_GAIN = 50.0
TRUE_LEADER_MIN_VOLUME = 1_000_000
MAX_TRACKED_LEADERS = 15
LOW_FLOAT_SHARES = 10_000_000

# Alert controls
MAX_ALERTS_PER_CYCLE = 2
ALERT_COOLDOWN_SECONDS = 480
SPIKE_RE_ALERT_COOLDOWN_SECONDS = 240
MAX_OFF_HOD_SPIKE = 12.0
MAX_OFF_HOD_SETUP = 18.0
DEAD_OFF_HOD = 30.0

# Trigger thresholds
FAST_SPIKE_PCT = 10.0
FAST_SPIKE_MIN_VOL_RATIO = 1.5
HOD_BREAKOUT_MIN_VOL_RATIO = 1.4
VWAP_RECLAIM_MIN_LAST3 = 1.5
SECOND_LEG_MAX_PULLBACK = 8.0
SECOND_LEG_MIN_VOL_RATIO = 1.1

# Data
MIN_CANDLES = 8
CANDLE_LIMIT = 90
QUOTE_CACHE_TTL = 15
CANDLE_CACHE_TTL = 12
NEWS_CACHE_TTL = 900
SOURCE_CACHE_TTL = 20
PROFILE_CACHE_TTL = 86400

# News
NEWS_PREFETCH_TOP_N = 8
NEWS_TIMEOUT = 2.0
NEWS_WORKERS = 3

# Score / risk / display
ALERT_MIN_SCORE = 4
FAST_SPIKE_SCORE_FLOOR = 7
HOD_BREAKOUT_SCORE_FLOOR = 6
SETUP_SCORE_FLOOR = 4

# RVOL is estimated from Alpaca 1m candles if historical avg volume is unavailable.
RVOL_STRONG = 10.0
RVOL_EXTREME = 25.0

# Dilution awareness: never blocks alerts and never lowers score.
ENABLE_DILUTION_AWARENESS = True
DILUTION_CACHE_TTL = 21600

# ============================================================
# STATE / CACHES
# ============================================================

QUOTE_CACHE = {}
CANDLE_CACHE = {}
SOURCE_CACHE = {}
NEWS_CACHE = {}
PROFILE_CACHE = {}
DILUTION_CACHE = {}

LAST_ALERT = {}
LEADER_STATE = {}
NEWS_PREFETCH_RUNNING = set()

SESSION_ALERT_COUNT = 0

# ============================================================
# WEB HEALTH
# ============================================================

app = Flask(__name__)

@app.route("/")
def home():
    return f"scanner alive — {BOOT_MARKER}", 200

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
# HELPERS
# ============================================================

def now_et():
    return datetime.now(ET)

def trading_day_key():
    return now_et().strftime("%Y-%m-%d")

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

def cache_get(cache, key, ttl):
    item = cache.get(key)
    if not item:
        return None
    ts, value = item
    if time.time() - ts > ttl:
        cache.pop(key, None)
        return None
    return value

def cache_set(cache, key, value):
    cache[key] = (time.time(), value)
    return value

def http_get(url, params=None, headers=None, timeout=6):
    h = {
        "User-Agent": "Mozilla/5.0 scannerbot/42.0",
        "Accept": "text/html,application/json,*/*",
    }
    if headers:
        h.update(headers)
    return requests.get(url, params=params, headers=h, timeout=timeout)

def http_post(url, payload=None, headers=None, timeout=6):
    h = {
        "User-Agent": "Mozilla/5.0 scannerbot/42.0",
        "Accept": "application/json,text/plain,*/*",
        "Content-Type": "application/json",
    }
    if headers:
        h.update(headers)
    return requests.post(url, json=payload or {}, headers=h, timeout=timeout)

def is_bad_ticker(ticker):
    t = str(ticker or "").upper().strip()
    if not t:
        return True
    if "-" in t or "/" in t:
        return True
    if len(t) > 4 and re.search(r"(W|WS|WT|WQ|RT|R|U)$", t):
        return True
    if any(word in t for word in ["WARRANT", "RIGHT", "UNIT", "PREFERRED"]):
        return True
    return False

def market_is_active():
    now = now_et()
    print(f"[TIME] Market clock ET: {now.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
    if now.weekday() >= 5:
        print("[MARKET] Weekend — sleeping")
        return False
    if SCAN_START <= now.time() <= SCAN_END:
        return True
    print(f"[MARKET] Closed — {now.strftime('%I:%M %p ET')}")
    return False

def get_session_label():
    t = now_et().time()
    if dtime(9, 20) <= t < dtime(9, 30):
        return "PRE-OPEN WARMUP"
    if dtime(9, 30) <= t < dtime(11, 0):
        return "OPEN"
    if dtime(11, 0) <= t < dtime(14, 30):
        return "MIDDAY"
    if dtime(14, 30) <= t <= dtime(16, 10):
        return "POWER HOUR"
    return "CLOSED"

def scan_sleep_seconds():
    if not market_is_active():
        return SCAN_SLEEP_WEEKEND if now_et().weekday() >= 5 else SCAN_SLEEP_CLOSED
    return SCAN_SLEEP_OPEN if get_session_label() == "OPEN" else SCAN_SLEEP_NORMAL

# ============================================================
# LEADER DISCOVERY
# ============================================================

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

def normalize_leader(ticker, price=0, gain=0, volume=0, market_cap=0, source="unknown"):
    return {
        "ticker": str(ticker or "").upper().strip(),
        "price": safe_float(price),
        "gain": safe_float(gain),
        "volume": safe_int(volume),
        "market_cap": safe_int(market_cap),
        "sources": [source],
        "source": source,
    }

def source_pass(item):
    ticker = item.get("ticker", "")
    if is_bad_ticker(ticker):
        return False
    gain = safe_float(item.get("gain"))
    price = safe_float(item.get("price"))
    volume = safe_int(item.get("volume"))

    if gain < DISCOVERY_MIN_GAIN:
        return False
    if price and (price < MIN_PRICE or price > MAX_PRICE):
        return False
    if volume and volume < 50_000:
        return False
    return True

def get_stockanalysis_leaders():
    cached = cache_get(SOURCE_CACHE, "stockanalysis", SOURCE_CACHE_TTL)
    if cached is not None:
        return cached

    url = "https://stockanalysis.com/markets/gainers/"
    results = []
    try:
        r = http_get(url, timeout=5)
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table tbody tr")
        for row in rows[:80]:
            cells = [clean_text(c.get_text(" ")) for c in row.find_all("td")]
            if len(cells) < 5:
                continue
            ticker = cells[1]
            gain = safe_float(cells[3].replace("+", "").replace("%", "")) if len(cells) > 3 else 0
            price = safe_float(cells[4].replace("$", "").replace(",", "")) if len(cells) > 4 else 0
            volume = parse_big_number_text(cells[5] if len(cells) > 5 else 0)
            market_cap = parse_big_number_text(cells[6] if len(cells) > 6 else 0)

            item = normalize_leader(ticker, price, gain, volume, market_cap, "StockAnalysis")
            if source_pass(item):
                results.append(item)

        print(f"[GAINERS] StockAnalysis returned {len(results)} leaders")
    except Exception as e:
        print(f"[GAINERS ERROR] StockAnalysis: {e}")

    return cache_set(SOURCE_CACHE, "stockanalysis", results)

def get_tradingview_leaders():
    cached = cache_get(SOURCE_CACHE, "tradingview", SOURCE_CACHE_TTL)
    if cached is not None:
        return cached

    url = "https://scanner.tradingview.com/america/scan"
    payload = {
        "filter": [
            {"left": "type", "operation": "equal", "right": "stock"},
            {"left": "subtype", "operation": "in_range", "right": ["common", "foreign-issuer"]},
            {"left": "exchange", "operation": "in_range", "right": ["NASDAQ", "NYSE", "AMEX"]},
            {"left": "change", "operation": "greater", "right": DISCOVERY_MIN_GAIN},
            {"left": "close", "operation": "greater", "right": MIN_PRICE},
            {"left": "close", "operation": "less", "right": MAX_PRICE},
            {"left": "volume", "operation": "greater", "right": 50_000},
        ],
        "options": {"lang": "en"},
        "markets": ["america"],
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change", "volume", "market_cap_basic"],
        "sort": {"sortBy": "change", "sortOrder": "desc"},
        "range": [0, 250],
    }

    results = []
    try:
        r = http_post(url, payload=payload, timeout=5)
        data = r.json()
        for row in data.get("data", []):
            d = row.get("d") or []
            if len(d) < 4:
                continue
            item = normalize_leader(
                ticker=d[0],
                price=d[1] if len(d) > 1 else 0,
                gain=d[2] if len(d) > 2 else 0,
                volume=d[3] if len(d) > 3 else 0,
                market_cap=d[4] if len(d) > 4 else 0,
                source="TradingView",
            )
            if source_pass(item):
                results.append(item)

        print(f"[GAINERS] TradingView returned {len(results)} leaders")
    except Exception as e:
        print(f"[GAINERS ERROR] TradingView: {e}")

    return cache_set(SOURCE_CACHE, "tradingview", results)

def merge_leaders(items):
    merged = {}
    for item in items:
        ticker = item.get("ticker", "").upper().strip()
        if is_bad_ticker(ticker):
            continue

        existing = merged.get(ticker)
        if not existing:
            merged[ticker] = dict(item)
            continue

        existing["gain"] = max(safe_float(existing.get("gain")), safe_float(item.get("gain")))
        existing["price"] = safe_float(item.get("price")) or safe_float(existing.get("price"))
        existing["volume"] = max(safe_int(existing.get("volume")), safe_int(item.get("volume")))
        existing["market_cap"] = safe_int(existing.get("market_cap")) or safe_int(item.get("market_cap"))

        for src in item.get("sources", [item.get("source", "unknown")]):
            if src not in existing["sources"]:
                existing["sources"].append(src)
        existing["source"] = "+".join(existing["sources"])

    leaders = list(merged.values())
    leaders.sort(key=lambda x: (
        safe_float(x.get("gain")) >= 100,
        safe_float(x.get("gain")) >= 50,
        safe_int(x.get("volume")),
        safe_float(x.get("gain")),
    ), reverse=True)
    return leaders

def get_true_leaders():
    leaders = []
    leaders.extend(get_stockanalysis_leaders())
    leaders.extend(get_tradingview_leaders())
    merged = merge_leaders(leaders)

    true = []
    skipped = 0
    for item in merged:
        gain = safe_float(item.get("gain"))
        vol = safe_int(item.get("volume"))
        if gain >= TRUE_LEADER_MIN_GAIN and vol >= TRUE_LEADER_MIN_VOLUME:
            true.append(item)
        else:
            skipped += 1

    true = true[:MAX_TRACKED_LEADERS]
    print(f"[v42 LEADER ONLY] true_leaders={len(true)} skipped_nonleaders={skipped} -> tracking {len(true)}")
    if true:
        print("[v42 TRACKING] " + " | ".join(
            f"{x['ticker']} +{safe_float(x.get('gain')):.1f}% vol={fmt_big_num(x.get('volume'))}"
            for x in true[:12]
        ))
    return true

# ============================================================
# QUOTES / CANDLES
# ============================================================

def get_finnhub_quote(ticker):
    cached = cache_get(QUOTE_CACHE, ticker, QUOTE_CACHE_TTL)
    if cached:
        return cached

    if not FINNHUB_API_KEY:
        return {"price": 0, "gain": 0, "source": "none", "confirmed": False, "reason": "missing FINNHUB_API_KEY"}

    try:
        url = "https://finnhub.io/api/v1/quote"
        r = http_get(url, params={"symbol": ticker, "token": FINNHUB_API_KEY}, timeout=2.2)
        data = r.json()
        price = safe_float(data.get("c"))
        prev_close = safe_float(data.get("pc"))
        gain = ((price - prev_close) / prev_close) * 100 if price > 0 and prev_close > 0 else 0
        q = {
            "price": price,
            "gain": gain,
            "source": "Finnhub",
            "confirmed": price > 0 and -95 < gain < 900,
            "reason": "",
        }
        if q["confirmed"]:
            return cache_set(QUOTE_CACHE, ticker, q)
        return q
    except Exception as e:
        return {"price": 0, "gain": 0, "source": "Finnhub", "confirmed": False, "reason": str(e)}

def alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY or "",
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY or "",
    }

def normalize_candle(raw):
    if not isinstance(raw, dict):
        return None
    o = raw.get("o", raw.get("open"))
    h = raw.get("h", raw.get("high"))
    l = raw.get("l", raw.get("low"))
    c = raw.get("c", raw.get("close"))
    v = raw.get("v", raw.get("volume", 0))
    t = raw.get("t", raw.get("timestamp", ""))
    if None in [o, h, l, c]:
        return None
    return {
        "t": t,
        "open": safe_float(o),
        "high": safe_float(h),
        "low": safe_float(l),
        "close": safe_float(c),
        "volume": safe_int(v),
    }

def get_alpaca_candles(ticker, limit=CANDLE_LIMIT):
    key = f"alpaca:{ticker}:{limit}"
    cached = cache_get(CANDLE_CACHE, key, CANDLE_CACHE_TTL)
    if cached is not None:
        return cached

    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        print(f"[CANDLES] {ticker}: missing Alpaca keys")
        return []

    try:
        end = datetime.now(ET)
        start = end - timedelta(hours=8)
        url = f"{ALPACA_BASE_URL}/v2/stocks/{ticker}/bars"
        params = {
            "timeframe": "1Min",
            "start": start.astimezone(ZoneInfo("UTC")).isoformat(),
            "end": end.astimezone(ZoneInfo("UTC")).isoformat(),
            "limit": limit,
            "adjustment": "raw",
            "feed": "iex",
            "sort": "asc",
        }
        r = http_get(url, params=params, headers=alpaca_headers(), timeout=3)
        data = r.json()
        bars = [normalize_candle(x) for x in data.get("bars", [])]
        bars = [x for x in bars if x]
        if bars:
            bars = bars[:-1] if len(bars) > 1 else bars  # ignore active bar
        print(f"[CANDLES] {ticker}: Alpaca {len(bars)} finalized bars")
        return cache_set(CANDLE_CACHE, key, bars)
    except Exception as e:
        print(f"[CANDLES ERROR] {ticker}: Alpaca {e}")
        return []

# ============================================================
# STRUCTURE / TRIGGERS
# ============================================================

def compute_vwap(candles):
    pv = 0.0
    vol = 0
    for c in candles:
        typical = (c["high"] + c["low"] + c["close"]) / 3
        v = safe_int(c["volume"])
        pv += typical * v
        vol += v
    return pv / vol if vol > 0 else 0

def percent_change(a, b):
    a = safe_float(a)
    b = safe_float(b)
    if b <= 0:
        return 0.0
    return ((a - b) / b) * 100

def moving_now_metrics(candles):
    if len(candles) < MIN_CANDLES:
        return {"ok": False, "reason": f"insufficient candles {len(candles)}/{MIN_CANDLES}"}

    last = candles[-1]
    price = last["close"]
    high = max(c["high"] for c in candles)
    low5 = min(c["low"] for c in candles[-5:])
    high5 = max(c["high"] for c in candles[-5:])
    vwap = compute_vwap(candles)

    last3_close = candles[-3]["close"] if len(candles) >= 3 else candles[0]["close"]
    last3_change = percent_change(price, last3_close)

    recent_vol = sum(c["volume"] for c in candles[-3:]) / max(1, min(3, len(candles)))
    prior_slice = candles[-8:-3] if len(candles) >= 8 else candles[:-3]
    prior_vol = sum(c["volume"] for c in prior_slice) / max(1, len(prior_slice)) if prior_slice else 0
    vol_ratio = recent_vol / prior_vol if prior_vol > 0 else 1.0

    off_hod = percent_change(high, price)
    from_low5 = percent_change(price, low5)
    rvol = estimate_rvol(candles)

    near_hod = off_hod <= 3.0
    fresh_hod = price >= high * 0.995 or high5 >= high * 0.995

    above_vwap = vwap > 0 and price >= vwap
    reclaim_vwap = above_vwap and len(candles) >= 6 and candles[-4]["close"] < vwap

    pullback_from_hod = off_hod
    second_leg = (
        above_vwap
        and pullback_from_hod <= SECOND_LEG_MAX_PULLBACK
        and last3_change >= 0
        and vol_ratio >= SECOND_LEG_MIN_VOL_RATIO
    )

    fast_spike = from_low5 >= FAST_SPIKE_PCT and vol_ratio >= FAST_SPIKE_MIN_VOL_RATIO and above_vwap
    hod_breakout = fresh_hod and vol_ratio >= HOD_BREAKOUT_MIN_VOL_RATIO and above_vwap
    vwap_reclaim = reclaim_vwap and last3_change >= VWAP_RECLAIM_MIN_LAST3

    return {
        "ok": True,
        "price": price,
        "high": high,
        "low5": low5,
        "vwap": vwap,
        "off_hod": off_hod,
        "from_low5": from_low5,
        "last3_change": last3_change,
        "vol_ratio": vol_ratio,
        "rvol": rvol,
        "above_vwap": above_vwap,
        "fresh_hod": fresh_hod,
        "near_hod": near_hod,
        "fast_spike": fast_spike,
        "hod_breakout": hod_breakout,
        "vwap_reclaim": vwap_reclaim,
        "second_leg": second_leg,
    }

def classify_trigger(metrics):
    if not metrics.get("ok"):
        return None, metrics.get("reason", "bad metrics")

    off_hod = safe_float(metrics.get("off_hod"))
    if off_hod >= DEAD_OFF_HOD:
        return None, f"dead leader — {off_hod:.1f}% off HOD"

    if metrics.get("fast_spike") and off_hod <= MAX_OFF_HOD_SPIKE:
        return "🚀 FAST LEADER SPIKE", (
            f"+{safe_float(metrics.get('from_low5')):.1f}% from 5m low | "
            f"vol {safe_float(metrics.get('vol_ratio')):.1f}x | "
            f"RVOL {safe_float(metrics.get('rvol')):.1f}x | "
            f"last3 {safe_float(metrics.get('last3_change')):+.1f}%"
        )

    if metrics.get("hod_breakout"):
        return "🔥 FRESH HOD BREAKOUT", (
            f"fresh HOD | vol {safe_float(metrics.get('vol_ratio')):.1f}x | "
            f"RVOL {safe_float(metrics.get('rvol')):.1f}x | "
            f"last3 {safe_float(metrics.get('last3_change')):+.1f}%"
        )

    if metrics.get("vwap_reclaim") and off_hod <= MAX_OFF_HOD_SETUP:
        return "🟢 VWAP RECLAIM PUSH", (
            f"VWAP reclaim | last3 {safe_float(metrics.get('last3_change')):+.1f}% | "
            f"vol {safe_float(metrics.get('vol_ratio')):.1f}x | RVOL {safe_float(metrics.get('rvol')):.1f}x"
        )

    if metrics.get("second_leg") and off_hod <= MAX_OFF_HOD_SETUP:
        return "📈 SECOND-LEG SETUP", (
            f"above VWAP | {off_hod:.1f}% off HOD | "
            f"vol {safe_float(metrics.get('vol_ratio')):.1f}x | RVOL {safe_float(metrics.get('rvol')):.1f}x"
        )

    if not metrics.get("above_vwap"):
        return None, "not above/reclaiming VWAP"

    return None, f"stale/no trigger — {off_hod:.1f}% off HOD"

# ============================================================
# NEWS
# ============================================================

STRONG_NEWS_WORDS = [
    "merger", "acquisition", "definitive agreement", "contract", "purchase order",
    "partnership", "collaboration", "license", "distribution", "fda", "clearance",
    "approval", "clinical", "phase 2", "phase 3", "trial", "positive results",
    "earnings", "guidance", "raises outlook", "financing", "strategic investment",
    "bitcoin", "crypto", "ai", "artificial intelligence", "defense", "energy",
]

JUNK_NEWS_PHRASES = [
    "stocks moving", "premarket movers", "market movers", "why shares are trading",
    "gap-ups and gap-downs", "most active", "top gainers", "biggest pre-market",
]

def classify_news(headline):
    h = clean_text(headline)
    low = h.lower()
    if not h:
        return "UNKNOWN CATALYST — INVESTIGATE", "UNKNOWN", 0
    if any(x in low for x in JUNK_NEWS_PHRASES):
        return h, "JUNK", 1
    if any(x in low for x in STRONG_NEWS_WORDS):
        return h, "STRONG", 9
    return h, "UNCLEAR", 4

def ticker_word_match(ticker, text):
    return re.search(rf"(?<![A-Z]){re.escape(ticker.upper())}(?![A-Z])", str(text).upper()) is not None

def get_finnhub_news(ticker):
    if not FINNHUB_API_KEY:
        return ""
    try:
        end = now_et().date()
        start = end - timedelta(days=3)
        url = "https://finnhub.io/api/v1/company-news"
        params = {
            "symbol": ticker,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "token": FINNHUB_API_KEY,
        }
        r = http_get(url, params=params, timeout=NEWS_TIMEOUT)
        rows = r.json() or []
        for row in rows[:8]:
            headline = clean_text(row.get("headline", ""))
            if headline and ticker_word_match(ticker, headline):
                return headline
        for row in rows[:8]:
            headline = clean_text(row.get("headline", ""))
            if headline:
                return headline
    except Exception as e:
        print(f"[NEWS FAST ERROR] Finnhub {ticker}: {e}")
    return ""

def scrape_globe_news(ticker):
    try:
        url = f"https://www.globenewswire.com/search/keyword/{ticker}"
        r = http_get(url, timeout=NEWS_TIMEOUT)
        soup = BeautifulSoup(r.text, "html.parser")
        text = clean_text(soup.get_text(" "))
        m = re.search(r"([A-Z][^\.]{30,180})", text)
        if m and ticker_word_match(ticker, m.group(1)):
            return m.group(1)
    except Exception:
        pass
    return ""

def scrape_prnewswire(ticker):
    try:
        url = f"https://www.prnewswire.com/search/news/?keyword={ticker}"
        r = http_get(url, timeout=NEWS_TIMEOUT)
        soup = BeautifulSoup(r.text, "html.parser")
        text = clean_text(soup.get_text(" "))
        m = re.search(r"([A-Z][^\.]{30,180})", text)
        if m and ticker_word_match(ticker, m.group(1)):
            return m.group(1)
    except Exception:
        pass
    return ""

def get_best_news(ticker):
    cached = cache_get(NEWS_CACHE, ticker, NEWS_CACHE_TTL)
    if cached:
        return cached

    candidates = []
    with ThreadPoolExecutor(max_workers=NEWS_WORKERS) as pool:
        futures = [
            pool.submit(get_finnhub_news, ticker),
            pool.submit(scrape_globe_news, ticker),
            pool.submit(scrape_prnewswire, ticker),
        ]
        for fut in as_completed(futures, timeout=NEWS_TIMEOUT + 1.0):
            try:
                headline = clean_text(fut.result(timeout=0))
                if headline:
                    candidates.append(headline)
            except Exception:
                pass

    best = ("UNKNOWN CATALYST — INVESTIGATE", "UNKNOWN", 0)
    for h in candidates:
        classified = classify_news(h)
        if classified[2] > best[2]:
            best = classified
    NEWS_CACHE[ticker] = (time.time(), best)
    print(f"[NEWS] {ticker}: {best[0]} ({best[1]} {best[2]}/10)")
    return best

def prefetch_news_for_leaders(leaders):
    tickers = [x["ticker"] for x in leaders[:NEWS_PREFETCH_TOP_N]]
    todo = []
    for t in tickers:
        if cache_get(NEWS_CACHE, t, NEWS_CACHE_TTL):
            continue
        if t in NEWS_PREFETCH_RUNNING:
            continue
        NEWS_PREFETCH_RUNNING.add(t)
        todo.append(t)

    if not todo:
        return

    def worker():
        try:
            with ThreadPoolExecutor(max_workers=NEWS_WORKERS) as pool:
                list(pool.map(get_best_news, todo))
        finally:
            for t in todo:
                NEWS_PREFETCH_RUNNING.discard(t)

    Thread(target=worker, daemon=True).start()
    print("[NEWS PREFETCH] started for " + ", ".join(todo))


# ============================================================
# SCORE / RVOL / DILUTION AWARENESS
# ============================================================

def estimate_rvol(candles):
    """
    Fast intraday RVOL proxy:
    recent 3-bar avg volume vs earlier same-session average.
    """
    if len(candles) < 12:
        return 1.0
    recent = sum(c["volume"] for c in candles[-3:]) / 3
    baseline_slice = candles[-30:-5] if len(candles) >= 35 else candles[:-5]
    if not baseline_slice:
        return 1.0
    baseline = sum(c["volume"] for c in baseline_slice) / max(1, len(baseline_slice))
    return recent / baseline if baseline > 0 else 1.0

def leader_score(gain, trigger, metrics, news):
    """
    v42.1 simplified 10-point leader score.

    Day gain:       0-3
    Live expansion: 0-3
    Structure:      0-2
    Volume/RVOL:    0-1
    Catalyst:       0-1

    Dilution is awareness only and never changes score.
    """
    score = 0.0
    g = safe_float(gain)

    if g >= 200:
        score += 3
    elif g >= 100:
        score += 2
    elif g >= 50:
        score += 1

    if "FAST LEADER SPIKE" in trigger:
        score += 2
        if metrics.get("fresh_hod"):
            score += 1
    elif "HOD BREAKOUT" in trigger:
        score += 2
    elif metrics.get("fresh_hod"):
        score += 1

    if metrics.get("above_vwap"):
        score += 1
    if "SECOND-LEG" in trigger or "VWAP RECLAIM" in trigger:
        score += 1

    if safe_float(metrics.get("vol_ratio")) >= 1.5 or safe_float(metrics.get("rvol")) >= RVOL_STRONG:
        score += 1

    news_quality = (news or ("", "UNKNOWN", 0))[1]
    if news_quality == "STRONG":
        score += 1

    return min(10, round(score, 1))

def score_floor_for_trigger(trigger):
    if "FAST LEADER SPIKE" in trigger:
        return FAST_SPIKE_SCORE_FLOOR
    if "HOD BREAKOUT" in trigger:
        return HOD_BREAKOUT_SCORE_FLOOR
    return SETUP_SCORE_FLOOR

DILUTION_KEYWORDS = [
    "atm", "at-the-market", "sales agreement", "equity distribution",
    "shelf", "s-1", "s-3", "f-1", "f-3", "424b5", "424b3",
    "registered direct", "public offering", "private placement",
    "warrant", "convertible", "resale", "equity line", "purchase agreement",
]

def normalize_dilution_text(text):
    t = clean_text(text)
    low = t.lower()
    if not t:
        return ""

    if any(x in low for x in ["priced offering", "announces pricing", "filed today"]):
        return "⚠️ OFFERING FILED/PRICED TODAY"

    if "at-the-market" in low or re.search(r"\batm\b", low) or "sales agreement" in low:
        return "ATM / share sales ability on file"

    if "warrant" in low:
        return "Warrants on file"

    if any(x in low for x in ["s-1", "s-3", "f-1", "f-3", "424b5", "424b3", "shelf", "resale"]):
        return "Shelf/resale registration on file"

    if any(x in low for x in ["registered direct", "public offering", "private placement", "convertible", "equity line"]):
        return "Financing/offering ability on file"

    return ""

def get_sec_company_tickers():
    cached = cache_get(DILUTION_CACHE, "__sec_tickers__", 86400)
    if cached:
        return cached
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {"User-Agent": "scannerbot contact@example.com"}
        r = http_get(url, headers=headers, timeout=4)
        data = r.json()
        mapping = {}
        for _, row in data.items():
            ticker = str(row.get("ticker", "")).upper()
            cik = str(row.get("cik_str", "")).zfill(10)
            if ticker and cik:
                mapping[ticker] = cik
        return cache_set(DILUTION_CACHE, "__sec_tickers__", mapping)
    except Exception as e:
        print(f"[SEC MAP ERROR] {e}")
        return {}

def get_dilution_awareness(ticker):
    """
    Lightweight SEC awareness scan.
    Never blocks alerts. Never lowers score.
    """
    if not ENABLE_DILUTION_AWARENESS:
        return ""

    cached = cache_get(DILUTION_CACHE, ticker, DILUTION_CACHE_TTL)
    if cached is not None:
        return cached

    try:
        mapping = get_sec_company_tickers()
        cik = mapping.get(ticker.upper())
        if not cik:
            return cache_set(DILUTION_CACHE, ticker, "")

        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        headers = {"User-Agent": "scannerbot contact@example.com"}
        r = http_get(url, headers=headers, timeout=3)
        data = r.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])[:20]
        dates = recent.get("filingDate", [])[:20]
        desc = recent.get("primaryDocDescription", [])[:20]

        findings = []
        today = now_et().date().isoformat()

        for form, date, d in zip(forms, dates, desc):
            blob = f"{form} {date} {d}"
            low = blob.lower()
            if str(date) == today and form in ["S-1", "S-3", "F-1", "F-3", "424B5", "424B3", "8-K", "6-K"]:
                findings.append("⚠️ OFFERING/FILING TODAY")
            if any(k in low for k in DILUTION_KEYWORDS) or form in ["S-1", "S-3", "F-1", "F-3", "424B5", "424B3"]:
                clean = normalize_dilution_text(blob) or f"{form} on file"
                findings.append(clean)

        out = []
        seen = set()
        for f in findings:
            if f and f not in seen:
                seen.add(f)
                out.append(f)
        summary = "; ".join(out[:3])
        if summary:
            print(f"[DILUTION] {ticker}: {summary}")
        return cache_set(DILUTION_CACHE, ticker, summary)

    except Exception as e:
        print(f"[DILUTION ERROR] {ticker}: {e}")
        return cache_set(DILUTION_CACHE, ticker, "")

# ============================================================
# ALERTS
# ============================================================

def alert_key(ticker):
    return f"{trading_day_key()}:{ticker}"

def can_alert(ticker, trigger, metrics):
    key = alert_key(ticker)
    now_ts = time.time()
    state = LAST_ALERT.get(key)

    if not state:
        return True, "first leader alert"

    last_ts = safe_float(state.get("time"))
    last_price = safe_float(state.get("price"))
    last_high = safe_float(state.get("high"))
    price = safe_float(metrics.get("price"))
    high = safe_float(metrics.get("high"))

    cooldown = SPIKE_RE_ALERT_COOLDOWN_SECONDS if "SPIKE" in trigger else ALERT_COOLDOWN_SECONDS
    if now_ts - last_ts < cooldown:
        # Let true fast spike bypass normal cooldown only if price expanded meaningfully.
        if "SPIKE" in trigger and last_price > 0 and price >= last_price * 1.08:
            return True, "fast spike expanded over last alert"
        left = int(cooldown - (now_ts - last_ts))
        return False, f"cooldown {left}s left"

    if last_price > 0 and price >= last_price * 1.06:
        return True, "price expanded over last alert"
    if last_high > 0 and high >= last_high * 1.02:
        return True, "new high expansion"
    if "SPIKE" in trigger:
        return True, "fresh fast spike"

    return False, "no meaningful expansion since last alert"

def build_alert(result):
    ticker = result["ticker"]
    news = result.get("news") or ("UNKNOWN CATALYST — INVESTIGATE", "UNKNOWN", 0)
    headline, quality, news_score = news
    dilution = result.get("dilution") or ""

    lines = [
        f"{result['trigger']} — {ticker}",
        "",
        f"{fmt_money(result['price'])} | +{result['gain']:.1f}%",
        f"Vol: {fmt_big_num(result['volume'])}",
        f"Leader Score: {result.get('score', 0)}/10",
        "",
        f"NEWS: {headline}",
        "",
        "WHY:",
        f"{result['why']}",
    ]

    if dilution:
        lines.extend([
            "",
            "⚠️ DILUTION RISK:",
            dilution,
        ])

    lines.extend([
        "",
        f"Data: {result['data_label']}",
    ])

    return "\n".join(lines).strip()

def send_leader_alert(result):
    msg = build_alert(result)
    send_alert(msg)
    key = alert_key(result["ticker"])
    LAST_ALERT[key] = {
        "time": time.time(),
        "price": result["price"],
        "high": result.get("high", result["price"]),
        "trigger": result["trigger"],
    }
    print(f"[ALERT SENT] {result['ticker']} {result['trigger']}")

# ============================================================
# SCAN LOGIC
# ============================================================

def validate_quote_against_source(ticker, source_gain, quote):
    price = safe_float(quote.get("price"))
    live_gain = safe_float(quote.get("gain"))
    if price <= 0:
        return False, "bad quote price"

    # If quote is wildly far from source gain, don't trust it unless source gain is stale lower and live is higher.
    diff = abs(live_gain - source_gain)
    if diff > 80 and live_gain < source_gain:
        return False, f"quote/source mismatch live {live_gain:.1f}% vs source {source_gain:.1f}%"

    return True, "quote ok"

def evaluate_leader(item):
    ticker = item["ticker"]
    source_gain = safe_float(item.get("gain"))
    source_volume = safe_int(item.get("volume"))
    source_price = safe_float(item.get("price"))

    quote = get_finnhub_quote(ticker)
    quote_ok, quote_reason = validate_quote_against_source(ticker, source_gain, quote)
    if not quote_ok:
        print(f"[v42 SKIP] {ticker}: {quote_reason}")
        return None

    candles = get_alpaca_candles(ticker)
    metrics = moving_now_metrics(candles)
    if not metrics.get("ok"):
        print(f"[v42 LOG ONLY] {ticker}: {metrics.get('reason')}")
        return None

    candle_price = safe_float(metrics.get("price"))
    quote_price = safe_float(quote.get("price"))

    # Use candle close as live trading truth when available.
    price = candle_price or quote_price or source_price
    live_gain = safe_float(quote.get("gain")) or source_gain
    volume = source_volume

    trigger, why = classify_trigger(metrics)
    if not trigger:
        print(f"[v42 LOG ONLY] {ticker}: {why}")
        return None

    allowed, reason = can_alert(ticker, trigger, metrics)
    if not allowed:
        print(f"[NO ALERT] {ticker}: {reason}")
        return None

    cached_news = cache_get(NEWS_CACHE, ticker, NEWS_CACHE_TTL)
    news = cached_news if cached_news else ("UNKNOWN CATALYST — INVESTIGATE", "UNKNOWN", 0)

    score = leader_score(live_gain, trigger, metrics, news)
    floor = score_floor_for_trigger(trigger)
    if score < floor:
        print(f"[NO ALERT] {ticker}: score {score}/10 below {floor} floor for {trigger}")
        return None

    dilution = get_dilution_awareness(ticker)

    data_label = "Alpaca candles + Finnhub quote"
    if not quote.get("confirmed"):
        data_label = "Alpaca candles + source gain"

    result = {
        "ticker": ticker,
        "trigger": trigger,
        "price": price,
        "gain": live_gain,
        "volume": volume,
        "high": safe_float(metrics.get("high")),
        "why": why,
        "news": news,
        "dilution": dilution,
        "score": score,
        "data_label": data_label,
    }

    print(
        f"[v42.1 ALERT OK] {ticker}: {trigger} score={score}/10 {why} "
        f"price={fmt_money(price)} gain={live_gain:.1f}%"
    )
    return result

def run_scan_cycle():
    if not market_is_active():
        return 0

    session = get_session_label()
    print(f"[SCAN] v42 clean leader-only scan ({session})")

    leaders = get_true_leaders()
    if not leaders:
        print("[SCAN] no true leaders found")
        return 0

    prefetch_news_for_leaders(leaders)

    sent = 0
    for item in leaders:
        if sent >= MAX_ALERTS_PER_CYCLE:
            print("[SCAN] max alerts per cycle reached")
            break

        result = evaluate_leader(item)
        if result:
            send_leader_alert(result)
            sent += 1

    if sent:
        print(f"[SCAN] Cycle complete — {sent} leader alert(s) sent")
    else:
        print("[SCAN] Cycle complete — no leader spike/setup alerts")
    return sent

def scanner_loop():
    print(f"[BOOT] {BOOT_MARKER}")
    while True:
        try:
            run_scan_cycle()
            time.sleep(scan_sleep_seconds())
        except Exception as e:
            print(f"[SCAN ERROR] {e}")
            time.sleep(30)

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    Thread(target=start_web_server, daemon=True).start()
    scanner_loop()
