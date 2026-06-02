from __future__ import annotations

import json
import logging
import math
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import requests
except Exception as exc:  # pragma: no cover
    raise RuntimeError("requests is required") from exc

try:
    from flask import Flask
except Exception:
    Flask = None

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


# =============================================================================
# CONFIG
# =============================================================================

VERSION = "v43-clean-rebuild-leader-first"
EASTERN_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone(timedelta(hours=-5))

PORT = int(os.getenv("PORT", "10000"))
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "45"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "5.5"))
MAX_ALERTS_PER_CYCLE = int(os.getenv("MAX_ALERTS_PER_CYCLE", "3"))

MIN_ALERT_GAIN_PCT = float(os.getenv("MIN_ALERT_GAIN_PCT", "25"))
MIN_SCAN_GAIN_PCT = float(os.getenv("MIN_SCAN_GAIN_PCT", "20"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.30"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "80"))
MIN_DAY_VOLUME = int(os.getenv("MIN_DAY_VOLUME", "300000"))
MIN_RECENT_VOLUME = int(os.getenv("MIN_RECENT_VOLUME", "75000"))
MAJOR_LEADER_GAIN_PCT = float(os.getenv("MAJOR_LEADER_GAIN_PCT", "50"))
FAST_SPIKE_PCT = float(os.getenv("FAST_SPIKE_PCT", "10"))
FAST_SPIKE_WINDOW_MIN = int(os.getenv("FAST_SPIKE_WINDOW_MIN", "5"))
MEANINGFUL_NEW_HIGH_PCT = float(os.getenv("MEANINGFUL_NEW_HIGH_PCT", "4"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "600"))
YAHOO_COOLDOWN_SECONDS = int(os.getenv("YAHOO_COOLDOWN_SECONDS", "300"))
SOURCE_COOLDOWN_SECONDS = int(os.getenv("SOURCE_COOLDOWN_SECONDS", "120"))
LEADER_CACHE_TTL_SECONDS = int(os.getenv("LEADER_CACHE_TTL_SECONDS", "900"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("scanner")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json,text/plain,*/*"})


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class Leader:
    ticker: str
    price: float = 0.0
    change_pct: float = 0.0
    volume: int = 0
    source: str = "unknown"
    name: str = ""
    market_cap: Optional[float] = None
    float_shares: Optional[float] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> "Leader":
        self.ticker = normalize_ticker(self.ticker)
        return self


@dataclass
class Candle:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass
class Quote:
    ticker: str
    price: float = 0.0
    prev_close: float = 0.0
    change_pct: float = 0.0
    day_volume: int = 0
    high: float = 0.0
    low: float = 0.0
    source: str = "unknown"
    age_seconds: Optional[int] = None


@dataclass
class Structure:
    above_vwap: bool = False
    vwap: Optional[float] = None
    recent_volume: int = 0
    last3_change_pct: float = 0.0
    last5_low: Optional[float] = None
    recent_high: Optional[float] = None
    hod: Optional[float] = None
    near_hod: bool = False
    new_high_push: bool = False
    higher_lows: bool = False
    breakout: bool = False
    data_ok: bool = False
    reason: str = ""


@dataclass
class NewsResult:
    grade: str = "NONE"  # STRONG, WEAK, JUNK, NONE
    headline: str = ""
    source: str = ""
    url: str = ""
    published_at: str = ""
    dilution_flag: bool = False
    dilution_note: str = ""


@dataclass
class AlertState:
    last_alert_ts: float = 0.0
    last_alert_price: float = 0.0
    last_alert_high: float = 0.0
    last_alert_type: str = ""
    last_quality: int = 0
    baseline_price: float = 0.0
    baseline_ts: float = 0.0


@dataclass
class CandidateDecision:
    ticker: str
    should_alert: bool
    alert_type: str = ""
    quality: int = 0
    reasons: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    quote: Optional[Quote] = None
    structure: Optional[Structure] = None
    news: Optional[NewsResult] = None
    leader: Optional[Leader] = None


# =============================================================================
# GLOBAL STATE
# =============================================================================

STATE_LOCK = threading.Lock()
SOURCE_BLOCKED_UNTIL: Dict[str, float] = {}
LAST_GOOD_LEADERS: List[Leader] = []
LAST_GOOD_LEADERS_TS: float = 0.0
ALERT_STATES: Dict[str, AlertState] = {}
RUNNING = True
LAST_CYCLE_SUMMARY: Dict[str, Any] = {}


# =============================================================================
# UTILITIES
# =============================================================================

def now_et() -> datetime:
    return datetime.now(EASTERN_TZ)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_ticker(ticker: Any) -> str:
    if ticker is None:
        return ""
    t = str(ticker).upper().strip()
    t = t.replace("$", "").replace(".", "-")
    t = re.sub(r"[^A-Z0-9\-]", "", t)
    return t


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
            if value in {"", "-", "N/A", "None"}:
                return default
        x = float(value)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if value in {"", "-", "N/A", "None"}:
                return default
            mult = 1
            if value[-1:].upper() == "K":
                mult = 1_000
                value = value[:-1]
            elif value[-1:].upper() == "M":
                mult = 1_000_000
                value = value[:-1]
            return int(float(value) * mult)
        return int(float(value))
    except Exception:
        return default


def pct_change(new: float, old: float) -> float:
    if old <= 0:
        return 0.0
    return ((new - old) / old) * 100.0


def clamp(n: float, low: float, high: float) -> float:
    return max(low, min(high, n))


def source_allowed(name: str) -> bool:
    return time.time() >= SOURCE_BLOCKED_UNTIL.get(name, 0.0)


def block_source(name: str, seconds: int, reason: str = "") -> None:
    SOURCE_BLOCKED_UNTIL[name] = time.time() + seconds
    extra = f" — {reason}" if reason else ""
    log.warning("[%s BLOCKED] cooling down %ss%s", name.upper(), seconds, extra)


def http_get(url: str, *, source: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None,
             timeout: float = HTTP_TIMEOUT) -> Optional[requests.Response]:
    if not source_allowed(source):
        log.warning("[%s SKIP] source cooling down", source.upper())
        return None
    try:
        resp = SESSION.get(url, params=params, headers=headers, timeout=timeout)
        log.info("[HTTP] %s status=%s", url.split("?")[0], resp.status_code)
        if resp.status_code == 429:
            block_source(source, YAHOO_COOLDOWN_SECONDS if source == "yahoo" else SOURCE_COOLDOWN_SECONDS, "429")
            return None
        if resp.status_code >= 500:
            block_source(source, SOURCE_COOLDOWN_SECONDS, f"{resp.status_code}")
            return None
        if resp.status_code >= 400:
            log.warning("[%s HTTP] status=%s body=%s", source.upper(), resp.status_code, resp.text[:200])
            return None
        return resp
    except Exception as exc:
        log.warning("[%s ERROR] %s", source.upper(), exc)
        return None


def parse_json_response(resp: Optional[requests.Response], source: str) -> Any:
    if resp is None:
        return None
    try:
        return resp.json()
    except Exception as exc:
        log.warning("[%s JSON ERROR] %s", source.upper(), exc)
        return None


# =============================================================================
# MARKET SESSION
# =============================================================================

def market_session(dt: Optional[datetime] = None) -> str:
    dt = dt or now_et()
    if dt.weekday() >= 5:
        return "WEEKEND"
    t = dt.time()
    mins = t.hour * 60 + t.minute
    if mins < 4 * 60:
        return "CLOSED"
    if mins < 9 * 60 + 30:
        return "PREMARKET"
    if mins < 11 * 60:
        return "OPEN"
    if mins < 14 * 60:
        return "MIDDAY"
    if mins < 16 * 60:
        return "POWER_HOUR"
    if mins < 16 * 60 + 10:
        return "CLOSING"
    if mins < 20 * 60:
        return "AFTERHOURS"
    return "CLOSED"


def scanning_enabled() -> bool:
    sess = market_session()
    return sess in {"PREMARKET", "OPEN", "MIDDAY", "POWER_HOUR", "CLOSING", "AFTERHOURS"}


# =============================================================================
# TICKER FILTERS
# =============================================================================

WARRANT_SUFFIXES = (
    "W", "WS", "WT", "WQ", "WSA", "WSC", "IW", "R", "U", "RIGHT", "UNIT"
)

BAD_TICKER_PATTERNS = [
    re.compile(r"^[A-Z]{1,5}W$"),
    re.compile(r"^[A-Z]{1,5}WS$"),
    re.compile(r"^[A-Z]{1,5}WT$"),
    re.compile(r"^[A-Z]{1,5}R$"),
    re.compile(r"^[A-Z]{1,5}U$"),
]


def is_probably_warrant_or_unit(ticker: str) -> bool:
    t = normalize_ticker(ticker)
    if not t:
        return True
    for pat in BAD_TICKER_PATTERNS:
        if pat.match(t):
            return True
    if "-" in t:
        tail = t.split("-")[-1]
        if tail in WARRANT_SUFFIXES:
            return True
    return False


def leader_basic_ok(leader: Leader) -> bool:
    t = normalize_ticker(leader.ticker)
    if not t or is_probably_warrant_or_unit(t):
        return False
    if leader.price and not (MIN_PRICE <= leader.price <= MAX_PRICE):
        return False
    if leader.change_pct and leader.change_pct < MIN_SCAN_GAIN_PCT:
        return False
    if leader.volume and leader.volume < 50_000:
        return False
    return True


# =============================================================================
# LEADER SOURCES
# =============================================================================

def get_nasdaq_gainers() -> List[Leader]:
    """Public Nasdaq screener fallback. Often usable when Yahoo is 429."""
    url = "https://api.nasdaq.com/api/screener/stocks"
    params = {
        "tableonly": "true",
        "limit": "100",
        "offset": "0",
        "download": "true",
    }
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/market-activity/stocks/screener",
    }
    resp = http_get(url, source="nasdaq", params=params, headers=headers)
    data = parse_json_response(resp, "nasdaq")
    rows = (((data or {}).get("data") or {}).get("rows") or []) if isinstance(data, dict) else []
    leaders: List[Leader] = []
    for row in rows:
        t = normalize_ticker(row.get("symbol"))
        price = safe_float(row.get("lastsale"))
        pct = safe_float(row.get("pctchange"))
        vol = safe_int(row.get("volume"))
        name = str(row.get("name") or "")
        if pct >= MIN_SCAN_GAIN_PCT:
            leaders.append(Leader(ticker=t, price=price, change_pct=pct, volume=vol, source="nasdaq", name=name, raw=row))
    leaders.sort(key=lambda x: (x.change_pct, x.volume), reverse=True)
    log.info("[NASDAQ GAINERS] %s names", len(leaders))
    return leaders[:80]


def get_yahoo_gainers() -> List[Leader]:
    """Yahoo is backup only. 429 triggers source cooldown."""
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {
        "scrIds": "day_gainers",
        "count": "100",
        "start": "0",
        "formatted": "false",
        "lang": "en-US",
        "region": "US",
    }
    resp = http_get(url, source="yahoo", params=params)
    data = parse_json_response(resp, "yahoo")
    quotes = []
    try:
        quotes = data["finance"]["result"][0]["quotes"]
    except Exception:
        quotes = []
    leaders: List[Leader] = []
    for q in quotes:
        t = normalize_ticker(q.get("symbol"))
        price = safe_float(q.get("regularMarketPrice"))
        pct = safe_float(q.get("regularMarketChangePercent"))
        vol = safe_int(q.get("regularMarketVolume"))
        name = str(q.get("shortName") or q.get("longName") or "")
        leaders.append(Leader(ticker=t, price=price, change_pct=pct, volume=vol, source="yahoo", name=name, raw=q))
    leaders = [x for x in leaders if x.change_pct >= MIN_SCAN_GAIN_PCT]
    leaders.sort(key=lambda x: (x.change_pct, x.volume), reverse=True)
    log.info("[YAHOO GAINERS] %s names", len(leaders))
    return leaders[:80]


def get_finnhub_movers_seed() -> List[Leader]:
    """Finnhub does not provide a perfect free gainers endpoint, so this is a quote refresh for cached/known leaders."""
    if not FINNHUB_API_KEY:
        return []
    seeds = [x.ticker for x in LAST_GOOD_LEADERS[:40]]
    out: List[Leader] = []
    for t in seeds:
        q = get_finnhub_quote(t)
        if q and q.change_pct >= MIN_SCAN_GAIN_PCT:
            out.append(Leader(ticker=t, price=q.price, change_pct=q.change_pct, volume=q.day_volume, source="finnhub_seed"))
    return out


def get_webull_gainers_placeholder() -> List[Leader]:
    """
    Webull gainers hook.

    Keep this function so you have a clean insert point. Webull endpoints change and often require signed/app headers.
    If you already have working Webull code, paste it here and return List[Leader].
    """
    return []


def dedupe_leaders(leaders: Iterable[Leader]) -> List[Leader]:
    best: Dict[str, Leader] = {}
    for item in leaders:
        item.normalized()
        if not leader_basic_ok(item):
            continue
        old = best.get(item.ticker)
        if old is None:
            best[item.ticker] = item
            continue
        old_score = old.change_pct * 1_000_000 + old.volume
        new_score = item.change_pct * 1_000_000 + item.volume
        if new_score > old_score:
            best[item.ticker] = item
    result = list(best.values())
    result.sort(key=lambda x: (x.change_pct, x.volume), reverse=True)
    return result


def get_leaders() -> List[Leader]:
    """Leader-first source chain. Yahoo is not allowed to blind the scanner anymore."""
    global LAST_GOOD_LEADERS, LAST_GOOD_LEADERS_TS
    all_rows: List[Leader] = []

    # Primary: Nasdaq. Backup: Webull hook. Backup: Yahoo. Last: Finnhub refresh of cached leaders.
    for name, fn in (
        ("nasdaq", get_nasdaq_gainers),
        ("webull", get_webull_gainers_placeholder),
        ("yahoo", get_yahoo_gainers),
        ("finnhub_seed", get_finnhub_movers_seed),
    ):
        try:
            rows = fn()
            if rows:
                all_rows.extend(rows)
        except Exception as exc:
            log.warning("[%s LEADERS ERROR] %s", name.upper(), exc)

    leaders = dedupe_leaders(all_rows)
    if leaders:
        LAST_GOOD_LEADERS = leaders[:80]
        LAST_GOOD_LEADERS_TS = time.time()
        log.info("[LEADERS] %s candidates: %s", len(leaders), ",".join(x.ticker for x in leaders[:20]))
        return leaders[:80]

    age = time.time() - LAST_GOOD_LEADERS_TS if LAST_GOOD_LEADERS_TS else 999999
    if LAST_GOOD_LEADERS and age <= LEADER_CACHE_TTL_SECONDS:
        log.warning("[LEADERS FALLBACK] live feeds failed — using last good leaders age=%ss", int(age))
        return LAST_GOOD_LEADERS[:50]

    log.warning("[LEADERS] 0 candidates — all sources failed")
    return []


# =============================================================================
# QUOTES
# =============================================================================

def get_finnhub_quote(ticker: str) -> Optional[Quote]:
    if not FINNHUB_API_KEY:
        return None
    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": ticker, "token": FINNHUB_API_KEY}
    resp = http_get(url, source="finnhub", params=params)
    data = parse_json_response(resp, "finnhub")
    if not isinstance(data, dict):
        return None
    price = safe_float(data.get("c"))
    prev = safe_float(data.get("pc"))
    high = safe_float(data.get("h"))
    low = safe_float(data.get("l"))
    if price <= 0:
        return None
    return Quote(ticker=ticker, price=price, prev_close=prev, change_pct=pct_change(price, prev), high=high, low=low, source="finnhub")


def get_yahoo_quote(ticker: str) -> Optional[Quote]:
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ticker, "fields": "regularMarketPrice,regularMarketPreviousClose,regularMarketChangePercent,regularMarketVolume,regularMarketDayHigh,regularMarketDayLow"}
    resp = http_get(url, source="yahoo_quote", params=params)
    data = parse_json_response(resp, "yahoo_quote")
    try:
        q = data["quoteResponse"]["result"][0]
    except Exception:
        return None
    price = safe_float(q.get("regularMarketPrice"))
    prev = safe_float(q.get("regularMarketPreviousClose"))
    pct = safe_float(q.get("regularMarketChangePercent"), pct_change(price, prev))
    vol = safe_int(q.get("regularMarketVolume"))
    high = safe_float(q.get("regularMarketDayHigh"))
    low = safe_float(q.get("regularMarketDayLow"))
    if price <= 0:
        return None
    return Quote(ticker=ticker, price=price, prev_close=prev, change_pct=pct, day_volume=vol, high=high, low=low, source="yahoo_quote")


def best_quote(ticker: str, leader: Optional[Leader] = None) -> Optional[Quote]:
    # Prefer Finnhub live quote if key exists, but do not require it.
    for fn in (get_finnhub_quote, get_yahoo_quote):
        q = fn(ticker)
        if q and q.price > 0:
            if leader:
                if not q.change_pct and leader.change_pct:
                    q.change_pct = leader.change_pct
                if not q.day_volume and leader.volume:
                    q.day_volume = leader.volume
            return q
    if leader and leader.price > 0:
        return Quote(ticker=ticker, price=leader.price, change_pct=leader.change_pct, day_volume=leader.volume, source=f"leader:{leader.source}")
    return None


# =============================================================================
# CANDLES
# =============================================================================

def parse_yahoo_chart_timestamps(ts_list: Sequence[int]) -> List[datetime]:
    out = []
    for ts in ts_list:
        try:
            out.append(datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(EASTERN_TZ))
        except Exception:
            out.append(now_et())
    return out


def get_yahoo_candles(ticker: str, interval: str = "1m", range_: str = "1d") -> List[Candle]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": interval, "range": range_, "includePrePost": "true"}
    resp = http_get(url, source="yahoo_candles", params=params)
    data = parse_json_response(resp, "yahoo_candles")
    try:
        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp") or []
        quote = result["indicators"]["quote"][0]
    except Exception:
        return []

    times = parse_yahoo_chart_timestamps(timestamps)
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    vols = quote.get("volume") or []
    candles: List[Candle] = []
    n = min(len(times), len(opens), len(highs), len(lows), len(closes), len(vols))
    for i in range(n):
        o = safe_float(opens[i])
        h = safe_float(highs[i])
        l = safe_float(lows[i])
        c = safe_float(closes[i])
        v = safe_int(vols[i])
        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            continue
        candles.append(Candle(times[i], o, h, l, c, v))
    log.info("[CANDLES] %s: Yahoo %s finalized bars", ticker, len(candles))
    return candles


def get_alpaca_candles(ticker: str) -> List[Candle]:
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        return []
    url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"
    start = (utc_now() - timedelta(hours=12)).isoformat()
    params = {"timeframe": "1Min", "start": start, "limit": "200", "adjustment": "raw", "feed": "iex"}
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY}
    resp = http_get(url, source="alpaca", params=params, headers=headers)
    data = parse_json_response(resp, "alpaca")
    bars = (data or {}).get("bars") or [] if isinstance(data, dict) else []
    candles: List[Candle] = []
    for b in bars:
        try:
            ts = datetime.fromisoformat(str(b.get("t")).replace("Z", "+00:00")).astimezone(EASTERN_TZ)
        except Exception:
            ts = now_et()
        candles.append(Candle(ts, safe_float(b.get("o")), safe_float(b.get("h")), safe_float(b.get("l")), safe_float(b.get("c")), safe_int(b.get("v"))))
    candles = [c for c in candles if c.open > 0 and c.close > 0]
    if candles:
        log.info("[CANDLES] %s: Alpaca %s bars", ticker, len(candles))
    return candles


def best_candles(ticker: str) -> List[Candle]:
    candles = get_alpaca_candles(ticker)
    if len(candles) >= 20:
        return candles
    candles = get_yahoo_candles(ticker)
    return candles


# =============================================================================
# STRUCTURE ENGINE
# =============================================================================

def calc_vwap(candles: Sequence[Candle]) -> Optional[float]:
    pv = 0.0
    vol = 0
    for c in candles:
        typical = (c.high + c.low + c.close) / 3.0
        pv += typical * c.volume
        vol += c.volume
    if vol <= 0:
        return None
    return pv / vol


def detect_higher_lows(candles: Sequence[Candle], lookback: int = 8) -> bool:
    if len(candles) < lookback:
        return False
    lows = [c.low for c in candles[-lookback:]]
    # Not strict every candle. Require recent low stair-step better than first half.
    first = min(lows[: lookback // 2])
    second = min(lows[lookback // 2 :])
    return second >= first * 0.995


def analyze_structure(candles: Sequence[Candle], quote: Quote) -> Structure:
    if len(candles) < 8 or quote.price <= 0:
        return Structure(data_ok=False, reason="not enough candles")
    recent = list(candles[-10:])
    vwap = calc_vwap(candles)
    recent_volume = sum(c.volume for c in candles[-5:])
    last3_change = pct_change(candles[-1].close, candles[-4].close) if len(candles) >= 4 else 0.0
    recent_high = max(c.high for c in candles[-12:]) if len(candles) >= 12 else max(c.high for c in candles)
    hod = max(c.high for c in candles)
    last5_low = min(c.low for c in candles[-5:])
    above_vwap = bool(vwap and quote.price >= vwap)
    near_hod = bool(hod and quote.price >= hod * 0.965)
    new_high_push = bool(recent_high and quote.price >= recent_high * 0.995)
    higher_lows = detect_higher_lows(candles)
    breakout = bool(recent_high and quote.price >= recent_high * 1.005 and recent_volume >= MIN_RECENT_VOLUME)

    reasons = []
    if above_vwap:
        reasons.append("above VWAP")
    if higher_lows:
        reasons.append("higher lows")
    if near_hod:
        reasons.append("near HOD")
    if breakout:
        reasons.append("breakout")
    if last3_change >= 3:
        reasons.append(f"last3 +{last3_change:.1f}%")

    return Structure(
        above_vwap=above_vwap,
        vwap=vwap,
        recent_volume=recent_volume,
        last3_change_pct=last3_change,
        last5_low=last5_low,
        recent_high=recent_high,
        hod=hod,
        near_hod=near_hod,
        new_high_push=new_high_push,
        higher_lows=higher_lows,
        breakout=breakout,
        data_ok=True,
        reason=" + ".join(reasons) if reasons else "structure neutral",
    )


# =============================================================================
# NEWS AND DILUTION AWARENESS
# =============================================================================

STRONG_NEWS_TERMS = [
    "fda", "approval", "clearance", "contract", "purchase order", "partnership", "merger",
    "acquisition", "definitive agreement", "earnings", "guidance", "phase", "clinical",
    "trial", "positive data", "license", "distribution", "nvidia", "ai", "battery", "facility",
    "mou", "memorandum of understanding", "financing agreement", "strategic", "collaboration",
]
WEAK_NEWS_TERMS = [
    "presentation", "conference", "appoints", "launch", "update", "announces", "expands",
]
JUNK_NEWS_PHRASES = [
    "stocks moving", "stock moving", "why shares are trading", "premarket movers", "most active",
    "gap-ups and gap-downs", "top gainers", "market movers", "52-week", "benzinga pro's top",
]
DILUTION_TERMS = [
    "registered direct", "private placement", "atm", "at-the-market", "shelf", "s-3", "s-1",
    "424b5", "warrant", "convertible", "equity line", "resale", "offering", "securities purchase agreement",
]


def classify_headline(headline: str) -> Tuple[str, bool, str]:
    h = headline.lower()
    dilution = any(term in h for term in DILUTION_TERMS)
    dilution_note = "offering/dilution language" if dilution else ""
    if any(p in h for p in JUNK_NEWS_PHRASES):
        return "JUNK", dilution, dilution_note
    if any(term in h for term in STRONG_NEWS_TERMS):
        return "STRONG", dilution, dilution_note
    if any(term in h for term in WEAK_NEWS_TERMS):
        return "WEAK", dilution, dilution_note
    return "NONE", dilution, dilution_note


def get_finnhub_news(ticker: str) -> Optional[NewsResult]:
    if not FINNHUB_API_KEY:
        return None
    end = utc_now().date()
    start = end - timedelta(days=3)
    url = "https://finnhub.io/api/v1/company-news"
    params = {"symbol": ticker, "from": str(start), "to": str(end), "token": FINNHUB_API_KEY}
    resp = http_get(url, source="finnhub_news", params=params)
    rows = parse_json_response(resp, "finnhub_news")
    if not isinstance(rows, list):
        return None
    best: Optional[NewsResult] = None
    rank = {"STRONG": 3, "WEAK": 2, "NONE": 1, "JUNK": 0}
    for item in rows[:20]:
        headline = str(item.get("headline") or "").strip()
        if not headline:
            continue
        grade, dilution, dilution_note = classify_headline(headline)
        nr = NewsResult(
            grade=grade,
            headline=headline,
            source=str(item.get("source") or "Finnhub"),
            url=str(item.get("url") or ""),
            published_at=str(item.get("datetime") or ""),
            dilution_flag=dilution,
            dilution_note=dilution_note,
        )
        if best is None or rank.get(nr.grade, 0) > rank.get(best.grade, 0):
            best = nr
        if nr.grade == "STRONG":
            break
    return best


def get_yahoo_news(ticker: str) -> Optional[NewsResult]:
    url = "https://query1.finance.yahoo.com/v1/finance/search"
    params = {"q": ticker, "quotesCount": "1", "newsCount": "8"}
    resp = http_get(url, source="yahoo_news", params=params)
    data = parse_json_response(resp, "yahoo_news")
    news = (data or {}).get("news") or [] if isinstance(data, dict) else []
    best: Optional[NewsResult] = None
    rank = {"STRONG": 3, "WEAK": 2, "NONE": 1, "JUNK": 0}
    ticker_word = re.compile(rf"\b{re.escape(ticker)}\b", re.I)
    for item in news:
        headline = str(item.get("title") or "").strip()
        if not headline:
            continue
        # Strict ticker matching when symbol is in title; if no ticker shown, still allow because Yahoo search is ticker-scoped.
        if ticker.upper() in headline.upper() and not ticker_word.search(headline):
            continue
        grade, dilution, dilution_note = classify_headline(headline)
        nr = NewsResult(
            grade=grade,
            headline=headline,
            source=str(item.get("publisher") or "Yahoo"),
            url=str(item.get("link") or ""),
            published_at=str(item.get("providerPublishTime") or ""),
            dilution_flag=dilution,
            dilution_note=dilution_note,
        )
        if best is None or rank.get(nr.grade, 0) > rank.get(best.grade, 0):
            best = nr
    return best


def best_news(ticker: str) -> NewsResult:
    for fn in (get_finnhub_news, get_yahoo_news):
        try:
            nr = fn(ticker)
            if nr and nr.headline:
                return nr
        except Exception as exc:
            log.warning("[NEWS ERROR] %s %s", ticker, exc)
    return NewsResult(grade="NONE", headline="UNKNOWN CATALYST — INVESTIGATE", source="none")


# =============================================================================
# DECISION ENGINE
# =============================================================================

def quality_score(leader: Leader, quote: Quote, structure: Structure, news: NewsResult) -> int:
    score = 0
    gain = max(quote.change_pct, leader.change_pct)
    vol = max(quote.day_volume, leader.volume)

    if gain >= 25: score += 2
    if gain >= 35: score += 1
    if gain >= 50: score += 1
    if vol >= 1_000_000: score += 2
    elif vol >= 300_000: score += 1
    if structure.above_vwap: score += 1
    if structure.higher_lows: score += 1
    if structure.near_hod: score += 1
    if structure.breakout or structure.last3_change_pct >= 3: score += 1
    if news.grade == "STRONG": score += 1
    if news.grade == "JUNK": score -= 1
    if quote.price < MIN_PRICE or quote.price > MAX_PRICE: score -= 3
    if structure.vwap and quote.price < structure.vwap * 0.96: score -= 2
    return int(clamp(score, 0, 10))


def update_baseline(ticker: str, price: float) -> AlertState:
    st = ALERT_STATES.setdefault(ticker, AlertState())
    now = time.time()
    if st.baseline_price <= 0 or now - st.baseline_ts > FAST_SPIKE_WINDOW_MIN * 60:
        st.baseline_price = price
        st.baseline_ts = now
    # Let baseline ratchet down during pullbacks so fast re-pushes are caught.
    if price < st.baseline_price:
        st.baseline_price = price
        st.baseline_ts = now
    return st


def is_meaningful_realert(ticker: str, alert_type: str, quote: Quote, structure: Structure, quality: int) -> bool:
    st = ALERT_STATES.setdefault(ticker, AlertState())
    now = time.time()
    if st.last_alert_ts <= 0:
        return True
    cooldown_done = (now - st.last_alert_ts) >= ALERT_COOLDOWN_SECONDS
    price_push = st.last_alert_price > 0 and quote.price >= st.last_alert_price * (1 + MEANINGFUL_NEW_HIGH_PCT / 100.0)
    high_push = st.last_alert_high > 0 and structure.hod and structure.hod >= st.last_alert_high * (1 + MEANINGFUL_NEW_HIGH_PCT / 100.0)
    quality_upgrade = quality >= st.last_quality + 2
    type_upgrade = alert_type != st.last_alert_type and alert_type in {"FAST SPIKE", "BREAKOUT OVER HOD"}
    return (cooldown_done and (price_push or high_push or quality_upgrade or type_upgrade)) or type_upgrade


def mark_alerted(ticker: str, alert_type: str, quote: Quote, structure: Structure, quality: int) -> None:
    st = ALERT_STATES.setdefault(ticker, AlertState())
    st.last_alert_ts = time.time()
    st.last_alert_price = quote.price
    st.last_alert_high = structure.hod or quote.price
    st.last_alert_type = alert_type
    st.last_quality = quality


def decide_candidate(leader: Leader) -> CandidateDecision:
    ticker = leader.ticker
    reasons: List[str] = []
    risks: List[str] = []

    if is_probably_warrant_or_unit(ticker):
        return CandidateDecision(ticker=ticker, should_alert=False, reasons=["warrant/unit filtered"], leader=leader)

    quote = best_quote(ticker, leader)
    if not quote:
        return CandidateDecision(ticker=ticker, should_alert=False, reasons=["no valid quote"], leader=leader)

    gain = max(quote.change_pct, leader.change_pct)
    if gain < MIN_ALERT_GAIN_PCT:
        return CandidateDecision(ticker=ticker, should_alert=False, reasons=[f"gain {gain:.1f}% under {MIN_ALERT_GAIN_PCT:.0f}% floor"], quote=quote, leader=leader)
    if not (MIN_PRICE <= quote.price <= MAX_PRICE):
        return CandidateDecision(ticker=ticker, should_alert=False, reasons=["price outside range"], quote=quote, leader=leader)

    candles = best_candles(ticker)
    structure = analyze_structure(candles, quote) if candles else Structure(data_ok=False, reason="no candles")
    news = best_news(ticker)
    quality = quality_score(leader, quote, structure, news)

    st = update_baseline(ticker, quote.price)
    spike_from_base = pct_change(quote.price, st.baseline_price) if st.baseline_price > 0 else 0.0

    day_vol = max(quote.day_volume, leader.volume)
    if day_vol < MIN_DAY_VOLUME and gain < MAJOR_LEADER_GAIN_PCT:
        risks.append("light total volume")
    if news.grade in {"NONE", "JUNK"} and gain >= 35:
        risks.append("UNKNOWN CATALYST — INVESTIGATE")
    if news.dilution_flag:
        risks.append(news.dilution_note or "dilution language")
    if not structure.above_vwap and structure.data_ok:
        risks.append("not above VWAP")

    alert_type = ""
    if spike_from_base >= FAST_SPIKE_PCT and structure.recent_volume >= MIN_RECENT_VOLUME:
        alert_type = "FAST SPIKE"
        reasons.append(f"fast +{spike_from_base:.1f}% push")
    elif gain >= MAJOR_LEADER_GAIN_PCT and day_vol >= MIN_DAY_VOLUME:
        alert_type = "MARKET LEADER"
        reasons.append(f"major leader +{gain:.1f}%")
    elif structure.breakout and structure.above_vwap:
        alert_type = "BREAKOUT OVER HOD"
        reasons.append("breakout + above VWAP")
    elif structure.above_vwap and structure.higher_lows and structure.near_hod and structure.recent_volume >= MIN_RECENT_VOLUME:
        alert_type = "VWAP HOLD CONTINUATION"
        reasons.append("VWAP hold continuation")
    elif gain >= 35 and structure.last3_change_pct >= 3:
        alert_type = "LIVE PUSH"
        reasons.append(f"last3 +{structure.last3_change_pct:.1f}%")

    if structure.reason:
        reasons.append(structure.reason)
    if news.grade == "STRONG":
        reasons.append("strong catalyst")
    elif news.grade == "WEAK":
        reasons.append("weak catalyst")

    should = bool(alert_type) and quality >= 5 and is_meaningful_realert(ticker, alert_type, quote, structure, quality)
    return CandidateDecision(
        ticker=ticker,
        should_alert=should,
        alert_type=alert_type,
        quality=quality,
        reasons=dedupe_text(reasons)[:5],
        risks=dedupe_text(risks)[:4],
        quote=quote,
        structure=structure,
        news=news,
        leader=leader,
    )


def dedupe_text(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        clean = str(item).strip()
        key = clean.lower()
        if clean and key not in seen:
            out.append(clean)
            seen.add(key)
    return out


# =============================================================================
# ALERTING
# =============================================================================

def format_volume(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(n)


def format_alert(d: CandidateDecision) -> str:
    q = d.quote or Quote(d.ticker)
    s = d.structure or Structure()
    n = d.news or NewsResult()
    vol = max(q.day_volume, d.leader.volume if d.leader else 0)
    gain = max(q.change_pct, d.leader.change_pct if d.leader else 0)
    title = f"🚀 {d.alert_type} — {d.ticker}"
    lines = [
        title,
        "",
        f"${q.price:.4g} | +{gain:.1f}%",
        f"Vol: {format_volume(vol)} | Quality: {d.quality}/10",
    ]
    if s.vwap:
        lines.append(f"VWAP: ${s.vwap:.4g} | RecentVol: {format_volume(s.recent_volume)}")
    if d.reasons:
        lines.append("Why: " + " + ".join(d.reasons))
    if n.headline:
        prefix = n.grade if n.grade != "NONE" else "D"
        lines.append(f"NEWS: {prefix} — {n.headline[:180]}")
    if d.risks:
        lines.append("Risk: " + " | ".join(d.risks))
    return "\n".join(lines)


def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.info("[ALERT DRY RUN]\n%s", text)
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        resp = SESSION.post(url, json=payload, timeout=HTTP_TIMEOUT)
        if resp.status_code >= 300:
            log.warning("[TELEGRAM ERROR] status=%s body=%s", resp.status_code, resp.text[:200])
            return False
        return True
    except Exception as exc:
        log.warning("[TELEGRAM ERROR] %s", exc)
        return False


# =============================================================================
# SCAN LOOP
# =============================================================================

def run_scan_cycle() -> Dict[str, Any]:
    started = time.time()
    session_name = market_session()
    sent = 0
    checked = 0
    alerts: List[str] = []

    if not scanning_enabled():
        summary = {"version": VERSION, "session": session_name, "checked": 0, "sent": 0, "message": "scanning disabled"}
        log.info("[CYCLE SKIP] %s", summary)
        return summary

    leaders = get_leaders()
    decisions: List[CandidateDecision] = []
    for leader in leaders[:50]:
        checked += 1
        try:
            d = decide_candidate(leader)
            decisions.append(d)
        except Exception as exc:
            log.warning("[DECISION ERROR] %s %s", leader.ticker, exc)

    # Sort alertable by quality, gain, then volume.
    alertable = [d for d in decisions if d.should_alert]
    alertable.sort(key=lambda d: (d.quality, max(d.quote.change_pct if d.quote else 0, d.leader.change_pct if d.leader else 0), d.quote.day_volume if d.quote else 0), reverse=True)

    for d in alertable[:MAX_ALERTS_PER_CYCLE]:
        text = format_alert(d)
        send_telegram(text)
        mark_alerted(d.ticker, d.alert_type, d.quote or Quote(d.ticker), d.structure or Structure(), d.quality)
        alerts.append(d.ticker)
        sent += 1

    elapsed = round(time.time() - started, 2)
    summary = {
        "version": VERSION,
        "session": session_name,
        "leaders": len(leaders),
        "checked": checked,
        "alertable": len(alertable),
        "sent": sent,
        "alerts": alerts,
        "elapsed": elapsed,
        "blocked_sources": {k: max(0, int(v - time.time())) for k, v in SOURCE_BLOCKED_UNTIL.items() if v > time.time()},
    }
    log.info("[CYCLE DONE] %s", json.dumps(summary, default=str))
    return summary


def scanner_loop() -> None:
    global LAST_CYCLE_SUMMARY
    log.info("[START] %s interval=%ss min_alert_gain=%s", VERSION, SCAN_INTERVAL_SECONDS, MIN_ALERT_GAIN_PCT)
    while RUNNING:
        try:
            LAST_CYCLE_SUMMARY = run_scan_cycle()
        except Exception as exc:
            log.exception("[CYCLE FATAL] %s", exc)
            LAST_CYCLE_SUMMARY = {"error": str(exc), "version": VERSION}
        time.sleep(SCAN_INTERVAL_SECONDS)


# =============================================================================
# FLASK HEALTH
# =============================================================================

def build_app():
    if Flask is None:
        return None
    app = Flask(__name__)

    @app.get("/")
    def health():
        return {
            "ok": True,
            "version": VERSION,
            "time_et": now_et().isoformat(),
            "session": market_session(),
            "last_cycle": LAST_CYCLE_SUMMARY,
            "last_good_leaders": [x.ticker for x in LAST_GOOD_LEADERS[:20]],
            "blocked_sources": {k: max(0, int(v - time.time())) for k, v in SOURCE_BLOCKED_UNTIL.items() if v > time.time()},
        }

    @app.get("/scan")
    def manual_scan():
        return run_scan_cycle()

    return app


# =============================================================================
# ENTRYPOINT
# =============================================================================

def main() -> None:
    app = build_app()
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()
    if app is not None:
        app.run(host="0.0.0.0", port=PORT)
    else:
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    main()
