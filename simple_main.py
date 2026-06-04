#!/usr/bin/env python3
"""
simple_main.py

v60-elite-dynamic-leader

Clean leader-first momentum scanner for Render.

Core rules:
- No alerts before 9:30 AM ET.
- No alerts after 4:10 PM ET.
- Dynamic gain floor: hot days ignore weak +27% names.
- Focus top live leaders only.
- Fast +10% spike trigger kept.
- Short Telegram alerts only.
- Float shown in alert.
- Quality/why/details kept internal, not displayed.
- News/dilution are awareness only, not automatic rejection.
"""

from __future__ import annotations

import html
import json
import logging
import math
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, time as dtime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import requests
except Exception as exc:
    raise RuntimeError("requests is required") from exc

try:
    from flask import Flask
except Exception:
    Flask = None

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


# =============================================================================
# CONFIG
# =============================================================================

VERSION = "v61.2-elite-alert-cleanup"
EASTERN_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone(timedelta(hours=-5))

PORT = int(os.getenv("PORT", "10000"))
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "45"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "4.0"))

MAX_ALERTS_PER_CYCLE = int(os.getenv("MAX_ALERTS_PER_CYCLE", "2"))
MAX_LEADERS_PER_CYCLE = int(os.getenv("MAX_LEADERS_PER_CYCLE", "12"))
MAX_LEADERS_FETCH = int(os.getenv("MAX_LEADERS_FETCH", "250"))
VERIFY_TOP_N_LEADERS = int(os.getenv("VERIFY_TOP_N_LEADERS", "120"))
ALLOW_SEED_ONLY_LEADERS = False  # scan-only: never trust unconfirmed seed/watchlist data
ALLOW_FINNHUB_LEADER_CONFIRM = False  # live-only: do not confirm leaders with quote-only data
REQUIRE_CONFIRMED_LEADER_DATA = os.getenv("REQUIRE_CONFIRMED_LEADER_DATA", "true").lower() in {"1", "true", "yes", "y"}
NEWS_TOP_N = int(os.getenv("NEWS_TOP_N", "12"))
MIN_ALERT_QUALITY = int(os.getenv("MIN_ALERT_QUALITY", "8"))
MIN_FAST_SPIKE_QUALITY = int(os.getenv("MIN_FAST_SPIKE_QUALITY", "7"))
MARKET_LEADER_OVERRIDE_QUALITY = int(os.getenv("MARKET_LEADER_OVERRIDE_QUALITY", "7"))

MIN_ALERT_GAIN_PCT = float(os.getenv("MIN_ALERT_GAIN_PCT", "25"))
MIN_SCAN_GAIN_PCT = float(os.getenv("MIN_SCAN_GAIN_PCT", "20"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.30"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "80"))
MIN_DAY_VOLUME = int(os.getenv("MIN_DAY_VOLUME", "1000000"))
MIN_RECENT_VOLUME = int(os.getenv("MIN_RECENT_VOLUME", "150000"))

MAJOR_LEADER_GAIN_PCT = float(os.getenv("MAJOR_LEADER_GAIN_PCT", "50"))
FAST_SPIKE_PCT = float(os.getenv("FAST_SPIKE_PCT", "10"))
FAST_SPIKE_WINDOW_MIN = int(os.getenv("FAST_SPIKE_WINDOW_MIN", "5"))
FAST_SPIKE_REALERT_SECONDS = int(os.getenv("FAST_SPIKE_REALERT_SECONDS", "300"))
FAST_SPIKE_FROM_LAST_ALERT_PCT = float(os.getenv("FAST_SPIKE_FROM_LAST_ALERT_PCT", "10"))
FAST_SPIKE_QUALITY_BONUS = int(os.getenv("FAST_SPIKE_QUALITY_BONUS", "2"))
MIN_FAST_SPIKE_DAY_VOLUME = int(os.getenv("MIN_FAST_SPIKE_DAY_VOLUME", "1000000"))
MIN_MARKET_LEADER_DAY_VOLUME = int(os.getenv("MIN_MARKET_LEADER_DAY_VOLUME", "5000000"))
LOW_FLOAT_BONUS_MAX_M = float(os.getenv("LOW_FLOAT_BONUS_MAX_M", "20"))
MAX_EXTENDED_FROM_VWAP_WARN_PCT = float(os.getenv("MAX_EXTENDED_FROM_VWAP_WARN_PCT", "18"))

# Elite v60 filters: cut alert spam on hot days.
ELITE_ONLY_MODE = os.getenv("ELITE_ONLY_MODE", "true").lower() in {"1", "true", "yes", "y"}
TOP_ELITE_LEADERS_ONLY = int(os.getenv("TOP_ELITE_LEADERS_ONLY", "10"))
MIN_ELITE_DAY_VOLUME = int(os.getenv("MIN_ELITE_DAY_VOLUME", "5_000_000".replace("_", "")))
MIN_ELITE_RVOL_RATIO = float(os.getenv("MIN_ELITE_RVOL_RATIO", "3.0"))
MIN_ELITE_POTENTIAL_SCORE = int(os.getenv("MIN_ELITE_POTENTIAL_SCORE", "6"))
DAILY_CACHE_TTL_SECONDS = int(os.getenv("DAILY_CACHE_TTL_SECONDS", "900"))
DAILY_BREAKOUT_LOOKBACK_DAYS = int(os.getenv("DAILY_BREAKOUT_LOOKBACK_DAYS", "60"))
HOT_DAY_NORMAL_TOP_GAIN = float(os.getenv("HOT_DAY_NORMAL_TOP_GAIN", "50"))
HOT_DAY_HOT_TOP_GAIN = float(os.getenv("HOT_DAY_HOT_TOP_GAIN", "100"))
HOT_DAY_INSANE_TOP_GAIN = float(os.getenv("HOT_DAY_INSANE_TOP_GAIN", "200"))
HOT_DAY_NORMAL_MIN_GAIN = float(os.getenv("HOT_DAY_NORMAL_MIN_GAIN", "30"))
HOT_DAY_HOT_MIN_GAIN = float(os.getenv("HOT_DAY_HOT_MIN_GAIN", "40"))
HOT_DAY_INSANE_MIN_GAIN = float(os.getenv("HOT_DAY_INSANE_MIN_GAIN", "50"))
FAST_SPIKE_ALLOWED_UNDER_DYNAMIC_FLOOR = os.getenv("FAST_SPIKE_ALLOWED_UNDER_DYNAMIC_FLOOR", "true").lower() in {"1", "true", "yes", "y"}
TOP3_LEADER_OVERRIDE = os.getenv("TOP3_LEADER_OVERRIDE", "true").lower() in {"1", "true", "yes", "y"}
TOP3_OVERRIDE_MIN_GAIN = float(os.getenv("TOP3_OVERRIDE_MIN_GAIN", "40"))
TOP3_OVERRIDE_MIN_VOLUME = int(os.getenv("TOP3_OVERRIDE_MIN_VOLUME", "10000000"))
HOD_BREAK_MIN_GAIN = float(os.getenv("HOD_BREAK_MIN_GAIN", "40"))
HOD_BREAK_MIN_VOLUME = int(os.getenv("HOD_BREAK_MIN_VOLUME", "5000000"))

HOT_SECTOR_TERMS = [
    "ai", "artificial intelligence", "quantum", "crypto", "bitcoin", "blockchain",
    "nuclear", "uranium", "defense", "drone", "robotics", "biotech", "fda",
    "clinical", "china", "ev", "battery", "semiconductor", "data center",
]

MIN_PRICE_MOVE_REPOST_PCT = float(os.getenv("MIN_PRICE_MOVE_REPOST_PCT", "7"))
MEANINGFUL_NEW_HIGH_PCT = float(os.getenv("MEANINGFUL_NEW_HIGH_PCT", "5"))
FRESH_LEADER_REPUSH_PCT = float(os.getenv("FRESH_LEADER_REPUSH_PCT", "5"))
FRESH_LEADER_REALERT_SECONDS = int(os.getenv("FRESH_LEADER_REALERT_SECONDS", "300"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "1200"))

YAHOO_COOLDOWN_SECONDS = int(os.getenv("YAHOO_COOLDOWN_SECONDS", "300"))
SOURCE_COOLDOWN_SECONDS = int(os.getenv("SOURCE_COOLDOWN_SECONDS", "120"))
LEADER_CACHE_TTL_SECONDS = int(os.getenv("LEADER_CACHE_TTL_SECONDS", "900"))

QUOTE_CACHE_TTL_SECONDS = int(os.getenv("QUOTE_CACHE_TTL_SECONDS", "20"))
NEWS_CACHE_TTL_SECONDS = int(os.getenv("NEWS_CACHE_TTL_SECONDS", "300"))
CANDLE_CACHE_TTL_SECONDS = int(os.getenv("CANDLE_CACHE_TTL_SECONDS", "5"))
CANDLE_MAX_AGE_SECONDS = int(os.getenv("CANDLE_MAX_AGE_SECONDS", "180"))
IDEAL_CANDLE_AGE_SECONDS = int(os.getenv("IDEAL_CANDLE_AGE_SECONDS", "20"))
WARN_CANDLE_AGE_SECONDS = int(os.getenv("WARN_CANDLE_AGE_SECONDS", "30"))
MIN_GOOD_CANDLES = int(os.getenv("MIN_GOOD_CANDLES", "20"))
PROFILE_CACHE_TTL_SECONDS = int(os.getenv("PROFILE_CACHE_TTL_SECONDS", "1800"))

ALERT_START_TIME = dtime(9, 30)
ALERT_END_TIME = dtime(16, 10)


def env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "")
        if value is None:
            continue
        value = str(value).strip().strip('"').strip("'")
        if value:
            return value
    return ""


TELEGRAM_BOT_TOKEN = env_first("TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "BOT_TOKEN")
TELEGRAM_CHAT_ID_RAW = env_first("TELEGRAM_CHAT_ID", "TELEGRAM_CHANNEL_ID", "CHAT_ID")
TELEGRAM_CHAT_IDS = [x.strip() for x in TELEGRAM_CHAT_ID_RAW.split(",") if x.strip()]

FINNHUB_API_KEY = env_first("FINNHUB_API_KEY", "FINNHUB_TOKEN")
ALPACA_API_KEY = env_first("ALPACA_API_KEY", "APCA_API_KEY_ID", "APCA_API_KEY")
ALPACA_SECRET_KEY = env_first("ALPACA_SECRET_KEY", "APCA_API_SECRET_KEY", "APCA_SECRET_KEY")

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
# MODELS
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
    higher_lows: bool = False
    breakout: bool = False
    extended_from_vwap_pct: float = 0.0
    holding_recent_low: bool = False
    setup_label: str = "CHECK CHART"
    below_vwap_reclaim_watch: bool = False
    data_ok: bool = False
    reason: str = ""


@dataclass
class NewsResult:
    grade: str = "NONE"
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
    leader_rank: int = 999
    rvol: float = 0.0
    reasons: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    quote: Optional[Quote] = None
    structure: Optional[Structure] = None
    news: Optional[NewsResult] = None
    leader: Optional[Leader] = None
    spike_pct: float = 0.0


# =============================================================================
# STATE
# =============================================================================

SOURCE_BLOCKED_UNTIL: Dict[str, float] = {}
LAST_GOOD_LEADERS: List[Leader] = []
LAST_GOOD_LEADERS_TS: float = 0.0
ALERT_STATES: Dict[str, AlertState] = {}
FIRST_SEEN: Dict[str, Dict[str, float]] = {}
FIRST_SEEN_INITIALIZED = False
LAST_CYCLE_SUMMARY: Dict[str, Any] = {}

QUOTE_CACHE: Dict[str, Tuple[float, Quote]] = {}
NEWS_CACHE: Dict[str, Tuple[float, NewsResult]] = {}
CANDLE_CACHE: Dict[str, Tuple[float, List[Candle]]] = {}
PROFILE_CACHE: Dict[str, Tuple[float, Optional[float]]] = {}
DAILY_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}

RUNNING = True
TRADE_DATE = ""
LAST_GOOD_LEADERS_TRADE_DATE = ""


# =============================================================================
# DAILY STATE / TODAY-ONLY GUARD
# =============================================================================

def current_trade_date() -> str:
    return now_et().date().isoformat()


def reset_daily_state_if_needed(force: bool = False) -> None:
    """Hard reset all intraday memory at the first scan of a new ET date.

    This prevents yesterday's LAST_GOOD_LEADERS, FIRST_SEEN rows, candle/news/quote
    caches, and alert cooldown state from recycling into today's live scanner.
    """
    global TRADE_DATE, FIRST_SEEN_INITIALIZED, LAST_GOOD_LEADERS, LAST_GOOD_LEADERS_TS
    global LAST_GOOD_LEADERS_TRADE_DATE

    today = current_trade_date()
    if not force and TRADE_DATE == today:
        return

    old_date = TRADE_DATE or "none"
    TRADE_DATE = today
    LAST_GOOD_LEADERS_TRADE_DATE = today

    FIRST_SEEN.clear()
    FIRST_SEEN_INITIALIZED = False
    ALERT_STATES.clear()
    LAST_GOOD_LEADERS = []
    LAST_GOOD_LEADERS_TS = 0.0

    QUOTE_CACHE.clear()
    NEWS_CACHE.clear()
    CANDLE_CACHE.clear()
    PROFILE_CACHE.clear()
    DAILY_CACHE.clear()

    log.info("[DAILY RESET] old_date=%s new_date=%s cleared intraday leader/alert/cache state", old_date, today)


def prune_not_today_state() -> None:
    """Remove any legacy rows that do not explicitly belong to today's ET date."""
    today = current_trade_date()

    stale_first_seen = [
        t for t, row in FIRST_SEEN.items()
        if not isinstance(row, dict) or row.get("date") != today
    ]
    for t in stale_first_seen:
        FIRST_SEEN.pop(t, None)

    if stale_first_seen:
        log.info("[TODAY GUARD] removed %s stale FIRST_SEEN rows", len(stale_first_seen))


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
            value = value.replace(",", "").replace("%", "").replace("$", "").strip()
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
            elif value[-1:].upper() == "B":
                mult = 1_000_000_000
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
    log.warning("[%s BLOCKED] %ss%s", name.upper(), seconds, extra)


def http_get(
    url: str,
    *,
    source: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = HTTP_TIMEOUT,
) -> Optional[requests.Response]:
    if not source_allowed(source):
        log.info("[%s SKIP] source cooling down", source.upper())
        return None
    try:
        resp = SESSION.get(url, params=params, headers=headers, timeout=timeout)
        log.info("[HTTP] %s status=%s", url.split("?")[0], resp.status_code)
        if resp.status_code == 429:
            cool = YAHOO_COOLDOWN_SECONDS if source.startswith("yahoo") else SOURCE_COOLDOWN_SECONDS
            block_source(source, cool, "429")
            return None
        if resp.status_code in {401, 403}:
            cool = YAHOO_COOLDOWN_SECONDS if source.startswith("yahoo") else SOURCE_COOLDOWN_SECONDS
            block_source(source, cool, str(resp.status_code))
            log.warning("[%s HTTP] body=%s", source.upper(), resp.text[:200])
            return None
        if resp.status_code >= 500:
            block_source(source, SOURCE_COOLDOWN_SECONDS, str(resp.status_code))
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


def format_volume(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(n)


def format_float_shares(shares: Optional[float]) -> str:
    if shares is None or shares <= 0:
        return "Unknown"
    if shares >= 1_000_000:
        return f"{shares / 1_000_000:.1f}M"
    return f"{shares:.1f}M"


def float_flag(shares: Optional[float]) -> str:
    """Finnhub profile2 shareOutstanding is usually in millions in this file."""
    if shares is None or shares <= 0:
        return ""
    if shares <= 10:
        return " ⭐ LOW FLOAT"
    if shares <= 30:
        return " 🟢 TRADABLE FLOAT"
    if shares <= 50:
        return " ⚠️ MID FLOAT"
    return " ⚠️ LARGE FLOAT"


def format_vol_rvol(volume: int, rvol: float) -> str:
    if volume <= 0 and rvol <= 0:
        return "Vol: Unknown"
    if rvol > 0:
        return f"Vol: {format_volume(volume)} ({rvol:.1f}x RVOL)"
    return f"Vol: {format_volume(volume)}"


def candle_age_seconds(candles: Sequence[Candle]) -> Optional[int]:
    if not candles:
        return None
    try:
        return int((now_et() - candles[-1].ts).total_seconds())
    except Exception:
        return None


def candles_fresh_enough(candles: Sequence[Candle]) -> bool:
    if len(candles) < MIN_GOOD_CANDLES:
        return False
    age = candle_age_seconds(candles)
    if age is None:
        return False
    return age <= CANDLE_MAX_AGE_SECONDS


def candle_source_rank(candles: Sequence[Candle]) -> Tuple[int, int]:
    """Rank candle set by freshness first, then length."""
    if not candles:
        return (-999999, 0)
    age = candle_age_seconds(candles)
    freshness = -999999 if age is None else -age
    return (freshness, len(candles))


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
# MARKET HOURS
# =============================================================================

def market_session(dt: Optional[datetime] = None) -> str:
    dt = dt or now_et()
    if dt.weekday() >= 5:
        return "WEEKEND"
    mins = dt.hour * 60 + dt.minute
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


def alerts_enabled_now(dt: Optional[datetime] = None) -> bool:
    dt = dt or now_et()
    if dt.weekday() >= 5:
        return False
    t = dt.time()
    return ALERT_START_TIME <= t < ALERT_END_TIME


def scanning_enabled() -> bool:
    return alerts_enabled_now()


# =============================================================================
# TICKER FILTERS
# =============================================================================

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
        if tail in {"W", "WS", "WT", "R", "U", "RIGHT", "UNIT"}:
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
        market_cap = safe_float(row.get("marketCap"), 0.0) or None
        if pct >= MIN_SCAN_GAIN_PCT:
            leaders.append(
                Leader(
                    ticker=t,
                    price=price,
                    change_pct=pct,
                    volume=vol,
                    source="nasdaq",
                    name=name,
                    market_cap=market_cap,
                    raw=row,
                )
            )

    leaders.sort(key=lambda x: (x.change_pct, x.volume), reverse=True)
    log.info("[NASDAQ GAINERS] %s names", len(leaders))
    return leaders[:80]


def _parse_yahoo_screener_quotes(data: Any, source_name: str) -> List[Leader]:
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
        market_cap = safe_float(q.get("marketCap"), 0.0) or None
        leaders.append(
            Leader(
                ticker=t,
                price=price,
                change_pct=pct,
                volume=vol,
                source=source_name,
                name=name,
                market_cap=market_cap,
                raw=q,
            )
        )
    return leaders


def get_yahoo_gainers() -> List[Leader]:
    """Fetch more than Yahoo's first page. Hot names can fall off page 1 while feeds reshuffle."""
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    leaders: List[Leader] = []
    page_size = 100
    max_fetch = max(100, MAX_LEADERS_FETCH)

    for start in range(0, max_fetch, page_size):
        params = {
            "scrIds": "day_gainers",
            "count": str(page_size),
            "start": str(start),
            "formatted": "false",
            "lang": "en-US",
            "region": "US",
        }
        resp = http_get(url, source="yahoo", params=params)
        data = parse_json_response(resp, "yahoo")
        rows = _parse_yahoo_screener_quotes(data, "yahoo")
        if not rows:
            break
        leaders.extend(rows)
        if len(rows) < page_size:
            break

    leaders = [x for x in leaders if x.change_pct >= MIN_SCAN_GAIN_PCT]
    leaders.sort(key=lambda x: (x.change_pct, x.volume), reverse=True)
    log.info("[YAHOO GAINERS] %s names", len(leaders))
    return leaders[:MAX_LEADERS_FETCH]


def get_yahoo_most_active() -> List[Leader]:
    """Secondary source. Some monster runners appear in most-active before day-gainers updates."""
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    leaders: List[Leader] = []
    page_size = 100
    max_fetch = min(MAX_LEADERS_FETCH, 200)

    for start in range(0, max_fetch, page_size):
        params = {
            "scrIds": "most_actives",
            "count": str(page_size),
            "start": str(start),
            "formatted": "false",
            "lang": "en-US",
            "region": "US",
        }
        resp = http_get(url, source="yahoo_active", params=params)
        data = parse_json_response(resp, "yahoo_active")
        rows = _parse_yahoo_screener_quotes(data, "yahoo_active")
        if not rows:
            break
        leaders.extend(rows)
        if len(rows) < page_size:
            break

    leaders = [x for x in leaders if x.change_pct >= MIN_SCAN_GAIN_PCT]
    leaders.sort(key=lambda x: (x.change_pct, x.volume), reverse=True)
    log.info("[YAHOO MOST ACTIVE HOT] %s names", len(leaders))
    return leaders[:MAX_LEADERS_FETCH]



def get_stockanalysis_gainers() -> List[Leader]:
    """HTML fallback source for current top gainers.

    This catches symbols that Yahoo/Nasdaq/Finnhub miss or stale-rank.
    It is only used as a seed list; every symbol still goes through live quote/chart validation.
    """
    url = "https://stockanalysis.com/markets/gainers/"
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://stockanalysis.com/",
    }
    resp = http_get(url, source="stockanalysis", headers=headers, timeout=min(HTTP_TIMEOUT, 4.0))
    if resp is None or not resp.text:
        return []

    text = resp.text
    leaders: List[Leader] = []

    # Table rows usually contain /stocks/SYMBOL/ plus price, change %, volume.
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", text, flags=re.I | re.S)
    for row in rows:
        m = re.search(r'/stocks/([a-z0-9\-]+)/', row, flags=re.I)
        if not m:
            continue
        ticker = normalize_ticker(m.group(1))
        clean = html.unescape(re.sub(r"<[^>]+>", " ", row))
        clean = re.sub(r"\s+", " ", clean).strip()

        pct_matches = re.findall(r'([+-]?\d+(?:\.\d+)?)\s*%', clean)
        if not pct_matches:
            continue
        pct = max(safe_float(x) for x in pct_matches)
        if pct < MIN_SCAN_GAIN_PCT:
            continue

        money = re.findall(r'\$\s*([0-9]+(?:\.[0-9]+)?)', clean)
        price = safe_float(money[0]) if money else 0.0

        vol = 0
        # Last compact K/M/B number is often volume. It is only a seed; live quote/candles validate later.
        compact_nums = re.findall(r'\b([0-9]+(?:\.[0-9]+)?[KMB])\b', clean, flags=re.I)
        if compact_nums:
            vol = safe_int(compact_nums[-1])

        leaders.append(Leader(ticker=ticker, price=price, change_pct=pct, volume=vol, source="stockanalysis", raw={"row": clean[:300]}))

    leaders = dedupe_leaders(leaders)
    leaders.sort(key=lambda x: (x.change_pct, x.volume), reverse=True)
    log.info("[STOCKANALYSIS GAINERS] %s names", len(leaders))
    return leaders[:MAX_LEADERS_FETCH]


def get_webull_gainers_placeholder() -> List[Leader]:
    """
    Insert a working Webull gainers endpoint here if you find a stable one.
    Kept blank on purpose so scanner does not slow down or break.
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


def get_finnhub_movers_seed() -> List[Leader]:
    if not FINNHUB_API_KEY:
        return []
    seeds = [x.ticker for x in LAST_GOOD_LEADERS[:30]]
    out: List[Leader] = []
    for t in seeds:
        q = get_finnhub_quote(t)
        if q and q.change_pct >= MIN_SCAN_GAIN_PCT:
            out.append(
                Leader(
                    ticker=t,
                    price=q.price,
                    change_pct=q.change_pct,
                    volume=q.day_volume,
                    source="finnhub_seed",
                )
            )
    return out




def get_yahoo_quote_snapshot(ticker: str) -> Optional[Quote]:
    """Fresh Yahoo chart quote used as the hard data validator.

    v58 fix:
    - Do not rely only on screener % gain.
    - Pull the actual 1m chart.
    - Use last valid close as current price.
    - Use chartPreviousClose/previousClose as base.
    - Require a fresh last candle, unless the chart only gives metadata.
    """
    ticker = normalize_ticker(ticker)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1m", "range": "1d", "includePrePost": "true"}
    resp = http_get(url, source="yahoo_verify", params=params, timeout=min(HTTP_TIMEOUT, 3.0))
    data = parse_json_response(resp, "yahoo_verify")
    try:
        result = data["chart"]["result"][0]
        meta = result.get("meta") or {}
        timestamps = result.get("timestamp") or []
        q = (result.get("indicators") or {}).get("quote") or [{}]
        quote = q[0] if q else {}
    except Exception:
        return None

    closes = quote.get("close") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    vols = quote.get("volume") or []

    last_i = None
    for i in range(min(len(closes), len(timestamps)) - 1, -1, -1):
        c = safe_float(closes[i])
        if c > 0:
            last_i = i
            break

    price = safe_float(meta.get("regularMarketPrice"))
    if (price <= 0 or last_i is not None) and last_i is not None:
        # Last chart close is usually fresher than regularMarketPrice on hot movers.
        price = safe_float(closes[last_i], price)

    prev = safe_float(meta.get("chartPreviousClose") or meta.get("previousClose"))
    high = safe_float(meta.get("regularMarketDayHigh"))
    low = safe_float(meta.get("regularMarketDayLow"))
    vol = safe_int(meta.get("regularMarketVolume"))

    if last_i is not None:
        try:
            last_ts = datetime.fromtimestamp(int(timestamps[last_i]), tz=timezone.utc).astimezone(EASTERN_TZ)
            age = int((now_et() - last_ts).total_seconds())
            if age > CANDLE_MAX_AGE_SECONDS:
                log.info("[YAHOO VERIFY STALE] %s age=%ss max=%ss", ticker, age, CANDLE_MAX_AGE_SECONDS)
                return None
        except Exception:
            pass
        if highs:
            high = max(high, max(safe_float(x) for x in highs if safe_float(x) > 0) if any(safe_float(x) > 0 for x in highs) else high)
        if lows:
            valid_lows = [safe_float(x) for x in lows if safe_float(x) > 0]
            if valid_lows:
                low = min([x for x in [low] if x > 0] + valid_lows)
        if vols:
            vol = max(vol, sum(safe_int(x) for x in vols))

    gain = pct_change(price, prev)
    if price <= 0 or prev <= 0 or gain < -95 or gain > 2000:
        return None
    return Quote(ticker=ticker, price=price, prev_close=prev, change_pct=gain, day_volume=vol, high=high, low=low, source="yahoo_verify_chart")


def consensus_live_leader(leader: Leader) -> Optional[Leader]:
    """Return a hard-confirmed live leader or None.

    v57 rules:
    - Yahoo fresh chart confirmation wins.
    - Finnhub can confirm only when Yahoo is unavailable.
    - Screener/seed-only data is dropped by default because it caused wrong names.
    - Seed-only/manual/watchlist data is disabled. Only scanned leaders confirmed by fresh chart data survive.
    """
    ticker = normalize_ticker(leader.ticker)
    if not ticker or is_probably_warrant_or_unit(ticker):
        return None

    yah = get_yahoo_quote_snapshot(ticker)
    fin = None
    if ALLOW_FINNHUB_LEADER_CONFIRM and not yah and FINNHUB_API_KEY and source_allowed("finnhub"):
        fin = get_finnhub_quote(ticker)

    if yah and yah.change_pct >= MIN_SCAN_GAIN_PCT:
        chosen = yah
        source = f"{leader.source}+yahoo_confirmed"
    elif fin and fin.change_pct >= MIN_SCAN_GAIN_PCT:
        chosen = fin
        source = f"{leader.source}+finnhub_confirmed"
    elif ALLOW_SEED_ONLY_LEADERS and leader.change_pct >= MIN_SCAN_GAIN_PCT and leader.price > 0:
        chosen = Quote(ticker=ticker, price=leader.price, change_pct=leader.change_pct, day_volume=leader.volume, source=leader.source)
        source = f"{leader.source}+seed_only_ALLOWED"
    else:
        log.info("[UNCONFIRMED LEADER DROP] %s source=%s seed_gain=%.1f", ticker, leader.source, leader.change_pct)
        return None

    if chosen.price <= 0 or not (MIN_PRICE <= chosen.price <= MAX_PRICE):
        log.info("[BAD PRICE DROP] %s price=%.4f", ticker, chosen.price)
        return None
    if chosen.change_pct < MIN_SCAN_GAIN_PCT:
        return None

    # If screener disagrees by a huge amount, keep confirmed chart data but log it.
    if leader.change_pct and abs(leader.change_pct - chosen.change_pct) >= 35:
        log.info("[GAIN CORRECTED] %s seed=%.1f confirmed=%.1f source=%s", ticker, leader.change_pct, chosen.change_pct, leader.source)

    leader.price = chosen.price or leader.price
    leader.change_pct = chosen.change_pct
    leader.volume = max(chosen.day_volume, leader.volume)
    leader.source = source
    return leader


def verify_and_rerank_leaders(leaders: Sequence[Leader]) -> List[Leader]:
    """Consensus live verification pass.

    The old version could show wrong leaders because a single feed could report stale/bad % gain.
    This version validates each seed with fresh Yahoo chart/meta and Finnhub quote, then reranks.
    """
    pool = dedupe_leaders(leaders)[:VERIFY_TOP_N_LEADERS]
    verified: List[Leader] = []

    for leader in pool:
        try:
            v = consensus_live_leader(leader)
            if v is None:
                log.info("[BAD LEADER DATA DROP] %s source=%s seed_gain=%.1f", leader.ticker, leader.source, leader.change_pct)
                continue
            verified.append(v)
        except Exception as exc:
            log.warning("[VERIFY ERROR] %s %s", leader.ticker, exc)

    verified = dedupe_leaders(verified)
    verified.sort(key=lambda x: (x.change_pct, x.volume), reverse=True)

    for rank, leader in enumerate(verified[:50], start=1):
        log.info("[LIVE LEADER] #%02d %s gain=%.1f vol=%s source=%s", rank, leader.ticker, leader.change_pct, leader.volume, leader.source)

    return verified

def get_leaders() -> List[Leader]:
    global LAST_GOOD_LEADERS, LAST_GOOD_LEADERS_TS, LAST_GOOD_LEADERS_TRADE_DATE

    all_rows: List[Leader] = []

    for name, fn in (
        ("nasdaq", get_nasdaq_gainers),
        ("webull", get_webull_gainers_placeholder),
        ("yahoo", get_yahoo_gainers),
        ("yahoo_active", get_yahoo_most_active),
        ("stockanalysis", get_stockanalysis_gainers),
    ):
        try:
            rows = fn()
            if rows:
                all_rows.extend(rows)
        except Exception as exc:
            log.warning("[%s LEADERS ERROR] %s", name.upper(), exc)

    # Scan-only rule: do NOT backfill from previous leaders, manual tickers, or watchlists.
    # If the live public leader feeds fail, return no leaders instead of recycling stale/bad names.
    if not all_rows:
        log.warning("[LEADERS] no scan-source rows; not using fallback/watchlist")
        return []

    leaders = verify_and_rerank_leaders(all_rows)

    if leaders:
        # Add float/profile only for top names to avoid slowing scanner.
        for leader in leaders[:MAX_LEADERS_PER_CYCLE]:
            leader.float_shares = best_float(leader.ticker)
        LAST_GOOD_LEADERS = leaders[:80]
        LAST_GOOD_LEADERS_TS = time.time()
        LAST_GOOD_LEADERS_TRADE_DATE = current_trade_date()
        log.info("[LEADERS] %s candidates: %s", len(leaders), ",".join(x.ticker for x in leaders[:20]))
        return leaders[:80]

    age = time.time() - LAST_GOOD_LEADERS_TS if LAST_GOOD_LEADERS_TS else 999999
    if (
        LAST_GOOD_LEADERS
        and LAST_GOOD_LEADERS_TRADE_DATE == current_trade_date()
        and age <= LEADER_CACHE_TTL_SECONDS
    ):
        log.warning("[LEADERS FALLBACK] using TODAY last good leaders age=%ss", int(age))
        return LAST_GOOD_LEADERS[:50]

    if LAST_GOOD_LEADERS and LAST_GOOD_LEADERS_TRADE_DATE != current_trade_date():
        log.warning("[LEADERS FALLBACK BLOCKED] cached leaders are from %s, today=%s", LAST_GOOD_LEADERS_TRADE_DATE, current_trade_date())

    log.warning("[LEADERS] 0 candidates")
    return []


# =============================================================================
# PROFILE / FLOAT
# =============================================================================

def get_finnhub_float(ticker: str) -> Optional[float]:
    if not FINNHUB_API_KEY:
        return None

    url = "https://finnhub.io/api/v1/stock/profile2"
    params = {"symbol": ticker, "token": FINNHUB_API_KEY}
    resp = http_get(url, source="finnhub_profile", params=params, timeout=min(HTTP_TIMEOUT, 2.5))
    data = parse_json_response(resp, "finnhub_profile")
    if not isinstance(data, dict):
        return None

    # Finnhub profile does not always include float. shareOutstanding is in millions.
    shares_m = safe_float(data.get("shareOutstanding"), 0.0)
    if shares_m > 0:
        return shares_m

    return None


def best_float(ticker: str) -> Optional[float]:
    ticker = normalize_ticker(ticker)
    cached = PROFILE_CACHE.get(ticker)
    if cached:
        ts, val = cached
        if time.time() - ts <= PROFILE_CACHE_TTL_SECONDS:
            return val

    val = get_finnhub_float(ticker)
    PROFILE_CACHE[ticker] = (time.time(), val)
    return val


# =============================================================================
# QUOTES
# =============================================================================

def get_finnhub_quote(ticker: str) -> Optional[Quote]:
    if not FINNHUB_API_KEY:
        return None

    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": ticker, "token": FINNHUB_API_KEY}
    resp = http_get(url, source="finnhub", params=params, timeout=min(HTTP_TIMEOUT, 2.5))
    data = parse_json_response(resp, "finnhub")
    if not isinstance(data, dict):
        return None

    price = safe_float(data.get("c"))
    prev = safe_float(data.get("pc"))
    high = safe_float(data.get("h"))
    low = safe_float(data.get("l"))

    if price <= 0:
        return None

    return Quote(
        ticker=ticker,
        price=price,
        prev_close=prev,
        change_pct=pct_change(price, prev),
        high=high,
        low=low,
        source="finnhub",
    )


def best_quote(ticker: str, leader: Optional[Leader] = None) -> Optional[Quote]:
    """Yahoo-chart first quote stack for hot small caps.

    v61: avoids Finnhub lag overriding the already Yahoo-confirmed leader price.
    Priority: Yahoo 1m chart -> confirmed leader snapshot -> Finnhub fallback.
    """
    ticker = normalize_ticker(ticker)
    cached = QUOTE_CACHE.get(ticker)
    if cached:
        ts, q = cached
        if time.time() - ts <= QUOTE_CACHE_TTL_SECONDS and q and q.price > 0:
            return q

    yah = get_yahoo_quote_snapshot(ticker)
    if yah and yah.price > 0:
        if leader:
            yah.day_volume = max(yah.day_volume, leader.volume)
            if not yah.change_pct and leader.change_pct:
                yah.change_pct = leader.change_pct
        QUOTE_CACHE[ticker] = (time.time(), yah)
        return yah

    if leader and leader.price > 0:
        q = Quote(
            ticker=ticker,
            price=leader.price,
            change_pct=leader.change_pct,
            day_volume=leader.volume,
            source=f"leader:{leader.source}",
        )
        QUOTE_CACHE[ticker] = (time.time(), q)
        return q

    q = get_finnhub_quote(ticker)
    if q and q.price > 0:
        QUOTE_CACHE[ticker] = (time.time(), q)
        return q

    return None


# =============================================================================
# CANDLES / STRUCTURE
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

    log.info("[CANDLES] %s Yahoo %s bars age=%s", ticker, len(candles), candle_age_seconds(candles))
    return candles


def get_alpaca_candles(ticker: str) -> List[Candle]:
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        return []

    url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"
    start = (utc_now() - timedelta(hours=12)).isoformat()
    params = {
        "timeframe": "1Min",
        "start": start,
        "limit": "200",
        "adjustment": "raw",
        "feed": "iex",
    }
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }
    resp = http_get(url, source="alpaca", params=params, headers=headers)
    data = parse_json_response(resp, "alpaca")
    bars = (data or {}).get("bars") or [] if isinstance(data, dict) else []

    candles: List[Candle] = []
    for b in bars:
        try:
            ts = datetime.fromisoformat(str(b.get("t")).replace("Z", "+00:00")).astimezone(EASTERN_TZ)
        except Exception:
            ts = now_et()
        c = Candle(
            ts=ts,
            open=safe_float(b.get("o")),
            high=safe_float(b.get("h")),
            low=safe_float(b.get("l")),
            close=safe_float(b.get("c")),
            volume=safe_int(b.get("v")),
        )
        if c.open > 0 and c.close > 0:
            candles.append(c)

    if candles:
        log.info("[CANDLES] %s Alpaca %s bars age=%s", ticker, len(candles), candle_age_seconds(candles))
    return candles



def get_finnhub_candles(ticker: str) -> List[Candle]:
    """Finnhub 1-minute candle fallback. Helps when Alpaca has sparse IEX bars or Yahoo is stale."""
    if not FINNHUB_API_KEY:
        return []

    end_ts = int(utc_now().timestamp())
    start_ts = int((utc_now() - timedelta(hours=8)).timestamp())
    url = "https://finnhub.io/api/v1/stock/candle"
    params = {
        "symbol": ticker,
        "resolution": "1",
        "from": start_ts,
        "to": end_ts,
        "token": FINNHUB_API_KEY,
    }
    resp = http_get(url, source="finnhub_candles", params=params, timeout=min(HTTP_TIMEOUT, 2.5))
    data = parse_json_response(resp, "finnhub_candles")
    if not isinstance(data, dict) or data.get("s") != "ok":
        return []

    ts_list = data.get("t") or []
    opens = data.get("o") or []
    highs = data.get("h") or []
    lows = data.get("l") or []
    closes = data.get("c") or []
    vols = data.get("v") or []

    candles: List[Candle] = []
    n = min(len(ts_list), len(opens), len(highs), len(lows), len(closes), len(vols))
    for i in range(n):
        o = safe_float(opens[i])
        h = safe_float(highs[i])
        l = safe_float(lows[i])
        c = safe_float(closes[i])
        v = safe_int(vols[i])
        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            continue
        try:
            ts = datetime.fromtimestamp(int(ts_list[i]), tz=timezone.utc).astimezone(EASTERN_TZ)
        except Exception:
            ts = now_et()
        candles.append(Candle(ts, o, h, l, c, v))

    log.info("[CANDLES] %s Finnhub %s bars age=%s", ticker, len(candles), candle_age_seconds(candles))
    return candles


def best_candles(ticker: str) -> List[Candle]:
    """Best available 1-minute candle set.

    Priority is not just source order. It checks:
    - enough bars
    - last candle age
    - fallback source freshness

    This fixes cases where Alpaca IEX only returns a few bars on hot small caps.
    """
    ticker = normalize_ticker(ticker)
    cached = CANDLE_CACHE.get(ticker)
    if cached:
        ts, candles = cached
        age = candle_age_seconds(candles)
        if (
            time.time() - ts <= CANDLE_CACHE_TTL_SECONDS
            and candles
            and age is not None
            and age <= CANDLE_MAX_AGE_SECONDS
        ):
            return candles

    candidates: List[Tuple[str, List[Candle]]] = []

    # Yahoo 1m is the primary live validation source for hot small caps.
    yahoo = get_yahoo_candles(ticker)
    if yahoo:
        candidates.append(("Yahoo", yahoo))

    # Alpaca IEX can be thin, but it can still beat Yahoo when Yahoo is stale.
    alpaca = get_alpaca_candles(ticker)
    if alpaca:
        candidates.append(("Alpaca", alpaca))

    if not candidates:
        return []

    source, candles = max(candidates, key=lambda item: candle_source_rank(item[1]))
    age = candle_age_seconds(candles)
    log.info("[CANDLES BEST] %s source=%s bars=%s age=%s", ticker, source, len(candles), age)

    # Hard live-data rule: never cache/return stale candle data as usable.
    if age is None or age > CANDLE_MAX_AGE_SECONDS:
        log.info("[CANDLES LIVE REJECT] %s source=%s age=%s max=%s", ticker, source, age, CANDLE_MAX_AGE_SECONDS)
        CANDLE_CACHE.pop(ticker, None)
        return []

    CANDLE_CACHE[ticker] = (time.time(), candles)
    return candles


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
    first = min(lows[: lookback // 2])
    second = min(lows[lookback // 2 :])
    return second >= first * 0.995


def analyze_structure(candles: Sequence[Candle], quote: Quote) -> Structure:
    if len(candles) < 8 or quote.price <= 0:
        return Structure(data_ok=False, reason="not enough candles", setup_label="NO DATA")
    age = candle_age_seconds(candles)
    if age is not None and age > CANDLE_MAX_AGE_SECONDS:
        return Structure(data_ok=False, reason=f"stale candles {age}s", setup_label="STALE DATA")

    vwap = calc_vwap(candles)
    recent_volume = sum(c.volume for c in candles[-5:])
    last3_change = pct_change(candles[-1].close, candles[-4].close) if len(candles) >= 4 else 0.0
    recent_high = max(c.high for c in candles[-12:]) if len(candles) >= 12 else max(c.high for c in candles)
    hod = max(c.high for c in candles)
    last5_low = min(c.low for c in candles[-5:])

    above_vwap = bool(vwap and quote.price >= vwap)
    near_hod = bool(hod and quote.price >= hod * 0.965)
    higher_lows = detect_higher_lows(candles)
    breakout = bool(recent_high and quote.price >= recent_high * 1.005 and recent_volume >= MIN_RECENT_VOLUME)
    extended_from_vwap_pct = pct_change(quote.price, vwap) if vwap else 0.0
    holding_recent_low = bool(last5_low and quote.price >= last5_low * 1.015)
    below_vwap_reclaim_watch = bool(
        vwap
        and quote.price < vwap
        and quote.price >= vwap * 0.97
        and (higher_lows or last3_change >= 3)
    )

    if above_vwap and higher_lows and near_hod:
        setup_label = "A+ VWAP HOLD"
    elif above_vwap and breakout:
        setup_label = "BREAKOUT PUSH"
    elif above_vwap and last3_change >= 3:
        setup_label = "LIVE MOMENTUM"
    elif above_vwap:
        setup_label = "ABOVE VWAP"
    elif below_vwap_reclaim_watch:
        setup_label = "RECLAIM WATCH"
    else:
        setup_label = "CHECK VWAP"

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
    if extended_from_vwap_pct >= 18:
        reasons.append(f"extended +{extended_from_vwap_pct:.0f}% from VWAP")

    return Structure(
        above_vwap=above_vwap,
        vwap=vwap,
        recent_volume=recent_volume,
        last3_change_pct=last3_change,
        last5_low=last5_low,
        recent_high=recent_high,
        hod=hod,
        near_hod=near_hod,
        higher_lows=higher_lows,
        breakout=breakout,
        extended_from_vwap_pct=extended_from_vwap_pct,
        holding_recent_low=holding_recent_low,
        setup_label=setup_label,
        below_vwap_reclaim_watch=below_vwap_reclaim_watch,
        data_ok=True,
        reason=" + ".join(reasons) if reasons else "structure neutral",
    )


# =============================================================================
# NEWS
# =============================================================================

STRONG_NEWS_TERMS = [
    "fda", "approval", "clearance", "contract", "purchase order", "partnership", "merger",
    "acquisition", "definitive agreement", "earnings", "guidance", "phase", "clinical",
    "trial", "positive data", "license", "distribution", "nvidia", "ai", "battery", "facility",
    "mou", "memorandum of understanding", "financing agreement", "strategic", "collaboration",
]
WEAK_NEWS_TERMS = [
    "presentation", "conference", "appoints", "appointment", "launch", "update", "announces", "expands",
]
JUNK_NEWS_PHRASES = [
    "stocks moving", "stock moving", "why shares are trading", "premarket movers", "most active",
    "gap-ups and gap-downs", "top gainers", "market movers", "52-week", "benzinga pro's top",
    "stocks that are on the move", "on the move in today", "moving in today", "closing bell",
    "intraday session", "pre-market session", "after the closing bell",
    # Officer/bio snippets are not catalysts. These caused alerts like:
    # NEWS: Founder, Executive Chairman of the Board, Chief Executive Officer
    "founder, executive chairman", "executive chairman of the board", "chief executive officer",
    "chief financial officer", "chief operating officer", "board of directors",
    "independent director", "president and ceo", "chairman and ceo",
]

OFFICER_BIO_NEWS_TERMS = [
    "founder", "co-founder", "executive chairman", "chairman of the board",
    "chief executive officer", "chief financial officer", "chief operating officer",
    "chief technology officer", "chief medical officer", "board of directors",
    "independent director", "president and ceo", "chairman and ceo",
    "ceo", "cfo", "coo", "cto", "cmo", "director", "officer",
]


def is_officer_bio_snippet(text: str) -> bool:
    """Reject management/title fragments that are bios, not tradable catalysts.

    Allows real corporate actions like appoints/resigns, but blocks fragments like
    "Founder, Executive Chairman of the Board, Chief Executive Officer".
    """
    h = (text or "").strip().lower()
    if not h:
        return False

    action_words = (
        "announces", "appoints", "appointed", "resigns", "resigned",
        "steps down", "named", "elects", "elected", "hires", "joins",
        "retire", "retires", "transition", "promotes", "promoted",
    )
    has_action = any(w in h for w in action_words)
    hits = sum(1 for term in OFFICER_BIO_NEWS_TERMS if re.search(rf"\b{re.escape(term)}\b", h))

    # Multiple titles with no action verb = bio/sidebar snippet, not news.
    if hits >= 2 and not has_action:
        return True

    # Short comma-separated title fragments are almost always profile text.
    if hits >= 1 and "," in h and len(h) <= 120 and not has_action:
        return True

    # Standalone officer/title fragments should never show as catalyst.
    if hits >= 1 and len(h.split()) <= 10 and not has_action:
        return True

    return False


DILUTION_TERMS = [
    "registered direct", "private placement", "atm", "at-the-market", "shelf", "s-3", "s-1",
    "424b5", "warrant", "convertible", "equity line", "resale", "offering", "securities purchase agreement",
]


def classify_headline(headline: str) -> Tuple[str, bool, str]:
    h = headline.lower()
    dilution = any(term in h for term in DILUTION_TERMS)
    dilution_note = "offering/dilution language" if dilution else ""

    if is_officer_bio_snippet(headline) or any(p in h for p in JUNK_NEWS_PHRASES):
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
    resp = http_get(url, source="finnhub_news", params=params, timeout=min(HTTP_TIMEOUT, 2.5))
    rows = parse_json_response(resp, "finnhub_news")
    if not isinstance(rows, list):
        return None

    rank = {"STRONG": 3, "WEAK": 2, "NONE": 1, "JUNK": 0}
    best: Optional[NewsResult] = None

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

    rank = {"STRONG": 3, "WEAK": 2, "NONE": 1, "JUNK": 0}
    best: Optional[NewsResult] = None
    ticker_word = re.compile(rf"\b{re.escape(ticker)}\b", re.I)

    for item in news:
        headline = str(item.get("title") or "").strip()
        if not headline:
            continue
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


def get_webull_news(ticker: str) -> Optional[NewsResult]:
    ticker = normalize_ticker(ticker)
    if not ticker:
        return None

    candidates = [
        f"https://www.webull.com/newslist/nasdaq-{ticker.lower()}",
        f"https://www.webull.com/newslist/nyse-{ticker.lower()}",
        f"https://www.webull.com/newslist/amex-{ticker.lower()}",
        f"https://www.webullapp.com/newslist/nasdaq-{ticker.lower()}",
        f"https://www.webullapp.com/newslist/nyse-{ticker.lower()}",
        f"https://www.webullapp.com/newslist/amex-{ticker.lower()}",
    ]
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.webull.com/",
    }

    rank = {"STRONG": 3, "WEAK": 2, "NONE": 1, "JUNK": 0}
    best: Optional[NewsResult] = None

    for url in candidates:
        resp = http_get(url, source="webull_news", headers=headers, timeout=min(HTTP_TIMEOUT, 3.0))
        if resp is None or not resp.text:
            continue

        text = resp.text
        raw_titles = re.findall(r'"title"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"', text)
        if not raw_titles:
            raw_titles = re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", text, flags=re.I | re.S)

        for raw in raw_titles[:20]:
            try:
                headline = bytes(raw, "utf-8").decode("unicode_escape")
            except Exception:
                headline = raw
            headline = re.sub(r"<[^>]+>", " ", headline)
            headline = html.unescape(headline)
            headline = re.sub(r"\s+", " ", headline).strip()

            if len(headline) < 12:
                continue
            if "webull" in headline.lower() and "news" in headline.lower() and len(headline) < 35:
                continue

            grade, dilution, dilution_note = classify_headline(headline)
            nr = NewsResult(
                grade=grade,
                headline=headline,
                source="Webull",
                url=url,
                dilution_flag=dilution,
                dilution_note=dilution_note,
            )
            if best is None or rank.get(nr.grade, 0) > rank.get(best.grade, 0):
                best = nr
            if nr.grade == "STRONG":
                return nr

        if best and best.grade in {"STRONG", "WEAK"}:
            return best

    return best


def should_check_news(ticker: str) -> bool:
    top = {x.ticker for x in LAST_GOOD_LEADERS[:NEWS_TOP_N]}
    return normalize_ticker(ticker) in top


def best_news(ticker: str) -> NewsResult:
    ticker = normalize_ticker(ticker)
    cached = NEWS_CACHE.get(ticker)
    if cached:
        ts, nr = cached
        if time.time() - ts <= NEWS_CACHE_TTL_SECONDS:
            return nr

    fallback_dilution = False
    fallback_dilution_note = ""

    for fn in (get_finnhub_news, get_yahoo_news, get_webull_news):
        try:
            nr = fn(ticker)
            if not nr or not nr.headline:
                continue
            if nr.dilution_flag:
                fallback_dilution = True
                fallback_dilution_note = nr.dilution_note
            if nr.grade == "JUNK":
                continue
            if nr.grade in {"STRONG", "WEAK"}:
                NEWS_CACHE[ticker] = (time.time(), nr)
                return nr
        except Exception as exc:
            log.warning("[NEWS ERROR] %s %s", ticker, exc)

    nr = NewsResult(
        grade="NONE",
        headline="UNKNOWN CATALYST — INVESTIGATE",
        source="none",
        dilution_flag=fallback_dilution,
        dilution_note=fallback_dilution_note,
    )
    NEWS_CACHE[ticker] = (time.time(), nr)
    return nr


def clean_news_text(n: NewsResult) -> str:
    if not n or not n.headline:
        return "UNKNOWN CATALYST — INVESTIGATE"
    headline = str(n.headline).strip()
    if headline.upper().startswith("UNKNOWN CATALYST"):
        return "UNKNOWN CATALYST — INVESTIGATE"
    if is_officer_bio_snippet(headline):
        return "UNKNOWN CATALYST — INVESTIGATE"
    grade, _, _ = classify_headline(headline)
    if grade == "JUNK":
        return "UNKNOWN CATALYST — INVESTIGATE"
    return headline[:120]

def summarize_news(headline: str) -> str:
    """Short trader-friendly news label for Telegram."""
    h = (headline or "").strip()
    low = h.lower()

    if not h or "unknown catalyst" in low or is_officer_bio_snippet(h):
        return "UNKNOWN CATALYST — INVESTIGATE"
    if "share consolidation" in low or "reverse split" in low:
        return "Share Consolidation"
    if "business combination" in low or "merger" in low or "acquisition" in low:
        return "Merger/business combo update"
    if "fda" in low or "clearance" in low or "approval" in low:
        return "FDA/regulatory headline"
    if "contract" in low or "purchase order" in low or "award" in low:
        return "Contract/order headline"
    if "phase" in low or "clinical" in low or "trial" in low or "data" in low:
        return "Clinical/data headline"
    if "offering" in low or "registered direct" in low or "private placement" in low:
        return "Offering/dilution language"
    if "appoints" in low or "appointment" in low or "resigns" in low or "cfo" in low or "ceo" in low:
        return "Management change"
    if "partnership" in low or "collaboration" in low or "strategic" in low:
        return "Partnership/collaboration"
    if "earnings" in low or "revenue" in low or "guidance" in low:
        return "Earnings/guidance headline"

    return h[:90]


def setup_alert_label(d: CandidateDecision) -> str:
    """Short public-facing setup label. Keeps alert fast to read."""
    s = d.structure or Structure()
    if d.alert_type == "FAST SPIKE":
        return "🔥 Fast Spike"
    if d.alert_type == "HOD BREAK":
        return "🚨 HOD Break"
    if d.alert_type == "MONSTER LEADER":
        return "⭐ Monster Leader"
    if d.alert_type == "ELITE RUNNER":
        return "🔥 Elite Runner"
    if s.above_vwap and s.higher_lows and s.near_hod:
        return "🟢 A+ VWAP Hold"
    if s.above_vwap and s.breakout:
        return "🟢 Breakout Push"
    if s.above_vwap and s.last3_change_pct >= 3:
        return "🟢 Live Momentum"
    if s.above_vwap:
        return "🟢 Above VWAP"
    if getattr(s, "below_vwap_reclaim_watch", False):
        return "🟡 Reclaim Watch"
    return "⚠️ Check VWAP"


def compact_float_bonus(float_shares: Optional[float]) -> int:
    """Float comes back as millions from Finnhub profile2 in this file."""
    if float_shares and 0 < float_shares <= LOW_FLOAT_BONUS_MAX_M:
        return 1
    return 0


# =============================================================================
# DECISION ENGINE
# =============================================================================

def market_minutes_elapsed(dt: Optional[datetime] = None) -> int:
    dt = dt or now_et()
    mins = dt.hour * 60 + dt.minute
    start = 9 * 60 + 30
    return max(1, mins - start + 1)


def relative_volume_score(day_volume: int, dt: Optional[datetime] = None) -> Tuple[int, str]:
    mins = market_minutes_elapsed(dt)
    expected_by_time = max(100_000, mins * 7_500)
    ratio = day_volume / expected_by_time if expected_by_time > 0 else 0.0

    if ratio >= 5 or day_volume >= 5_000_000:
        return 2, f"rVol hot {ratio:.1f}x"
    if ratio >= 2 or day_volume >= 1_000_000:
        return 1, f"rVol strong {ratio:.1f}x"
    return 0, f"rVol {ratio:.1f}x"


def relative_volume_ratio(day_volume: int, dt: Optional[datetime] = None) -> float:
    mins = market_minutes_elapsed(dt)
    expected_by_time = max(100_000, mins * 7_500)
    return day_volume / expected_by_time if expected_by_time > 0 else 0.0


def market_regime_from_leaders(leaders: Sequence[Leader]) -> Tuple[str, float, float]:
    """Return regime, top gain, and dynamic minimum alert gain.

    On hot days, +27% names become noise. This raises the floor when the
    leading gainers are +50%, +100%, or +200%+.
    """
    top_gain = max((safe_float(x.change_pct) for x in leaders), default=0.0)
    if top_gain >= HOT_DAY_INSANE_TOP_GAIN:
        return "INSANE", top_gain, max(MIN_ALERT_GAIN_PCT, HOT_DAY_INSANE_MIN_GAIN)
    if top_gain >= HOT_DAY_HOT_TOP_GAIN:
        return "HOT", top_gain, max(MIN_ALERT_GAIN_PCT, HOT_DAY_HOT_MIN_GAIN)
    if top_gain >= HOT_DAY_NORMAL_TOP_GAIN:
        return "NORMAL-HOT", top_gain, max(MIN_ALERT_GAIN_PCT, HOT_DAY_NORMAL_MIN_GAIN)
    return "DEAD/NORMAL", top_gain, MIN_ALERT_GAIN_PCT


def get_dynamic_min_gain() -> float:
    regime, _, min_gain = market_regime_from_leaders(LAST_GOOD_LEADERS)
    return min_gain


def get_daily_context(ticker: str) -> Dict[str, Any]:
    ticker = normalize_ticker(ticker)
    cached = DAILY_CACHE.get(ticker)
    if cached and time.time() - cached[0] <= DAILY_CACHE_TTL_SECONDS:
        return cached[1]

    ctx: Dict[str, Any] = {
        "daily_breakout": False,
        "near_60d_high": False,
        "room_to_52w_high_pct": None,
        "above_20d": False,
        "above_50d": False,
        "reason": "daily unavailable",
    }

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1d", "range": "1y", "includePrePost": "false"}
    resp = http_get(url, source="yahoo_daily", params=params, timeout=min(HTTP_TIMEOUT, 3.0))
    data = parse_json_response(resp, "yahoo_daily")
    try:
        result = data["chart"]["result"][0]
        quote = result["indicators"]["quote"][0]
        highs = [safe_float(x) for x in (quote.get("high") or []) if safe_float(x) > 0]
        closes = [safe_float(x) for x in (quote.get("close") or []) if safe_float(x) > 0]
    except Exception:
        DAILY_CACHE[ticker] = (time.time(), ctx)
        return ctx

    if len(closes) < 20 or not highs:
        DAILY_CACHE[ticker] = (time.time(), ctx)
        return ctx

    last = closes[-1]
    lookback = max(20, min(DAILY_BREAKOUT_LOOKBACK_DAYS, len(highs) - 1))
    prev_high = max(highs[-lookback-1:-1]) if len(highs) > lookback else max(highs[:-1] or highs)
    high_52w = max(highs)
    ma20 = sum(closes[-20:]) / 20
    ma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else ma20
    room = pct_change(high_52w, last) if last > 0 else None

    daily_breakout = bool(last >= prev_high * 0.995)
    near_60d_high = bool(last >= prev_high * 0.97)
    ctx = {
        "daily_breakout": daily_breakout,
        "near_60d_high": near_60d_high,
        "room_to_52w_high_pct": room,
        "above_20d": last >= ma20,
        "above_50d": last >= ma50,
        "reason": "daily breakout" if daily_breakout else ("near daily high" if near_60d_high else "daily not breaking out"),
    }
    DAILY_CACHE[ticker] = (time.time(), ctx)
    return ctx


def hot_sector_score(leader: Leader, news: NewsResult) -> Tuple[int, str]:
    text = " ".join([leader.ticker or "", leader.name or "", news.headline or ""]).lower()
    hits = [term for term in HOT_SECTOR_TERMS if term in text]
    if hits:
        return 1, f"hot theme: {hits[0]}"
    return 0, ""


def elite_potential_score(leader: Leader, quote: Quote, structure: Structure, news: NewsResult, daily: Dict[str, Any]) -> Tuple[int, List[str]]:
    score = 0
    why: List[str] = []
    gain = max(quote.change_pct, leader.change_pct)
    vol = max(quote.day_volume, leader.volume)
    rvol = relative_volume_ratio(vol)

    if vol >= 25_000_000:
        score += 2; why.append("monster volume")
    elif vol >= MIN_ELITE_DAY_VOLUME:
        score += 1; why.append("elite volume")

    if rvol >= 5:
        score += 2; why.append(f"rVol {rvol:.1f}x")
    elif rvol >= MIN_ELITE_RVOL_RATIO:
        score += 1; why.append(f"rVol {rvol:.1f}x")

    if leader.float_shares and 0 < leader.float_shares <= LOW_FLOAT_BONUS_MAX_M:
        score += 2; why.append("low float")

    if daily.get("daily_breakout"):
        score += 2; why.append("daily breakout")
    elif daily.get("near_60d_high"):
        score += 1; why.append("near daily high")

    if structure.above_vwap and structure.higher_lows:
        score += 2; why.append("VWAP + higher lows")
    elif structure.above_vwap:
        score += 1; why.append("above VWAP")

    if structure.near_hod or structure.breakout:
        score += 1; why.append("near HOD/breakout")

    if news.grade == "STRONG":
        score += 1; why.append("strong catalyst")

    sec_score, sec_why = hot_sector_score(leader, news)
    score += sec_score
    if sec_why:
        why.append(sec_why)

    if gain >= 100:
        score += 1; why.append("monster gainer")

    if structure.extended_from_vwap_pct >= 45 and not structure.near_hod:
        score -= 2; why.append("too extended")

    return int(clamp(score, 0, 10)), dedupe_text(why)


def initialize_first_seen(leaders: Sequence[Leader]) -> None:
    global FIRST_SEEN_INITIALIZED
    if FIRST_SEEN_INITIALIZED or not leaders:
        return
    now = time.time()
    for leader in leaders:
        ticker = normalize_ticker(leader.ticker)
        if ticker and ticker not in FIRST_SEEN:
            FIRST_SEEN[ticker] = {"date": current_trade_date(), "gain": leader.change_pct, "ts": now, "price": leader.price}
    FIRST_SEEN_INITIALIZED = True
    log.info("[FIRST SEEN] initialized with %s leaders", len(FIRST_SEEN))


def remember_first_seen(leader: Leader, quote: Quote) -> Tuple[bool, float]:
    ticker = normalize_ticker(leader.ticker)
    gain = max(quote.change_pct, leader.change_pct)
    now = time.time()
    row = FIRST_SEEN.get(ticker)
    if row and row.get("date") != current_trade_date():
        FIRST_SEEN.pop(ticker, None)
        row = None

    if not row:
        FIRST_SEEN[ticker] = {"date": current_trade_date(), "gain": gain, "ts": now, "price": quote.price}
        return FIRST_SEEN_INITIALIZED, 0.0

    age = now - float(row.get("ts", now))
    return age <= 900, age


def quality_score(leader: Leader, quote: Quote, structure: Structure, news: NewsResult) -> int:
    score = 0
    gain = max(quote.change_pct, leader.change_pct)
    vol = max(quote.day_volume, leader.volume)

    # Leader strength
    if gain >= 25:
        score += 2
    if gain >= 35:
        score += 1
    if gain >= 50:
        score += 1
    if gain >= 100:
        score += 1

    # Liquidity / attention
    if vol >= 5_000_000:
        score += 3
    elif vol >= 1_000_000:
        score += 2
    elif vol >= 300_000:
        score += 1

    rvol_bonus, _ = relative_volume_score(vol)
    score += rvol_bonus

    # Structure
    if structure.above_vwap:
        score += 1
    if structure.higher_lows:
        score += 1
    if structure.near_hod:
        score += 1
    if structure.breakout or structure.last3_change_pct >= 3:
        score += 1

    # Low float deserves a small bump, but not enough to override terrible structure.
    score += compact_float_bonus(leader.float_shares)

    # News helps but unknown catalyst does not kill major leaders.
    if news.grade == "STRONG":
        score += 1
    elif news.grade == "JUNK" and gain < 50:
        score -= 1

    # Risk penalties
    if quote.price < MIN_PRICE or quote.price > MAX_PRICE:
        score -= 3
    if structure.vwap and quote.price < structure.vwap * 0.96:
        score -= 2
    if structure.data_ok and structure.extended_from_vwap_pct >= 30 and not structure.near_hod:
        score -= 1

    # Live-candle quality penalty. Good scans need fresh bars; stale bars should not alert.
    age = None
    try:
        age_txt = structure.reason if structure.reason else ""
        m = re.search(r"stale candles (\d+)s", age_txt)
        if m:
            age = int(m.group(1))
    except Exception:
        age = None
    if not structure.data_ok:
        score -= 3
    elif age is not None and age > WARN_CANDLE_AGE_SECONDS:
        score -= 2

    return int(clamp(score, 0, 10))


def update_baseline(ticker: str, price: float) -> AlertState:
    st = ALERT_STATES.setdefault(ticker, AlertState())
    now = time.time()

    if st.baseline_price <= 0 or now - st.baseline_ts > FAST_SPIKE_WINDOW_MIN * 60:
        st.baseline_price = price
        st.baseline_ts = now

    if price < st.baseline_price:
        st.baseline_price = price
        st.baseline_ts = now

    return st


def is_meaningful_realert(ticker: str, alert_type: str, quote: Quote, structure: Structure, quality: int) -> bool:
    st = ALERT_STATES.setdefault(ticker, AlertState())
    now = time.time()

    if st.last_alert_ts <= 0:
        return True

    seconds_since = now - st.last_alert_ts
    price_push_pct = pct_change(quote.price, st.last_alert_price) if st.last_alert_price > 0 else 0.0
    high_push_pct = pct_change(structure.hod or quote.price, st.last_alert_high) if st.last_alert_high > 0 else 0.0
    quality_upgrade = quality >= st.last_quality + 2

    # CRITICAL FIX:
    # If a leader makes a real fresh +10% fast spike, do NOT let old market-leader
    # cooldown logic hide it. This was blocking names like WCT in logs.
    if alert_type in {"FAST SPIKE", "HOD BREAK"}:
        if seconds_since < 60:
            return False
        return True

    # Big leaders can re-alert faster on a fresh +5% push/new high.
    if alert_type == "MARKET LEADER":
        if seconds_since >= FRESH_LEADER_REALERT_SECONDS and (
            price_push_pct >= FRESH_LEADER_REPUSH_PCT
            or high_push_pct >= MEANINGFUL_NEW_HIGH_PCT
            or quality_upgrade
        ):
            return True

    # Setup upgrade can re-alert after a shorter delay if the chart meaningfully improves.
    setup_upgrade = alert_type in {"A+ VWAP HOLD", "BREAKOUT PUSH"} and st.last_alert_type not in {"A+ VWAP HOLD", "BREAKOUT PUSH"}

    cooldown_done = seconds_since >= ALERT_COOLDOWN_SECONDS
    meaningful_push = price_push_pct >= MIN_PRICE_MOVE_REPOST_PCT or high_push_pct >= MEANINGFUL_NEW_HIGH_PCT
    return (cooldown_done and (meaningful_push or quality_upgrade)) or (seconds_since >= 600 and setup_upgrade)


def mark_alerted(ticker: str, alert_type: str, quote: Quote, structure: Structure, quality: int) -> None:
    st = ALERT_STATES.setdefault(ticker, AlertState())
    now = time.time()

    st.last_alert_ts = now
    st.last_alert_price = quote.price
    st.last_alert_high = structure.hod or quote.price
    st.last_alert_type = alert_type
    st.last_quality = quality
    st.baseline_price = quote.price
    st.baseline_ts = now


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
    day_vol = max(quote.day_volume, leader.volume)
    remember_first_seen(leader, quote)

    regime, top_gain, dynamic_min_gain = market_regime_from_leaders(LAST_GOOD_LEADERS)
    leader_rank = next((i + 1 for i, x in enumerate(LAST_GOOD_LEADERS) if normalize_ticker(x.ticker) == ticker), 999)

    if not (MIN_PRICE <= quote.price <= MAX_PRICE):
        return CandidateDecision(ticker=ticker, should_alert=False, reasons=["price outside range"], quote=quote, leader=leader)

    candles = best_candles(ticker)
    structure = analyze_structure(candles, quote) if candles else Structure(data_ok=False, reason="no candles", setup_label="NO DATA")

    news = best_news(ticker) if should_check_news(ticker) else NewsResult(
        grade="NONE",
        headline="UNKNOWN CATALYST — INVESTIGATE",
        source="skipped",
    )

    daily = get_daily_context(ticker)
    potential, potential_reasons = elite_potential_score(leader, quote, structure, news, daily)

    rvol = relative_volume_ratio(day_vol)
    _, rvol_label = relative_volume_score(day_vol)
    reasons.append(f"{regime} tape top +{top_gain:.0f}% / floor +{dynamic_min_gain:.0f}%")
    reasons.append(rvol_label)
    reasons.extend(potential_reasons)

    st = update_baseline(ticker, quote.price)
    spike_from_base = pct_change(quote.price, st.baseline_price) if st.baseline_price > 0 else 0.0
    if spike_from_base >= FAST_SPIKE_PCT:
        reasons.append(f"fast +{spike_from_base:.1f}% push")

    if structure.reason:
        reasons.append(structure.reason)
    if daily.get("reason"):
        reasons.append(str(daily.get("reason")))

    if news.grade == "STRONG":
        reasons.append("strong catalyst")
    elif news.grade == "WEAK":
        reasons.append("weak catalyst")

    if news.grade in {"NONE", "JUNK"} and gain >= 35:
        risks.append("UNKNOWN CATALYST — INVESTIGATE")
    if news.dilution_flag:
        risks.append(news.dilution_note or "dilution language")
    if structure.data_ok and not structure.above_vwap:
        risks.append("not above VWAP")
    if structure.extended_from_vwap_pct >= MAX_EXTENDED_FROM_VWAP_WARN_PCT:
        risks.append(f"extended +{structure.extended_from_vwap_pct:.0f}% from VWAP")
    if not structure.data_ok:
        risks.append(structure.reason or "structure data weak")
    if day_vol < MIN_ELITE_DAY_VOLUME:
        risks.append("below elite volume")
    if potential < MIN_ELITE_POTENTIAL_SCORE:
        risks.append(f"potential {potential}/10 below {MIN_ELITE_POTENTIAL_SCORE}")

    quality = quality_score(leader, quote, structure, news)
    quality = max(quality, potential)

    if gain >= 200:
        quality = max(quality, 10)
    elif gain >= 150:
        quality = max(quality, 9)
    elif gain >= 100:
        quality = max(quality, 8)

    if spike_from_base >= FAST_SPIKE_PCT:
        quality = int(clamp(quality + FAST_SPIKE_QUALITY_BONUS, 0, 10))

    alert_type = ""

    fast_spike_ok = (
        spike_from_base >= FAST_SPIKE_PCT
        and gain >= MIN_ALERT_GAIN_PCT
        and day_vol >= MIN_FAST_SPIKE_DAY_VOLUME
        and structure.data_ok
    )
    top3_override_ok = (
        TOP3_LEADER_OVERRIDE
        and leader_rank <= 3
        and gain >= TOP3_OVERRIDE_MIN_GAIN
        and day_vol >= TOP3_OVERRIDE_MIN_VOLUME
        and structure.data_ok
    )
    hod_break_ok = (
        gain >= HOD_BREAK_MIN_GAIN
        and day_vol >= HOD_BREAK_MIN_VOLUME
        and structure.data_ok
        and structure.above_vwap
        and (structure.breakout or structure.near_hod)
        and structure.recent_volume >= MIN_RECENT_VOLUME
    )

    # On hot/insane days, do not alert a lazy +27% name. A true fresh +10% push
    # can bypass the dynamic floor. Also allow top-3 volume monsters so the scanner
    # does not go blind when the tape is very hot.
    passes_dynamic_gain = (
        gain >= dynamic_min_gain
        or (FAST_SPIKE_ALLOWED_UNDER_DYNAMIC_FLOOR and fast_spike_ok)
        or hod_break_ok
        or top3_override_ok
    )
    if not passes_dynamic_gain:
        return CandidateDecision(
            ticker=ticker,
            should_alert=False,
            reasons=[f"gain {gain:.1f}% under dynamic {dynamic_min_gain:.0f}% floor ({regime})"],
            risks=dedupe_text(risks),
            leader_rank=leader_rank,
            rvol=rvol,
            quote=quote,
            structure=structure,
            news=news,
            leader=leader,
            spike_pct=spike_from_base,
        )

    # Must have fresh candles for all alerts.
    if not structure.data_ok:
        alert_type = ""
    elif fast_spike_ok and potential >= max(4, MIN_ELITE_POTENTIAL_SCORE - 2):
        alert_type = "FAST SPIKE"
    elif hod_break_ok and potential >= max(4, MIN_ELITE_POTENTIAL_SCORE - 2):
        alert_type = "HOD BREAK"
    elif top3_override_ok and potential >= 4:
        alert_type = "MONSTER LEADER"
    elif gain >= 100 and day_vol >= MIN_ELITE_DAY_VOLUME and potential >= 5:
        alert_type = "MONSTER LEADER"
    elif (
        gain >= dynamic_min_gain
        and day_vol >= MIN_ELITE_DAY_VOLUME
        and rvol >= MIN_ELITE_RVOL_RATIO
        and structure.above_vwap
        and potential >= MIN_ELITE_POTENTIAL_SCORE
    ):
        alert_type = "ELITE RUNNER"

    # Block random extended leaders unless they are a real fast spike or monster 100%+ leader.
    if (
        alert_type == "ELITE RUNNER"
        and structure.extended_from_vwap_pct >= 45
        and spike_from_base < FAST_SPIKE_PCT
        and gain < 100
    ):
        risks.append("blocked: too extended without fresh spike")
        alert_type = ""

    if alert_type == "FAST SPIKE":
        required_quality = MIN_FAST_SPIKE_QUALITY
    elif alert_type == "HOD BREAK":
        required_quality = MIN_FAST_SPIKE_QUALITY
    elif alert_type == "MONSTER LEADER":
        required_quality = 8
    elif alert_type == "ELITE RUNNER":
        required_quality = MIN_ALERT_QUALITY
    else:
        required_quality = 99

    should = bool(alert_type) and quality >= required_quality and is_meaningful_realert(
        ticker,
        alert_type,
        quote,
        structure,
        quality,
    )

    log.info(
        "[CHECK] %s rank=%s regime=%s top=%.1f floor=%.1f type=%s alert=%s q=%s req=%s pot=%s gain=%.1f spike=%.1f rvol=%.1f vol=%s daily=%s structure=%s news=%s risks=%s",
        ticker,
        leader_rank,
        regime,
        top_gain,
        dynamic_min_gain,
        alert_type or "NONE",
        should,
        quality,
        required_quality if alert_type else "-",
        potential,
        gain,
        spike_from_base,
        rvol,
        day_vol,
        daily.get("reason"),
        structure.reason,
        news.grade,
        "|".join(risks),
    )

    return CandidateDecision(
        ticker=ticker,
        should_alert=should,
        alert_type=alert_type,
        quality=quality,
        reasons=dedupe_text(reasons)[:6],
        risks=dedupe_text(risks)[:4],
        leader_rank=leader_rank,
        rvol=rvol,
        quote=quote,
        structure=structure,
        news=news,
        leader=leader,
        spike_pct=spike_from_base,
    )

# =============================================================================
# ALERTING
# =============================================================================

def dedupe_alert_lines(text: str) -> str:
    """Remove accidental duplicate non-blank alert lines while preserving spacing."""
    seen = set()
    out = []
    for line in text.splitlines():
        key = line.strip().lower()
        if key and key in seen:
            continue
        out.append(line.rstrip())
        if key:
            seen.add(key)
    return "\n".join(out).strip()


def format_alert(d: CandidateDecision) -> str:
    q = d.quote or Quote(d.ticker)
    s = d.structure or Structure()
    n = d.news or NewsResult()

    gain = max(q.change_pct, d.leader.change_pct if d.leader else 0)
    day_vol = max(q.day_volume, d.leader.volume if d.leader else 0)
    float_m = d.leader.float_shares if d.leader else None
    float_txt = format_float_shares(float_m) + float_flag(float_m)
    setup_line = setup_alert_label(d)
    rank_txt = f"🏆 Leader #{d.leader_rank}" if d.leader_rank and d.leader_rank < 999 else "🏆 Live Leader"
    vol_line = format_vol_rvol(day_vol, d.rvol or relative_volume_ratio(day_vol))

    if d.spike_pct >= FAST_SPIKE_PCT:
        action_line = f"🔥 +{d.spike_pct:.1f}% in {FAST_SPIKE_WINDOW_MIN} min"
    elif d.alert_type == "HOD BREAK":
        action_line = "🚨 New High / HOD Push"
    elif d.alert_type in {"MONSTER LEADER", "ELITE RUNNER"}:
        # Do not duplicate the setup line (e.g. ⭐ Monster Leader twice).
        action_line = ""
    else:
        action_line = "📈 Live Push"

    news_line = summarize_news(clean_news_text(n))

    structure_parts = []
    if s.above_vwap:
        structure_parts.append("🟢 Above VWAP")
    elif getattr(s, "below_vwap_reclaim_watch", False):
        structure_parts.append("🟡 Reclaim VWAP")
    else:
        structure_parts.append("⚠️ Confirm VWAP")

    if s.near_hod:
        structure_parts.append("🟢 Near HOD")
    elif s.recent_high:
        structure_parts.append("clear recent high")

    if s.extended_from_vwap_pct >= MAX_EXTENDED_FROM_VWAP_WARN_PCT:
        structure_parts.append(f"⚠️ Extended +{s.extended_from_vwap_pct:.0f}%")

    structure_line = " | ".join(structure_parts[:3])

    next_line = "Watch HOD break" if s.near_hod or d.alert_type == "HOD BREAK" else ("Hold VWAP" if s.above_vwap else "Wait for VWAP reclaim")

    signal_lines = [setup_line]
    if action_line and action_line != setup_line:
        signal_lines.append(action_line)
    signal_block = "\n".join(dedupe_text(signal_lines))

    alert_text = f"""🚀 {d.ticker} +{gain:.0f}% | ${q.price:.2f}

{rank_txt}
Float: {float_txt}
{vol_line}

{signal_block}

NEWS:
{news_line}

STRUCTURE:
{structure_line}

NEXT:
{next_line}"""
    return dedupe_alert_lines(alert_text)


def send_telegram(text: str) -> bool:
    first_line = text.splitlines()[0] if text else ""
    ticker = first_line.split()[1] if len(first_line.split()) > 1 else "unknown"

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        log.warning(
            "[ALERT DRY RUN] missing Telegram config token=%s chats=%s ticker=%s\n%s",
            bool(TELEGRAM_BOT_TOKEN),
            len(TELEGRAM_CHAT_IDS),
            ticker,
            text,
        )
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    delivered = 0

    for chat_id in TELEGRAM_CHAT_IDS:
        payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
        try:
            resp = SESSION.post(url, json=payload, timeout=10)
            log.info("[TELEGRAM SEND] ticker=%s chat=%s status=%s", ticker, chat_id, resp.status_code)
            if resp.status_code >= 300:
                log.warning("[TELEGRAM ERROR] chat=%s status=%s body=%s", chat_id, resp.status_code, resp.text[:300])
                continue
            delivered += 1
        except Exception as exc:
            log.warning("[TELEGRAM ERROR] ticker=%s chat=%s error=%s", ticker, chat_id, exc)

    return delivered > 0


# =============================================================================
# SCAN LOOP
# =============================================================================

def run_scan_cycle() -> Dict[str, Any]:
    reset_daily_state_if_needed()
    prune_not_today_state()

    started = time.time()
    session_name = market_session()
    sent = 0
    checked = 0
    alerts: List[str] = []

    if not alerts_enabled_now():
        summary = {
            "version": VERSION,
            "session": session_name,
            "checked": 0,
            "sent": 0,
            "message": "alerts blocked outside 9:30 AM–4:10 PM ET",
        }
        log.info("[CYCLE SKIP] %s", summary)
        return summary

    leaders = get_leaders()
    initialize_first_seen(leaders)

    decisions: List[CandidateDecision] = []
    for leader in leaders[:MAX_LEADERS_PER_CYCLE]:
        checked += 1
        try:
            d = decide_candidate(leader)
            decisions.append(d)
        except Exception as exc:
            log.warning("[DECISION ERROR] %s %s", leader.ticker, exc)

    alertable = [d for d in decisions if d.should_alert]

    def alert_priority(d: CandidateDecision) -> Tuple[int, float, int, float, int]:
        type_rank = {"FAST SPIKE": 4, "MONSTER LEADER": 3, "ELITE RUNNER": 2}.get(d.alert_type, 0)
        gain_rank = max(d.quote.change_pct if d.quote else 0, d.leader.change_pct if d.leader else 0)
        vol_rank = max(d.quote.day_volume if d.quote else 0, d.leader.volume if d.leader else 0)
        return (type_rank, d.spike_pct, d.quality, gain_rank, vol_rank)

    alertable.sort(key=alert_priority, reverse=True)
    attempted = min(len(alertable), MAX_ALERTS_PER_CYCLE)

    for d in alertable[:MAX_ALERTS_PER_CYCLE]:
        text = format_alert(d)
        if send_telegram(text):
            mark_alerted(d.ticker, d.alert_type, d.quote or Quote(d.ticker), d.structure or Structure(), d.quality)
            alerts.append(d.ticker)
            sent += 1
        else:
            log.error("[ALERT LOST] %s not delivered; cooldown not applied", d.ticker)

    elapsed = round(time.time() - started, 2)
    summary = {
        "version": VERSION,
        "session": session_name,
        "leaders": len(leaders),
        "market_regime": market_regime_from_leaders(leaders)[0],
        "dynamic_min_gain": market_regime_from_leaders(leaders)[2],
        "checked": checked,
        "alertable": len(alertable),
        "attempted": attempted,
        "sent": sent,
        "alerts": alerts,
        "elapsed": elapsed,
        "max_leaders_per_cycle": MAX_LEADERS_PER_CYCLE,
        "news_top_n": NEWS_TOP_N,
        "cache_sizes": {
            "quotes": len(QUOTE_CACHE),
            "news": len(NEWS_CACHE),
            "candles": len(CANDLE_CACHE),
            "profiles": len(PROFILE_CACHE),
                "daily": len(DAILY_CACHE),
        },
        "first_seen_count": len(FIRST_SEEN),
        "first_seen_initialized": FIRST_SEEN_INITIALIZED,
        "trade_date": TRADE_DATE,
        "last_good_leaders_trade_date": LAST_GOOD_LEADERS_TRADE_DATE,
        "blocked_sources": {
            k: max(0, int(v - time.time()))
            for k, v in SOURCE_BLOCKED_UNTIL.items()
            if v > time.time()
        },
    }

    log.info("[DELIVERY] attempted=%s delivered=%s", attempted, sent)
    log.info("[CYCLE DONE] %s", json.dumps(summary, default=str))
    return summary


def scanner_loop() -> None:
    global LAST_CYCLE_SUMMARY

    log.info(
        "[START] %s interval=%ss min_alert_gain=%s alert_window=9:30-16:10ET",
        VERSION,
        SCAN_INTERVAL_SECONDS,
        MIN_ALERT_GAIN_PCT,
    )
    log.info(
        "[TELEGRAM CONFIG] token=%s chats=%s token_len=%s chat_count=%s",
        bool(TELEGRAM_BOT_TOKEN),
        bool(TELEGRAM_CHAT_IDS),
        len(TELEGRAM_BOT_TOKEN),
        len(TELEGRAM_CHAT_IDS),
    )
    log.info(
        "[DATA CONFIG] finnhub=%s alpaca_key=%s alpaca_secret=%s",
        bool(FINNHUB_API_KEY),
        bool(ALPACA_API_KEY),
        bool(ALPACA_SECRET_KEY),
    )

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
            "alerts_enabled_now": alerts_enabled_now(),
            "last_cycle": LAST_CYCLE_SUMMARY,
            "last_good_leaders": [x.ticker for x in LAST_GOOD_LEADERS[:20]],
            "max_leaders_per_cycle": MAX_LEADERS_PER_CYCLE,
            "news_top_n": NEWS_TOP_N,
            "cache_sizes": {
                "quotes": len(QUOTE_CACHE),
                "news": len(NEWS_CACHE),
                "candles": len(CANDLE_CACHE),
                "profiles": len(PROFILE_CACHE),
                "daily": len(DAILY_CACHE),
            },
            "first_seen_count": len(FIRST_SEEN),
            "first_seen_initialized": FIRST_SEEN_INITIALIZED,
            "trade_date": TRADE_DATE,
            "last_good_leaders_trade_date": LAST_GOOD_LEADERS_TRADE_DATE,
            "blocked_sources": {
                k: max(0, int(v - time.time()))
                for k, v in SOURCE_BLOCKED_UNTIL.items()
                if v > time.time()
            },
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
