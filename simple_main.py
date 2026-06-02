#!/usr/bin/env python3
# ============================================================
# SIMPLE LEADER SCANNER v43
# Leader-first momentum scanner with ranked catalyst engine
# ============================================================
#
# Core goals:
# - Focus only live market leaders
# - No alerts under 25%
# - Reject junk headlines
# - Rank news sources: SEC/PR/Globe > Alpaca > Finnhub > Yahoo
# - Use UNKNOWN CATALYST — INVESTIGATE instead of fake catalyst
# - Include dilution as awareness, not auto-kill
# - Fast +10% leader spike trigger
# - Anti-spam: alert only on meaningful change
#
# Required env:
#   DISCORD_WEBHOOK_URL optional
#   ALPACA_KEY optional
#   ALPACA_SECRET optional
#   FINNHUB_KEY optional
#
# Install:
#   pip install requests flask pytz pandas
#
# Run:
#   python scanner_v43.py
# ============================================================

import os
import re
import time
import json
import math
import html
import pytz
import queue
import random
import logging
import threading
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dtime
from typing import Dict, List, Optional, Any, Tuple

import requests
from flask import Flask, jsonify

try:
    import pandas as pd
except Exception:
    pd = None


# ============================================================
# CONFIG
# ============================================================

VERSION = "v43.3-no-score-fast-news"

TZ = pytz.timezone("America/New_York")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

ALPACA_KEY = os.getenv("ALPACA_KEY", "").strip()
ALPACA_SECRET = os.getenv("ALPACA_SECRET", "").strip()
FINNHUB_KEY = os.getenv("FINNHUB_KEY", "").strip()

# v43.3: Finnhub is often slow during live scans.
# Keep OFF by default so it cannot freeze/drag alerts.
USE_FINNHUB_NEWS = os.getenv("USE_FINNHUB_NEWS", "0").strip() == "1"
FINNHUB_NEWS_TIMEOUT = float(os.getenv("FINNHUB_NEWS_TIMEOUT", "1.2"))

PORT = int(os.getenv("PORT", "10000"))

SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "45"))

MIN_ALERT_GAIN = float(os.getenv("MIN_ALERT_GAIN", "25"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.50"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "80"))
MIN_DAY_VOLUME = int(os.getenv("MIN_DAY_VOLUME", "500000"))
LEADER_DAY_VOLUME = int(os.getenv("LEADER_DAY_VOLUME", "2000000"))

MAX_ALERTS_PER_CYCLE = int(os.getenv("MAX_ALERTS_PER_CYCLE", "4"))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "10"))

FAST_SPIKE_PCT = float(os.getenv("FAST_SPIKE_PCT", "10"))
FAST_SPIKE_LOOKBACK_MIN = int(os.getenv("FAST_SPIKE_LOOKBACK_MIN", "5"))

# v43.1 leader override rules
FRESH_HOD_LEADER_GAIN = float(os.getenv("FRESH_HOD_LEADER_GAIN", "50"))
FRESH_HOD_NEAR_HIGH_PCT = float(os.getenv("FRESH_HOD_NEAR_HIGH_PCT", "3.0"))
LEADER_TRACK_BELOW_VWAP_GAIN = float(os.getenv("LEADER_TRACK_BELOW_VWAP_GAIN", "50"))

STALE_CANDLE_SECONDS = int(os.getenv("STALE_CANDLE_SECONDS", "180"))

GAINERS_LIMIT = int(os.getenv("GAINERS_LIMIT", "40"))
MAX_DEEP_SCAN = int(os.getenv("MAX_DEEP_SCAN", "15"))

BLOCK_SUFFIXES = (
    "W", "WS", "WT", "WQ", "WSA", "WSC", "IW",
    "R", "U"
)

USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
    "Mobile/15E148 Safari/604.1"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json,text/html,*/*",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

app = Flask(__name__)
last_heartbeat = {"ts": None, "version": VERSION, "last_cycle": None}


# ============================================================
# STATE
# ============================================================

@dataclass
class AlertState:
    last_alert_ts: float = 0.0
    last_price: float = 0.0
    last_gain: float = 0.0
    last_high: float = 0.0
    last_title: str = ""
    alert_count: int = 0


@dataclass
class TickerMemory:
    rolling_prices: List[Tuple[float, float]] = field(default_factory=list)


ALERT_STATE: Dict[str, AlertState] = {}
TICKER_MEMORY: Dict[str, TickerMemory] = {}


# ============================================================
# UTIL
# ============================================================

def now_et() -> datetime:
    return datetime.now(TZ)


def market_session() -> str:
    n = now_et().time()
    if dtime(4, 0) <= n < dtime(9, 30):
        return "PREMARKET"
    if dtime(9, 30) <= n < dtime(11, 0):
        return "OPEN"
    if dtime(11, 0) <= n < dtime(14, 0):
        return "MIDDAY"
    if dtime(14, 0) <= n < dtime(16, 0):
        return "POWER HOUR"
    if dtime(16, 0) <= n < dtime(20, 0):
        return "AFTERHOURS"
    return "CLOSED"


def scanner_active() -> bool:
    n = now_et()
    if n.weekday() >= 5:
        return False
    return dtime(4, 0) <= n.time() <= dtime(16, 10)


def safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def pct_change(a: float, b: float) -> float:
    if not a:
        return 0.0
    return ((b - a) / a) * 100.0


def clean_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(str(s))
    s = re.sub(r"\s+", " ", s).strip()
    return s


def ticker_allowed(ticker: str) -> bool:
    t = ticker.upper().strip()
    if not re.match(r"^[A-Z]{1,5}$", t):
        return False
    for suf in BLOCK_SUFFIXES:
        if t.endswith(suf) and len(t) > 4:
            return False
    return True


def strict_ticker_match(ticker: str, text: str) -> bool:
    if not text:
        return False
    return re.search(rf"(?<![A-Z0-9]){re.escape(ticker.upper())}(?![A-Z0-9])", text.upper()) is not None


def requests_get(url: str, params=None, timeout=8, headers=None) -> Optional[requests.Response]:
    try:
        r = requests.get(url, params=params, timeout=timeout, headers=headers or HEADERS)
        if r.status_code == 200:
            return r
        logging.info(f"[HTTP] {url} status={r.status_code}")
    except Exception as e:
        logging.info(f"[HTTP ERR] {url} {e}")
    return None


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class Candidate:
    ticker: str
    price: float
    gain_pct: float
    volume: int
    source: str = "unknown"
    rank: int = 999
    name: str = ""


@dataclass
class CandleBundle:
    source: str
    bars: List[Dict[str, Any]]
    age_seconds: Optional[int] = None


@dataclass
class StructureResult:
    above_vwap: bool = False
    vwap: float = 0.0
    recent_high: float = 0.0
    hod: float = 0.0
    off_hod_pct: float = 0.0
    last3_pct: float = 0.0
    vol_ratio: float = 0.0
    higher_lows: bool = False
    new_high_push: bool = False
    stale: bool = False
    reason: List[str] = field(default_factory=list)


@dataclass
class NewsCandidate:
    source: str
    headline: str
    url: str = ""
    published_at: str = ""


@dataclass
class NewsResult:
    found: bool
    source: Optional[str]
    headline: str
    grade: str
    type: str
    confidence: str
    score: float
    ranked: List[Dict[str, Any]] = field(default_factory=list)
    dilution_flags: List[str] = field(default_factory=list)


def unknown_news_result() -> NewsResult:
    return NewsResult(
        found=False,
        source=None,
        headline="UNKNOWN CATALYST — INVESTIGATE",
        grade="D",
        type="UNKNOWN",
        confidence="D",
        score=0,
        ranked=[],
        dilution_flags=[],
    )


# ============================================================
# GAINER SOURCES
# ============================================================

def yahoo_quote_summary(symbols: List[str]) -> Dict[str, Any]:
    if not symbols:
        return {}
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(symbols)}
    r = requests_get(url, params=params, timeout=8)
    if not r:
        return {}
    try:
        data = r.json()
        out = {}
        for q in data.get("quoteResponse", {}).get("result", []):
            sym = q.get("symbol", "").upper()
            out[sym] = q
        return out
    except Exception:
        return {}


def yahoo_gainers() -> List[Candidate]:
    """
    Uses Yahoo predefined screener endpoint.
    """
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {
        "scrIds": "day_gainers",
        "count": GAINERS_LIMIT,
    }
    r = requests_get(url, params=params, timeout=10)
    candidates = []
    if not r:
        return candidates

    try:
        data = r.json()
        quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        for i, q in enumerate(quotes, start=1):
            ticker = q.get("symbol", "").upper()
            if not ticker_allowed(ticker):
                continue
            price = safe_float(q.get("regularMarketPrice"))
            gain = safe_float(q.get("regularMarketChangePercent"))
            volume = safe_int(q.get("regularMarketVolume"))
            if price < MIN_PRICE or price > MAX_PRICE:
                continue
            if volume < MIN_DAY_VOLUME:
                continue
            candidates.append(Candidate(
                ticker=ticker,
                price=price,
                gain_pct=gain,
                volume=volume,
                source="Yahoo Gainers",
                rank=i,
                name=q.get("shortName", "") or q.get("longName", "")
            ))
    except Exception as e:
        logging.info(f"[GAINERS ERR] {e}")
    return candidates


def yahoo_most_actives_backup() -> List[Candidate]:
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {
        "scrIds": "most_actives",
        "count": GAINERS_LIMIT,
    }
    r = requests_get(url, params=params, timeout=10)
    candidates = []
    if not r:
        return candidates
    try:
        data = r.json()
        quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        for i, q in enumerate(quotes, start=1):
            ticker = q.get("symbol", "").upper()
            if not ticker_allowed(ticker):
                continue
            price = safe_float(q.get("regularMarketPrice"))
            gain = safe_float(q.get("regularMarketChangePercent"))
            volume = safe_int(q.get("regularMarketVolume"))
            if gain < 12:
                continue
            if price < MIN_PRICE or price > MAX_PRICE:
                continue
            if volume < MIN_DAY_VOLUME:
                continue
            candidates.append(Candidate(
                ticker=ticker,
                price=price,
                gain_pct=gain,
                volume=volume,
                source="Yahoo Actives",
                rank=100 + i,
                name=q.get("shortName", "") or q.get("longName", "")
            ))
    except Exception as e:
        logging.info(f"[ACTIVES ERR] {e}")
    return candidates


def get_leaders() -> List[Candidate]:
    raw = yahoo_gainers() + yahoo_most_actives_backup()
    by_ticker: Dict[str, Candidate] = {}
    for c in raw:
        old = by_ticker.get(c.ticker)
        if old is None or c.gain_pct > old.gain_pct:
            by_ticker[c.ticker] = c

    leaders = list(by_ticker.values())
    leaders.sort(key=lambda x: (x.gain_pct, x.volume), reverse=True)

    # Keep top names only
    leaders = leaders[:MAX_DEEP_SCAN]
    logging.info(f"[LEADERS] {len(leaders)} candidates: " + ", ".join([f"{c.ticker}+{c.gain_pct:.1f}%" for c in leaders[:10]]))
    return leaders


# ============================================================
# QUOTES / CANDLES
# ============================================================

def finnhub_quote(ticker: str) -> Optional[Dict[str, Any]]:
    if not FINNHUB_KEY:
        return None
    url = "https://finnhub.io/api/v1/quote"
    r = requests_get(url, params={"symbol": ticker, "token": FINNHUB_KEY}, timeout=6)
    if not r:
        return None
    try:
        j = r.json()
        if "c" in j and safe_float(j.get("c")) > 0:
            return j
    except Exception:
        pass
    return None


def yahoo_chart_1m(ticker: str) -> Optional[CandleBundle]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {
        "range": "1d",
        "interval": "1m",
        "includePrePost": "true",
    }
    r = requests_get(url, params=params, timeout=8)
    if not r:
        return None
    try:
        data = r.json()
        result = data.get("chart", {}).get("result", [None])[0]
        if not result:
            return None
        ts = result.get("timestamp", [])
        quote = result.get("indicators", {}).get("quote", [{}])[0]

        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        vols = quote.get("volume", [])

        bars = []
        for i, epoch in enumerate(ts):
            try:
                o, h, l, c, v = opens[i], highs[i], lows[i], closes[i], vols[i]
                if o is None or h is None or l is None or c is None:
                    continue
                bars.append({
                    "t": int(epoch),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(c),
                    "volume": int(v or 0),
                })
            except Exception:
                continue

        if not bars:
            return None

        age = int(time.time() - bars[-1]["t"])
        return CandleBundle(source="Yahoo 1m candles", bars=bars, age_seconds=age)
    except Exception as e:
        logging.info(f"[CANDLE ERROR] {ticker}: {e}")
        return None


def alpaca_bars_1m(ticker: str) -> Optional[CandleBundle]:
    if not ALPACA_KEY or not ALPACA_SECRET:
        return None

    end = datetime.utcnow()
    start = end - timedelta(hours=10)
    url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"
    params = {
        "timeframe": "1Min",
        "start": start.isoformat() + "Z",
        "end": end.isoformat() + "Z",
        "adjustment": "raw",
        "limit": 500,
    }
    headers = {
        **HEADERS,
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    r = requests_get(url, params=params, headers=headers, timeout=8)
    if not r:
        return None
    try:
        j = r.json()
        raw = j.get("bars", [])
        bars = []
        for b in raw:
            t = b.get("t")
            epoch = int(datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp())
            bars.append({
                "t": epoch,
                "open": float(b.get("o")),
                "high": float(b.get("h")),
                "low": float(b.get("l")),
                "close": float(b.get("c")),
                "volume": int(b.get("v") or 0),
            })
        if not bars:
            return None
        age = int(time.time() - bars[-1]["t"])
        return CandleBundle(source="Alpaca 1m candles", bars=bars, age_seconds=age)
    except Exception as e:
        logging.info(f"[ALPACA CANDLE ERROR] {ticker}: {e}")
        return None


def get_candles(ticker: str) -> Optional[CandleBundle]:
    cb = alpaca_bars_1m(ticker)
    if cb and cb.bars and (cb.age_seconds is None or cb.age_seconds <= STALE_CANDLE_SECONDS):
        return cb

    y = yahoo_chart_1m(ticker)
    if y and y.bars:
        return y

    return cb


# ============================================================
# STRUCTURE ENGINE
# ============================================================

def compute_vwap(bars: List[Dict[str, Any]]) -> float:
    total_pv = 0.0
    total_v = 0.0
    for b in bars:
        typical = (b["high"] + b["low"] + b["close"]) / 3.0
        v = b.get("volume", 0)
        total_pv += typical * v
        total_v += v
    return total_pv / total_v if total_v else 0.0


def detect_higher_lows(bars: List[Dict[str, Any]], lookback=8) -> bool:
    if len(bars) < lookback:
        return False
    lows = [b["low"] for b in bars[-lookback:]]
    count = 0
    for i in range(1, len(lows)):
        if lows[i] >= lows[i-1] * 0.995:
            count += 1
    return count >= max(4, lookback // 2)


def analyze_structure(ticker: str, candles: CandleBundle) -> StructureResult:
    bars = candles.bars
    sr = StructureResult()
    if not bars or len(bars) < 10:
        sr.reason.append("not enough candles")
        return sr

    last = bars[-1]
    close = last["close"]
    recent = bars[-12:]
    prev = bars[-20:-5] if len(bars) >= 25 else bars[:-5]

    sr.vwap = compute_vwap(bars)
    sr.above_vwap = close > sr.vwap if sr.vwap else False
    sr.hod = max(b["high"] for b in bars)
    sr.recent_high = max(b["high"] for b in recent)
    sr.off_hod_pct = pct_change(sr.hod, close) * -1 if sr.hod else 0
    sr.higher_lows = detect_higher_lows(bars)

    if len(bars) >= 4:
        old = bars[-4]["close"]
        sr.last3_pct = pct_change(old, close)

    recent_vol = sum(b.get("volume", 0) for b in bars[-3:]) / 3
    base_vol = sum(b.get("volume", 0) for b in bars[-20:-5]) / max(1, len(bars[-20:-5]))
    sr.vol_ratio = recent_vol / base_vol if base_vol else 0.0

    prev_high = max([b["high"] for b in prev], default=0)
    sr.new_high_push = close >= prev_high * 1.005 if prev_high else False
    sr.stale = candles.age_seconds is not None and candles.age_seconds > STALE_CANDLE_SECONDS

    if sr.above_vwap:
        sr.reason.append("above VWAP")
    else:
        sr.reason.append("below VWAP")

    if sr.last3_pct >= 2:
        sr.reason.append(f"last3 +{sr.last3_pct:.1f}%")
    if sr.vol_ratio >= 1.5:
        sr.reason.append(f"vol {sr.vol_ratio:.1f}x")
    if sr.higher_lows:
        sr.reason.append("higher lows")
    if sr.new_high_push:
        sr.reason.append("new high push")
    if sr.off_hod_pct <= 6:
        sr.reason.append("near HOD")
    if sr.stale:
        sr.reason.append(f"stale candle {candles.age_seconds}s")

    return sr


# ============================================================
# NEWS ENGINE v43
# ============================================================

JUNK_HEADLINE_PHRASES = [
    # generic mover / aggregator garbage — never show as catalyst
    "stocks moving", "stock moving", "why shares are trading",
    "why is", "why are", "shares are trading", "shares trading",
    "gap up", "gap down", "gap up and gap down",
    "gap up and gap down stocks",
    "top gainers", "premarket movers", "most active",
    "biggest movers", "market update", "midday movers",
    "after-hours movers", "session:", "tuesday's session",
    "monday's session", "wednesday's session",
    "thursday's session", "friday's session",
    "watch these stocks", "hot penny stocks", "trending stocks",
    "movers to watch", "stock market today",
    "biggest pre-market stock movers", "biggest stock movers",
    "what's going on with", "what is going on with",
]

STRONG_CATALYST_WORDS = [
    "fda", "approval", "clearance", "contract", "purchase order",
    "partnership", "strategic", "acquisition", "merger",
    "license", "distribution", "earnings beat", "raises guidance",
    "guidance", "clinical", "phase 1", "phase 2", "phase 3",
    "trial", "positive results", "ai", "nvidia", "government",
    "award", "order", "mou", "memorandum of understanding",
    "collaboration", "launches", "commercialization",
]

DILUTION_WORDS = [
    "shelf", "atm", "at-the-market", "registered direct",
    "private placement", "warrants", "convertible", "resale",
    "offering", "securities purchase agreement", "equity line",
    "s-1", "s-3", "f-1", "f-3", "424b5", "424b3",
]

SEC_BULLISH_ITEMS = [
    "1.01", "2.02", "7.01", "8.01", "9.01",
    "material definitive agreement",
    "results of operations",
    "regulation fd",
    "other events",
    "financial statements and exhibits",
]


def is_junk_headline(title: str) -> bool:
    if not title:
        return True

    t = clean_text(title).lower()

    # Phrase blacklist catches Benzinga/Yahoo/aggregator "mover" articles.
    if any(p in t for p in JUNK_HEADLINE_PHRASES):
        return True

    # Day/session style aggregator headlines:
    # "Tuesday's session: gap up and gap down stocks"
    if re.search(r"\b(monday|tuesday|wednesday|thursday|friday)'?s session\b", t):
        return True

    # Generic listicle/mover style headlines.
    if re.search(r"\b\d+\s+(stocks|penny stocks)\s+(moving|to watch|trending)\b", t):
        return True

    if "gap" in t and ("stocks" in t or "session" in t):
        return True

    return False


def classify_headline(title: str) -> Dict[str, Any]:
    if not title or is_junk_headline(title):
        return {"grade": "F", "score": 0, "type": "JUNK", "headline": None}

    t = title.lower()

    if any(w in t for w in DILUTION_WORDS):
        return {"grade": "X", "score": 2, "type": "DILUTION", "headline": clean_text(title)}

    if any(w in t for w in STRONG_CATALYST_WORDS):
        return {"grade": "A", "score": 9, "type": "STRONG", "headline": clean_text(title)}

    return {"grade": "C", "score": 5, "type": "WEAK", "headline": clean_text(title)}


def rank_news_candidates(candidates: List[NewsCandidate], dilution_flags=None) -> NewsResult:
    source_weight = {
        "SEC 8-K": 10,
        "SEC Exhibit 99.1": 10,
        "SEC Exhibit": 10,
        "PR Newswire": 10,
        "GlobeNewswire": 10,
        "Company IR": 9,
        "Alpaca": 7,
        "Finnhub": 5,
        "Yahoo": 3,
    }

    ranked = []
    dilution_flags = dilution_flags or []

    for item in candidates:
        headline = clean_text(item.headline)
        source = item.source

        result = classify_headline(headline)

        if result["type"] == "JUNK":
            continue

        if result["type"] == "DILUTION":
            dilution_flags.append(headline)

        total_score = result["score"] + source_weight.get(source, 1)

        ranked.append({
            "source": source,
            "headline": result["headline"],
            "grade": result["grade"],
            "type": result["type"],
            "score": total_score,
            "url": item.url,
            "published_at": item.published_at,
        })

    ranked.sort(key=lambda x: x["score"], reverse=True)

    # Prefer non-dilution catalyst for catalyst headline.
    non_dilution = [r for r in ranked if r["type"] != "DILUTION"]
    chosen_pool = non_dilution if non_dilution else ranked

    if chosen_pool:
        best = chosen_pool[0]
        return NewsResult(
            found=True,
            source=best["source"],
            headline=best["headline"],
            grade=best["grade"],
            type=best["type"],
            confidence=best["grade"],
            score=best["score"],
            ranked=ranked,
            dilution_flags=list(dict.fromkeys(dilution_flags))[:5],
        )

    return NewsResult(
        found=False,
        source=None,
        headline="UNKNOWN CATALYST — INVESTIGATE",
        grade="D",
        type="UNKNOWN",
        confidence="D",
        score=0,
        ranked=[],
        dilution_flags=list(dict.fromkeys(dilution_flags))[:5],
    )


# -------------------------
# Alpaca News
# -------------------------

def get_alpaca_news_candidates(ticker: str) -> List[NewsCandidate]:
    if not ALPACA_KEY or not ALPACA_SECRET:
        return []

    url = "https://data.alpaca.markets/v1beta1/news"
    params = {
        "symbols": ticker,
        "limit": 10,
        "sort": "desc",
    }
    headers = {
        **HEADERS,
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    r = requests_get(url, params=params, headers=headers, timeout=8)
    if not r:
        return []

    out = []
    try:
        for n in r.json().get("news", []):
            title = clean_text(n.get("headline") or n.get("summary") or "")
            full_text = f"{title} {' '.join(n.get('symbols', []))}"
            if title and (ticker.upper() in n.get("symbols", []) or strict_ticker_match(ticker, full_text)):
                out.append(NewsCandidate(
                    source="Alpaca",
                    headline=title,
                    url=n.get("url", ""),
                    published_at=n.get("created_at", ""),
                ))
    except Exception as e:
        logging.info(f"[ALPACA NEWS ERR] {ticker}: {e}")

    return out


# -------------------------
# Finnhub News
# -------------------------

def get_finnhub_news_candidates(ticker: str) -> List[NewsCandidate]:
    # v43.3: disabled by default because Finnhub timeouts were slowing live scans.
    # Turn on with USE_FINNHUB_NEWS=1 if wanted.
    if not FINNHUB_KEY or not USE_FINNHUB_NEWS:
        return []

    to_date = now_et().date()
    from_date = to_date - timedelta(days=4)
    url = "https://finnhub.io/api/v1/company-news"
    params = {
        "symbol": ticker,
        "from": str(from_date),
        "to": str(to_date),
        "token": FINNHUB_KEY,
    }
    r = requests_get(url, params=params, timeout=FINNHUB_NEWS_TIMEOUT)
    if not r:
        logging.info(f"[FINNHUB NEWS SKIP] {ticker}: timeout/no response")
        return []

    out = []
    try:
        for n in r.json()[:6]:
            title = clean_text(n.get("headline", ""))
            if title and not is_junk_headline(title):
                out.append(NewsCandidate(
                    source="Finnhub",
                    headline=title,
                    url=n.get("url", ""),
                    published_at=str(n.get("datetime", "")),
                ))
    except Exception as e:
        logging.info(f"[FINNHUB NEWS ERR] {ticker}: {e}")
    return out


# -------------------------
# Yahoo News
# -------------------------

def get_yahoo_news_candidates(ticker: str) -> List[NewsCandidate]:
    url = f"https://query1.finance.yahoo.com/v1/finance/search"
    params = {
        "q": ticker,
        "newsCount": 8,
        "quotesCount": 0,
    }
    r = requests_get(url, params=params, timeout=8)
    if not r:
        return []

    out = []
    try:
        for n in r.json().get("news", []):
            title = clean_text(n.get("title", ""))
            publisher = clean_text(n.get("publisher", ""))
            link = n.get("link", "")
            if not title:
                continue
            # Strict match helps avoid false ticker words.
            blob = f"{title} {publisher} {link}"
            if not strict_ticker_match(ticker, blob) and ticker.upper() not in link.upper():
                continue
            out.append(NewsCandidate(
                source="Yahoo",
                headline=title,
                url=link,
                published_at=str(n.get("providerPublishTime", "")),
            ))
    except Exception as e:
        logging.info(f"[YAHOO NEWS ERR] {ticker}: {e}")

    return out


# -------------------------
# PR Newswire / GlobeNewswire search hooks
# -------------------------

def get_prnewswire_candidates(ticker: str) -> List[NewsCandidate]:
    """
    Lightweight web search style endpoint is unreliable without paid search API.
    This function uses PR Newswire site search URL HTML best-effort.
    If it fails, scanner continues.
    """
    query = f"{ticker} press release"
    url = "https://www.prnewswire.com/search/news/"
    params = {"keyword": query, "pagesize": 10}
    r = requests_get(url, params=params, timeout=8)
    if not r:
        return []
    text = r.text
    out = []

    # Best-effort title extraction.
    titles = re.findall(r'<h3[^>]*>(.*?)</h3>', text, flags=re.I | re.S)
    for raw in titles[:8]:
        title = clean_text(re.sub("<.*?>", " ", raw))
        if title and (strict_ticker_match(ticker, title) or ticker.upper() in title.upper()):
            out.append(NewsCandidate(source="PR Newswire", headline=title, url=r.url))
    return out


def get_globenewswire_candidates(ticker: str) -> List[NewsCandidate]:
    """
    Best-effort GlobeNewswire search.
    """
    url = "https://www.globenewswire.com/search/keyword"
    params = {"keyword": ticker}
    r = requests_get(url, params=params, timeout=8)
    if not r:
        return []
    text = r.text
    out = []

    titles = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', text, flags=re.I | re.S)
    for href, raw in titles[:20]:
        title = clean_text(re.sub("<.*?>", " ", raw))
        if len(title) < 12:
            continue
        if strict_ticker_match(ticker, title) or ticker.upper() in title.upper():
            link = href if href.startswith("http") else f"https://www.globenewswire.com{href}"
            out.append(NewsCandidate(source="GlobeNewswire", headline=title, url=link))
    return out[:8]


# -------------------------
# SEC EDGAR
# -------------------------

def sec_company_tickers() -> Dict[str, str]:
    """
    Returns ticker -> CIK padded.
    SEC requires User-Agent. This endpoint is public.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        **HEADERS,
        "User-Agent": "leader-scanner-v43 contact@example.com",
    }
    r = requests_get(url, timeout=10, headers=headers)
    if not r:
        return {}
    try:
        data = r.json()
        out = {}
        for _, row in data.items():
            ticker = row.get("ticker", "").upper()
            cik = str(row.get("cik_str", "")).zfill(10)
            if ticker and cik:
                out[ticker] = cik
        return out
    except Exception:
        return {}


_SEC_TICKER_CACHE = {"ts": 0.0, "data": {}}

def get_cik_for_ticker(ticker: str) -> Optional[str]:
    if time.time() - _SEC_TICKER_CACHE["ts"] > 86400 or not _SEC_TICKER_CACHE["data"]:
        _SEC_TICKER_CACHE["data"] = sec_company_tickers()
        _SEC_TICKER_CACHE["ts"] = time.time()
    return _SEC_TICKER_CACHE["data"].get(ticker.upper())


def sec_recent_filings(ticker: str) -> Tuple[List[NewsCandidate], List[str]]:
    """
    Finds recent 8-K and dilution forms.
    Attempts to create SEC headline from filing form/items/exhibit.
    """
    cik = get_cik_for_ticker(ticker)
    if not cik:
        return [], []

    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {
        **HEADERS,
        "User-Agent": "leader-scanner-v43 contact@example.com",
    }
    r = requests_get(url, timeout=8, headers=headers)
    if not r:
        return [], []

    candidates: List[NewsCandidate] = []
    dilution_flags: List[str] = []

    try:
        j = r.json()
        recent = j.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        items = recent.get("items", [])

        for idx, form in enumerate(forms[:25]):
            form = str(form).upper()
            filing_date = dates[idx] if idx < len(dates) else ""
            accession = accessions[idx] if idx < len(accessions) else ""
            primary_doc = primary_docs[idx] if idx < len(primary_docs) else ""
            item_text = items[idx] if idx < len(items) else ""

            if form in ("S-1", "S-3", "F-1", "F-3", "424B5", "424B3"):
                dilution_flags.append(f"{form} filed {filing_date}")

            if form in ("8-K", "6-K"):
                headline = f"{ticker} {form} filed {filing_date}"
                if item_text:
                    headline = f"{ticker} {form}: {item_text}"

                if any(x in item_text.lower() for x in SEC_BULLISH_ITEMS) or form in ("8-K", "6-K"):
                    candidates.append(NewsCandidate(
                        source="SEC 8-K",
                        headline=headline,
                        url=sec_filing_url(cik, accession, primary_doc),
                        published_at=filing_date,
                    ))

                exhibit_title = sec_extract_exhibit_headline(cik, accession)
                if exhibit_title:
                    candidates.append(NewsCandidate(
                        source="SEC Exhibit 99.1",
                        headline=exhibit_title,
                        url=sec_filing_index_url(cik, accession),
                        published_at=filing_date,
                    ))

            # Extra dilution language scan from form names
            if form in ("8-K", "6-K") and accession:
                maybe = sec_scan_filing_for_dilution(cik, accession, primary_doc)
                dilution_flags.extend(maybe)

    except Exception as e:
        logging.info(f"[SEC ERR] {ticker}: {e}")

    return candidates, list(dict.fromkeys(dilution_flags))[:8]


def sec_filing_url(cik: str, accession: str, primary_doc: str) -> str:
    if not cik or not accession or not primary_doc:
        return ""
    cik_int = str(int(cik))
    acc_clean = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{primary_doc}"


def sec_filing_index_url(cik: str, accession: str) -> str:
    if not cik or not accession:
        return ""
    cik_int = str(int(cik))
    acc_clean = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/"


def sec_extract_exhibit_headline(cik: str, accession: str) -> Optional[str]:
    """
    Best-effort:
    - Open filing index
    - Find ex99/ex-99/exhibit 99 doc
    - Pull <title> or first strong headline-looking line
    """
    if not cik or not accession:
        return None

    index_url = sec_filing_index_url(cik, accession)
    headers = {
        **HEADERS,
        "User-Agent": "leader-scanner-v43 contact@example.com",
    }
    r = requests_get(index_url, timeout=8, headers=headers)
    if not r:
        return None

    try:
        html_text = r.text
        hrefs = re.findall(r'href="([^"]+)"', html_text, flags=re.I)
        ex_docs = [h for h in hrefs if re.search(r'(ex|exhibit).*99|ex99|ex-99', h, flags=re.I)]
        if not ex_docs:
            return None

        doc = ex_docs[0]
        doc_url = doc if doc.startswith("http") else index_url.rstrip("/") + "/" + doc.split("/")[-1]
        rr = requests_get(doc_url, timeout=8, headers=headers)
        if not rr:
            return None

        txt = rr.text

        title_m = re.search(r"<title[^>]*>(.*?)</title>", txt, flags=re.I | re.S)
        if title_m:
            title = clean_text(re.sub("<.*?>", " ", title_m.group(1)))
            if len(title) > 10 and not is_junk_headline(title):
                return title[:180]

        # Strip tags and find a headline-like line.
        plain = clean_text(re.sub("<.*?>", "\n", txt))
        lines = [clean_text(x) for x in plain.split("\n") if clean_text(x)]
        for line in lines[:80]:
            if 20 <= len(line) <= 180:
                low = line.lower()
                if any(w in low for w in STRONG_CATALYST_WORDS):
                    return line[:180]
    except Exception as e:
        logging.info(f"[SEC EXHIBIT ERR] {e}")

    return None


def sec_scan_filing_for_dilution(cik: str, accession: str, primary_doc: str) -> List[str]:
    url = sec_filing_url(cik, accession, primary_doc)
    if not url:
        return []
    headers = {
        **HEADERS,
        "User-Agent": "leader-scanner-v43 contact@example.com",
    }
    r = requests_get(url, timeout=8, headers=headers)
    if not r:
        return []

    text = clean_text(re.sub("<.*?>", " ", r.text)).lower()
    flags = []
    for w in DILUTION_WORDS:
        if w in text:
            flags.append(w)
    if flags:
        return [f"8-K/filing language: {', '.join(sorted(set(flags))[:6])}"]
    return []


def find_ranked_news(ticker: str) -> NewsResult:
    candidates: List[NewsCandidate] = []
    dilution_flags: List[str] = []

    # Highest trust first
    sec_cands, sec_dilution = sec_recent_filings(ticker)
    candidates.extend(sec_cands)
    dilution_flags.extend(sec_dilution)

    # PR sources
    candidates.extend(get_prnewswire_candidates(ticker))
    candidates.extend(get_globenewswire_candidates(ticker))

    # API/news fallbacks
    candidates.extend(get_alpaca_news_candidates(ticker))
    candidates.extend(get_finnhub_news_candidates(ticker))
    candidates.extend(get_yahoo_news_candidates(ticker))

    result = rank_news_candidates(candidates, dilution_flags=dilution_flags)

    logging.info(f"[NEWS] {ticker}: {result.type} {result.confidence} {result.headline[:90]}")
    return result


# ============================================================
# FLOAT / PROFILE
# ============================================================

def yahoo_float_estimate(ticker: str) -> Optional[float]:
    """
    Yahoo quoteSummary sometimes provides floatShares.
    Returns float in millions.
    """
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
    params = {"modules": "defaultKeyStatistics"}
    r = requests_get(url, params=params, timeout=8)
    if not r:
        return None
    try:
        result = r.json().get("quoteSummary", {}).get("result", [None])[0]
        stats = result.get("defaultKeyStatistics", {})
        fs = stats.get("floatShares", {})
        raw = fs.get("raw")
        if raw:
            return raw / 1_000_000.0
    except Exception:
        pass
    return None


# ============================================================
# ALERT LOGIC
# ============================================================

def update_ticker_memory(ticker: str, price: float):
    mem = TICKER_MEMORY.setdefault(ticker, TickerMemory())
    ts = time.time()
    mem.rolling_prices.append((ts, price))
    cutoff = ts - 60 * 20
    mem.rolling_prices = [(t, p) for t, p in mem.rolling_prices if t >= cutoff]


def fast_spike_detected(ticker: str, price: float) -> Tuple[bool, float]:
    mem = TICKER_MEMORY.get(ticker)
    if not mem or len(mem.rolling_prices) < 2:
        return False, 0.0

    cutoff = time.time() - FAST_SPIKE_LOOKBACK_MIN * 60
    recent = [(t, p) for t, p in mem.rolling_prices if t >= cutoff]
    if len(recent) < 2:
        return False, 0.0

    low = min(p for _, p in recent if p > 0)
    spike = pct_change(low, price)
    return spike >= FAST_SPIKE_PCT, spike


def should_alert(c: Candidate, sr: StructureResult, news: NewsResult, price: float) -> Tuple[bool, str]:
    """
    v43.3 alert rules:
    - No alerts under MIN_ALERT_GAIN.
    - NO SCORE FLOOR. Score must never block FAST SPIKE or FRESH HOD.
    - Below VWAP leaders are tracked/logged, not pushed to Discord.
    - Fresh HOD breakout on a +50% true leader bypasses weak catalyst/news.
    - PUSH requires VWAP + expansion.
    - Fast +10% leader spike always alerts when data is valid.
    """
    if c.gain_pct < MIN_ALERT_GAIN:
        return False, f"gain under {MIN_ALERT_GAIN:.0f}%"

    if sr.stale:
        return False, "stale candle"

    leader_override = c.rank <= 5 or c.volume >= LEADER_DAY_VOLUME or c.gain_pct >= FRESH_HOD_LEADER_GAIN
    fresh_hod_breakout = (
        leader_override
        and c.gain_pct >= FRESH_HOD_LEADER_GAIN
        and (sr.new_high_push or sr.off_hod_pct <= FRESH_HOD_NEAR_HIGH_PCT)
    )

    fast_spike, spike_pct = fast_spike_detected(c.ticker, price)

    # v43.3: Fast spike is the one exception. If it makes a valid fast +10% leader push,
    # do not let VWAP block it. This catches explosive leader re-acceleration.
    if fast_spike and leader_override and sr.off_hod_pct <= 12:
        raw_reason = f"fast +{spike_pct:.1f}% leader spike"
    else:
        # Important: below VWAP should not alert as a PUSH, but it should be logged as a tracked leader.
        if not sr.above_vwap:
            if c.gain_pct >= LEADER_TRACK_BELOW_VWAP_GAIN or leader_override:
                return False, "LEADER BELOW VWAP — WATCH RECLAIM"
            return False, "below VWAP"
        raw_reason = None

    if sr.off_hod_pct > 15 and not fast_spike:
        return False, f"{sr.off_hod_pct:.1f}% off HOD"

    if c.volume < MIN_DAY_VOLUME:
        return False, "volume too low"

    structure_ok = (
        sr.new_high_push
        or sr.last3_pct >= 2.0
        or sr.vol_ratio >= 1.8
        or sr.higher_lows
    )

    # v43.3: HOD/FAST leader breakout can bypass weak score/news. No score floor exists here.
    if raw_reason:
        pass
    elif fresh_hod_breakout:
        raw_reason = "fresh HOD breakout leader override"
    elif fast_spike:
        raw_reason = f"fast +{spike_pct:.1f}% leader spike"
    elif sr.new_high_push:
        raw_reason = "new high push"
    elif sr.last3_pct >= 2:
        raw_reason = "VWAP reclaim push"
    elif leader_override and structure_ok:
        raw_reason = "market leader continuation"
    elif not structure_ok:
        return False, "no meaningful push"
    else:
        raw_reason = "valid leader setup"

    state = ALERT_STATE.setdefault(c.ticker, AlertState())
    minutes_since = (time.time() - state.last_alert_ts) / 60 if state.last_alert_ts else 999

    meaningful_change = False

    if not state.last_alert_ts:
        meaningful_change = True
    if price >= state.last_high * 1.05 and state.last_high > 0:
        meaningful_change = True
    if c.gain_pct >= state.last_gain + 8:
        meaningful_change = True
    if fast_spike:
        meaningful_change = True
    if fresh_hod_breakout:
        meaningful_change = True
    if news.found and news.headline != state.last_title:
        meaningful_change = True

    if minutes_since < ALERT_COOLDOWN_MINUTES and not meaningful_change:
        return False, f"cooldown {minutes_since:.1f}m no meaningful change"

    if not meaningful_change:
        return False, "repeat alert suppressed"

    return True, raw_reason

def alert_title(reason: str) -> str:
    r = reason.lower()
    if "fresh hod" in r:
        return "🔥 FRESH HOD LEADER BREAKOUT"
    if "fast" in r:
        return "🔥 FAST LEADER PUSH"
    if "new high" in r:
        return "🔥 NEW HIGH LEADER PUSH"
    if "vwap" in r:
        return "🟢 VWAP RECLAIM PUSH"
    return "🔥 LEADER PUSH"


def format_volume(v: int) -> str:
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v/1_000:.0f}K"
    return str(v)


def format_alert(c: Candidate, sr: StructureResult, news: NewsResult, reason: str, candles: CandleBundle, float_m: Optional[float]) -> str:
    title = alert_title(reason)
    why = " | ".join(sr.reason[:5])
    float_txt = f"{float_m:.1f}M" if float_m else "Unknown"

    catalyst = news.headline if news.found else "UNKNOWN CATALYST — INVESTIGATE"
    source_line = f"{news.source} | Confidence {news.confidence}" if news.found else "Confidence D"

    dilution = "None found"
    if news.dilution_flags:
        dilution = "; ".join(news.dilution_flags[:3])

    return (
        f"{title} — {c.ticker}\n\n"
        f"${c.price:.2f} | +{c.gain_pct:.1f}%\n"
        f"Vol: {format_volume(c.volume)} | Float: {float_txt}\n\n"
        # No Leader Score displayed. If it alerts, trigger quality matters more than old score noise.
        f"CATALYST: {catalyst}\n"
        f"Source: {source_line}\n\n"
        f"WHY: {why}\n"
        f"Trigger: {reason}\n\n"
        f"⚠️ DILUTION RISK:\n{dilution}\n\n"
        f"Data: {candles.source} + ranked news engine + SEC check\n"
        f"{VERSION}"
    )


def send_discord(text: str):
    if not DISCORD_WEBHOOK_URL:
        logging.info("[ALERT NO WEBHOOK]\n" + text)
        return
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json={"content": text[:1900]}, timeout=8)
        logging.info(f"[DISCORD] status={r.status_code}")
    except Exception as e:
        logging.info(f"[DISCORD ERR] {e}")


def mark_alerted(c: Candidate, sr: StructureResult, price: float, title_or_headline: str):
    state = ALERT_STATE.setdefault(c.ticker, AlertState())
    state.last_alert_ts = time.time()
    state.last_price = price
    state.last_gain = c.gain_pct
    state.last_high = max(state.last_high, sr.hod, price)
    state.last_title = title_or_headline or ""
    state.alert_count += 1


# ============================================================
# SCAN CYCLE
# ============================================================

def scan_one(c: Candidate) -> Optional[str]:
    ticker = c.ticker
    update_ticker_memory(ticker, c.price)

    candles = get_candles(ticker)
    if not candles or not candles.bars:
        logging.info(f"[SKIP] {ticker}: no candles")
        return None

    last_price = candles.bars[-1]["close"]
    if last_price > 0:
        c.price = last_price

    sr = analyze_structure(ticker, candles)

    if c.gain_pct < MIN_ALERT_GAIN:
        logging.info(f"[SKIP] {ticker}: +{c.gain_pct:.1f}% under floor")
        return None

    # v43.2 speed fix:
    # Do NOT run SEC/PR/Alpaca/Yahoo news on every tracked leader.
    # First decide if the chart/leader trigger is valid using dummy unknown news.
    dummy_news = unknown_news_result()
    should, reason = should_alert(c, sr, dummy_news, c.price)

    if not should and "WATCH RECLAIM" in reason:
        logging.info(f"[TRACK ONLY] {ticker} +{c.gain_pct:.1f}% ${c.price:.2f}: {reason}")
        return None

    if not should:
        logging.info(f"[CHECK] {ticker} +{c.gain_pct:.1f}% ${c.price:.2f}: False {reason}")
        return None

    logging.info(f"[TRIGGER] {ticker} +{c.gain_pct:.1f}% ${c.price:.2f}: {reason} — pulling ranked news")

    # Only now pull ranked news/SEC.
    news = find_ranked_news(ticker)

    float_m = yahoo_float_estimate(ticker)

    msg = format_alert(c, sr, news, reason, candles, float_m)
    mark_alerted(c, sr, c.price, news.headline)
    return msg


def scan_cycle():
    last_heartbeat["last_cycle"] = now_et().isoformat()

    leaders = get_leaders()
    sent = 0

    for c in leaders:
        if sent >= MAX_ALERTS_PER_CYCLE:
            break
        try:
            msg = scan_one(c)
            if msg:
                send_discord(msg)
                sent += 1
                time.sleep(1)
        except Exception as e:
            logging.info(f"[SCAN ONE ERR] {c.ticker}: {e}\n{traceback.format_exc()}")

    logging.info(f"[CYCLE DONE] sent={sent}")


def scanner_loop():
    logging.info(f"[START] {VERSION}")
    while True:
        try:
            last_heartbeat["ts"] = now_et().isoformat()
            if scanner_active():
                scan_cycle()
            else:
                logging.info(f"[IDLE] session={market_session()}")
        except Exception as e:
            logging.info(f"[LOOP ERR] {e}\n{traceback.format_exc()}")

        time.sleep(SCAN_SECONDS)


# ============================================================
# FLASK HEALTH
# ============================================================

@app.route("/")
def home():
    return jsonify({
        "ok": True,
        "version": VERSION,
        "session": market_session(),
        "active": scanner_active(),
        "heartbeat": last_heartbeat,
        "alert_state_count": len(ALERT_STATE),
    })


@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "version": VERSION,
        "time": now_et().isoformat(),
        "note": "If Render logs do not show this version, old file is still deployed.",
    })


@app.route("/debug/state")
def debug_state():
    return jsonify({
        "version": VERSION,
        "alerts": {k: vars(v) for k, v in ALERT_STATE.items()},
        "memory": {k: len(v.rolling_prices) for k, v in TICKER_MEMORY.items()},
    })


# ============================================================
# MAIN
# ============================================================

def main():
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
