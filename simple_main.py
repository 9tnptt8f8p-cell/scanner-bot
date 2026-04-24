"""Lightweight Render-compatible momentum scanner with immediate logging."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import sys
import threading
import time
from typing import Any

import requests

try:
    from flask import Flask, jsonify
except ImportError:  # pragma: no cover - optional on local machines
    Flask = None
    jsonify = None

SCAN_INTERVAL_SECONDS = 10
HEARTBEAT_INTERVAL_SECONDS = 10
MIN_ALERT_SCORE = 7
MEANINGFUL_PRICE_CHANGE_PCT = 2.0
MIN_ALERT_GAP_SECONDS = 1800
REQUEST_TIMEOUT_SECONDS = 10
DEFAULT_PORT = 10000
AVERAGE_VOLUME_DAYS = 10

FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"
FINNHUB_CANDLE_URL = "https://finnhub.io/api/v1/stock/candle"
TELEGRAM_API_TEMPLATE = "https://api.telegram.org/bot{token}/sendMessage"

WATCHLIST = (
    "AKAN",
    "AUUD",
    "SOUN",
    "RGTI",
    "PLTR",
    "SKLZ",
    "CPXI",
    "EUDA",
    "PAPL",
    "MITI",
)

app = Flask(__name__) if Flask is not None else None
STATUS_LOCK = threading.Lock()
STATUS: dict[str, Any] = {
    "scanner_running": False,
    "last_cycle_time": None,
    "last_alert_time": None,
    "api_key_loaded": False,
    "telegram_loaded": False,
    "last_warning": "",
}


@dataclass(frozen=True)
class ScanResult:
    ticker: str
    price: float
    previous_close: float
    percent_gain: float
    volume: int
    average_volume: int
    relative_volume: float
    score: int
    reasons: tuple[str, ...]
    price_source: str


if app is not None:

    @app.get("/")
    def healthcheck() -> Any:
        return jsonify(get_status_snapshot())


def run_web() -> None:
    port = safe_int(os.getenv("PORT", DEFAULT_PORT)) or DEFAULT_PORT

    if app is not None:
        print(f"[WEB] Flask health server listening on port {port}", flush=True)
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
        return

    print(f"[WEB] basic health server listening on port {port}", flush=True)
    serve_basic_healthcheck(port)


def serve_basic_healthcheck(port: int) -> None:
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/":
                body = json.dumps(get_status_snapshot()).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(404)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    with ThreadingHTTPServer(("0.0.0.0", port), HealthHandler) as server:
        server.serve_forever()


def heartbeat_loop() -> None:
    while True:
        print("[HEARTBEAT] alive", flush=True)
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


def scanner_loop() -> None:
    print("[BOOT] Scanner started", flush=True)
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    run_scanner()


def run_scanner() -> None:
    finnhub_api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    telegram_token = (
        os.getenv("TELEGRAM_TOKEN", "").strip()
        or os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        or os.getenv("BOT_TOKEN", "").strip()
    )
    chat_ids = parse_chat_ids(
        os.getenv("TELEGRAM_CHAT_IDS")
        or os.getenv("CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
    )

    session = requests.Session()
    alert_state: dict[str, dict[str, Any]] = {}

    while True:
        update_status(
            scanner_running=True,
            last_cycle_time=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            api_key_loaded=bool(finnhub_api_key),
            telegram_loaded=bool(telegram_token and chat_ids),
            last_warning="",
        )

        print("[SCAN] Cycle started", flush=True)

        if not finnhub_api_key:
            warning = "FINNHUB_API_KEY is missing. Scanner is idle but web health is up."
            update_status(last_warning=warning)
            print(f"[WARN] {warning}", flush=True)
            print("[SCAN] Cycle complete", flush=True)
            time.sleep(SCAN_INTERVAL_SECONDS)
            continue

        results: list[ScanResult] = []
        for ticker in WATCHLIST:
            result = scan_ticker(session=session, ticker=ticker, api_key=finnhub_api_key)
            if result is not None:
                results.append(result)

        ranked = sorted(results, key=lambda item: (item.score, item.percent_gain, item.relative_volume), reverse=True)

        if ranked:
            top_lines = " | ".join(
                f"{item.ticker} {item.score}/10 ${format_price(item.price)}"
                for item in ranked[:5]
            )
            print(f"[SCAN] Top ranked: {top_lines}", flush=True)
        else:
            print("[SCAN] Top ranked: none", flush=True)

        for result in ranked:
            if result.score < MIN_ALERT_SCORE:
                continue
            if not should_send_alert(result=result, alert_state=alert_state):
                continue

            message = build_alert(result)
            delivered = send_telegram_alert(
                session=session,
                telegram_token=telegram_token,
                chat_ids=chat_ids,
                message=message,
            )
            alert_state[result.ticker] = {
                "price": result.price,
                "score": result.score,
                "sent_at": datetime.now(timezone.utc),
            }
            update_status(last_alert_time=datetime.now(timezone.utc).isoformat(timespec="seconds"))

            if delivered:
                print(f"[ALERT] Sent: {result.ticker}", flush=True)
            else:
                print(f"[ALERT] Logged only: {result.ticker}", flush=True)
                print(message, flush=True)

        print("[SCAN] Cycle complete", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)


def scan_ticker(session: requests.Session, ticker: str, api_key: str) -> ScanResult | None:
    quote = fetch_quote(session=session, ticker=ticker, api_key=api_key)
    if quote is None:
        return None

    price = safe_float(quote.get("c"))
    previous_close = safe_float(quote.get("pc"))
    volume = safe_int(quote.get("v"))

    if price <= 0 or previous_close <= 0 or volume <= 0:
        print(f"[SCAN] Skipping {ticker}: invalid live quote", flush=True)
        return None

    average_volume = fetch_average_volume(session=session, ticker=ticker, api_key=api_key)
    relative_volume = round(volume / average_volume, 2) if average_volume > 0 else 0.0
    percent_gain = ((price - previous_close) / previous_close) * 100
    score, reasons = score_ticker(
        price=price,
        percent_gain=percent_gain,
        volume=volume,
        relative_volume=relative_volume,
    )

    return ScanResult(
        ticker=ticker,
        price=price,
        previous_close=previous_close,
        percent_gain=percent_gain,
        volume=volume,
        average_volume=average_volume,
        relative_volume=relative_volume,
        score=score,
        reasons=tuple(reasons),
        price_source="Finnhub quote c",
    )


def fetch_quote(session: requests.Session, ticker: str, api_key: str) -> dict[str, Any] | None:
    try:
        response = session.get(
            FINNHUB_QUOTE_URL,
            params={"symbol": ticker, "token": api_key},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as error:
        print(f"[ERROR] {ticker}: quote request failed: {error}", flush=True)
        return None
    except ValueError as error:
        print(f"[ERROR] {ticker}: quote JSON decode failed: {error}", flush=True)
        return None

    if not isinstance(payload, dict):
        return None
    return payload


def fetch_average_volume(session: requests.Session, ticker: str, api_key: str) -> int:
    now = datetime.now(timezone.utc)
    from_timestamp = int((now - timedelta(days=AVERAGE_VOLUME_DAYS + 5)).timestamp())
    to_timestamp = int(now.timestamp())

    try:
        response = session.get(
            FINNHUB_CANDLE_URL,
            params={
                "symbol": ticker,
                "resolution": "D",
                "from": from_timestamp,
                "to": to_timestamp,
                "token": api_key,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as error:
        print(f"[ERROR] {ticker}: candle request failed: {error}", flush=True)
        return 0
    except ValueError as error:
        print(f"[ERROR] {ticker}: candle JSON decode failed: {error}", flush=True)
        return 0

    volumes = payload.get("v", []) if isinstance(payload, dict) else []
    if not isinstance(volumes, list) or not volumes:
        return 0

    cleaned = [safe_int(item) for item in volumes if safe_int(item) > 0]
    if not cleaned:
        return 0

    recent = cleaned[-AVERAGE_VOLUME_DAYS:]
    return max(1, int(sum(recent) / len(recent)))


def score_ticker(price: float, percent_gain: float, volume: int, relative_volume: float) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    if percent_gain >= 10:
        score += 3
        reasons.append("10%+ breakout")
    elif percent_gain >= 5:
        score += 2
        reasons.append("5%+ gain")

    score += 2
    reasons.append("valid live price")

    if 0 < price < 20:
        score += 1
        reasons.append("under $20")

    if relative_volume >= 2:
        reasons.append(f"{relative_volume:.1f}x volume surge")
    elif volume >= 500_000:
        reasons.append(f"volume {volume:,}")

    return min(score, 10), reasons


def should_send_alert(result: ScanResult, alert_state: dict[str, dict[str, Any]]) -> bool:
    last = alert_state.get(result.ticker)
    if last is None:
        return True

    last_price = safe_float(last.get("price"))
    last_sent_at = last.get("sent_at")
    if last_price <= 0 or not isinstance(last_sent_at, datetime):
        return True

    seconds_since_last = max(0.0, (datetime.now(timezone.utc) - last_sent_at).total_seconds())
    if seconds_since_last < MIN_ALERT_GAP_SECONDS:
        return False

    return True


def send_telegram_alert(
    session: requests.Session,
    telegram_token: str,
    chat_ids: tuple[str, ...],
    message: str,
) -> bool:
    if not telegram_token or not chat_ids:
        print("[WARN] Telegram credentials missing. Alert will be logged only.", flush=True)
        return False

    delivered = False
    url = TELEGRAM_API_TEMPLATE.format(token=telegram_token)

    for chat_id in chat_ids:
        try:
            response = session.post(
                url,
                data={"chat_id": chat_id, "text": message},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            delivered = True
        except requests.RequestException as error:
            print(f"[ERROR] Telegram send failed for chat {chat_id}: {error}", flush=True)

    return delivered


def build_alert(result: ScanResult) -> str:
    reasons_text = ", ".join(result.reasons) if result.reasons else "watching tape"
    return "\n".join(
        [
            "🚨 MOMENTUM ALERT",
            "",
            result.ticker,
            f"Price: ${format_price(result.price)}",
            f"Move: {result.percent_gain:+.1f}%",
            f"Score: {result.score}/10",
            f"Reason: {reasons_text}",
            "Risk: Check SEC filings before entry.",
            "Bias: Watch VWAP hold / continuation.",
        ]
    )


def parse_chat_ids(raw_value: str | None) -> tuple[str, ...]:
    if not raw_value:
        return ()

    chat_ids: list[str] = []
    for part in raw_value.split(","):
        chat_id = part.strip()
        if chat_id and chat_id not in chat_ids:
            chat_ids.append(chat_id)
    return tuple(chat_ids)


def update_status(**updates: Any) -> None:
    with STATUS_LOCK:
        STATUS.update(updates)


def get_status_snapshot() -> dict[str, Any]:
    with STATUS_LOCK:
        return dict(STATUS)


def format_price(price: float) -> str:
    if price < 1:
        return f"{price:.4f}"
    return f"{price:.2f}"


def safe_float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value: object) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    threading.Thread(target=run_web, daemon=True).start()
    scanner_loop()

