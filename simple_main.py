"""Lightweight Render-compatible momentum scanner with live news and volume checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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

SCAN_INTERVAL_SECONDS = 60
HEARTBEAT_INTERVAL_SECONDS = 10
MIN_ALERT_SCORE = 7
MIN_ALERT_GAP_SECONDS = 1800
REQUEST_TIMEOUT_SECONDS = 10
DEFAULT_PORT = 10000

FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"
FINNHUB_NEWS_URL = "https://finnhub.io/api/v1/company-news"
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

CATALYST_RULES: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("FDA", ("fda", "approval", "clearance"), "Regulatory progress can unlock fresh upside."),
    ("contract", ("contract", "award", "customer"), "Commercial validation can support continuation."),
    ("partnership", ("partnership", "partner"), "Partnership news can attract momentum buyers."),
    ("earnings", ("earnings", "guidance", "revenue"), "Financial strength can fuel follow-through."),
    ("merger", ("merger", "acquisition"), "Deal news can quickly reprice the stock."),
    ("patent", ("patent", "intellectual property"), "IP progress can strengthen the story."),
    ("AI", ("ai", "artificial intelligence"), "AI angle can pull in speculative momentum."),
)

DILUTION_KEYWORDS = (
    "offering",
    "registered direct",
    "atm",
    "shelf",
    "warrants",
    "s-1",
    "f-1",
    "securities purchase agreement",
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
    score: int
    reason: str
    catalyst: str
    risk: str
    bias: str


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
    alert_state: dict[str, datetime] = {}

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

        ranked = sorted(results, key=lambda item: (item.score, item.percent_gain, item.price), reverse=True)

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
            if not should_send_alert(result.ticker, alert_state):
                continue

            message = build_alert(result)
            delivered = send_telegram_alert(
                session=session,
                telegram_token=telegram_token,
                chat_ids=chat_ids,
                message=message,
            )
            alert_state[result.ticker] = datetime.now(timezone.utc)
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

    now_timestamp = time.time()
    quote_timestamp = safe_int(quote.get("t"))
    if quote_timestamp == 0 or now_timestamp - quote_timestamp > 15 * 60:
        print(f"[SCAN] Skipping {ticker}: stale quote", flush=True)
        return None

    price = safe_float(quote.get("c"))
    previous_close = safe_float(quote.get("pc"))
    if price <= 0 or previous_close <= 0:
        print(f"[SCAN] Skipping {ticker}: invalid live quote", flush=True)
        return None

    news_items = fetch_company_news(session=session, ticker=ticker, api_key=api_key)
    catalyst_text, catalyst_score = analyze_catalyst(news_items)
    dilution_risk, dilution_terms = detect_dilution_risk(news_items)

    print(f"[NEWS] {ticker} catalyst={catalyst_text}", flush=True)
    print(f"[RISK] {ticker} dilution={'yes' if dilution_risk else 'no'}", flush=True)

    percent_gain = ((price - previous_close) / previous_close) * 100

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

    if catalyst_score > 0:
        score += 2
        reasons.append("real catalyst")

    if dilution_risk:
        score -= 2
        reasons.append("dilution penalty")

    bias = derive_bias(score=score, dilution_risk=dilution_risk)
    risk = "Dilution risk: " + ", ".join(dilution_terms) if dilution_risk else "none"
    reason_text = ", ".join(reasons) if reasons else "watching tape"

    return ScanResult(
        ticker=ticker,
        price=price,
        previous_close=previous_close,
        percent_gain=percent_gain,
        score=max(0, min(score, 10)),
        reason=reason_text,
        catalyst=catalyst_text,
        risk=risk,
        bias=bias,
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


def fetch_company_news(session: requests.Session, ticker: str, api_key: str) -> list[dict[str, Any]]:
    today = date.today()
    from_date = today - timedelta(days=3)

    try:
        response = session.get(
            FINNHUB_NEWS_URL,
            params={
                "symbol": ticker,
                "from": from_date.isoformat(),
                "to": today.isoformat(),
                "token": api_key,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as error:
        print(f"[ERROR] {ticker}: news request failed: {error}", flush=True)
        return []
    except ValueError as error:
        print(f"[ERROR] {ticker}: news JSON decode failed: {error}", flush=True)
        return []

    return payload[:10] if isinstance(payload, list) else []


def analyze_catalyst(news_items: list[dict[str, Any]]) -> tuple[str, int]:
    if not news_items:
        return "No clear catalyst found.", 0

    combined_text = " ".join(
        f"{item.get('headline', '')} {item.get('summary', '')}" for item in news_items
    ).lower()

    for label, keywords, explanation in CATALYST_RULES:
        if any(keyword in combined_text for keyword in keywords):
            return f"{label}: {explanation}", 1

    return "No clear catalyst found.", 0


def detect_dilution_risk(news_items: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    combined_text = " ".join(
        f"{item.get('headline', '')} {item.get('summary', '')}" for item in news_items
    ).lower()
    matches = [keyword for keyword in DILUTION_KEYWORDS if keyword in combined_text]
    return bool(matches), matches


def derive_bias(score: int, dilution_risk: bool) -> str:
    if dilution_risk or score <= 4:
        return "Trap"
    if score >= 8:
        return "Runner"
    return "Caution"


def should_send_alert(ticker: str, alert_state: dict[str, datetime]) -> bool:
    last_sent_at = alert_state.get(ticker)
    if last_sent_at is None:
        return True

    seconds_since_last = max(0.0, (datetime.now(timezone.utc) - last_sent_at).total_seconds())
    return seconds_since_last >= MIN_ALERT_GAP_SECONDS


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
    return "\n".join(
        [
            f"🚨 {result.ticker}",
            f"Price: ${format_price(result.price)}",
            f"Change: {result.percent_gain:+.1f}%",
            f"Score: {result.score}/10",
            f"Catalyst: {result.catalyst}",
            f"Risk: {result.risk}",
            f"Bias: {result.bias}",
            f"Reason: {result.reason}",
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

