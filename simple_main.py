"""Render-compatible quote-only momentum scanner."""

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
except ImportError:  # pragma: no cover - optional locally
    Flask = None
    jsonify = None

BUILD_TAG = "quote-only rebuild 2026-04-24 v3"

SCAN_INTERVAL_SECONDS = 60
HEARTBEAT_INTERVAL_SECONDS = 10
MIN_ALERT_SCORE = 7
MIN_ALERT_GAP_SECONDS = 1800
STALE_QUOTE_SECONDS = 15 * 60
REQUEST_TIMEOUT_SECONDS = 10
DEFAULT_PORT = 10000

FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"
FINNHUB_NEWS_URL = "https://finnhub.io/api/v1/company-news"
TELEGRAM_API_TEMPLATE = "https://api.telegram.org/bot{token}/sendMessage"

CATALYST_RULES: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("fda", ("fda", "approval", "clearance"), "Regulatory progress can support upside."),
    ("contract", ("contract", "award", "customer"), "Commercial validation can attract buyers."),
    ("partnership", ("partnership", "partner"), "Partnership news can drive momentum."),
    ("earnings", ("earnings", "guidance", "revenue"), "Strong results can fuel continuation."),
    ("merger", ("merger", "acquisition"), "Deal activity can reprice the stock."),
    ("patent", ("patent", "intellectual property"), "IP progress can strengthen the story."),
    ("ai", ("ai", "artificial intelligence"), "AI angle can pull in speculative flows."),
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
    "build": BUILD_TAG,
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
        print(f"[WEB] {BUILD_TAG} listening on port {port}", flush=True)
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
    print("[BOOT] sending TELEGRAM TEST...")

test_sent = send_telegram("✅ TEST ALERT FROM RENDER")

if test_sent:
    print("[BOOT] Telegram SUCCESS")
else:
    print("[BOOT] Telegram FAILED")

print("[BOOT] starting scanner")
def run_scanner():
    print(f"[BOOT] Scanner started | {BOOT_MARKER}", flush=True)
    print(f"[BOOT] Watchlist: {', '.join(WATCHLIST)}", flush=True)

  if not FINNHUB_API_KEY:
        print("[BOOT] Missing FINNHUB_API_KEY. Scanner cannot start.", flush=True)
        return

