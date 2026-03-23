# Workflow 7.5: Engagement Tracking — Lightweight Flask Tracking Server
#
# Routes:
#   GET /health
#   GET /track/open/<tracking_id>   → log open + return 1x1 transparent GIF
#   GET /track/click/<tracking_id>?url=<encoded>  → log click + 302 redirect
#
# IMPORTANT LIMITATIONS:
# - Open tracking is APPROXIMATE, not perfectly reliable.
# - Apple Mail Privacy Protection (MPP) pre-fetches pixels → inflated opens.
# - Gmail and others may proxy image loads on behalf of recipients → inflated open counts.
# - Click tracking is significantly more reliable than open tracking.
# - Reply tracking is NOT implemented here; it belongs to Workflow 8.
#
# Run locally:
#   py -m src.workflow_7_5_engagement_tracking.tracking_server

import base64
import csv
from urllib.parse import unquote, urlparse

# 1x1 transparent GIF (35 bytes)
_PIXEL_GIF = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)

try:
    from flask import Flask, request, redirect, Response, abort
    _FLASK_AVAILABLE = True
except ImportError:
    _FLASK_AVAILABLE = False

from config.settings import SEND_LOGS_FILE
from src.workflow_7_5_engagement_tracking.engagement_logger import (
    append_engagement_event, build_event_row,
)


def _lookup_metadata(tracking_id: str) -> dict:
    """
    Enrich event with message_id, company_name, kp_email from send_logs.csv.
    Returns {} if not found or file missing.
    """
    if not SEND_LOGS_FILE.exists():
        return {}
    try:
        with open(SEND_LOGS_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("tracking_id", "") == tracking_id:
                    return {
                        "message_id":   row.get("message_id", ""),
                        "company_name": row.get("company_name", ""),
                        "kp_email":     row.get("kp_email", ""),
                    }
    except Exception:
        pass
    return {}


def _log_event(tracking_id: str, event_type: str, target_url: str = "") -> None:
    meta = _lookup_metadata(tracking_id)
    ip   = ""
    ua   = ""
    if _FLASK_AVAILABLE:
        try:
            ip = request.remote_addr or ""
            ua = request.headers.get("User-Agent", "")
        except RuntimeError:
            pass  # outside request context
    row = build_event_row(
        tracking_id  = tracking_id,
        event_type   = event_type,
        message_id   = meta.get("message_id", ""),
        company_name = meta.get("company_name", ""),
        kp_email     = meta.get("kp_email", ""),
        target_url   = target_url,
        ip           = ip,
        user_agent   = ua,
    )
    append_engagement_event(row)


if _FLASK_AVAILABLE:
    app = Flask(__name__)

    @app.route("/health")
    def health():
        return {"status": "ok"}, 200

    @app.route("/track/open/<tracking_id>")
    def track_open(tracking_id: str):
        try:
            _log_event(tracking_id, "open")
        except Exception:
            pass
        return Response(_PIXEL_GIF, mimetype="image/gif")

    @app.route("/track/click/<tracking_id>")
    def track_click(tracking_id: str):
        raw_url = request.args.get("url", "").strip()
        if not raw_url:
            abort(400)
        target_url = unquote(raw_url)
        try:
            parsed = urlparse(target_url)
            if parsed.scheme not in ("http", "https"):
                abort(400)
        except Exception:
            abort(400)
        try:
            _log_event(tracking_id, "click", target_url=target_url)
        except Exception:
            pass
        return redirect(target_url, code=302)

    def run_server(host: str = "0.0.0.0", port: int = 5000, debug: bool = False):
        app.run(host=host, port=port, debug=debug)

else:
    app = None  # type: ignore

    def run_server(host: str = "0.0.0.0", port: int = 5000, debug: bool = False):
        print("[Workflow 7.5] Flask not installed. Install with: pip install flask")


if __name__ == "__main__":
    run_server(debug=True)
