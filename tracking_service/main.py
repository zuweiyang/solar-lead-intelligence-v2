"""
OmniSol Tracking Service — Cloud Run entrypoint.

Self-contained Flask app. Does NOT import from the main pipeline.
Handles open pixel and click redirect tracking only.

Storage: appends events to /tmp/engagement_logs.csv inside the container.
  - VALIDATION USE: acceptable. Events are visible via Cloud Run logs and
    the /events debug endpoint during the container's lifetime.
  - PRODUCTION USE: NOT safe. /tmp is ephemeral — data is lost on container
    restart or when Cloud Run scales to zero. Replace with Cloud Storage or
    Firestore before production use.

Environment variables (all optional except PORT):
  PORT            — injected by Cloud Run (default 8080 locally)
  SERVICE_NAME    — label shown in /health (default "omni-tracking")
"""

import csv
import base64
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from flask import Flask, Response, abort, jsonify, redirect, request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PORT         = int(os.environ.get("PORT", "8080"))
LOG_PATH     = Path(tempfile.gettempdir()) / "engagement_logs.csv"
SERVICE_NAME = os.environ.get("SERVICE_NAME", "omni-tracking")

LOG_FIELDS = [
    "timestamp", "tracking_id", "event_type",
    "target_url", "ip", "user_agent",
]

LOG_LOCK = threading.Lock()

# 1×1 transparent GIF (35 bytes)
_PIXEL_GIF = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)

# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------

def _append_event(tracking_id: str, event_type: str, target_url: str = "") -> None:
    ip = request.remote_addr or ""
    ua = request.headers.get("User-Agent", "")
    ts = datetime.now(tz=timezone.utc).isoformat()
    row = {
        "timestamp":   ts,
        "tracking_id": tracking_id,
        "event_type":  event_type,
        "target_url":  target_url,
        "ip":          ip,
        "user_agent":  ua,
    }
    with LOG_LOCK:
        file_exists = LOG_PATH.exists()
        with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    # Always log to stdout so Cloud Run captures it in Cloud Logging
    print(f"[tracking] {event_type.upper()} tid={tracking_id} ip={ip}", flush=True)

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": SERVICE_NAME}), 200


@app.route("/track/open/<tracking_id>")
def track_open(tracking_id: str):
    try:
        _append_event(tracking_id, "open")
    except Exception as exc:
        print(f"[tracking] open log error: {exc}", flush=True)
    return Response(_PIXEL_GIF, mimetype="image/gif",
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


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
        _append_event(tracking_id, "click", target_url=target_url)
    except Exception as exc:
        print(f"[tracking] click log error: {exc}", flush=True)
    return redirect(target_url, code=302)


@app.route("/events")
def list_events():
    """Debug endpoint — returns all logged events as JSON."""
    if not LOG_PATH.exists():
        return jsonify([]), 200
    with LOG_LOCK:
        with open(LOG_PATH, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    return jsonify(rows), 200


# ---------------------------------------------------------------------------
# Entrypoint (gunicorn picks up `app`; this block is for local dev only)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
