"""
Control-panel heartbeat helpers for the local queue runner.

The Streamlit control panel is the operator's only interaction surface.
When it disappears, the background runner should pause instead of continuing
to process queue jobs unattended.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

_ROOT_DIR = Path(__file__).resolve().parent.parent.parent
_DATA_DIR = _ROOT_DIR / "data"
_HEARTBEAT_PATH_OVERRIDE = os.getenv("CONTROL_PANEL_HEARTBEAT_FILE", "").strip()
_CONTROL_PANEL_HEARTBEAT_FILE = (
    Path(_HEARTBEAT_PATH_OVERRIDE)
    if _HEARTBEAT_PATH_OVERRIDE
    else _DATA_DIR / "control_panel_heartbeat.json"
)
_CONTROL_PANEL_HEARTBEAT_TIMEOUT_SECONDS = float(
    os.getenv("CONTROL_PANEL_HEARTBEAT_TIMEOUT_SECONDS", "20")
)
_CONTROL_PANEL_HEARTBEAT_INTERVAL_SECONDS = float(
    os.getenv("CONTROL_PANEL_HEARTBEAT_INTERVAL_SECONDS", "5")
)
_HEARTBEAT_THREAD: threading.Thread | None = None
_HEARTBEAT_STOP = threading.Event()


def _now_ts() -> float:
    return time.time()


def write_control_panel_heartbeat(source: str = "streamlit") -> None:
    """Persist a best-effort liveness heartbeat for the operator UI."""
    payload = {
        "source": source,
        "timestamp": _now_ts(),
        "pid": os.getpid(),
    }
    path = _CONTROL_PANEL_HEARTBEAT_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_control_panel_heartbeat() -> dict:
    """Load the last control-panel heartbeat payload, or {} if missing/corrupt."""
    path = _CONTROL_PANEL_HEARTBEAT_FILE
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def is_control_panel_heartbeat_stale(
    timeout_seconds: float | None = None,
) -> bool:
    """
    Return True when the Streamlit UI heartbeat is missing or too old.

    The timeout defaults to CONTROL_PANEL_HEARTBEAT_TIMEOUT_SECONDS.
    """
    timeout = float(timeout_seconds or _CONTROL_PANEL_HEARTBEAT_TIMEOUT_SECONDS)
    payload = read_control_panel_heartbeat()
    ts = payload.get("timestamp")
    try:
        last_seen = float(ts)
    except (TypeError, ValueError):
        return True
    return (_now_ts() - last_seen) > max(timeout, 1.0)


def get_control_panel_heartbeat_age_seconds() -> float | None:
    """Return the age of the most recent heartbeat, or None when unavailable."""
    payload = read_control_panel_heartbeat()
    ts = payload.get("timestamp")
    try:
        last_seen = float(ts)
    except (TypeError, ValueError):
        return None
    return max(_now_ts() - last_seen, 0.0)


def _heartbeat_loop(source: str) -> None:
    """Background writer bound to the lifetime of the Streamlit process."""
    interval = max(_CONTROL_PANEL_HEARTBEAT_INTERVAL_SECONDS, 1.0)
    while not _HEARTBEAT_STOP.wait(interval):
        write_control_panel_heartbeat(source)


def start_control_panel_heartbeat_thread(source: str = "streamlit_process") -> None:
    """
    Start a singleton background heartbeat writer for the Streamlit process.

    This makes liveness depend on the control-panel process staying alive,
    rather than on frequent page rerenders, which can be throttled by the
    browser or paused during long-running UI work.
    """
    global _HEARTBEAT_THREAD
    if _HEARTBEAT_THREAD and _HEARTBEAT_THREAD.is_alive():
        return
    _HEARTBEAT_STOP.clear()
    write_control_panel_heartbeat(source)
    _HEARTBEAT_THREAD = threading.Thread(
        target=_heartbeat_loop,
        args=(source,),
        name="control-panel-heartbeat",
        daemon=True,
    )
    _HEARTBEAT_THREAD.start()
