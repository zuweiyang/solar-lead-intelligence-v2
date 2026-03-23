"""
Workflow 9 — Campaign Runner: State Management

Persists run progress to data/campaign_run_state.json so campaigns can
resume after interruption.
"""
from __future__ import annotations

import json
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import CAMPAIGN_RUN_STATE_FILE, RUNS_DIR
from src.workflow_9_campaign_runner.campaign_config import CampaignConfig

# Valid status values
STATUS_INITIALIZED = "initialized"
STATUS_RUNNING     = "running"
STATUS_COMPLETED   = "completed"
STATUS_FAILED      = "failed"
STATUS_PAUSED      = "paused"

# Valid cloud deploy status values
CLOUD_DEPLOY_NOT_ENABLED = "not_enabled"
CLOUD_DEPLOY_PENDING = "pending"
CLOUD_DEPLOY_STARTED = "started"
CLOUD_DEPLOY_COMPLETED = "completed"
CLOUD_DEPLOY_FAILED = "failed"

# Valid cloud send status values
CLOUD_SEND_NOT_QUEUED = "not_queued"
CLOUD_SEND_QUEUED = "queued"
CLOUD_SEND_SYNCED = "synced"
CLOUD_SEND_WAITING_WINDOW = "waiting_window"
CLOUD_SEND_SENDING = "sending"
CLOUD_SEND_COMPLETED = "completed"
CLOUD_SEND_FAILED = "failed"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _slugify(value: str) -> str:
    """
    Convert a human-readable location label into a filesystem-friendly slug.

    Examples:
      "Los Angeles" -> "los-angeles"
      "São Paulo"   -> "sao-paulo"
    """
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text.lower()).strip("-")
    return slug


def build_campaign_id(config: CampaignConfig, now: datetime | None = None) -> str:
    """
    Build a readable, unique campaign id for run directories.

    Format:
      <base-city>_<YYYYMMDD_HHMMSS>_<rand4>

    Example:
      miami_20260321_210315_a1b2
    """
    location_source = (
        config.base_city
        or config.city
        or config.region
        or config.country
        or "run"
    )
    location_slug = _slugify(location_source)[:40] or "run"
    dt = now or datetime.now()
    timestamp = dt.strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:4]
    return f"{location_slug}_{timestamp}_{suffix}"


def _config_to_dict(config: CampaignConfig) -> dict:
    """Serialize CampaignConfig to a plain dict for JSON storage."""
    return {
        "country":          config.country,
        "region":           config.region,
        "city":             config.city,
        "base_city":        config.base_city,
        "metro_mode":       config.metro_mode,
        "metro_sub_cities": list(config.metro_sub_cities),
        "search_cities":    list(config.search_cities),
        "keyword_mode":     config.keyword_mode,
        "keywords":         config.keywords,
        "company_limit":    config.company_limit,
        "crawl_limit":      config.crawl_limit,
        "enrich_limit":     config.enrich_limit,
        "send_mode":        config.send_mode,
        "run_until":        config.run_until,
        "resume":           config.resume,
        "dry_run":          config.dry_run,
    }


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _run_state_path(campaign_id: str) -> Path:
    return RUNS_DIR / campaign_id / CAMPAIGN_RUN_STATE_FILE.name


def get_cloud_deploy_status_path(campaign_id: str) -> Path:
    return RUNS_DIR / campaign_id / "cloud_deploy_status.json"


def get_cloud_send_status_path(campaign_id: str) -> Path:
    return RUNS_DIR / campaign_id / "cloud_send_status.json"


def _build_cloud_deploy_state(
    campaign_id: str,
    status: str,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "campaign_id": campaign_id,
        "cloud_deploy_status": status,
        "cloud_deploy_updated_at": _now(),
        "cloud_deploy_error": error_message,
    }
    if details:
        payload.update(details)
    return payload


def _build_cloud_send_state(
    campaign_id: str,
    status: str,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "campaign_id": campaign_id,
        "cloud_send_status": status,
        "cloud_send_updated_at": _now(),
        "cloud_send_error": error_message,
    }
    if details:
        payload.update(details)
    return payload


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def initialize_campaign_state(
    config: CampaignConfig,
    path: Path = CAMPAIGN_RUN_STATE_FILE,
) -> dict:
    """
    Create a fresh campaign state, write it to disk, and return it.
    """
    state: dict[str, Any] = {
        "campaign_id":         build_campaign_id(config),
        "started_at":          _now(),
        "updated_at":          _now(),
        "last_completed_step": None,
        "status":              STATUS_INITIALIZED,
        "cloud_deploy_status": CLOUD_DEPLOY_NOT_ENABLED,
        "cloud_deploy_updated_at": _now(),
        "cloud_deploy_error":  None,
        "cloud_send_status":   CLOUD_SEND_NOT_QUEUED,
        "cloud_send_updated_at": _now(),
        "cloud_send_error":    None,
        "config":              _config_to_dict(config),
        "error_message":       None,
    }
    save_campaign_state(state, path)
    save_cloud_deploy_status(state["campaign_id"], CLOUD_DEPLOY_NOT_ENABLED)
    save_cloud_send_status(state["campaign_id"], CLOUD_SEND_NOT_QUEUED)
    return state


def load_campaign_state(path: Path = CAMPAIGN_RUN_STATE_FILE) -> dict | None:
    """
    Load the persisted campaign state. Returns None if no state file exists.
    """
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_campaign_state(state: dict, path: Path = CAMPAIGN_RUN_STATE_FILE) -> None:
    """Write campaign state to disk."""
    _write_json(path, state)
    if path == CAMPAIGN_RUN_STATE_FILE:
        campaign_id = str(state.get("campaign_id") or "").strip()
        if campaign_id:
            _write_json(_run_state_path(campaign_id), state)


def update_campaign_state(
    step_name: str,
    status: str,
    error_message: str | None = None,
    path: Path = CAMPAIGN_RUN_STATE_FILE,
) -> dict:
    """
    Load state, update it for the given step outcome, save, and return the updated state.

    - On step completion  : set last_completed_step, status = running
    - On failure          : set status = failed, error_message
    - On full completion  : status = completed
    """
    state = load_campaign_state(path) or {}
    state["updated_at"] = _now()
    state["error_message"] = error_message

    if status == "completed":
        state["last_completed_step"] = step_name
        # If this is the final step requested, mark whole run completed
        run_until = state.get("config", {}).get("run_until", "campaign_status")
        if step_name == run_until:
            state["status"] = STATUS_COMPLETED
        else:
            state["status"] = STATUS_RUNNING
    elif status == "failed":
        state["status"] = STATUS_FAILED
    elif status == "paused":
        state["status"] = STATUS_PAUSED

    save_campaign_state(state, path)
    return state


def save_cloud_deploy_status(
    campaign_id: str,
    status: str,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist cloud deploy status in the run directory for a specific campaign."""
    payload = _build_cloud_deploy_state(campaign_id, status, error_message=error_message, details=details)
    _write_json(get_cloud_deploy_status_path(campaign_id), payload)
    return payload


def load_cloud_deploy_status(campaign_id: str) -> dict[str, Any]:
    """Load the per-run cloud deploy status file, or return {} if missing."""
    path = get_cloud_deploy_status_path(campaign_id)
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_cloud_send_status(
    campaign_id: str,
    status: str,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist cloud send worker status in the run directory for a specific campaign."""
    payload = _build_cloud_send_state(campaign_id, status, error_message=error_message, details=details)
    _write_json(get_cloud_send_status_path(campaign_id), payload)
    return payload


def load_cloud_send_status(campaign_id: str) -> dict[str, Any]:
    """Load the per-run cloud send status file, or return {} if missing."""
    path = get_cloud_send_status_path(campaign_id)
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def sync_cloud_deploy_status(
    campaign_id: str,
    status: str,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
    path: Path = CAMPAIGN_RUN_STATE_FILE,
) -> dict[str, Any]:
    """
    Persist cloud deploy status to the run-scoped file and mirror it into the
    current global campaign state when it refers to the same campaign.
    """
    payload = save_cloud_deploy_status(
        campaign_id,
        status,
        error_message=error_message,
        details=details,
    )
    state = load_campaign_state(path)
    if state and str(state.get("campaign_id") or "").strip() == campaign_id:
        state["cloud_deploy_status"] = payload["cloud_deploy_status"]
        state["cloud_deploy_updated_at"] = payload["cloud_deploy_updated_at"]
        state["cloud_deploy_error"] = payload["cloud_deploy_error"]
        for key, value in payload.items():
            if key.startswith("cloud_deploy_") or key in {"campaign_id"}:
                continue
            state[key] = value
        save_campaign_state(state, path)
    return payload


def sync_cloud_send_status(
    campaign_id: str,
    status: str,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
    path: Path = CAMPAIGN_RUN_STATE_FILE,
) -> dict[str, Any]:
    """
    Persist cloud send worker status to the run-scoped file and mirror it into
    the current global campaign state when it refers to the same campaign.
    """
    payload = save_cloud_send_status(
        campaign_id,
        status,
        error_message=error_message,
        details=details,
    )
    state = load_campaign_state(path)
    if state and str(state.get("campaign_id") or "").strip() == campaign_id:
        state["cloud_send_status"] = payload["cloud_send_status"]
        state["cloud_send_updated_at"] = payload["cloud_send_updated_at"]
        state["cloud_send_error"] = payload["cloud_send_error"]
        for key, value in payload.items():
            if key.startswith("cloud_send_") or key in {"campaign_id"}:
                continue
            state[key] = value
        save_campaign_state(state, path)
    return payload
