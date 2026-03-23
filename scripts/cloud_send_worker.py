"""
Google Cloud VM worker for market-local auto-send.

This worker:
  1. polls GCS for run manifests
  2. downloads any new run folder into the local workspace
  3. waits until target-market local send time
  4. runs Gmail API send + campaign status refresh
  5. uploads updated run outputs and marks the manifest processed
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.run_context import clear_active_run, set_active_run
from config.settings import (
    CLOUD_WORKER_ALERT_WEBHOOK,
    CLOUD_WORKER_POLL_SECONDS,
    DATA_DIR,
    GCS_BUCKET,
    GCS_FAILED_PREFIX,
    GCS_MANIFESTS_PREFIX,
    GCS_PROCESSED_PREFIX,
    GCS_RUNS_PREFIX,
    RUNS_DIR,
)
from src.workflow_9_campaign_runner.campaign_state import (
    CLOUD_SEND_COMPLETED,
    CLOUD_SEND_FAILED,
    CLOUD_SEND_SENDING,
    CLOUD_SEND_SYNCED,
    CLOUD_SEND_WAITING_WINDOW,
    load_cloud_send_status,
    sync_cloud_send_status,
)
from scripts.auto_send_runs import _campaign_next_due, _run_campaign_send

STATE_FILE = DATA_DIR / "cloud_send_worker_state.json"
ALERTS_FILE = DATA_DIR / "cloud_worker_alerts.jsonl"


def _resolve_gcloud_bin() -> str:
    configured = os.getenv("GCLOUD_BIN", "").strip()
    candidates = [configured, "gcloud.cmd", "gcloud.exe", "gcloud"]
    for candidate in candidates:
        if not candidate:
            continue
        resolved = shutil.which(candidate) or (candidate if Path(candidate).exists() else "")
        if resolved:
            return resolved
    raise RuntimeError(
        "Could not find gcloud executable. Set GCLOUD_BIN or add gcloud.cmd to PATH."
    )


GCLOUD_BIN = _resolve_gcloud_bin()


def _now_utc() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _bucket_uri(*parts: str) -> str:
    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET is not configured in .env / settings")
    clean = [part.strip("/").replace("\\", "/") for part in parts if part]
    suffix = "/".join(clean)
    return f"gs://{GCS_BUCKET}/{suffix}" if suffix else f"gs://{GCS_BUCKET}"


def _run_cmd(args: list[str], capture_output: bool = False) -> subprocess.CompletedProcess[str]:
    cmd = [GCLOUD_BIN, *args]
    print(f"[CloudWorker] RUN: {' '.join(cmd)}")
    return subprocess.run(
        cmd,
        check=True,
        text=True,
        capture_output=capture_output,
    )


def _directory_upload_stats(local_dir: Path) -> dict[str, int]:
    file_count = 0
    total_bytes = 0
    for path in local_dir.rglob("*"):
        if not path.is_file():
            continue
        file_count += 1
        try:
            total_bytes += path.stat().st_size
        except OSError:
            pass
    return {
        "file_count": file_count,
        "total_bytes": total_bytes,
    }


def _upload_directory(local_dir: Path, remote_dir_uri: str) -> dict[str, float | int]:
    stats = _directory_upload_stats(local_dir)
    started = time.perf_counter()
    _run_cmd(["storage", "cp", "--recursive", f"{local_dir}{os.sep}*", remote_dir_uri.rstrip("/") + "/"])
    elapsed = round(time.perf_counter() - started, 2)
    print(
        f"[CloudWorker] Uploaded directory in one recursive pass: "
        f"{stats['file_count']} files, {stats['total_bytes']} bytes, {elapsed}s"
    )
    return {
        "file_count": stats["file_count"],
        "total_bytes": stats["total_bytes"],
        "elapsed_seconds": elapsed,
    }


def _upload_file(local_path: Path, remote_uri: str) -> None:
    _run_cmd(["storage", "cp", str(local_path), remote_uri])


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {"completed_campaigns": [], "failed_campaigns": [], "synced_manifests": {}}
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)
            state.setdefault("completed_campaigns", [])
            state.setdefault("failed_campaigns", [])
            state.setdefault("synced_manifests", {})
            state.setdefault("last_poll_at", "")
            state.setdefault("last_success_at", "")
            state.setdefault("last_error_at", "")
            state.setdefault("active_campaign_id", "")
            state.setdefault("last_idle_reason", "")
            state.setdefault("last_manifest_count", 0)
            state.setdefault("last_wait_campaign_id", "")
            state.setdefault("last_wait_due_at", "")
            state.setdefault("last_completed_campaign_id", "")
            state.setdefault("last_failed_campaign_id", "")
            state.setdefault("last_processed_manifest_uri", "")
            return state
    except Exception:
        return {"completed_campaigns": [], "failed_campaigns": [], "synced_manifests": {}}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def _post_alert(payload: dict) -> None:
    if not CLOUD_WORKER_ALERT_WEBHOOK:
        return
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        CLOUD_WORKER_ALERT_WEBHOOK,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
        response.read()


def _record_alert(
    *,
    level: str,
    event_type: str,
    message: str,
    campaign_id: str = "",
    manifest_uri: str = "",
    details: dict | None = None,
) -> None:
    payload = {
        "timestamp": _now_utc(),
        "level": level,
        "event_type": event_type,
        "message": message,
        "campaign_id": campaign_id,
        "manifest_uri": manifest_uri,
        "details": details or {},
    }
    ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    try:
        _post_alert(payload)
    except Exception as exc:
        print(f"[CloudWorker] Alert delivery failed: {exc}")


def _update_worker_state(state: dict, **updates: object) -> None:
    state.update(updates)
    _save_state(state)


def _reconcile_manifest_with_run_state(
    campaign_id: str,
    manifest_uri: str,
    state: dict,
    completed: set[str],
    failed: set[str],
) -> bool:
    """
    Return True when the manifest has been fully handled and should not continue
    through normal candidate preparation.
    """
    run_state = load_cloud_send_status(campaign_id)
    send_state = str(run_state.get("cloud_send_status") or "").strip().lower()

    if send_state == CLOUD_SEND_COMPLETED:
        print(f"[CloudWorker] Campaign already marked completed in run state, reconciling manifest: {campaign_id}")
        processed_uri = _mark_manifest_processed(manifest_uri, campaign_id)
        completed.add(campaign_id)
        sync_cloud_send_status(
            campaign_id,
            CLOUD_SEND_COMPLETED,
            details={
                "cloud_send_manifest_uri": manifest_uri,
                "cloud_send_processed_manifest_uri": processed_uri,
                "cloud_send_reconciled_at": _now_utc(),
            },
        )
        _update_worker_state(
            state,
            completed_campaigns=sorted(completed),
            last_success_at=_now_utc(),
            last_completed_campaign_id=campaign_id,
            last_processed_manifest_uri=processed_uri,
            last_idle_reason="reconciled_completed_manifest",
        )
        return True

    if send_state == CLOUD_SEND_FAILED:
        print(f"[CloudWorker] Campaign already marked failed in run state, skipping manifest: {campaign_id}")
        failed_uri = _mark_manifest_failed(manifest_uri, campaign_id)
        failed.add(campaign_id)
        sync_cloud_send_status(
            campaign_id,
            CLOUD_SEND_FAILED,
            error_message=run_state.get("cloud_send_error"),
            details={
                "cloud_send_manifest_uri": manifest_uri,
                "cloud_send_failed_manifest_uri": failed_uri,
                "cloud_send_reconciled_at": _now_utc(),
            },
        )
        _update_worker_state(
            state,
            failed_campaigns=sorted(failed),
            last_failed_campaign_id=campaign_id,
            last_idle_reason="awaiting_manual_recovery",
        )
        return True

    return False


def _list_manifest_uris() -> list[str]:
    prefix = _bucket_uri(GCS_MANIFESTS_PREFIX)
    try:
        result = _run_cmd(["storage", "ls", "--recursive", prefix], capture_output=True)
    except subprocess.CalledProcessError:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip().endswith(".json")]


def _download_manifest(manifest_uri: str) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        local_path = Path(tmp) / "manifest.json"
        _run_cmd(["storage", "cp", manifest_uri, str(local_path)])
        with open(local_path, encoding="utf-8") as f:
            return json.load(f)


def _sync_run_to_local(campaign_id: str) -> Path:
    target_dir = RUNS_DIR / campaign_id
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    run_uri = _bucket_uri(GCS_RUNS_PREFIX, campaign_id)
    _run_cmd(["storage", "cp", "--recursive", f"{run_uri}/**", str(target_dir)])
    return target_dir


def _manifest_sync_key(manifest_uri: str, manifest: dict) -> str:
    uploaded_at = (manifest.get("uploaded_at") or "").strip()
    return f"{manifest_uri}|{uploaded_at}"


def _ensure_run_synced(
    campaign_id: str,
    manifest_uri: str,
    manifest: dict,
    state: dict,
) -> tuple[Path, bool]:
    sync_key = _manifest_sync_key(manifest_uri, manifest)
    synced = state.setdefault("synced_manifests", {})
    target_dir = RUNS_DIR / campaign_id
    if synced.get(campaign_id) == sync_key and target_dir.exists():
        return target_dir, False
    _sync_run_to_local(campaign_id)
    synced[campaign_id] = sync_key
    return target_dir, True


def _upload_run_outputs(campaign_id: str) -> dict[str, float | int]:
    run_dir = RUNS_DIR / campaign_id
    run_uri = _bucket_uri(GCS_RUNS_PREFIX, campaign_id)
    stats = _upload_directory(run_dir, run_uri)
    print(f"[CloudWorker] Uploaded updated outputs for {campaign_id} -> {run_uri}")
    return stats


def _mark_manifest_processed(manifest_uri: str, campaign_id: str) -> str:
    processed_uri = _bucket_uri(GCS_PROCESSED_PREFIX, f"{campaign_id}-{int(time.time())}.json")
    _run_cmd(["storage", "mv", manifest_uri, processed_uri])
    print(f"[CloudWorker] Moved manifest to processed: {processed_uri}")
    return processed_uri


def _mark_manifest_failed(manifest_uri: str, campaign_id: str) -> str:
    failed_uri = _bucket_uri(GCS_FAILED_PREFIX, f"{campaign_id}-{int(time.time())}.json")
    _run_cmd(["storage", "mv", manifest_uri, failed_uri])
    print(f"[CloudWorker] Moved manifest to failed: {failed_uri}")
    return failed_uri


def _write_cloud_result(
    campaign_id: str,
    *,
    status: str,
    processed_manifest_uri: str = "",
    error_message: str | None = None,
) -> Path:
    run_dir = RUNS_DIR / campaign_id
    result_path = run_dir / "cloud_send_result.json"
    payload = {
        "campaign_id": campaign_id,
        "status": status,
        "completed_at": _now_utc(),
        "environment": "gcp_vm",
        "processed_manifest_uri": processed_manifest_uri,
        "error_message": error_message,
    }
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return result_path


def _process_campaign(campaign_id: str, manifest_uri: str, ctx: dict) -> str:
    print(f"[CloudWorker] Preparing campaign {campaign_id}")
    sync_cloud_send_status(
        campaign_id,
        CLOUD_SEND_SENDING,
        details={
            "cloud_send_started_at": _now_utc(),
            "cloud_send_manifest_uri": manifest_uri,
            "cloud_send_market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
            "cloud_send_timezone": ctx.get("timezone", ""),
        },
    )
    set_active_run(campaign_id)
    try:
        _run_campaign_send(campaign_id)
        _write_cloud_result(
            campaign_id,
            status=CLOUD_SEND_COMPLETED,
        )
        upload_stats = _upload_run_outputs(campaign_id)
        processed_uri = _mark_manifest_processed(manifest_uri, campaign_id)
        result_path = _write_cloud_result(
            campaign_id,
            status=CLOUD_SEND_COMPLETED,
            processed_manifest_uri=processed_uri,
        )
        _upload_file(result_path, f"{_bucket_uri(GCS_RUNS_PREFIX, campaign_id).rstrip('/')}/cloud_send_result.json")
        sync_cloud_send_status(
            campaign_id,
            CLOUD_SEND_COMPLETED,
            details={
                "cloud_send_completed_at": _now_utc(),
                "cloud_send_manifest_uri": manifest_uri,
                "cloud_send_processed_manifest_uri": processed_uri,
                "cloud_send_market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
                "cloud_send_timezone": ctx.get("timezone", ""),
                "cloud_send_upload_mode": "recursive_directory_cp",
                "cloud_send_uploaded_file_count": upload_stats["file_count"],
                "cloud_send_uploaded_bytes": upload_stats["total_bytes"],
                "cloud_send_upload_elapsed_seconds": upload_stats["elapsed_seconds"],
            },
        )
        return processed_uri
    finally:
        clear_active_run()


def run_worker(poll_seconds: float = CLOUD_WORKER_POLL_SECONDS) -> None:
    state = _load_state()
    completed = set(state.get("completed_campaigns", []))
    failed = set(state.get("failed_campaigns", []))
    print(f"[CloudWorker] Started. Poll={poll_seconds}s")

    while True:
        now_utc = datetime.now(tz=timezone.utc)
        _update_worker_state(state, last_poll_at=_now_utc())
        manifest_uris = _list_manifest_uris()
        _update_worker_state(state, last_manifest_count=len(manifest_uris))
        if not manifest_uris:
            print("[CloudWorker] No manifests found. Sleeping.")
            _update_worker_state(state, last_idle_reason="no_manifests")
            time.sleep(poll_seconds)
            continue

        candidates: list[tuple[datetime, str, str, dict]] = []
        for manifest_uri in manifest_uris:
            try:
                manifest = _download_manifest(manifest_uri)
            except Exception as exc:
                print(f"[CloudWorker] Failed to load manifest {manifest_uri}: {exc}")
                _update_worker_state(state, last_error_at=_now_utc())
                _record_alert(
                    level="error",
                    event_type="manifest_load_failed",
                    message=str(exc),
                    manifest_uri=manifest_uri,
                )
                continue

            campaign_id = (manifest.get("campaign_id") or "").strip()
            if not campaign_id:
                print(f"[CloudWorker] Invalid manifest, missing campaign_id: {manifest_uri}")
                _update_worker_state(state, last_error_at=_now_utc())
                _record_alert(
                    level="error",
                    event_type="manifest_invalid",
                    message="Manifest missing campaign_id",
                    manifest_uri=manifest_uri,
                )
                continue
            try:
                if _reconcile_manifest_with_run_state(campaign_id, manifest_uri, state, completed, failed):
                    continue
            except Exception as exc:
                print(f"[CloudWorker] Failed to reconcile manifest {campaign_id}: {exc}")
                _update_worker_state(state, last_error_at=_now_utc(), last_failed_campaign_id=campaign_id)
                _record_alert(
                    level="error",
                    event_type="manifest_reconcile_failed",
                    message=str(exc),
                    campaign_id=campaign_id,
                    manifest_uri=manifest_uri,
                )
                continue
            if campaign_id in completed:
                print(f"[CloudWorker] Campaign already completed locally, skipping manifest: {campaign_id}")
                continue
            if campaign_id in failed:
                print(f"[CloudWorker] Campaign previously failed, skipping until manual recovery: {campaign_id}")
                continue

            try:
                _, did_sync = _ensure_run_synced(campaign_id, manifest_uri, manifest, state)
                if did_sync:
                    sync_cloud_send_status(
                        campaign_id,
                        CLOUD_SEND_SYNCED,
                        details={
                            "cloud_send_manifest_uri": manifest_uri,
                            "cloud_send_synced_at": _now_utc(),
                        },
                    )
                    _update_worker_state(state, synced_manifests=state.get("synced_manifests", {}))
                due, ctx = _campaign_next_due(campaign_id)
            except Exception as exc:
                print(f"[CloudWorker] Failed to prepare campaign {campaign_id}: {exc}")
                sync_cloud_send_status(
                    campaign_id,
                    CLOUD_SEND_FAILED,
                    error_message=str(exc),
                    details={
                        "cloud_send_manifest_uri": manifest_uri,
                        "cloud_send_failed_at": _now_utc(),
                        "cloud_send_failed_stage": "prepare",
                    },
                )
                failed_uri = ""
                try:
                    failed_uri = _mark_manifest_failed(manifest_uri, campaign_id)
                except Exception as move_exc:
                    print(f"[CloudWorker] Failed to move prepare-failed manifest for {campaign_id}: {move_exc}")
                if failed_uri:
                    sync_cloud_send_status(
                        campaign_id,
                        CLOUD_SEND_FAILED,
                        error_message=str(exc),
                        details={
                            "cloud_send_manifest_uri": manifest_uri,
                            "cloud_send_failed_manifest_uri": failed_uri,
                            "cloud_send_failed_at": _now_utc(),
                            "cloud_send_failed_stage": "prepare",
                        },
                    )
                failed.add(campaign_id)
                state.setdefault("synced_manifests", {}).pop(campaign_id, None)
                _update_worker_state(state, failed_campaigns=sorted(failed))
                _update_worker_state(state, last_error_at=_now_utc())
                _record_alert(
                    level="error",
                    event_type="campaign_prepare_failed",
                    message=str(exc),
                    campaign_id=campaign_id,
                    manifest_uri=manifest_uri,
                )
                continue

            candidates.append((due, campaign_id, manifest_uri, ctx))

        if not candidates:
            print("[CloudWorker] No actionable manifests found. Sleeping.")
            _update_worker_state(state, last_idle_reason="no_actionable_manifests")
            time.sleep(poll_seconds)
            continue

        candidates.sort(key=lambda item: item[0])
        due, campaign_id, manifest_uri, ctx = candidates[0]

        if due > now_utc:
            wait_seconds = max((due - now_utc).total_seconds(), 0.0)
            capped_wait = min(wait_seconds, poll_seconds)
            sync_cloud_send_status(
                campaign_id,
                CLOUD_SEND_WAITING_WINDOW,
                details={
                    "cloud_send_manifest_uri": manifest_uri,
                    "cloud_send_due_at": due.isoformat(),
                    "cloud_send_wait_seconds": round(wait_seconds, 1),
                    "cloud_send_market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
                    "cloud_send_timezone": ctx.get("timezone", ""),
                },
            )
            _update_worker_state(
                state,
                last_idle_reason="waiting_window",
                last_wait_campaign_id=campaign_id,
                last_wait_due_at=due.isoformat(),
            )
            print(
                f"[CloudWorker] Next campaign window: {campaign_id} at {due.isoformat()} UTC | "
                f"market={ctx.get('city')}, {ctx.get('country')} | tz={ctx.get('timezone')} | "
                f"sleeping {capped_wait:.0f}s"
            )
            time.sleep(capped_wait)
            continue

        try:
            _update_worker_state(
                state,
                active_campaign_id=campaign_id,
                last_idle_reason="sending",
                last_wait_campaign_id="",
                last_wait_due_at="",
            )
            processed_uri = _process_campaign(campaign_id, manifest_uri, ctx)
            completed.add(campaign_id)
            failed.discard(campaign_id)
            state.setdefault("synced_manifests", {}).pop(campaign_id, None)
            _update_worker_state(
                state,
                completed_campaigns=sorted(completed),
                failed_campaigns=sorted(failed),
                last_success_at=_now_utc(),
                active_campaign_id="",
                last_completed_campaign_id=campaign_id,
                last_processed_manifest_uri=processed_uri,
                last_idle_reason="",
            )
        except Exception as exc:
            print(f"[CloudWorker] Campaign failed: {campaign_id}: {exc}")
            result_path = _write_cloud_result(campaign_id, status=CLOUD_SEND_FAILED, error_message=str(exc))
            try:
                upload_stats = _upload_run_outputs(campaign_id)
                _upload_file(result_path, f"{_bucket_uri(GCS_RUNS_PREFIX, campaign_id).rstrip('/')}/cloud_send_result.json")
            except Exception as upload_exc:
                print(f"[CloudWorker] Failed to upload failure artifacts for {campaign_id}: {upload_exc}")
                upload_stats = None
            sync_cloud_send_status(
                campaign_id,
                CLOUD_SEND_FAILED,
                error_message=str(exc),
                details={
                    "cloud_send_manifest_uri": manifest_uri,
                    "cloud_send_failed_at": _now_utc(),
                    "cloud_send_failed_stage": "send",
                    "cloud_send_market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
                    "cloud_send_timezone": ctx.get("timezone", ""),
                    "cloud_send_upload_mode": "recursive_directory_cp",
                    "cloud_send_uploaded_file_count": int(upload_stats["file_count"]) if upload_stats else 0,
                    "cloud_send_uploaded_bytes": int(upload_stats["total_bytes"]) if upload_stats else 0,
                    "cloud_send_upload_elapsed_seconds": float(upload_stats["elapsed_seconds"]) if upload_stats else 0.0,
                },
            )
            failed_uri = ""
            try:
                failed_uri = _mark_manifest_failed(manifest_uri, campaign_id)
            except Exception as move_exc:
                print(f"[CloudWorker] Failed to move failed manifest for {campaign_id}: {move_exc}")
            if failed_uri:
                sync_cloud_send_status(
                    campaign_id,
                    CLOUD_SEND_FAILED,
                    error_message=str(exc),
                    details={
                        "cloud_send_manifest_uri": manifest_uri,
                        "cloud_send_failed_manifest_uri": failed_uri,
                        "cloud_send_failed_at": _now_utc(),
                        "cloud_send_failed_stage": "send",
                    },
                )
            failed.add(campaign_id)
            state.setdefault("synced_manifests", {}).pop(campaign_id, None)
            _update_worker_state(
                state,
                last_error_at=_now_utc(),
                active_campaign_id="",
                failed_campaigns=sorted(failed),
                last_failed_campaign_id=campaign_id,
                last_idle_reason="awaiting_manual_recovery",
            )
            _record_alert(
                level="error",
                event_type="campaign_send_failed",
                message=str(exc),
                campaign_id=campaign_id,
                manifest_uri=manifest_uri,
                details={
                    "market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
                    "timezone": ctx.get("timezone", ""),
                },
            )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Google Cloud VM send worker")
    parser.add_argument(
        "--poll",
        type=float,
        default=CLOUD_WORKER_POLL_SECONDS,
        help="Idle poll interval in seconds",
    )
    args = parser.parse_args()
    run_worker(poll_seconds=args.poll)


if __name__ == "__main__":
    main()
