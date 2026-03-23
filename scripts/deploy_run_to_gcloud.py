"""
Upload a completed run to Google Cloud Storage and register it for cloud send.

Usage:
    D:\Python\python.exe scripts/deploy_run_to_gcloud.py --campaign rio-de-janeiro_20260322_145618_296c
    D:\Python\python.exe scripts/deploy_run_to_gcloud.py --campaign rio-... --campaign sao-...
    D:\Python\python.exe scripts/deploy_run_to_gcloud.py --all-ready --limit 5

This script:
  1. validates the run exists and contains final_send_queue.csv
  2. uploads data/runs/<campaign_id>/ to GCS
  3. uploads a manifest JSON under the configured manifests prefix

The cloud worker uses the manifest to discover new runs to send.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    GCS_BUCKET,
    GCS_MANIFESTS_PREFIX,
    GCS_RUNS_PREFIX,
    RUNS_DIR,
)
from src.workflow_9_campaign_runner.campaign_state import (
    CLOUD_DEPLOY_COMPLETED,
    CLOUD_DEPLOY_FAILED,
    CLOUD_DEPLOY_NOT_ENABLED,
    CLOUD_DEPLOY_PENDING,
    CLOUD_DEPLOY_STARTED,
    CLOUD_SEND_QUEUED,
    load_cloud_deploy_status,
    sync_cloud_deploy_status,
    sync_cloud_send_status,
)


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


def _run_cmd(args: list[str]) -> None:
    cmd = [GCLOUD_BIN, *args]
    print(f"[GCloudDeploy] RUN: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def _try_cmd(args: list[str]) -> None:
    cmd = [GCLOUD_BIN, *args]
    print(f"[GCloudDeploy] TRY: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        pass


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
        f"[GCloudDeploy] Uploaded directory in one recursive pass: "
        f"{stats['file_count']} files, {stats['total_bytes']} bytes, {elapsed}s"
    )
    return {
        "file_count": stats["file_count"],
        "total_bytes": stats["total_bytes"],
        "elapsed_seconds": elapsed,
    }


def _load_campaign_config(run_dir: Path) -> dict:
    state_path = run_dir / "campaign_run_state.json"
    if state_path.exists():
        try:
            with open(state_path, encoding="utf-8") as f:
                return json.load(f).get("config", {})
        except Exception:
            pass
    return {}


def _is_dry_run_config(cfg: dict) -> bool:
    send_mode = str(cfg.get("send_mode", "") or "").strip().lower()
    dry_run_flag = str(cfg.get("dry_run", "") or "").strip().lower() == "true"
    return send_mode == "dry_run" or dry_run_flag


def _build_manifest(campaign_id: str, cfg: dict) -> dict:
    return {
        "deploy_version": 1,
        "campaign_id": campaign_id,
        "uploaded_at": _now_utc(),
        "uploaded_from_host": socket.gethostname(),
        "run_uri": _bucket_uri(GCS_RUNS_PREFIX, campaign_id),
        "send_mode": "gmail_api",
        "status": "queued_for_cloud_send",
        "country": cfg.get("country", ""),
        "region": cfg.get("region", ""),
        "city": cfg.get("base_city") or cfg.get("city") or "",
        "run_until": cfg.get("run_until", ""),
    }


def _is_deploy_blocked(status: str) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized in {
        CLOUD_DEPLOY_PENDING,
        CLOUD_DEPLOY_STARTED,
        CLOUD_DEPLOY_COMPLETED,
    }


def _discover_ready_campaigns(force: bool = False, limit: int = 0) -> list[str]:
    candidates: list[tuple[str, float]] = []
    for run_dir in sorted(RUNS_DIR.iterdir()) if RUNS_DIR.exists() else []:
        if not run_dir.is_dir():
            continue
        campaign_id = run_dir.name
        if not (run_dir / "final_send_queue.csv").exists():
            continue

        cfg = _load_campaign_config(run_dir)
        if _is_dry_run_config(cfg):
            continue

        deploy_state = str(load_cloud_deploy_status(campaign_id).get("cloud_deploy_status") or "").strip().lower()
        if not force and _is_deploy_blocked(deploy_state):
            continue

        try:
            mtime = (run_dir / "final_send_queue.csv").stat().st_mtime
        except OSError:
            mtime = 0.0
        candidates.append((campaign_id, mtime))

    candidates.sort(key=lambda item: item[1], reverse=True)
    campaign_ids = [campaign_id for campaign_id, _ in candidates]
    return campaign_ids[:limit] if limit > 0 else campaign_ids


def deploy_run(campaign_id: str) -> dict:
    run_dir = RUNS_DIR / campaign_id
    if not run_dir.exists():
        raise FileNotFoundError(f"Run directory not found: {run_dir}")

    final_queue = run_dir / "final_send_queue.csv"
    if not final_queue.exists():
        raise FileNotFoundError(f"final_send_queue.csv not found for {campaign_id}: {final_queue}")

    cfg = _load_campaign_config(run_dir)
    if _is_dry_run_config(cfg):
        reason = "Dry-run campaigns are not eligible for cloud deploy."
        sync_cloud_deploy_status(campaign_id, CLOUD_DEPLOY_NOT_ENABLED, error_message=reason)
        raise RuntimeError(reason)

    manifest = _build_manifest(campaign_id, cfg)
    run_uri = _bucket_uri(GCS_RUNS_PREFIX, campaign_id)
    manifest_uri = _bucket_uri(GCS_MANIFESTS_PREFIX, f"{campaign_id}.json")

    sync_cloud_deploy_status(
        campaign_id,
        CLOUD_DEPLOY_STARTED,
        details={
            "cloud_deploy_run_uri": run_uri,
            "cloud_deploy_manifest_uri": manifest_uri,
            "cloud_deploy_upload_mode": "recursive_directory_cp",
        },
    )

    try:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / f"{campaign_id}.json"
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)

            _try_cmd(["storage", "rm", "--recursive", f"{run_uri}/**"])
            upload_stats = _upload_directory(run_dir, run_uri)
            _run_cmd(["storage", "cp", str(manifest_path), manifest_uri])
    except Exception as exc:
        sync_cloud_deploy_status(campaign_id, CLOUD_DEPLOY_FAILED, error_message=str(exc))
        raise

    sync_cloud_deploy_status(
        campaign_id,
        CLOUD_DEPLOY_COMPLETED,
        details={
            "cloud_deploy_run_uri": run_uri,
            "cloud_deploy_manifest_uri": manifest_uri,
            "cloud_deploy_uploaded_at": manifest["uploaded_at"],
            "cloud_deploy_upload_mode": "recursive_directory_cp",
            "cloud_deploy_file_count": upload_stats["file_count"],
            "cloud_deploy_bytes": upload_stats["total_bytes"],
            "cloud_deploy_elapsed_seconds": upload_stats["elapsed_seconds"],
        },
    )
    sync_cloud_send_status(
        campaign_id,
        CLOUD_SEND_QUEUED,
        details={
            "cloud_send_manifest_uri": manifest_uri,
            "cloud_send_run_uri": run_uri,
            "cloud_send_queued_at": manifest["uploaded_at"],
        },
    )
    print(f"[GCloudDeploy] Uploaded run: {campaign_id}")
    print(f"[GCloudDeploy] Manifest: {manifest_uri}")
    return manifest


def deploy_runs(campaign_ids: list[str], force: bool = False) -> dict[str, object]:
    ordered_ids = list(dict.fromkeys(campaign_ids))
    results: list[dict[str, str]] = []
    deployed = 0
    skipped = 0
    failed = 0

    for campaign_id in ordered_ids:
        deploy_state = str(load_cloud_deploy_status(campaign_id).get("cloud_deploy_status") or "").strip().lower()
        if not force and _is_deploy_blocked(deploy_state):
            print(f"[GCloudDeploy] SKIP {campaign_id}: already {deploy_state}")
            skipped += 1
            results.append({
                "campaign_id": campaign_id,
                "result": "skipped",
                "reason": f"already_{deploy_state}",
            })
            continue

        try:
            deploy_run(campaign_id)
            deployed += 1
            results.append({
                "campaign_id": campaign_id,
                "result": "deployed",
                "reason": "",
            })
        except Exception as exc:
            failed += 1
            results.append({
                "campaign_id": campaign_id,
                "result": "failed",
                "reason": str(exc),
            })

    summary = {
        "requested": len(ordered_ids),
        "deployed": deployed,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }
    print(
        f"[GCloudDeploy] Batch summary | requested={summary['requested']} "
        f"deployed={deployed} skipped={skipped} failed={failed}"
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy a completed run to Google Cloud")
    parser.add_argument(
        "--campaign",
        action="append",
        dest="campaigns",
        help="Campaign/run id to deploy; may be passed multiple times",
    )
    parser.add_argument(
        "--all-ready",
        action="store_true",
        help="Auto-discover all eligible completed runs with final_send_queue.csv",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="When using --all-ready, limit the number of newest campaigns to deploy",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow redeploy of runs even if cloud_deploy_status is pending/started/completed",
    )
    args = parser.parse_args()

    campaign_ids = list(args.campaigns or [])
    if args.all_ready:
        campaign_ids.extend(_discover_ready_campaigns(force=args.force, limit=args.limit))

    ordered_ids = list(dict.fromkeys(campaign_ids))
    if not ordered_ids:
        raise SystemExit("Provide --campaign ... or use --all-ready.")

    summary = deploy_runs(ordered_ids, force=args.force)
    if int(summary["failed"]) > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
