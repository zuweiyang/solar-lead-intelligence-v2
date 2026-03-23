"""
Auto-send completed campaign runs when the target market enters its next local
business send window.

Usage:
    D:\Python\python.exe scripts/auto_send_runs.py --campaign sao-paulo_... --campaign rio-de-janeiro_...

Behavior:
    1. Loads each run's final_send_queue.csv
    2. Resolves the market-local next eligible send time from send_guard
    3. Sleeps until the earliest campaign is eligible
    4. Runs Workflow 7 send in gmail_api mode
    5. Rebuilds campaign_status summary

Notes:
    - Intended for already-completed runs that have a final_send_queue.csv
    - Uses the new target-market local-time send-window logic
    - One-shot per campaign: after send+status completes, the campaign is removed
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
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

from config.run_context import clear_active_run, set_active_run
from config.settings import RUNS_DIR
from src.workflow_7_email_sending.send_guard import (
    get_target_market_context,
    next_eligible_send_time,
)
from src.workflow_7_email_sending.send_pipeline import run as run_send
from src.workflow_8_5_campaign_status.status_pipeline import run as run_status


def _load_queue_rows(campaign_id: str) -> list[dict]:
    path = RUNS_DIR / campaign_id / "final_send_queue.csv"
    if not path.exists():
        raise FileNotFoundError(f"final_send_queue.csv not found for {campaign_id}: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _campaign_next_due(campaign_id: str) -> tuple[datetime, dict]:
    rows = _load_queue_rows(campaign_id)
    if not rows:
        raise RuntimeError(f"No rows in final_send_queue.csv for {campaign_id}")
    now_utc = datetime.now(tz=timezone.utc)
    first = rows[0]
    due = min(next_eligible_send_time(row, now=now_utc, campaign_id=campaign_id) for row in rows)
    ctx = get_target_market_context(first, campaign_id=campaign_id)
    ctx["queue_count"] = len(rows)
    return due, ctx


def _run_campaign_send(campaign_id: str) -> None:
    print(f"\n[AutoSend] Launching gmail_api send for {campaign_id}")
    set_active_run(campaign_id)
    original_mode = os.environ.get("EMAIL_SEND_MODE")
    os.environ["EMAIL_SEND_MODE"] = "gmail_api"
    try:
        send_summary = run_send(campaign_id=campaign_id, send_mode="gmail_api")
        status_summary = run_status(campaign_id=campaign_id)
    finally:
        clear_active_run()
        if original_mode is None:
            os.environ.pop("EMAIL_SEND_MODE", None)
        else:
            os.environ["EMAIL_SEND_MODE"] = original_mode
    success_count = int(send_summary.get("sent") or 0) + int(send_summary.get("dry_run") or 0)
    failed_count = int(send_summary.get("failed") or 0)
    if success_count == 0 and failed_count > 0:
        raise RuntimeError(
            f"Campaign send produced no successful deliveries "
            f"(sent=0 dry_run=0 failed={failed_count})."
        )
    print(f"[AutoSend] Send summary for {campaign_id}: {send_summary}")
    print(f"[AutoSend] Status summary updated for {campaign_id}: {status_summary}")


def run_auto_send(campaign_ids: list[str], poll_seconds: float = 60.0) -> None:
    pending = list(dict.fromkeys(campaign_ids))
    print(f"[AutoSend] Watching {len(pending)} campaign(s): {', '.join(pending)}")

    while pending:
        now_utc = datetime.now(tz=timezone.utc)
        due_rows: list[tuple[str, datetime, dict]] = []
        for campaign_id in list(pending):
            try:
                due, ctx = _campaign_next_due(campaign_id)
                due_rows.append((campaign_id, due, ctx))
            except Exception as exc:
                print(f"[AutoSend] Removing {campaign_id}: {exc}")
                pending.remove(campaign_id)

        if not due_rows:
            print("[AutoSend] No valid campaigns remain.")
            return

        due_rows.sort(key=lambda item: item[1])
        campaign_id, due, ctx = due_rows[0]

        if due <= now_utc:
            _run_campaign_send(campaign_id)
            pending.remove(campaign_id)
            continue

        wait_seconds = max((due - now_utc).total_seconds(), 0.0)
        capped_wait = min(wait_seconds, poll_seconds)
        print(
            "[AutoSend] Next campaign window: "
            f"{campaign_id} at {due.isoformat()} UTC | "
            f"market={ctx.get('city')}, {ctx.get('country')} | "
            f"tz={ctx.get('timezone')} | queue={ctx.get('queue_count')}"
        )
        print(f"[AutoSend] Sleeping {capped_wait:.0f}s")
        time.sleep(capped_wait)

    print("[AutoSend] All watched campaigns processed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Auto-send completed runs at target-market local time")
    parser.add_argument(
        "--campaign",
        action="append",
        dest="campaigns",
        required=True,
        help="Campaign/run id to watch; may be passed multiple times",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=60.0,
        help="Max seconds between wakeups while waiting (default: 60)",
    )
    args = parser.parse_args()
    run_auto_send(args.campaigns, poll_seconds=args.poll)


if __name__ == "__main__":
    main()
