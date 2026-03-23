"""
Run Workflow 7.8 reply intelligence and refresh Workflow 7.5 engagement summary.

Intended for VM-side scheduled execution so inbound replies / bounces are
converted into suppression state without relying on the send worker loop.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (  # noqa: E402
    DATA_DIR,
    REPLY_INTELLIGENCE_HOURS_BACK,
    REPLY_INTELLIGENCE_MAX_RESULTS,
    REPLY_INTELLIGENCE_OUR_EMAIL,
    SMTP_FROM_EMAIL,
)
from src.workflow_7_5_engagement_tracking.engagement_aggregator import run as run_engagement_aggregator  # noqa: E402
from src.workflow_7_8_reply_intelligence.reply_pipeline import run as run_reply_pipeline  # noqa: E402

STATUS_FILE = DATA_DIR / "reply_intelligence_status.json"


def _now_utc() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _write_status(payload: dict) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def run_once(hours_back: int, max_results: int, our_email: str) -> dict:
    resolved_email = (our_email or REPLY_INTELLIGENCE_OUR_EMAIL or SMTP_FROM_EMAIL).strip()
    started_at = _now_utc()
    try:
        reply_summary = run_reply_pipeline(
            hours_back=hours_back,
            max_results=max_results,
            our_email=resolved_email,
        )
        engagement_rows = run_engagement_aggregator()
        payload = {
            "status": "completed",
            "started_at": started_at,
            "completed_at": _now_utc(),
            "hours_back": hours_back,
            "max_results": max_results,
            "our_email": resolved_email,
            "reply_summary": reply_summary,
            "engagement_summary_rows": len(engagement_rows),
        }
        _write_status(payload)
        print(f"[ReplyIntelligence] Completed: {payload}")
        return payload
    except Exception as exc:
        payload = {
            "status": "failed",
            "started_at": started_at,
            "failed_at": _now_utc(),
            "hours_back": hours_back,
            "max_results": max_results,
            "our_email": resolved_email,
            "error": str(exc),
        }
        _write_status(payload)
        print(f"[ReplyIntelligence] Failed: {payload}")
        raise


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run reply intelligence + engagement aggregation")
    parser.add_argument("--hours-back", type=int, default=REPLY_INTELLIGENCE_HOURS_BACK)
    parser.add_argument("--max-results", type=int, default=REPLY_INTELLIGENCE_MAX_RESULTS)
    parser.add_argument("--our-email", default=REPLY_INTELLIGENCE_OUR_EMAIL)
    args = parser.parse_args()
    run_once(args.hours_back, args.max_results, args.our_email)


if __name__ == "__main__":
    main()
