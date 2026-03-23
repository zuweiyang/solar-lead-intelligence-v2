"""
Convenience launcher for the Campaign Queue Scheduler.

Usage:
    py scripts/run_queue_scheduler.py
    py scripts/run_queue_scheduler.py --poll 10

The scheduler runs until Ctrl+C.  Keep it running in a separate terminal
while the Streamlit control panel is open.
"""
import os
import sys
from pathlib import Path

# Force UTF-8 for all I/O before any workflow module is imported.
# Without this, open() calls without explicit encoding default to the
# system codec (GBK on Chinese Windows) and crash on Arabic/CJK text.
os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_9_queue_scheduler.queue_runner import run_scheduler

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--poll", type=float, default=5.0,
                        help="Seconds between queue checks when idle")
    args = parser.parse_args()
    run_scheduler(poll_interval=args.poll)
