# Workflow 7: Email Sending
# Schedules daily email batches and enforces send windows / rate limits.

import time
from datetime import datetime
from .email_sender import run_send_batch

# Send window: only send emails during business hours (24-hour clock, local time)
SEND_WINDOW_START = 8   # 08:00
SEND_WINDOW_END   = 17  # 17:00

# Seconds to wait between individual sends within a batch (spam protection)
INTER_SEND_DELAY = 30


def is_within_send_window() -> bool:
    """Return True if current local time is within the configured send window."""
    now = datetime.now()
    return SEND_WINDOW_START <= now.hour < SEND_WINDOW_END


def run_scheduled_batch() -> None:
    """
    Run one email batch if inside the send window.
    Call this function via a cron job or scheduler (e.g. APScheduler, cron).

    TODO: Integrate with APScheduler for automated daily runs:
        from apscheduler.schedulers.blocking import BlockingScheduler
        scheduler = BlockingScheduler()
        scheduler.add_job(run_scheduled_batch, 'cron', hour=9, minute=0)
        scheduler.start()
    """
    if not is_within_send_window():
        print(
            f"[Scheduler] Outside send window "
            f"({SEND_WINDOW_START}:00–{SEND_WINDOW_END}:00). Skipping."
        )
        return

    print(f"[Scheduler] Starting send batch at {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    run_send_batch()


if __name__ == "__main__":
    run_scheduled_batch()
