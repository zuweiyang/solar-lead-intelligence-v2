"""
ngrok tracking validation — end-to-end open/click test.

Steps:
  1. Start local Flask tracking server on port 5000 (background thread)
  2. Open ngrok HTTPS tunnel for port 5000
  3. Patch TRACKING_BASE_URL in the running process + print .env line
  4. Send one real tracked email to yangzuwei@gmail.com
  5. Wait for user to open + click
  6. Report open/click events from engagement_logs.csv

Usage:
    py scripts/ngrok_tracking_validation.py
"""

import csv
import os
import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ---------------------------------------------------------------------------
# Step 1 — Start Flask tracking server in background thread
# ---------------------------------------------------------------------------

print("[Step 1] Starting local Flask tracking server on port 5000...")

from src.workflow_7_5_engagement_tracking.tracking_server import run_server

server_thread = threading.Thread(
    target=run_server,
    kwargs={"host": "0.0.0.0", "port": 5000, "debug": False},
    daemon=True,
)
server_thread.start()
time.sleep(2)  # give Flask time to bind

# Verify it's up
import urllib.request
try:
    with urllib.request.urlopen("http://127.0.0.1:5000/health", timeout=5) as r:
        print(f"[Step 1] Server OK — {r.read().decode()}")
except Exception as e:
    print(f"[Step 1] WARNING: health check failed: {e}")

# ---------------------------------------------------------------------------
# Step 2 — Open ngrok HTTPS tunnel
# ---------------------------------------------------------------------------

print("\n[Step 2] Opening ngrok tunnel for port 5000...")

import json
import re
import subprocess

# Use localtunnel (lt) — zero auth, no account required.
# On Windows lt is a .ps1 script — invoke via powershell.
# lt prints: "your url is: https://xxxx.loca.lt"
import shutil

lt_cmd = shutil.which("lt")  # may resolve .cmd wrapper
if lt_cmd:
    lt_args = [lt_cmd, "--port", "5000"]
else:
    # Fallback: use PowerShell to run the .ps1 directly
    npm_prefix = subprocess.check_output(
        ["powershell", "-Command", "npm root -g"],
        encoding="utf-8", errors="replace"
    ).strip()
    lt_ps1 = str(Path(npm_prefix).parent / "lt.ps1")
    lt_args = ["powershell", "-ExecutionPolicy", "Bypass", "-File", lt_ps1, "--port", "5000"]

lt_proc = subprocess.Popen(
    lt_args,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    encoding="utf-8",
    errors="replace",
)

public_url = None
deadline = time.monotonic() + 20
while time.monotonic() < deadline:
    line = lt_proc.stdout.readline()
    if not line:
        time.sleep(0.2)
        continue
    print(f"[Step 2] lt: {line.strip()}")
    match = re.search(r"https://[^\s]+\.loca\.lt", line)
    if match:
        public_url = match.group(0).rstrip("/")
        break

if not public_url:
    print("[Step 2] ERROR: Could not get localtunnel public URL.")
    sys.exit(1)

print(f"[Step 2] Tunnel active: {public_url}")

# ---------------------------------------------------------------------------
# Step 3 — Patch TRACKING_BASE_URL in this process
# ---------------------------------------------------------------------------

print(f"\n[Step 3] Patching TRACKING_BASE_URL → {public_url}")

import config.settings as _settings
_settings.TRACKING_BASE_URL = public_url

print(f"[Step 3] To persist this across restarts, add to .env:")
print(f"         TRACKING_BASE_URL={public_url}")

# Also update the .env file so it persists
_env_path = Path(os.path.dirname(os.path.dirname(__file__))) / ".env"
if _env_path.exists():
    lines = _env_path.read_text(encoding="utf-8").splitlines()
    new_lines = []
    replaced = False
    for line in lines:
        if line.startswith("TRACKING_BASE_URL="):
            new_lines.append(f"TRACKING_BASE_URL={public_url}")
            replaced = True
        else:
            new_lines.append(line)
    if not replaced:
        new_lines.append(f"TRACKING_BASE_URL={public_url}")
    _env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    print(f"[Step 3] .env updated with new TRACKING_BASE_URL")

# ---------------------------------------------------------------------------
# Step 4 — Send one tracked validation email
# ---------------------------------------------------------------------------

print("\n[Step 4] Sending tracked validation email...")

from src.workflow_7_5_engagement_tracking.email_tracking_injector import prepare_tracked_email
from src.workflow_7_email_sending.email_sender import send_one

RECIPIENT   = "yangzuwei@gmail.com"
SUBJECT     = "OmniSol Tracking Validation (ngrok)"
PLAIN_BODY  = (
    "This is a real tracking validation email from OmniSol.\n"
    "Please open this email and click the test link below.\n\n"
    "Test link:\n"
    "https://omnisolglobal.com/calculator"
)

TRACKING_ID = f"ngrok-val-{uuid.uuid4().hex[:16]}"

print(f"[Step 4] tracking_id : {TRACKING_ID}")
print(f"[Step 4] pixel URL   : {public_url}/track/open/{TRACKING_ID}")

tracked = prepare_tracked_email(PLAIN_BODY, TRACKING_ID, public_url)
html_body = tracked["html_body"]

assert "track/open/" in html_body,  "FAIL: no open pixel in HTML"
assert "track/click/" in html_body, "FAIL: no click link in HTML"
print(f"[Step 4] HTML OK — {tracked['tracked_links_count']} link(s) rewritten")

record = {
    "kp_email":   RECIPIENT,
    "subject":    SUBJECT,
    "email_body": PLAIN_BODY,
    "html_body":  html_body,
}

result = send_one(record)
print(f"[Step 4] send_status         : {result['send_status']}")
print(f"[Step 4] provider            : {result['provider']}")
print(f"[Step 4] provider_message_id : {result['provider_message_id']}")
if result["error_message"]:
    print(f"[Step 4] error : {result['error_message']}")
    sys.exit(1)

if result["send_status"] != "sent":
    print("[Step 4] Send failed — aborting.")
    sys.exit(1)

print(f"\n[Step 4] Email delivered. Message ID: {result['provider_message_id']}")

# ---------------------------------------------------------------------------
# Step 5 — Wait for user to open + click
# ---------------------------------------------------------------------------

print("\n" + "="*60)
print("ACTION REQUIRED:")
print(f"  1. Open yangzuwei@gmail.com")
print(f"  2. Find the email: '{SUBJECT}'")
print(f"  3. Open it (triggers open pixel)")
print(f"  4. Click the OmniSol calculator link (triggers click)")
print("="*60)
print(f"\nWaiting 120 seconds for events... (Ctrl+C to check early)\n")

WAIT_SECONDS = 120
LOG_FILE = Path(os.path.dirname(os.path.dirname(__file__))) / "data" / "engagement_logs.csv"

for remaining in range(WAIT_SECONDS, 0, -5):
    time.sleep(5)
    # Quick check — any events for our tracking_id yet?
    if LOG_FILE.exists():
        with open(LOG_FILE, newline="", encoding="utf-8") as f:
            rows = [r for r in csv.DictReader(f) if r.get("tracking_id") == TRACKING_ID]
        if rows:
            print(f"[Step 5] Event(s) detected early! ({len(rows)} so far) — continuing to collect...")
            # Give a few more seconds in case click comes after open
            time.sleep(8)
            break
    print(f"[Step 5] {remaining}s remaining — no events yet...")

# ---------------------------------------------------------------------------
# Step 6 — Report results
# ---------------------------------------------------------------------------

print("\n[Step 6] Reading engagement_logs.csv...")

event_rows = []
if LOG_FILE.exists():
    with open(LOG_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("tracking_id") == TRACKING_ID:
                event_rows.append(row)

opens  = [r for r in event_rows if r.get("event_type") == "open"]
clicks = [r for r in event_rows if r.get("event_type") == "click"]

print("\n" + "="*60)
print("TRACKING VALIDATION REPORT")
print("="*60)
print(f"  public TRACKING_BASE_URL : {public_url}")
print(f"  tracking_id              : {TRACKING_ID}")
print(f"  email message_id         : {result['provider_message_id']}")
print(f"  open captured            : {'YES' if opens  else 'NO'} ({len(opens)} event(s))")
print(f"  click captured           : {'YES' if clicks else 'NO'} ({len(clicks)} event(s))")
print()

if event_rows:
    print("  Exact log rows:")
    for r in event_rows:
        print(f"    [{r['event_type'].upper()}] {r['timestamp']}  ip={r.get('ip','')}  url={r.get('target_url','')}")
else:
    print("  No events recorded for this tracking_id.")
    print("  Possible reasons:")
    print("    - Email not yet opened/clicked")
    print("    - Gmail cached/blocked the pixel")
    print("    - ngrok session expired")

print("="*60)

# Engagement summary check
summary_files = list(Path(os.path.dirname(os.path.dirname(__file__))).glob("data/**/engagement_summary.csv"))
print(f"\n  engagement_summary.csv files found: {len(summary_files)}")
for sf in summary_files:
    print(f"    {sf}")
