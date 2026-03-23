"""
Starts Flask tracking server + localtunnel. Stays alive until Ctrl+C.
Prints the public URL and tracking_id, then waits for events.

Usage:
    py scripts/start_tracking_tunnel.py
"""

import csv
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

LOG_FILE = Path(os.path.dirname(os.path.dirname(__file__))) / "data" / "engagement_logs.csv"

# ---------------------------------------------------------------------------
# Step 1 — Flask tracking server
# ---------------------------------------------------------------------------
print("[1] Starting Flask tracking server on port 5000...")
from src.workflow_7_5_engagement_tracking.tracking_server import run_server

threading.Thread(
    target=run_server,
    kwargs={"host": "0.0.0.0", "port": 5000, "debug": False},
    daemon=True,
).start()
time.sleep(2)

try:
    with urllib.request.urlopen("http://127.0.0.1:5000/health", timeout=5) as r:
        print(f"[1] Server OK — {r.read().decode()}")
except Exception as e:
    print(f"[1] WARNING: {e}")

# ---------------------------------------------------------------------------
# Step 2 — localtunnel
# ---------------------------------------------------------------------------
print("\n[2] Opening localtunnel...")
import shutil

lt_cmd = shutil.which("lt")
if lt_cmd:
    lt_args = [lt_cmd, "--port", "5000"]
else:
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
    print(f"[2] lt: {line.strip()}")
    match = re.search(r"https://[^\s]+\.loca\.lt", line)
    if match:
        public_url = match.group(0).rstrip("/")
        break

if not public_url:
    print("[2] ERROR: Could not get tunnel URL.")
    sys.exit(1)

print(f"[2] Tunnel: {public_url}")

# ---------------------------------------------------------------------------
# Step 3 — Patch settings + .env
# ---------------------------------------------------------------------------
import config.settings as _settings
_settings.TRACKING_BASE_URL = public_url

_env_path = Path(os.path.dirname(os.path.dirname(__file__))) / ".env"
if _env_path.exists():
    lines = _env_path.read_text(encoding="utf-8").splitlines()
    new_lines, replaced = [], False
    for line in lines:
        if line.startswith("TRACKING_BASE_URL="):
            new_lines.append(f"TRACKING_BASE_URL={public_url}")
            replaced = True
        else:
            new_lines.append(line)
    if not replaced:
        new_lines.append(f"TRACKING_BASE_URL={public_url}")
    _env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    print(f"[3] .env updated: TRACKING_BASE_URL={public_url}")

# ---------------------------------------------------------------------------
# Step 4 — Send one tracked email
# ---------------------------------------------------------------------------
print("\n[4] Sending tracked validation email...")
from src.workflow_7_5_engagement_tracking.email_tracking_injector import prepare_tracked_email
from src.workflow_7_email_sending.email_sender import send_one

TRACKING_ID = f"ngrok-val-{uuid.uuid4().hex[:16]}"
RECIPIENT   = "yangzuwei@gmail.com"
SUBJECT     = "OmniSol Tracking Validation (live tunnel)"
PLAIN_BODY  = (
    "This is a real tracking validation email from OmniSol.\n"
    "Please open this email and click the test link below.\n\n"
    "Test link:\n"
    "https://omnisolglobal.com/calculator"
)

tracked   = prepare_tracked_email(PLAIN_BODY, TRACKING_ID, public_url)
html_body = tracked["html_body"]
result    = send_one({
    "kp_email":   RECIPIENT,
    "subject":    SUBJECT,
    "email_body": PLAIN_BODY,
    "html_body":  html_body,
})

if result["send_status"] != "sent":
    print(f"[4] Send failed: {result['error_message']}")
    sys.exit(1)

print(f"[4] Sent  — message_id : {result['provider_message_id']}")
print(f"[4]       — tracking_id: {TRACKING_ID}")
print(f"[4]       — pixel URL  : {public_url}/track/open/{TRACKING_ID}")

# ---------------------------------------------------------------------------
# Step 5 — Keep alive, poll for events
# ---------------------------------------------------------------------------
print("\n" + "="*60)
print("WAITING — open yangzuwei@gmail.com, find the email")
print(f"'{SUBJECT}', open it and click the calculator link.")
print("This script stays alive until you Ctrl+C.")
print("="*60 + "\n")

reported = set()
try:
    while True:
        time.sleep(3)
        if LOG_FILE.exists():
            with open(LOG_FILE, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if row.get("tracking_id") != TRACKING_ID:
                        continue
                    key = (row["event_type"], row.get("timestamp", ""))
                    if key not in reported:
                        reported.add(key)
                        etype = row["event_type"].upper()
                        print(f"  [{etype}] {row['timestamp']}  ip={row.get('ip','')}  url={row.get('target_url','')}")
except KeyboardInterrupt:
    pass

# ---------------------------------------------------------------------------
# Final report
# ---------------------------------------------------------------------------
print("\n" + "="*60)
print("FINAL TRACKING REPORT")
print("="*60)
event_rows = []
if LOG_FILE.exists():
    with open(LOG_FILE, newline="", encoding="utf-8") as f:
        event_rows = [r for r in csv.DictReader(f) if r.get("tracking_id") == TRACKING_ID]

opens  = [r for r in event_rows if r["event_type"] == "open"]
clicks = [r for r in event_rows if r["event_type"] == "click"]

print(f"  public TRACKING_BASE_URL : {public_url}")
print(f"  tracking_id              : {TRACKING_ID}")
print(f"  email message_id         : {result['provider_message_id']}")
print(f"  open captured            : {'YES' if opens  else 'NO'} ({len(opens)} event(s))")
print(f"  click captured           : {'YES' if clicks else 'NO'} ({len(clicks)} event(s))")
if event_rows:
    print("  Exact log rows:")
    for r in event_rows:
        print(f"    [{r['event_type'].upper()}] {r['timestamp']}  ip={r.get('ip','')}  url={r.get('target_url','')}")
print("="*60)

lt_proc.terminate()
