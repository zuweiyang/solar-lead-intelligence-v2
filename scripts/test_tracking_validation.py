"""
Tracking validation send — one real email with open/click tracking injected.

Sends to yangzuwei@gmail.com via Gmail API.
Verifies that:
  - prepare_tracked_email() is called
  - MIME email contains both text/plain and text/html
  - HTML contains open pixel + rewritten click link
  - Gmail API accepts the message

Usage:
    py scripts/test_tracking_validation.py
"""

import sys
import os
import uuid
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.settings import TRACKING_BASE_URL
from src.workflow_7_5_engagement_tracking.email_tracking_injector import (
    prepare_tracked_email,
)
from src.workflow_7_email_sending.email_sender import send_one

RECIPIENT    = "yangzuwei@gmail.com"
SUBJECT      = "OmniSol Tracking Validation Test"
PLAIN_BODY   = (
    "This is a real tracking validation email from OmniSol.\n"
    "Please open the email and click the test link below.\n\n"
    "Test link:\n"
    "https://omnisolglobal.com/calculator"
)

# Generate a stable tracking_id for this test
TRACKING_ID = f"validation-{uuid.uuid4().hex[:16]}"

print(f"[Validation] TRACKING_BASE_URL : {TRACKING_BASE_URL}")
print(f"[Validation] tracking_id       : {TRACKING_ID}")
print()

# Step 1 — prepare tracked HTML
tracked = prepare_tracked_email(PLAIN_BODY, TRACKING_ID, TRACKING_BASE_URL)
html_body          = tracked["html_body"]
tracked_links_count = tracked["tracked_links_count"]

print(f"[Validation] tracked_links_count : {tracked_links_count}")
print(f"[Validation] pixel URL           : {TRACKING_BASE_URL.rstrip('/')}/track/open/{TRACKING_ID}")
print(f"[Validation] HTML length         : {len(html_body)} chars")

# Quick sanity checks
assert "track/open/" in html_body,  "FAIL: no open-tracking pixel in HTML"
assert "track/click/" in html_body, "FAIL: no click-tracking link in HTML"
assert "omnisolglobal.com/calculator" not in html_body or "track/click" in html_body, \
    "FAIL: calculator URL not rewritten"
print("[Validation] HTML structure OK (pixel + rewritten link confirmed)")

# Step 2 — send via Gmail API
record = {
    "kp_email":   RECIPIENT,
    "subject":    SUBJECT,
    "email_body": PLAIN_BODY,
    "html_body":  html_body,
}

result = send_one(record)
print()
print(f"[Validation] send_status         : {result['send_status']}")
print(f"[Validation] provider            : {result['provider']}")
print(f"[Validation] provider_message_id : {result['provider_message_id']}")
if result["error_message"]:
    print(f"[Validation] error               : {result['error_message']}")

if result["send_status"] == "sent":
    print()
    print("SUCCESS — email delivered via Gmail API.")
    print()
    print("Next steps to verify tracking:")
    if "localhost" in TRACKING_BASE_URL:
        print(f"  WARNING: TRACKING_BASE_URL is '{TRACKING_BASE_URL}'")
        print("  The tracking pixel and click links point to localhost.")
        print("  Remote recipients cannot reach this endpoint.")
        print("  Open/click events will NOT fire until a public URL is configured.")
        print()
        print("  To enable real tracking:")
        print("    1. Deploy tracking_server.py to a public host (or use a tunnel)")
        print("    2. Set TRACKING_BASE_URL=https://your-public-host in .env")
        print("    3. Re-run this test")
    else:
        print(f"  Tracking server must be running at: {TRACKING_BASE_URL}")
        print("  Open the email at yangzuwei@gmail.com to trigger an open event.")
        print("  Click the calculator link to trigger a click event.")
        print("  Then run: py -m src.workflow_7_5_engagement_tracking.engagement_aggregator")
        print("  Check: data/crm/engagement_logs.csv and data/*/engagement_summary.csv")
else:
    print("FAILED.")
    sys.exit(1)
