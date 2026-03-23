"""
Quick smoke-test: send one email via Gmail API.

Usage:
    py scripts/test_gmail_send.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.workflow_7_email_sending.email_sender import send_one

record = {
    "kp_email":   "yangzuwei@gmail.com",
    "subject":    "Test — Solar Lead Intelligence (Gmail API)",
    "email_body": (
        "Hi,\n\n"
        "This is a test email sent via Gmail API from the Solar Lead Intelligence pipeline.\n\n"
        "If you received this, the OAuth2 send path is working correctly.\n\n"
        "Best,\nWayne | OmniSol"
    ),
}

result = send_one(record)
print(result)

if result["send_status"] == "sent":
    print("\nSUCCESS — email delivered.")
else:
    print(f"\nFAILED: {result['error_message']}")
