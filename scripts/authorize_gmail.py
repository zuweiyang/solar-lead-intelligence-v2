"""
One-time Gmail OAuth2 authorization script.
Run this to create (or refresh) config/gmail_token.json.

Usage:
    py scripts/authorize_gmail.py

IMPORTANT — scope update (Workflow 7.8 Reply Intelligence):
    gmail.readonly was added alongside gmail.send so the system can
    fetch inbound replies for reply matching.
    If you previously authorized with only gmail.send, delete
    config/gmail_token.json and re-run this script to grant both scopes.
"""

from google_auth_oauthlib.flow import InstalledAppFlow
from config.settings import GMAIL_CLIENT_SECRET_FILE, GMAIL_TOKEN_FILE

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]

flow = InstalledAppFlow.from_client_secrets_file(
    str(GMAIL_CLIENT_SECRET_FILE),
    scopes=SCOPES,
)
creds = flow.run_local_server(port=0)

with open(str(GMAIL_TOKEN_FILE), "w") as f:
    f.write(creds.to_json())

print(f"Token saved → {GMAIL_TOKEN_FILE}")
print(f"Scopes authorized: {SCOPES}")
