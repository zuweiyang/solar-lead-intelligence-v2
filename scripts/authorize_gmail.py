"""
One-time Gmail OAuth2 authorization helper.

Creates or refreshes config/gmail_token.json.

Modes:
    python scripts/authorize_gmail.py
        Runs the original localhost callback flow.

    python scripts/authorize_gmail.py --manual-start
        Prints an authorization URL and saves a pending PKCE session file.

    python scripts/authorize_gmail.py --manual-finish --code "<code>"
        Exchanges the pasted code for a token using the saved PKCE verifier.

This manual two-step flow is useful on headless VMs where opening a browser or
looping a localhost callback back into the VM is awkward.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

from google_auth_oauthlib.flow import InstalledAppFlow

from config.settings import GMAIL_CLIENT_SECRET_FILE, GMAIL_TOKEN_FILE

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]

PENDING_SESSION_FILE = GMAIL_TOKEN_FILE.with_name("gmail_oauth_pending.json")


def _build_flow():
    return InstalledAppFlow.from_client_secrets_file(
        str(GMAIL_CLIENT_SECRET_FILE),
        scopes=SCOPES,
    )


def _extract_code(value: str) -> str:
    value = value.strip()
    if "code=" not in value:
        return value
    parsed = urlparse(value)
    code = parse_qs(parsed.query).get("code", [""])[0].strip()
    if not code:
        raise ValueError("No code= parameter found in the provided URL.")
    return code


def _save_pending_session(payload: dict) -> None:
    with open(PENDING_SESSION_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _load_pending_session() -> dict:
    if not PENDING_SESSION_FILE.exists():
        raise FileNotFoundError(
            f"Pending OAuth session not found: {PENDING_SESSION_FILE}. "
            "Run --manual-start first."
        )
    with open(PENDING_SESSION_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _delete_pending_session() -> None:
    if PENDING_SESSION_FILE.exists():
        PENDING_SESSION_FILE.unlink()


def _run_local_server(port: int) -> None:
    flow = _build_flow()
    creds = flow.run_local_server(port=port)
    with open(str(GMAIL_TOKEN_FILE), "w", encoding="utf-8") as f:
        f.write(creds.to_json())
    print(f"Token saved -> {GMAIL_TOKEN_FILE}")
    print(f"Scopes authorized: {SCOPES}")


def _manual_start(redirect_uri: str) -> None:
    flow = _build_flow()
    flow.redirect_uri = redirect_uri
    auth_url, state = flow.authorization_url(
        access_type="offline",
        prompt="consent",
    )

    _save_pending_session(
        {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "redirect_uri": redirect_uri,
            "state": state,
            "scopes": SCOPES,
        }
    )

    print("Open this URL in your browser:\n")
    print(auth_url)
    print(
        "\nAfter Google redirects, copy the full redirected URL or just the "
        "code= value, then run:\n"
    )
    print('PYTHONPATH=$PWD python scripts/authorize_gmail.py --manual-finish --code "<paste_code_or_url>"')
    print(f"\nPending session saved -> {PENDING_SESSION_FILE}")


def _manual_finish(code_or_url: str) -> None:
    pending = _load_pending_session()
    code = _extract_code(code_or_url)

    flow = _build_flow()
    flow.redirect_uri = pending["redirect_uri"]
    flow.fetch_token(code=code)

    with open(str(GMAIL_TOKEN_FILE), "w", encoding="utf-8") as f:
        f.write(flow.credentials.to_json())

    _delete_pending_session()
    print(f"Token saved -> {GMAIL_TOKEN_FILE}")
    print(f"Scopes authorized: {SCOPES}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0, help="Local callback port for the browser flow.")
    parser.add_argument(
        "--manual-start",
        action="store_true",
        help="Print an auth URL and save a pending PKCE session for manual completion.",
    )
    parser.add_argument(
        "--manual-finish",
        action="store_true",
        help="Exchange a pasted code for a token using the saved pending PKCE session.",
    )
    parser.add_argument(
        "--redirect-uri",
        default="http://127.0.0.1:8080/",
        help="Redirect URI to use for the manual two-step flow.",
    )
    parser.add_argument(
        "--code",
        default="",
        help="Authorization code or full redirected URL used with --manual-finish.",
    )
    args = parser.parse_args()

    if args.manual_start and args.manual_finish:
        raise SystemExit("Choose either --manual-start or --manual-finish, not both.")

    if args.manual_start:
        _manual_start(args.redirect_uri)
        return

    if args.manual_finish:
        if not args.code.strip():
            raise SystemExit("--manual-finish requires --code.")
        _manual_finish(args.code)
        return

    _run_local_server(args.port)


if __name__ == "__main__":
    main()
