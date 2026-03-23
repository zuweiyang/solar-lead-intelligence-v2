# Workflow 5.9 — Email Verification Gateway: Provider Abstraction Layer
#
# Defines an abstract provider interface and two concrete implementations:
#   HunterVerificationProvider  — calls Hunter.io /v2/email-verifier (live)
#   MockVerificationProvider    — deterministic offline stub for testing
#
# get_provider() factory selects the implementation based on provider_name + live flag.
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Raw provider response — provider-agnostic intermediate struct
# ---------------------------------------------------------------------------

@dataclass
class RawVerificationResponse:
    """
    Normalised raw response from any verification provider.
    Fields map to a common vocabulary regardless of provider-specific JSON shapes.
    """
    deliverable:   bool   # True if the address is confirmed deliverable
    risky:         bool   # True if deliverable but high-risk (catch-all, webmail, etc.)
    undeliverable: bool   # True if provider confirmed undeliverable
    accept_all:    bool   # True if domain accepts all mail (catch-all)
    is_webmail:    bool   # True if hosted on webmail service (gmail, yahoo, etc.)
    is_block:      bool   # True if provider flagged as spam/temporary/disposable
    smtp_check:    bool   # True if SMTP probe succeeded
    result:        str    # raw result string ("deliverable" / "risky" / "undeliverable" / "unknown")
    provider_name: str    # which provider produced this
    error:         str = ""


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class AbstractVerificationProvider(ABC):
    """Provider-agnostic interface for email address verification."""

    @abstractmethod
    def verify(self, email: str) -> RawVerificationResponse:
        """
        Verify a single email address.
        Must return a RawVerificationResponse even on error (use error field).
        Must never raise — all exceptions are caught and surfaced via error field.
        """
        ...


# ---------------------------------------------------------------------------
# Hunter.io provider
# ---------------------------------------------------------------------------

class HunterVerificationProvider(AbstractVerificationProvider):
    """
    Calls Hunter.io /v2/email-verifier.

    Response JSON shape (relevant fields):
        data.result      — "deliverable" | "risky" | "undeliverable" | "unknown"
        data.score       — 0..100 (not used in v1 tier logic)
        data.regexp      — bool (passes basic regex)
        data.disposable  — bool (disposable/temporary address)
        data.webmail     — bool
        data.mx_records  — bool
        data.smtp_server — bool (SMTP server responds)
        data.smtp_check  — bool (SMTP probe confirmed)
        data.accept_all  — bool (catch-all domain)
        data.block       — bool (flagged as spam/block)
        errors           — list of error dicts when call fails
    """

    _BASE_URL = "https://api.hunter.io/v2/email-verifier"

    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise ValueError("HunterVerificationProvider requires a non-empty api_key")
        self._api_key = api_key

    def verify(self, email: str) -> RawVerificationResponse:
        try:
            import json as _json
            import urllib.parse
            import urllib.request
            params = urllib.parse.urlencode({"email": email, "api_key": self._api_key})
            url = f"{self._BASE_URL}?{params}"
            with urllib.request.urlopen(url, timeout=10) as resp:
                body = _json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            return RawVerificationResponse(
                deliverable=False, risky=False, undeliverable=False,
                accept_all=False, is_webmail=False, is_block=False, smtp_check=False,
                result="unknown", provider_name="hunter", error=str(exc),
            )

        data = body.get("data", {}) or {}
        result = (data.get("result") or "unknown").lower()
        return RawVerificationResponse(
            deliverable   = result == "deliverable",
            risky         = result == "risky",
            undeliverable = result == "undeliverable",
            accept_all    = bool(data.get("accept_all")),
            is_webmail    = bool(data.get("webmail")),
            is_block      = bool(data.get("block")),
            smtp_check    = bool(data.get("smtp_check")),
            result        = result,
            provider_name = "hunter",
        )


# ---------------------------------------------------------------------------
# Mock provider — deterministic offline stub
# ---------------------------------------------------------------------------

class MockVerificationProvider(AbstractVerificationProvider):
    """
    Offline stub for testing.  Does not make any network calls.

    Rules (deterministic by email prefix / domain):
      - prefix starts with "bounce_" or domain starts with "invalid"  → undeliverable (E0)
      - domain contains "catchall" or "catch-all"                     → catch-all/risky (E3)
      - domain is gmail/yahoo/hotmail/outlook/live/icloud              → risky + webmail (E2)
      - everything else                                                → deliverable (E1)

    Generic prefix detection is handled in email_verifier.py (not here).
    """

    _WEBMAIL_DOMAINS = frozenset({
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
        "yahoo.co.uk", "live.com", "icloud.com",
    })

    def verify(self, email: str) -> RawVerificationResponse:
        local, _, domain = email.lower().partition("@")
        domain = domain.strip()

        if local.startswith("bounce_") or domain.startswith("invalid"):
            return RawVerificationResponse(
                deliverable=False, risky=False, undeliverable=True,
                accept_all=False, is_webmail=False, is_block=False, smtp_check=False,
                result="undeliverable", provider_name="mock",
            )

        if "catchall" in domain or "catch-all" in domain:
            return RawVerificationResponse(
                deliverable=False, risky=True, undeliverable=False,
                accept_all=True, is_webmail=False, is_block=False, smtp_check=False,
                result="risky", provider_name="mock",
            )

        if domain in self._WEBMAIL_DOMAINS:
            return RawVerificationResponse(
                deliverable=False, risky=True, undeliverable=False,
                accept_all=False, is_webmail=True, is_block=False, smtp_check=False,
                result="risky", provider_name="mock",
            )

        return RawVerificationResponse(
            deliverable=True, risky=False, undeliverable=False,
            accept_all=False, is_webmail=False, is_block=False, smtp_check=True,
            result="deliverable", provider_name="mock",
        )


# ---------------------------------------------------------------------------
# Provider factory
# ---------------------------------------------------------------------------

def get_provider(
    provider_name: str = "hunter",
    live: bool = False,
) -> AbstractVerificationProvider:
    """
    Return the appropriate verification provider.

    Args:
        provider_name:  "hunter" (default) or "mock"
        live:           When False (default), always return MockVerificationProvider.
                        When True, return the real provider for the given name.

    Raises ValueError for unknown provider names when live=True.
    """
    if not live:
        return MockVerificationProvider()

    pname = (provider_name or "hunter").lower().strip()
    if pname == "hunter":
        from config.settings import HUNTER_API_KEY
        return HunterVerificationProvider(api_key=HUNTER_API_KEY)
    if pname == "mock":
        return MockVerificationProvider()

    raise ValueError(f"Unknown verification provider: {provider_name!r}")
