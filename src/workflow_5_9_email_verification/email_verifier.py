# Workflow 5.9 — Email Verification Gateway: Core Verification Logic
#
# Responsibilities:
#   - Generic mailbox (role-address) detection via prefix frozenset
#   - Provider response → internal E0-E4 tier normalization
#   - verify_email() entrypoint: wraps provider call + tier assignment + result construction
from __future__ import annotations

from datetime import datetime, timezone

from src.workflow_5_9_email_verification.verification_models import (
    TIER_E0, TIER_E1, TIER_E2, TIER_E3, TIER_E4,
    TIER_TO_ELIGIBILITY, TIER_TO_POOL,
    VerificationResult,
)
from src.workflow_5_9_email_verification.verification_provider import (
    AbstractVerificationProvider,
    RawVerificationResponse,
)
from src.market_localization import get_generic_mailbox_local_parts


# ---------------------------------------------------------------------------
# Generic mailbox (role-address) prefixes
# ---------------------------------------------------------------------------

_GENERIC_PREFIXES: frozenset[str] = frozenset(
    tuple(get_generic_mailbox_local_parts())
    + tuple(get_generic_mailbox_local_parts("Brazil"))
    + (
        "procurement",
        "purchasing",
    )
)


def is_generic_mailbox(email: str) -> bool:
    """
    Return True if the local-part (prefix) of the email address is a well-known
    role-based / generic address (e.g. info@, sales@, support@).

    Comparison is case-insensitive.  Returns False for malformed addresses.
    """
    local = email.lower().partition("@")[0].strip()
    return local in _GENERIC_PREFIXES


# ---------------------------------------------------------------------------
# Provider response → internal tier normalization
# ---------------------------------------------------------------------------

def _normalize_to_tier(resp: RawVerificationResponse, generic: bool) -> str:
    """
    Map a provider raw response to an internal E0-E4 confidence tier.

    Priority rules (safety-first):
      E0 — provider confirmed undeliverable, OR api call errored with no delivery signal
      E4 — generic role mailbox (regardless of deliverability signal)
      E3 — accept_all / catch-all domain, OR result == "unknown" without a hard error
      E2 — deliverable but webmail, risky, or block-flagged
      E1 — clean deliverable with smtp_check confirmation and no risk flags
    """
    # Hard error with no deliverability signal → E0 (conservative)
    if resp.error and not resp.deliverable:
        return TIER_E0

    if resp.undeliverable:
        return TIER_E0

    # Generic mailbox — assign E4 regardless of deliverability result
    # (a delivered generic address is less valuable than a named contact)
    if generic:
        return TIER_E4

    # Catch-all or totally unknown → E3
    if resp.accept_all or resp.result == "unknown":
        return TIER_E3

    # Risky signals (webmail, risky flag, block) → E2 regardless of smtp delivery status
    # Hunter reports result="risky" for webmail and catch-all; catch-all is already handled above.
    if resp.is_webmail or resp.risky or resp.is_block:
        return TIER_E2

    if resp.deliverable and resp.smtp_check:
        return TIER_E1

    # Deliverable without smtp_check confirmation → treat as E2 (watch)
    if resp.deliverable:
        return TIER_E2

    # Fallback — shouldn't normally be reached
    return TIER_E3


# ---------------------------------------------------------------------------
# Main verify_email entrypoint
# ---------------------------------------------------------------------------

def verify_email(
    email: str,
    provider: AbstractVerificationProvider,
    source_mode: str = "live",
) -> VerificationResult:
    """
    Verify a single email address using the provided provider instance.

    Args:
        email:        The email address to verify (normalised to lower-case internally).
        provider:     A concrete AbstractVerificationProvider instance.
        source_mode:  "live" | "mock" | "cached" — passed through to VerificationResult.

    Returns a VerificationResult with all tier, eligibility, pool, and metadata fields set.
    """
    email_lower = email.lower().strip()
    generic = is_generic_mailbox(email_lower)

    resp = provider.verify(email_lower)
    tier = _normalize_to_tier(resp, generic)

    return VerificationResult(
        kp_email              = email_lower,
        email_confidence_tier = tier,
        send_eligibility      = TIER_TO_ELIGIBILITY[tier],
        send_pool             = TIER_TO_POOL[tier],
        is_generic_mailbox    = generic,
        provider_result       = resp.result,
        provider_name         = resp.provider_name,
        verified_at           = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        source_mode           = source_mode,
        error                 = resp.error,
    )
