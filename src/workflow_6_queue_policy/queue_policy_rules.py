# Workflow 6 — Queue Policy Enforcement (P1-3A)
# Pure, deterministic policy decision functions.
#
# All functions are side-effect-free (no I/O, no API calls, no LLM).
# Every decision is logged to send_policy_reason for auditability.

from __future__ import annotations

from src.workflow_6_queue_policy.queue_policy_models import (
    POLICY_BLOCK,
    POLICY_GENERIC_ONLY,
    POLICY_HOLD,
    POLICY_QUEUE_LIMITED,
    POLICY_QUEUE_NORMAL,
    QueuePolicyRecord,
)

# Import eligibility constants from Ticket 3 to avoid string literals
from src.workflow_5_9_email_verification.verification_models import (
    ELIGIBILITY_ALLOW,
    ELIGIBILITY_ALLOW_LIMITED,
    ELIGIBILITY_BLOCK,
    ELIGIBILITY_GENERIC_POOL,
    ELIGIBILITY_HOLD,
)


# ---------------------------------------------------------------------------
# Core policy decision function
# ---------------------------------------------------------------------------

def decide_policy(
    send_eligibility: str,
    is_generic: bool,
    has_verification: bool,
    has_email: bool,
) -> tuple[str, str]:
    """
    Return (policy_action, policy_reason) for one contact candidate.

    Decision tree (evaluated top-to-bottom):

    1. No email address at all → block (no contact to send to)
    2. Verified block (E0, eligibility=="block") → block
    3. Verified hold (E3, eligibility=="hold") → hold
    4. Verified generic_pool_only (E4) → block
    5. Verified allow_limited (E2) → queue_limited
    6. Verified allow (E1) → queue_normal
    7. No verification data + no email → block  (handled in case 1)
    8. No verification data + generic mailbox → block
    9. No verification data + named email → queue_limited  (unverified, caution)

    Args:
        send_eligibility: Value from Ticket 3 ("allow", "allow_limited", …, or "" if absent).
        is_generic:       True when is_generic_mailbox == "true".
        has_verification: True when send_eligibility came from a real verification source.
        has_email:        True when the selected contact has a non-empty kp_email.

    Returns:
        (action, reason) — both are non-empty strings.
    """
    # ── Guard: no email at all ────────────────────────────────────────────
    if not has_email:
        return POLICY_BLOCK, "no_email_address"

    # ── Verified paths ────────────────────────────────────────────────────
    if has_verification:
        if send_eligibility == ELIGIBILITY_BLOCK:
            return POLICY_BLOCK, "verified_e0_invalid"
        if send_eligibility == ELIGIBILITY_HOLD:
            return POLICY_HOLD, "verified_e3_catchall"
        if send_eligibility == ELIGIBILITY_GENERIC_POOL:
            return POLICY_BLOCK, "verified_e4_generic_mailbox"
        if send_eligibility == ELIGIBILITY_ALLOW_LIMITED:
            return POLICY_QUEUE_LIMITED, "verified_e2_limited"
        if send_eligibility == ELIGIBILITY_ALLOW:
            return POLICY_QUEUE_NORMAL, "verified_e1_allow"
        # Unknown eligibility value — treat conservatively
        return POLICY_QUEUE_LIMITED, f"verified_unknown_eligibility:{send_eligibility}"

    # ── Fallback paths (no verification data) ────────────────────────────
    if is_generic:
        return POLICY_BLOCK, "unverified_generic_mailbox"

    return POLICY_QUEUE_LIMITED, "unverified_named_email"


# ---------------------------------------------------------------------------
# Apply policy to a fully-populated QueuePolicyRecord (in-place)
# ---------------------------------------------------------------------------

def apply_policy(record: QueuePolicyRecord) -> None:
    """
    Populate send_policy_action and send_policy_reason on record (in-place).

    Reads the verification + contact fields that should already be set on the
    record and calls decide_policy() to compute the action.
    """
    has_email       = bool((record.selected_contact_email or "").strip())
    is_generic      = record.selected_contact_is_generic == "true"
    eligibility     = record.selected_send_eligibility or ""
    has_verification = record.verification_source in ("scored_contacts", "verified_leads")

    action, reason = decide_policy(
        send_eligibility  = eligibility,
        is_generic        = is_generic,
        has_verification  = has_verification,
        has_email         = has_email,
    )

    record.send_policy_action = action
    record.send_policy_reason = reason
