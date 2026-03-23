# Workflow 5.9 — Email Verification Gateway: Data Models & Constants
#
# Internal confidence tiers (E0-E4) map provider-native results to a unified
# risk scale. Tiers are independent of any single provider's terminology.
#
# Tier → send eligibility → send pool mapping is defined here as lookup tables
# so that pipeline and downstream merge code import a single source of truth.
from __future__ import annotations

from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Internal confidence tier constants
# ---------------------------------------------------------------------------

TIER_E0 = "E0"  # Invalid / undeliverable — must not enter normal send flow
TIER_E1 = "E1"  # Verified safe — high confidence deliverable
TIER_E2 = "E2"  # Verified but watch — deliverable with caveats (risky domain, webmail, etc.)
TIER_E3 = "E3"  # Catch-all / unknown — cannot confirm or deny
TIER_E4 = "E4"  # Generic role mailbox — info@, sales@, etc.

ALL_TIERS = [TIER_E0, TIER_E1, TIER_E2, TIER_E3, TIER_E4]


# ---------------------------------------------------------------------------
# Send eligibility constants
# ---------------------------------------------------------------------------

ELIGIBILITY_ALLOW         = "allow"             # E1 — send normally
ELIGIBILITY_ALLOW_LIMITED = "allow_limited"     # E2 — send with reduced frequency / priority
ELIGIBILITY_HOLD          = "hold"              # E3 — hold pending manual review or enrichment
ELIGIBILITY_GENERIC_POOL  = "generic_pool_only" # E4 — route to generic/role-address campaign
ELIGIBILITY_BLOCK         = "block"             # E0 — do not send

ALL_ELIGIBILITIES = [
    ELIGIBILITY_ALLOW,
    ELIGIBILITY_ALLOW_LIMITED,
    ELIGIBILITY_HOLD,
    ELIGIBILITY_GENERIC_POOL,
    ELIGIBILITY_BLOCK,
]


# ---------------------------------------------------------------------------
# Send pool constants
# ---------------------------------------------------------------------------

POOL_PRIMARY = "primary_pool"  # E1
POOL_LIMITED = "limited_pool"  # E2
POOL_RISK    = "risk_pool"     # E3
POOL_GENERIC = "generic_pool"  # E4
POOL_BLOCKED = "blocked_pool"  # E0

ALL_POOLS = [POOL_PRIMARY, POOL_LIMITED, POOL_RISK, POOL_GENERIC, POOL_BLOCKED]


# ---------------------------------------------------------------------------
# Lookup tables: tier → eligibility and tier → pool
# ---------------------------------------------------------------------------

TIER_TO_ELIGIBILITY: dict[str, str] = {
    TIER_E0: ELIGIBILITY_BLOCK,
    TIER_E1: ELIGIBILITY_ALLOW,
    TIER_E2: ELIGIBILITY_ALLOW_LIMITED,
    TIER_E3: ELIGIBILITY_HOLD,
    TIER_E4: ELIGIBILITY_GENERIC_POOL,
}

TIER_TO_POOL: dict[str, str] = {
    TIER_E0: POOL_BLOCKED,
    TIER_E1: POOL_PRIMARY,
    TIER_E2: POOL_LIMITED,
    TIER_E3: POOL_RISK,
    TIER_E4: POOL_GENERIC,
}


# ---------------------------------------------------------------------------
# VerificationResult dataclass
# ---------------------------------------------------------------------------

@dataclass
class VerificationResult:
    """
    Complete verification result for a single email address.
    All fields are persisted to the email_verification DB table and
    verified_enriched_leads.csv.
    """
    kp_email:               str   # the verified address (normalised lower-case)
    email_confidence_tier:  str   # E0-E4
    send_eligibility:       str   # allow | allow_limited | hold | generic_pool_only | block
    send_pool:              str   # primary_pool | limited_pool | risk_pool | generic_pool | blocked_pool
    is_generic_mailbox:     bool  # True for info@, sales@, etc.
    provider_result:        str   # raw result string from provider (e.g. "deliverable")
    provider_name:          str   # "hunter" | "mock" | "none"
    verified_at:            str   # ISO-8601 UTC timestamp
    source_mode:            str   # "live" | "mock" | "cached" | "skipped"
    error:                  str = ""  # non-empty if verification call failed


# ---------------------------------------------------------------------------
# CSV output fields for verified_enriched_leads.csv
# ---------------------------------------------------------------------------

# These are the ADDITIONAL fields appended to the standard ENRICHED_FIELDS.
# The pipeline writes all ENRICHED_FIELDS + VERIFICATION_EXTRA_FIELDS.
VERIFICATION_EXTRA_FIELDS: list[str] = [
    "email_confidence_tier",
    "send_eligibility",
    "send_pool",
    "is_generic_mailbox",
    "provider_result",
    "provider_name",
    "verified_at",
    "source_mode",
    "verification_error",
]
