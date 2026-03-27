# Workflow 6 — Queue Policy Enforcement (P1-3A)
# Data models: policy action constants, output field lists, record dataclass,
# summary counters.
#
# Policy layer sits between contact scoring (5.6) / verification (5.9)
# and email generation (6).  It decides which contacts are:
#   queue_normal   — safe, enter normal queue
#   queue_limited  — eligible but flagged (limited pool, unverified, etc.)
#   hold           — do not queue; preserve for manual review
#   generic_only   — legacy/deprecated generic mailbox action kept only for
#                    backward-compatible reads of older artifacts
#   block          — do not queue at all; preserve in blocked rows
#
# Backward compatibility: Workflow 6 (email_generation) continues to read
# enriched_leads.csv.  P1-3B will wire queue_policy.csv into send-time logic.

from __future__ import annotations

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Policy action constants
# ---------------------------------------------------------------------------

POLICY_QUEUE_NORMAL   = "queue_normal"    # safe, unrestricted queue entry
POLICY_QUEUE_LIMITED  = "queue_limited"   # queueable but limited / flagged
POLICY_HOLD           = "hold"            # do not queue; preserve for review
POLICY_GENERIC_ONLY   = "generic_only"    # legacy only; new policy blocks generics
POLICY_BLOCK          = "block"           # do not queue; blocked with reason

ALL_POLICY_ACTIONS = [
    POLICY_QUEUE_NORMAL,
    POLICY_QUEUE_LIMITED,
    POLICY_HOLD,
    POLICY_GENERIC_ONLY,
    POLICY_BLOCK,
]

# Version tag written to every output row for auditability
QUEUE_POLICY_VERSION = "v1_deterministic"


# ---------------------------------------------------------------------------
# Policy source constants
# ---------------------------------------------------------------------------

SOURCE_SCORED_CONTACTS  = "scored_contacts"   # verification fields came from P1-2B
SOURCE_VERIFIED_LEADS   = "verified_leads"    # enriched from verified_enriched_leads.csv
SOURCE_FALLBACK         = "fallback"          # no verification data; conservative rules used


# ---------------------------------------------------------------------------
# Output field list — written to queue_policy.csv
# ---------------------------------------------------------------------------

QUEUE_POLICY_FIELDS: list[str] = [
    # ── Company identity (pass-through) ───────────────────────────────────
    "company_name",
    "website",
    "place_id",
    "lead_score",
    "qualification_status",
    "target_tier",
    "company_type",
    "market_focus",
    # ── Selected primary contact (from P1-2B) ─────────────────────────────
    "selected_contact_email",
    "selected_contact_name",
    "selected_contact_title",
    "selected_contact_rank",
    "selected_contact_is_generic",    # "true" / "false"
    "selected_contact_source",        # enrichment_source value (apollo / hunter / …)
    "contact_fit_score",              # P1-2B fit score
    "contact_selection_reason",       # P1-2B selection reason
    # ── Verification data (from Ticket 3 / P1-2B enrichment) ──────────────
    "selected_send_eligibility",      # allow | allow_limited | hold | generic_pool_only | block | ""
    "selected_send_pool",             # primary_pool | limited_pool | … | ""
    "selected_email_confidence_tier", # E0-E4 | ""
    "verification_source",            # scored_contacts | verified_leads | fallback
    # ── Policy decision ───────────────────────────────────────────────────
    "send_policy_action",             # one of ALL_POLICY_ACTIONS
    "send_policy_reason",             # human-readable one-liner
    # ── Metadata ──────────────────────────────────────────────────────────
    "policy_version",                 # QUEUE_POLICY_VERSION
]


# ---------------------------------------------------------------------------
# Queue policy record dataclass
# ---------------------------------------------------------------------------

@dataclass
class QueuePolicyRecord:
    """
    Holds all policy fields for one company's primary contact candidate.

    All fields have safe defaults so partial upstream data never raises
    AttributeError.
    """
    # Company identity
    company_name:         str = ""
    website:              str = ""
    place_id:             str = ""
    lead_score:           str = ""
    qualification_status: str = ""
    target_tier:          str = ""
    company_type:         str = ""
    market_focus:         str = ""
    # Selected primary contact
    selected_contact_email:        str = ""
    selected_contact_name:         str = ""
    selected_contact_title:        str = ""
    selected_contact_rank:         str = ""
    selected_contact_is_generic:   str = "false"
    selected_contact_source:       str = ""
    contact_fit_score:             str = ""
    contact_selection_reason:      str = ""
    # Verification
    selected_send_eligibility:       str = ""
    selected_send_pool:              str = ""
    selected_email_confidence_tier:  str = ""
    verification_source:             str = ""
    # Policy
    send_policy_action:  str = ""
    send_policy_reason:  str = ""
    # Metadata
    policy_version:      str = QUEUE_POLICY_VERSION

    def to_csv_row(self) -> dict:
        """Serialise for csv.DictWriter."""
        return {f: getattr(self, f, "") for f in QUEUE_POLICY_FIELDS}


# ---------------------------------------------------------------------------
# Summary counter dataclass
# ---------------------------------------------------------------------------

@dataclass
class QueuePolicyStats:
    total_evaluated:        int = 0
    queue_normal_count:     int = 0
    queue_limited_count:    int = 0
    hold_count:             int = 0
    generic_only_count:     int = 0
    block_count:            int = 0
    named_primary_count:    int = 0    # primary is a named (non-generic) contact
    generic_primary_count:  int = 0    # primary is a generic mailbox
    by_eligibility:         dict = field(default_factory=dict)
    by_pool:                dict = field(default_factory=dict)
    error_count:            int = 0

    def record(self, rec: "QueuePolicyRecord") -> None:
        """Update counters from a fully-populated record."""
        self.total_evaluated += 1
        action = rec.send_policy_action
        if action == POLICY_QUEUE_NORMAL:
            self.queue_normal_count += 1
        elif action == POLICY_QUEUE_LIMITED:
            self.queue_limited_count += 1
        elif action == POLICY_HOLD:
            self.hold_count += 1
        elif action == POLICY_GENERIC_ONLY:
            self.generic_only_count += 1
        elif action == POLICY_BLOCK:
            self.block_count += 1

        if rec.selected_contact_is_generic == "true":
            self.generic_primary_count += 1
        else:
            self.named_primary_count += 1

        elig = rec.selected_send_eligibility or "(none)"
        self.by_eligibility[elig] = self.by_eligibility.get(elig, 0) + 1

        pool = rec.selected_send_pool or "(none)"
        self.by_pool[pool] = self.by_pool.get(pool, 0) + 1

    def print_summary(self) -> None:
        print(f"[Workflow 6 — Queue Policy]  Evaluated : {self.total_evaluated}")
        print(f"[Workflow 6 — Queue Policy]  queue_normal  : {self.queue_normal_count}")
        print(f"[Workflow 6 — Queue Policy]  queue_limited : {self.queue_limited_count}")
        print(f"[Workflow 6 — Queue Policy]  hold          : {self.hold_count}")
        print(f"[Workflow 6 — Queue Policy]  generic_only  : {self.generic_only_count}")
        print(f"[Workflow 6 — Queue Policy]  block         : {self.block_count}")
        print(f"[Workflow 6 — Queue Policy]  named primary : {self.named_primary_count}")
        print(f"[Workflow 6 — Queue Policy]  generic primary: {self.generic_primary_count}")
        if self.by_eligibility:
            for elig, cnt in sorted(self.by_eligibility.items()):
                print(f"[Workflow 6 — Queue Policy]    eligibility={elig!r}: {cnt}")
        if self.by_pool:
            for pool, cnt in sorted(self.by_pool.items()):
                print(f"[Workflow 6 — Queue Policy]    pool={pool!r}: {cnt}")
        if self.error_count:
            print(f"[Workflow 6 — Queue Policy]  errors        : {self.error_count}")
