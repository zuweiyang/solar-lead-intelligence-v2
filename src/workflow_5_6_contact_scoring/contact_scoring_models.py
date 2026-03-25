# Workflow 5.6 — Contact Scoring + Priority Selection (P1-2B)
# Data models: scored contact record, output field lists, summary counters.

from __future__ import annotations

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Scoring version
# ---------------------------------------------------------------------------

CONTACT_SCORING_VERSION = "v1_deterministic"


# ---------------------------------------------------------------------------
# Output field lists
# ---------------------------------------------------------------------------

# All fields written to scored_contacts.csv.
# Superset of ENRICHED_CONTACTS_FIELDS — adds scoring + selection columns.
SCORED_CONTACTS_FIELDS: list[str] = [
    # ── Identity (pass-through from enriched_contacts) ──────────────────────
    "company_name", "website", "place_id",
    "company_type", "market_focus", "services_detected",
    "confidence_score", "classification_method",
    "lead_score", "score_breakdown", "target_tier",
    # ── Contact info ─────────────────────────────────────────────────────────
    "kp_name", "kp_title", "kp_email", "enrichment_source",
    "site_phone",
    "whatsapp_phone",
    "email_sendable", "contact_channel", "alt_outreach_possible",
    "contact_trust", "skip_reason",
    # ── P1-2A rank / generic flag ─────────────────────────────────────────────
    "contact_rank",        # original P1-2A rank (1/2/3)
    "is_generic_mailbox",  # "true" / "false"
    # ── Verification fields (optional — present when 5.9 has run) ────────────
    "email_confidence_tier",   # E0–E4 or ""
    "send_eligibility",        # allow / block / hold / …  or ""
    "send_pool",               # primary_pool / blocked_pool / …  or ""
    # ── P1-2B scoring components ──────────────────────────────────────────────
    "title_score",             # 0–40
    "source_score",            # 0–20
    "email_quality_score",     # 0–20
    "generic_penalty",         # 0 or −25 (stored as signed int, serialised as str)
    "contact_fit_score",       # final weighted sum, floored at 0
    "contact_scoring_version", # e.g. "v1_deterministic"
    # ── P1-2B selection fields ────────────────────────────────────────────────
    "contact_priority_rank",             # 1 = best, 2 = second-best, …
    "contact_priority_bucket",           # primary / fallback / generic_fallback
    "is_primary_contact",                # "true" / "false"
    "is_fallback_contact",               # "true" / "false"
    "alternate_contact_review_candidate", # "true" / "false"
    # ── Explainability ────────────────────────────────────────────────────────
    "contact_selection_reason",  # human-readable one-liner
    "contact_score_breakdown",   # pipe-separated breakdown string
]


# ---------------------------------------------------------------------------
# Scored contact dataclass
# ---------------------------------------------------------------------------

@dataclass
class ScoredContact:
    """
    Holds all scoring and selection outputs for one contact.

    All fields have safe defaults so partial data never causes AttributeError.
    """
    # Pass-through identity fields (populated from enriched_contacts row)
    company_name:            str = ""
    website:                 str = ""
    place_id:                str = ""
    company_type:            str = ""
    market_focus:            str = ""
    services_detected:       str = ""
    confidence_score:        str = ""
    classification_method:   str = ""
    lead_score:              str = ""
    score_breakdown:         str = ""
    target_tier:             str = ""
    kp_name:                 str = ""
    kp_title:                str = ""
    kp_email:                str = ""
    enrichment_source:       str = ""
    site_phone:              str = ""
    whatsapp_phone:          str = ""
    email_sendable:          str = ""
    contact_channel:         str = ""
    alt_outreach_possible:   str = ""
    contact_trust:           str = ""
    skip_reason:             str = ""
    contact_rank:            str = ""
    is_generic_mailbox:      str = "false"
    # Verification fields (optional)
    email_confidence_tier:   str = ""
    send_eligibility:        str = ""
    send_pool:               str = ""
    # Scoring components (integers for computation, serialised to str for CSV)
    title_score:             int = 0
    source_score:            int = 0
    email_quality_score:     int = 0
    generic_penalty:         int = 0   # 0 or −25
    contact_fit_score:       int = 0
    contact_scoring_version: str = CONTACT_SCORING_VERSION
    # Selection fields
    contact_priority_rank:               int  = 0
    contact_priority_bucket:             str  = ""
    is_primary_contact:                  bool = False
    is_fallback_contact:                 bool = False
    alternate_contact_review_candidate:  bool = False
    # Explainability
    contact_selection_reason: str = ""
    contact_score_breakdown:  str = ""

    def to_csv_row(self) -> dict:
        """Serialise for csv.DictWriter — booleans as lowercase strings."""
        row = {}
        for f in SCORED_CONTACTS_FIELDS:
            val = getattr(self, f, "")
            if isinstance(val, bool):
                row[f] = "true" if val else "false"
            else:
                row[f] = val
        return row


# ---------------------------------------------------------------------------
# Summary counters
# ---------------------------------------------------------------------------

@dataclass
class ContactScoringStats:
    total_companies:          int = 0
    total_contacts:           int = 0
    primary_selected:         int = 0
    fallback_contacts:        int = 0
    generic_as_primary:       int = 0
    named_as_primary:         int = 0
    zero_contact_companies:   int = 0
    errors:                   int = 0
    title_bucket_counts:      dict = field(default_factory=dict)

    def record_title(self, bucket: str) -> None:
        self.title_bucket_counts[bucket] = (
            self.title_bucket_counts.get(bucket, 0) + 1
        )

    def avg_contacts(self) -> float:
        if self.total_companies == 0:
            return 0.0
        return self.total_contacts / self.total_companies

    def print_summary(self) -> None:
        print(
            f"\n[Workflow 5.6] Contact Scoring Summary:\n"
            f"  Companies processed        : {self.total_companies}\n"
            f"  Total contacts scored      : {self.total_contacts}\n"
            f"  Avg contacts / company     : {self.avg_contacts():.1f}\n"
            f"  Primary contacts selected  : {self.primary_selected}\n"
            f"  Fallback contacts retained : {self.fallback_contacts}\n"
            f"  Generic used as primary    : {self.generic_as_primary}\n"
            f"  Named used as primary      : {self.named_as_primary}\n"
            f"  Zero-contact companies     : {self.zero_contact_companies}\n"
            f"  Errors                     : {self.errors}"
        )
        if self.title_bucket_counts:
            print("\n  Title bucket distribution:")
            for bucket, cnt in sorted(
                self.title_bucket_counts.items(), key=lambda x: -x[1]
            ):
                print(f"    {bucket:<28}: {cnt}")
        print()
