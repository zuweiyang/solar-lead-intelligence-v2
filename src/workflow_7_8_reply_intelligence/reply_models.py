# Workflow 7.8 — Reply Intelligence: Data Model
#
# ReplyRecord is the canonical data structure for a single inbound reply event.
# Fields are populated in two stages:
#   1. Raw fields    — populated by reply_fetcher (Gmail metadata)
#   2. Match fields  — populated by reply_matcher (deterministic join to send_logs)
#
# TODO (Ticket 2): add reply_type, intent_confidence, suggested_action fields
#                  once reply classification is implemented.

from dataclasses import dataclass, asdict


@dataclass
class ReplyRecord:
    """
    A single inbound email reply event.

    Raw fields come from the Gmail API.
    Match fields are populated by deterministic matching against send_logs.csv.
    """

    # ----- Raw Gmail metadata -----
    timestamp: str = ""         # ISO 8601 UTC — when this record was parsed
    gmail_message_id: str = ""  # Gmail internal message ID (e.g. "19cff2dadb400b39")
    gmail_thread_id: str = ""   # Gmail thread ID — shared with outbound if same thread
    from_email: str = ""        # sender's email address (the prospect)
    from_name: str = ""         # sender's display name
    to_email: str = ""          # recipient (our email address)
    subject: str = ""           # email subject as received
    snippet: str = ""           # Gmail's preview snippet (~100 chars, always available)
    body_text: str = ""         # plain text body if extractable, else ""
    message_date: str = ""      # RFC 2822 Date header from the email
    in_reply_to: str = ""       # In-Reply-To header — Message-ID of the original email
    references: str = ""        # References header — full chain of Message-IDs

    # ----- Matching result -----
    matched: bool = False
    match_method: str = ""
    # match_method values (in priority order):
    #   "thread_id"     — outbound Gmail message ID found in the same thread
    #   "in_reply_to"   — In-Reply-To local-part matched a provider_message_id
    #   "references"    — References local-part matched a provider_message_id
    #   "email_subject" — kp_email + normalized subject matched a send_log row
    #   "email_recent"  — kp_email only (most recent send); manual_review_required=True
    #   ""              — no match found

    matched_send_log_row_id: str = ""  # timestamp+kp_email of matched row (for audit)
    matched_tracking_id: str = ""
    matched_campaign_id: str = ""
    matched_company_name: str = ""
    matched_kp_email: str = ""
    matched_place_id: str = ""
    manual_review_required: bool = False
    match_error: str = ""

    # ----- Operational metadata -----
    logged_at: str = ""         # ISO 8601 UTC — when persisted to CSV/DB

    # ----- Classification (Ticket 2) -----
    reply_type: str = ""                  # bounce | unsubscribe | hard_no | wrong_person | out_of_office |
                                          # auto_reply_other | request_quote | request_info |
                                          # forwarded | positive_interest | soft_no | unknown
    classification_method: str = ""       # "rule_based" | "manual" | ""
    classification_confidence: float = 0.0
    classification_reason: str = ""       # matched trigger phrase or "no_pattern_matched"

    # ----- Operational state (Ticket 2) -----
    suppression_status: str = ""          # none | paused | suppressed | handoff_to_human
    followup_paused: bool = False
    alternate_contact_review_required: bool = False  # wrong_person flag for reroute review

    def to_csv_row(self) -> dict:
        """Return a flat dict for CSV writing. Booleans serialized as 'true'/'false'."""
        d = asdict(self)
        d["matched"]                           = "true" if d["matched"] else "false"
        d["manual_review_required"]            = "true" if d["manual_review_required"] else "false"
        d["followup_paused"]                   = "true" if d["followup_paused"] else "false"
        d["alternate_contact_review_required"] = "true" if d["alternate_contact_review_required"] else "false"
        return d

    @classmethod
    def from_csv_row(cls, row: dict) -> "ReplyRecord":
        """Reconstruct a ReplyRecord from a CSV DictReader row."""
        # String fields — use get with "" default for backward compatibility
        str_fields = [f for f in CSV_FIELDS if f not in (
            "matched", "manual_review_required",
            "followup_paused", "alternate_contact_review_required",
            "classification_confidence",
        )]
        kwargs: dict = {k: row.get(k, "") for k in str_fields}

        # Bool fields
        kwargs["matched"]                           = str(row.get("matched", "")).lower() == "true"
        kwargs["manual_review_required"]            = str(row.get("manual_review_required", "")).lower() == "true"
        kwargs["followup_paused"]                   = str(row.get("followup_paused", "")).lower() == "true"
        kwargs["alternate_contact_review_required"] = str(row.get("alternate_contact_review_required", "")).lower() == "true"

        # Float field
        try:
            kwargs["classification_confidence"] = float(row.get("classification_confidence", 0.0) or 0.0)
        except (ValueError, TypeError):
            kwargs["classification_confidence"] = 0.0

        return cls(**kwargs)


# Canonical field order for CSV serialization — must match ReplyRecord field declaration order.
CSV_FIELDS = [
    "timestamp",
    "gmail_message_id",
    "gmail_thread_id",
    "from_email",
    "from_name",
    "to_email",
    "subject",
    "snippet",
    "body_text",
    "message_date",
    "in_reply_to",
    "references",
    "matched",
    "match_method",
    "matched_send_log_row_id",
    "matched_tracking_id",
    "matched_campaign_id",
    "matched_company_name",
    "matched_kp_email",
    "matched_place_id",
    "manual_review_required",
    "match_error",
    # Ticket 2 — classification
    "reply_type",
    "classification_method",
    "classification_confidence",
    "classification_reason",
    # Ticket 2 — operational state
    "suppression_status",
    "followup_paused",
    "alternate_contact_review_required",
    "logged_at",
]
