# Workflow 7.8 — Reply Intelligence: Rule-Based Reply Classifier
#
# Classifies an inbound ReplyRecord into one of the canonical reply types
# using a deterministic priority cascade.  No AI or probabilistic scoring.
#
# Priority order (safety-first):
#   1. unsubscribe      — explicit opt-out; must win over any positive language
#   2. hard_no          — explicit rejection
#   3. wrong_person     — contact routing issue
#   4. out_of_office    — standard OOO / vacation auto-response
#   5. auto_reply_other — non-OOO automated responses
#   6. request_quote    — pricing / proposal / MOQ request
#   7. request_info     — more details / catalog / specs request
#   8. forwarded        — recipient forwarded internally
#   9. positive_interest — direct interest / call request
#  10. soft_no          — deferral / timing-based decline
#  11. unknown          — nothing matched; manual review
#
# Why this order: high-risk stop categories must win first.
# A reply containing both "remove me" and "sounds interesting"
# must be treated as unsubscribe, not positive_interest.

import re
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Reply type constants
# ---------------------------------------------------------------------------

REPLY_TYPE_UNSUBSCRIBE       = "unsubscribe"
REPLY_TYPE_BOUNCE            = "bounce"
REPLY_TYPE_HARD_NO           = "hard_no"
REPLY_TYPE_WRONG_PERSON      = "wrong_person"
REPLY_TYPE_OUT_OF_OFFICE     = "out_of_office"
REPLY_TYPE_AUTO_REPLY_OTHER  = "auto_reply_other"
REPLY_TYPE_REQUEST_QUOTE     = "request_quote"
REPLY_TYPE_REQUEST_INFO      = "request_info"
REPLY_TYPE_FORWARDED         = "forwarded"
REPLY_TYPE_POSITIVE_INTEREST = "positive_interest"
REPLY_TYPE_SOFT_NO           = "soft_no"
REPLY_TYPE_UNKNOWN           = "unknown"

ALL_REPLY_TYPES = [
    REPLY_TYPE_BOUNCE,
    REPLY_TYPE_UNSUBSCRIBE,
    REPLY_TYPE_HARD_NO,
    REPLY_TYPE_WRONG_PERSON,
    REPLY_TYPE_OUT_OF_OFFICE,
    REPLY_TYPE_AUTO_REPLY_OTHER,
    REPLY_TYPE_REQUEST_QUOTE,
    REPLY_TYPE_REQUEST_INFO,
    REPLY_TYPE_FORWARDED,
    REPLY_TYPE_POSITIVE_INTEREST,
    REPLY_TYPE_SOFT_NO,
    REPLY_TYPE_UNKNOWN,
]

# Types that must block all follow-up (suppressed or handoff)
BLOCKING_REPLY_TYPES = {
    REPLY_TYPE_BOUNCE,
    REPLY_TYPE_UNSUBSCRIBE,
    REPLY_TYPE_HARD_NO,
    REPLY_TYPE_POSITIVE_INTEREST,
    REPLY_TYPE_REQUEST_INFO,
    REPLY_TYPE_REQUEST_QUOTE,
    REPLY_TYPE_FORWARDED,
}

# Types that pause follow-up (revisit later)
PAUSING_REPLY_TYPES = {
    REPLY_TYPE_WRONG_PERSON,
    REPLY_TYPE_SOFT_NO,
    REPLY_TYPE_OUT_OF_OFFICE,
    REPLY_TYPE_AUTO_REPLY_OTHER,
}


# ---------------------------------------------------------------------------
# Rule library — ordered by priority
# ---------------------------------------------------------------------------
# Each rule is a tuple of (reply_type, [trigger_phrases]).
# Matching is case-insensitive substring search on normalized combined text.
# The first rule whose ANY trigger phrase is found in the text wins.

_RULES: list[tuple[str, list[str]]] = [

    # 0. Bounce / DSN — delivery failure notifications must hard-stop future sends
    (REPLY_TYPE_BOUNCE, [
        "delivery status notification",
        "mail delivery failed",
        "delivery has failed",
        "delivery failed to these recipients",
        "undeliverable",
        "address not found",
        "recipient address rejected",
        "user unknown",
        "unknown user",
        "mailbox unavailable",
        "mailbox not found",
        "550 5.1.1",
        "550-5.1.1",
        "554 5.7.1",
        "dsn",
        "returned mail",
        "could not be delivered",
        "delivery incomplete",
    ]),

    # 1. Unsubscribe — explicit opt-out (highest priority for safety)
    (REPLY_TYPE_UNSUBSCRIBE, [
        "unsubscribe",
        "remove me from",
        "remove me from your",
        "remove my email",
        "take me off your",
        "take me off this",
        "stop emailing me",
        "stop emailing us",
        "stop sending me",
        "do not contact me",
        "do not email me",
        "do not email us",
        "please remove",
        "opt out",
        "opt-out",
        "i want to be removed",
        "please unsubscribe",
        "cease all communication",
        "please stop contacting",
    ]),

    # 2. Hard no — explicit rejection (must beat weak interest language)
    (REPLY_TYPE_HARD_NO, [
        "not interested",
        "not interested in",
        "no interest",
        "we are not interested",
        "i am not interested",
        "i'm not interested",
        "no thanks",
        "no thank you",
        "not for us",
        "not a fit",
        "please do not email",
        "please don't email",
        "don't contact",
        "do not contact",
        "we don't need",
        "we do not need",
        "this is not relevant",
        "not relevant to us",
        "not looking for",
        "we already have",
        "we're not looking",
        "we are not looking",
        "not in a position",
        "decline",
    ]),

    # 3. Wrong person — routing issue; contact should be paused
    # Trigger phrases must be specific to routing/misdirection to avoid false-positives.
    # Removed: "should contact" and "please contact" — too broad; would match
    # positive replies like "you should contact us" or "please contact our team".
    (REPLY_TYPE_WRONG_PERSON, [
        "wrong person",
        "not the right person",
        "not responsible for this",
        "i am not responsible for",
        "i'm not responsible for",
        "not my area",
        "not my department",
        "not my remit",
        "contact procurement",
        "contact purchasing",
        "contact our buyer",
        "contact our purchasing",
        "contact the procurement",
        "not the correct contact",
        "not the correct person",
        "you have the wrong",
        "you've got the wrong",
        "incorrect person",
        "wrong contact",
    ]),

    # 4. Out of office — standard vacation / leave response
    # Operationally distinct from auto_reply_other (type 5) even though both pause.
    # Keeping them separate allows future OOO-resume logic without schema changes.
    # TODO (future ticket): parse return date from OOO body and auto-unpause.
    (REPLY_TYPE_OUT_OF_OFFICE, [
        "out of office",
        "out-of-office",
        "on vacation",
        "on annual leave",
        "on parental leave",
        "on maternity leave",
        "on paternity leave",
        "on leave",
        "away from the office",
        "away from office",
        "i am currently away",
        "i'm currently away",
        "currently out",
        "will return",
        "back in the office",
        "back on",
        "i am out",
        "i'm out",
        "not in office",
    ]),

    # 5. Auto-reply (non-OOO) — automated acknowledgements, ticket systems, etc.
    # Intentionally separate from out_of_office so each can evolve independently.
    (REPLY_TYPE_AUTO_REPLY_OTHER, [
        "this is an automated",
        "this is an automatic",
        "automated response",
        "automatic reply",
        "do not reply to this email",
        "please do not reply to this",
        "this email was sent automatically",
        "support ticket has been received",
        "ticket has been created",
        "ticket #",
        "ticket id:",
        "thank you for contacting us",
        "thank you for reaching out to us",
        "thank you for your enquiry",
        "thank you for your inquiry",
        "we have received your",
        "our team will get back to you",
        "this mailbox is not monitored",
        "this inbox is not monitored",
        "no-reply",
        "noreply",
        "mailbox receipt",
        "auto-acknowledgement",
        "auto acknowledgement",
    ]),

    # 6. Request quote — pricing / proposal / commercial terms
    # Removed bare "proposal" (too broad — can appear in rejection context).
    # Removed would-be ambiguous single words; kept phrase-level patterns.
    (REPLY_TYPE_REQUEST_QUOTE, [
        "send us a quote",
        "provide a quote",
        "get a quote",
        "a quotation",
        "request a quotation",
        "request for quotation",
        "request for proposal",
        "rfq",
        "rfp",
        "price list",
        "price sheet",
        "pricing",
        "moq",
        "minimum order quantity",
        "minimum order",
        "lead time",
        "delivery time",
        "sample cost",
        "cost of",
        "how much does",
        "how much is",
        "what is the price",
        "what's the price",
        "what are your rates",
        "your pricing",
        "cost breakdown",
    ]),

    # 7. Request info — catalog / specs / more details
    (REPLY_TYPE_REQUEST_INFO, [
        "send details",
        "send us details",
        "send more details",
        "send catalog",
        "send catalogue",
        "send information",
        "send us information",
        "more information",
        "more info",
        "more details",
        "share specs",
        "send specs",
        "product information",
        "product details",
        "send brochure",
        "send a brochure",
        "tell me more",
        "can you share",
        "please share",
        "send over",
        "interested to know more",
    ]),

    # 8. Forwarded — recipient forwarded / looped in the right contact
    (REPLY_TYPE_FORWARDED, [
        "i have forwarded",
        "i've forwarded",
        "i forwarded",
        "forwarding this",
        "forwarded your email",
        "i have copied",
        "i've copied",
        "i copied",
        "looped in",
        "cc'd",
        "cc'ed",
        "cced",
        "introduced you to",
        "i am introducing",
        "i'm introducing",
        "please connect with",
        "i have included",
        "i've included",
        "copied the right person",
    ]),

    # 9. Positive interest — genuine interest in a call / discussion
    # Removed "tell me more" — it is already a trigger in request_info (rule 7)
    # and since request_info comes first in the cascade, "tell me more" here was dead code.
    (REPLY_TYPE_POSITIVE_INTEREST, [
        "sounds interesting",
        "sounds great",
        "let's talk",
        "lets talk",
        "let's have a call",
        "let's schedule",
        "happy to discuss",
        "happy to learn",
        "happy to connect",
        "happy to hop on",
        "we are interested",
        "i am interested",
        "i'm interested",
        "interested in",
        "would love to",
        "can we schedule",
        "can we set up a call",
        "available for a call",
        "open to a call",
        "when are you available",
        "please reach out",
        "yes, please",
        "yes please",
    ]),

    # 10. Soft no — deferral / timing-based; not a permanent rejection
    (REPLY_TYPE_SOFT_NO, [
        "not now",
        "not at this time",
        "maybe later",
        "try later",
        "next quarter",
        "next year",
        "circle back",
        "reach out later",
        "contact me later",
        "touch base later",
        "in a few months",
        "follow up later",
        "currently not",
        "not currently",
        "no budget",
        "budget freeze",
        "not in the budget",
        "on hold",
        "give us some time",
        "get back to you",
    ]),
]


# ---------------------------------------------------------------------------
# Classification result
# ---------------------------------------------------------------------------

class ClassificationResult(NamedTuple):
    reply_type:               str    # one of ALL_REPLY_TYPES
    classification_method:    str    # always "rule_based" in v1
    classification_confidence: float # v1 PLACEHOLDER — not a calibrated probability.
                                     # 1.0 means a trigger phrase was matched (deterministic hit).
                                     # 0.0 means no rule matched (unknown fallback).
                                     # Do NOT interpret as a probability score. Future tickets
                                     # may introduce a properly calibrated model and replace this.
    classification_reason:    str    # matched trigger phrase, "no_pattern_matched", or "empty_text"


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    return _WHITESPACE_RE.sub(" ", (text or "").lower().strip())


def _combined_text(reply) -> str:
    """
    Combine subject + snippet + body_text into one normalised string.
    Falls back gracefully if reply is a dict or has empty fields.
    """
    if hasattr(reply, "subject"):
        subject   = getattr(reply, "subject",   "") or ""
        snippet   = getattr(reply, "snippet",   "") or ""
        body_text = getattr(reply, "body_text", "") or ""
    elif isinstance(reply, dict):
        subject   = reply.get("subject",   "") or ""
        snippet   = reply.get("snippet",   "") or ""
        body_text = reply.get("body_text", "") or ""
    else:
        return ""

    # Combine with space separator; cap body_text to avoid O(n) blowup
    combined = f"{subject} {snippet} {body_text[:2000]}"
    return _normalize(combined)


# ---------------------------------------------------------------------------
# Core classifier
# ---------------------------------------------------------------------------

def classify_reply(reply) -> ClassificationResult:
    """
    Classify a ReplyRecord (or dict) into a canonical reply type.

    Uses a deterministic priority cascade: the first rule whose ANY trigger
    phrase appears in the combined normalised text wins.

    Returns a ClassificationResult namedtuple.
    """
    text = _combined_text(reply)
    from_email = ""
    if hasattr(reply, "from_email"):
        from_email = (getattr(reply, "from_email", "") or "").lower().strip()
    elif isinstance(reply, dict):
        from_email = (reply.get("from_email", "") or "").lower().strip()

    if not text.strip():
        # No text to classify — fail safe to unknown
        return ClassificationResult(
            reply_type               = REPLY_TYPE_UNKNOWN,
            classification_method    = "rule_based",
            classification_confidence= 0.0,
            classification_reason    = "empty_text",
        )

    # Sender-address heuristic for delivery-status messages
    if any(token in from_email for token in ("mailer-daemon", "postmaster")):
        return ClassificationResult(
            reply_type               = REPLY_TYPE_BOUNCE,
            classification_method    = "rule_based",
            classification_confidence= 1.0,
            classification_reason    = "bounce_sender_address",
        )

    for reply_type, triggers in _RULES:
        for trigger in triggers:
            if trigger in text:
                return ClassificationResult(
                    reply_type               = reply_type,
                    classification_method    = "rule_based",
                    classification_confidence= 1.0,
                    classification_reason    = trigger,
                )

    # Nothing matched — conservative unknown / manual review
    return ClassificationResult(
        reply_type               = REPLY_TYPE_UNKNOWN,
        classification_method    = "rule_based",
        classification_confidence= 0.0,
        classification_reason    = "no_pattern_matched",
    )


def apply_classification_to_reply(reply, result: ClassificationResult) -> None:
    """
    Write classification result fields onto a ReplyRecord in-place.
    No-ops silently if the record doesn't have the expected fields
    (forward-compatibility guard).
    """
    _set = lambda field, val: setattr(reply, field, val) if hasattr(reply, field) else None
    _set("reply_type",                result.reply_type)
    _set("classification_method",     result.classification_method)
    _set("classification_confidence", result.classification_confidence)
    _set("classification_reason",     result.classification_reason)
