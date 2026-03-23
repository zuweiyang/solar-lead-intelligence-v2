# Workflow 7.8 — Reply Intelligence: Operational State Manager
#
# Maps classified reply types to operational state transitions.
# Defines what the system should DO once a reply is classified.
#
# State model:
#   suppression_status   — none | paused | suppressed | handoff_to_human
#   followup_paused      — bool: True means Workflow 8 must not schedule
#   alternate_contact_review_required — bool: True for wrong_person replies
#
# Policy:
#   - Safety first: suppressed contacts must not receive further automation
#   - Handoff: positive / quote / info replies go to human handling
#   - Pause: wrong_person / soft_no / OOO / auto_reply_other are temporary holds
#   - Unknown: paused + manual_review_required — uncertainty must not permit automation
#
# out_of_office vs auto_reply_other are kept operationally distinct
# even though both map to paused in v1.  Their stored reply_type differs
# so future tickets can implement OOO-specific resume logic without a schema change.
# TODO (future ticket): implement OOO resume — unpause automatically when the
#   contact's stated return date has passed, based on body_text / snippet parsing.
#
# Ticket 2 does NOT implement automatic contact switching.
# alternate_contact_review_required=True is a flag only — no send execution.

from src.workflow_7_8_reply_intelligence.reply_classifier import (
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
)

# ---------------------------------------------------------------------------
# Suppression status constants
# ---------------------------------------------------------------------------

SUPPRESSION_NONE          = "none"
SUPPRESSION_PAUSED        = "paused"
SUPPRESSION_SUPPRESSED    = "suppressed"
SUPPRESSION_HANDOFF       = "handoff_to_human"

# Priority ordering for "worst wins" deduplication across multiple replies
_SUPPRESSION_RANK = {
    "":                     0,
    SUPPRESSION_NONE:       1,
    SUPPRESSION_PAUSED:     2,
    SUPPRESSION_HANDOFF:    3,
    SUPPRESSION_SUPPRESSED: 4,   # suppressed is final — once suppressed, stays suppressed
}


def suppression_rank(status: str) -> int:
    """Return severity rank for a suppression status string."""
    return _SUPPRESSION_RANK.get(status, 0)


def worst_suppression(a: str, b: str) -> str:
    """Return the more restrictive of two suppression statuses."""
    return a if suppression_rank(a) >= suppression_rank(b) else b


# ---------------------------------------------------------------------------
# State transition table
# ---------------------------------------------------------------------------
# Each entry: (suppression_status, followup_paused, alternate_contact_review_required)

_STATE_TABLE: dict[str, tuple[str, bool, bool]] = {
    REPLY_TYPE_BOUNCE:            (SUPPRESSION_SUPPRESSED, True,  False),
    REPLY_TYPE_UNSUBSCRIBE:       (SUPPRESSION_SUPPRESSED, True,  False),
    REPLY_TYPE_HARD_NO:           (SUPPRESSION_SUPPRESSED, True,  False),
    REPLY_TYPE_POSITIVE_INTEREST: (SUPPRESSION_HANDOFF,    True,  False),
    REPLY_TYPE_REQUEST_INFO:      (SUPPRESSION_HANDOFF,    True,  False),
    REPLY_TYPE_REQUEST_QUOTE:     (SUPPRESSION_HANDOFF,    True,  False),
    REPLY_TYPE_FORWARDED:         (SUPPRESSION_HANDOFF,    True,  False),
    REPLY_TYPE_WRONG_PERSON:      (SUPPRESSION_PAUSED,     True,  True),   # flag for reroute review
    REPLY_TYPE_SOFT_NO:           (SUPPRESSION_PAUSED,     True,  False),
    REPLY_TYPE_OUT_OF_OFFICE:     (SUPPRESSION_PAUSED,     True,  False),  # OOO: distinct from auto_reply_other
    REPLY_TYPE_AUTO_REPLY_OTHER:  (SUPPRESSION_PAUSED,     True,  False),  # auto: distinct from OOO
    # unknown — pause follow-up AND require manual review; uncertainty must not permit automation
    REPLY_TYPE_UNKNOWN:           (SUPPRESSION_PAUSED,     True,  False),
}


# ---------------------------------------------------------------------------
# State derivation
# ---------------------------------------------------------------------------

class ReplyState:
    """Operational state derived from a classified reply."""

    __slots__ = (
        "suppression_status",
        "followup_paused",
        "alternate_contact_review_required",
        "manual_review_required",
    )

    def __init__(
        self,
        suppression_status:                 str,
        followup_paused:                    bool,
        alternate_contact_review_required:  bool,
        manual_review_required:             bool,
    ) -> None:
        self.suppression_status                = suppression_status
        self.followup_paused                   = followup_paused
        self.alternate_contact_review_required = alternate_contact_review_required
        self.manual_review_required            = manual_review_required

    def __repr__(self) -> str:
        return (
            f"ReplyState(suppression={self.suppression_status!r}, "
            f"paused={self.followup_paused}, "
            f"reroute={self.alternate_contact_review_required}, "
            f"manual_review={self.manual_review_required})"
        )


def derive_state(reply_type: str) -> ReplyState:
    """
    Derive the operational state for a given reply type.

    Unknown replies are conservative: follow-up is paused and
    manual_review_required is set True so operators can inspect.
    Uncertainty must not result in continued automation.
    All other types follow the _STATE_TABLE policy above.
    """
    if reply_type not in _STATE_TABLE:
        # Unknown or unexpected type — failsafe to no automation
        return ReplyState(
            suppression_status                = SUPPRESSION_NONE,
            followup_paused                   = False,
            alternate_contact_review_required = False,
            manual_review_required            = True,
        )

    sup_status, paused, reroute = _STATE_TABLE[reply_type]
    manual_review = (reply_type == REPLY_TYPE_UNKNOWN)

    return ReplyState(
        suppression_status                = sup_status,
        followup_paused                   = paused,
        alternate_contact_review_required = reroute,
        manual_review_required            = manual_review,
    )


def apply_state_to_reply(reply, state: ReplyState) -> None:
    """
    Write state fields onto a ReplyRecord in-place.
    Uses setattr with hasattr guard for forward compatibility.
    Also propagates manual_review_required only if not already True from matching.
    """
    _set = lambda field, val: setattr(reply, field, val) if hasattr(reply, field) else None

    _set("suppression_status",                state.suppression_status)
    _set("followup_paused",                   state.followup_paused)
    _set("alternate_contact_review_required", state.alternate_contact_review_required)

    # Only upgrade manual_review_required — never downgrade it
    # (match-level manual_review_required=True from Ticket 1 must be preserved)
    if state.manual_review_required:
        _set("manual_review_required", True)
