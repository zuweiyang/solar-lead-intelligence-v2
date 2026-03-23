# Workflow 5.6 — Contact Scoring + Priority Selection (P1-2B)
# Deterministic scoring rules: title, source, email quality, generic penalty.
#
# All functions are pure (no I/O, no API calls, no LLM).
# Every scoring decision is logged to a breakdown string for auditability.

from __future__ import annotations

from __future__ import annotations

from src.workflow_5_6_contact_scoring.contact_scoring_models import ScoredContact


# ---------------------------------------------------------------------------
# Title scoring constants
# ---------------------------------------------------------------------------

# Fragments that earn the highest title score.
# These indicate direct procurement / ownership authority.
_TITLE_TIER_A: frozenset[str] = frozenset({
    "procurement", "purchasing", "sourcing",
    "owner", "founder", "co-founder",
    "ceo", "chief executive",
    "managing director", "md",
})

# Strong operational / decision-maker roles — good outbound targets.
_TITLE_TIER_B: frozenset[str] = frozenset({
    "president", "partner",
    "director", "head of",
    "general manager", "country manager", "regional manager",
    "chief operating",
    "operations manager", "operations director",
    "commercial manager", "commercial director",
    "business development",
    "project director",
})

# Solid but slightly less certain signals — technical / project roles.
_TITLE_TIER_C: frozenset[str] = frozenset({
    "project manager", "engineering manager", "technical manager",
    "technical director", "chief technical", "cto",
    "principal engineer", "principal consultant",
    "installation manager",
})

# Weak signals — title is present but role is not a clear procurement target.
_TITLE_TIER_D: frozenset[str] = frozenset({
    "manager", "executive", "consultant", "specialist",
    "coordinator", "officer", "analyst",
    "supervisor", "lead",
})

# Score values per tier
_TITLE_SCORES: dict[str, int] = {
    "tier_A": 40,
    "tier_B": 30,
    "tier_C": 20,
    "tier_D": 10,
    "empty":   0,
    "unknown": 5,  # title present but unrecognised — better than empty
}


def _classify_title(title: str) -> str:
    """Return tier key for a contact title."""
    if not title or not title.strip():
        return "empty"
    lower = title.lower()
    if any(f in lower for f in _TITLE_TIER_A):
        return "tier_A"
    if any(f in lower for f in _TITLE_TIER_B):
        return "tier_B"
    if any(f in lower for f in _TITLE_TIER_C):
        return "tier_C"
    if any(f in lower for f in _TITLE_TIER_D):
        return "tier_D"
    return "unknown"


def score_title(contact: ScoredContact) -> tuple[int, str]:
    """
    Return (title_score, breakdown_note).

    Scoring tiers:
      tier_A (procurement/owner/CEO/MD) → 40
      tier_B (director/GM/operations/BD) → 30
      tier_C (project/engineering/technical mgr) → 20
      tier_D (generic manager/exec/specialist) → 10
      unknown (non-empty but unrecognised)   →  5
      empty                                  →  0
    """
    tier = _classify_title(contact.kp_title)
    score = _TITLE_SCORES[tier]
    note = f"title={tier}({score})"
    return score, note


# ---------------------------------------------------------------------------
# Source scoring constants
# ---------------------------------------------------------------------------

# Higher = more reliable person record.
_SOURCE_SCORES: dict[str, int] = {
    "apollo":   20,   # structured people-search; strongest person signal
    "hunter":   18,   # domain-verified; high reliability
    "website":  10,   # scraped from site — real but unverified person
    "guessed":   3,   # role-address pattern (info@, sales@) — weakest named
    "mock":      0,   # smoke-test only
    "none":      0,   # no enrichment
    "":          0,
}


def score_source(contact: ScoredContact) -> tuple[int, str]:
    """
    Return (source_score, breakdown_note).

    apollo(20) > hunter(18) > website(10) > guessed(3) > mock/none/empty(0)
    """
    src = (contact.enrichment_source or "").strip().lower()
    score = _SOURCE_SCORES.get(src, 5)  # 5 for unrecognised sources
    note = f"source={src}({score})"
    return score, note


# ---------------------------------------------------------------------------
# Email quality scoring (Ticket-3 verification tiers)
# ---------------------------------------------------------------------------

# Maps verification tier to score bonus.
# Gracefully degrades: missing or empty tier → 0 (no bonus, no penalty).
_EMAIL_TIER_SCORES: dict[str, int] = {
    "e1": 20,   # verified deliverable + SMTP check → safest to send
    "e2": 12,   # risky / webmail / no SMTP check → usable but lower confidence
    "e3":  5,   # catch-all or unknown — risky
    "e4":  3,   # generic role mailbox (info@) per verification — very risky
    "e0":  0,   # invalid / undeliverable — no bonus (also penalised by generic logic)
    "":    8,   # verification not run — assume moderate quality
}


def score_email_quality(contact: ScoredContact) -> tuple[int, str]:
    """
    Return (email_quality_score, breakdown_note).

    Uses email_confidence_tier if available (Ticket 3 output).
    Falls back to 8/20 when verification has not been run.

    Generic mailboxes already receive a separate penalty; this function
    scores the email quality independently of genericness.
    """
    tier = (contact.email_confidence_tier or "").strip().lower()
    score = _EMAIL_TIER_SCORES.get(tier, 8)
    source = "verified" if tier else "unverified"
    note = f"email_quality={tier or 'no_tier'}({score},{source})"
    return score, note


# ---------------------------------------------------------------------------
# Generic mailbox penalty
# ---------------------------------------------------------------------------

_GENERIC_PENALTY = -25


def score_generic_penalty(contact: ScoredContact) -> tuple[int, str]:
    """
    Return (penalty, breakdown_note).  Penalty is negative (or 0).

    Generic mailboxes (info@, sales@, etc.) are valid fallbacks but should
    rank below named contacts.  The penalty pushes them to the bottom of the
    ladder without discarding them.
    """
    is_generic = str(contact.is_generic_mailbox).lower().strip() == "true"
    penalty = _GENERIC_PENALTY if is_generic else 0
    note = f"generic_penalty={penalty}" if is_generic else "generic_penalty=0"
    return penalty, note


# ---------------------------------------------------------------------------
# Junk / platform email hard penalty
# ---------------------------------------------------------------------------

# Addresses that are website-builder artifacts or CMS placeholders — these
# are never valid outreach targets.  Apply a large negative penalty so they
# never surface as primary contacts even when no better option exists.
_JUNK_EMAIL_DOMAINS: frozenset[str] = frozenset({
    "wix.com", "godaddy.com", "squarespace.com", "weebly.com",
    "jimdo.com", "yola.com", "webnode.com", "site123.com", "strikingly.com",
})

_JUNK_LOCALPART_EXACT: frozenset[str] = frozenset({
    "filler", "placeholder", "noreply", "no-reply", "donotreply",
    "do-not-reply", "mailer-daemon", "postmaster", "abuse", "webmaster",
})

_JUNK_LOCALPART_PREFIX: tuple[str, ...] = ("wixofday", "filler")

_JUNK_EMAIL_PENALTY = -100   # pushes well below any legitimate contact


def score_junk_email_penalty(contact: ScoredContact) -> tuple[int, str]:
    """
    Return (penalty, breakdown_note).

    Returns -100 for known website-builder / filler / no-reply addresses.
    Returns 0 for all other addresses.
    """
    email = (contact.kp_email or "").lower().strip()
    if not email or "@" not in email:
        return 0, "junk_penalty=0"

    local, _, domain = email.partition("@")

    if domain in _JUNK_EMAIL_DOMAINS:
        return _JUNK_EMAIL_PENALTY, f"junk_penalty={_JUNK_EMAIL_PENALTY}(domain:{domain})"

    if local in _JUNK_LOCALPART_EXACT:
        return _JUNK_EMAIL_PENALTY, f"junk_penalty={_JUNK_EMAIL_PENALTY}(localpart:{local})"

    for prefix in _JUNK_LOCALPART_PREFIX:
        if local.startswith(prefix):
            return _JUNK_EMAIL_PENALTY, f"junk_penalty={_JUNK_EMAIL_PENALTY}(prefix:{prefix})"

    return 0, "junk_penalty=0"


# ---------------------------------------------------------------------------
# Final fit score
# ---------------------------------------------------------------------------

def compute_contact_fit_score(contact: ScoredContact) -> ScoredContact:
    """
    Populate all scoring fields on `contact` in-place and return it.

    contact_fit_score = title_score + source_score + email_quality_score
                        + generic_penalty + junk_email_penalty
    Floored at 0.

    Breakdown string format:
      "title=tier_B(30) | source=hunter(18) | email_quality=e1(20,verified) |
       generic_penalty=0 | junk_penalty=0"
    """
    t_score, t_note  = score_title(contact)
    s_score, s_note  = score_source(contact)
    eq_score, eq_note = score_email_quality(contact)
    g_penalty, g_note = score_generic_penalty(contact)
    j_penalty, j_note = score_junk_email_penalty(contact)

    raw   = t_score + s_score + eq_score + g_penalty + j_penalty
    final = max(0, raw)

    contact.title_score             = t_score
    contact.source_score            = s_score
    contact.email_quality_score     = eq_score
    contact.generic_penalty         = g_penalty
    contact.contact_fit_score       = final
    contact.contact_score_breakdown = " | ".join([t_note, s_note, eq_note, g_note, j_note])

    return contact


# ---------------------------------------------------------------------------
# Title bucket helper (for summary reporting)
# ---------------------------------------------------------------------------

def title_bucket(title: str) -> str:
    """Return a human-readable title classification bucket for summary reports."""
    tier = _classify_title(title)
    return {
        "tier_A": "procurement/owner/CEO",
        "tier_B": "director/GM/operations",
        "tier_C": "project/technical/engineering",
        "tier_D": "generic_manager/specialist",
        "unknown": "other_named",
        "empty": "no_title",
    }.get(tier, tier)


# ---------------------------------------------------------------------------
# Priority selection
# ---------------------------------------------------------------------------

def _sort_key(contact: ScoredContact) -> tuple:
    """
    Stable tie-break sort key for a single contact.
    Lower tuple value = better ranking (sort ascending, then reverse for priority).

    Tie-break order (applied left to right):
      1. contact_fit_score        — higher is better (negate for ascending sort)
      2. is_generic_mailbox       — non-generic beats generic (False < True)
      3. title_score              — higher is better
      4. source_score             — higher is better
      5. email_quality_score      — higher is better
      6. contact_rank (P1-2A)     — lower original rank is better (1 > 2 > 3)
      7. kp_name lexical          — stable deterministic tiebreak
    """
    is_generic = str(contact.is_generic_mailbox).lower().strip() == "true"
    original_rank = int(contact.contact_rank or 99)
    return (
        -contact.contact_fit_score,   # negate so higher score sorts first
         is_generic,                  # False(0) before True(1)
        -contact.title_score,
        -contact.source_score,
        -contact.email_quality_score,
         original_rank,
        (contact.kp_name or "").lower(),
    )


def assign_priority(contacts: list[ScoredContact]) -> list[ScoredContact]:
    """
    Sort contacts for one company and assign all selection fields.

    Returns the same list, mutated in-place, sorted best-first.

    Selection semantics:
      contact_priority_rank = 1 … N  (1 = best)
      contact_priority_bucket:
        "primary"          — rank 1 and is the only contact, or rank 1 named contact
        "fallback"         — rank 2+ named/structured contacts
        "generic_fallback" — generic mailbox, not primary
      is_primary_contact  = True for rank 1 only
      is_fallback_contact = True for rank 2+
      alternate_contact_review_candidate = True for rank 2+ when is_generic_mailbox=False
        (named fallbacks are useful wrong-person reroute candidates)
    """
    if not contacts:
        return contacts

    sorted_contacts = sorted(contacts, key=_sort_key)

    for i, contact in enumerate(sorted_contacts):
        rank = i + 1
        is_generic = str(contact.is_generic_mailbox).lower().strip() == "true"
        is_primary = (rank == 1)
        is_fallback = (rank > 1)

        contact.contact_priority_rank = rank
        contact.is_primary_contact    = is_primary
        contact.is_fallback_contact   = is_fallback

        if is_primary:
            contact.contact_priority_bucket = "primary"
        elif is_generic:
            contact.contact_priority_bucket = "generic_fallback"
        else:
            contact.contact_priority_bucket = "fallback"

        # Flag named fallbacks as candidates for wrong-person reroute review.
        # Generic fallbacks are not useful as reroute candidates — they are
        # role aliases, not specific individuals.
        contact.alternate_contact_review_candidate = (
            is_fallback and not is_generic
        )

        # Build selection reason
        primary_tag = "primary" if is_primary else f"fallback-{rank}"
        generic_tag = " [generic]" if is_generic else ""
        contact.contact_selection_reason = (
            f"{primary_tag}{generic_tag}; "
            f"fit={contact.contact_fit_score} "
            f"(title={contact.title_score},"
            f"src={contact.source_score},"
            f"email={contact.email_quality_score},"
            f"generic_pen={contact.generic_penalty})"
        )

    return sorted_contacts
