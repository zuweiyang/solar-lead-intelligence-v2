# Workflow 6.5: Email Quality Scoring — Rule-Based Scoring Engine

import re

SPAM_WORDS = [
    "free", "guarantee", "best price", "act now", "urgent",
    "limited time", "amazing", "revolutionary", "exclusive offer",
]

BUZZWORDS = [
    "industry-leading", "world-class", "cutting-edge", "game-changer",
    "synergy", "leverage", "disruptive", "innovative solution",
]

GENERIC_OPENINGS = [
    "i hope this finds you well",
    "i wanted to reach out",
    "i am writing to",
    "my name is",
]

ANGLE_KEYWORDS: dict[str, list[str]] = {
    "Mention battery storage support":                    ["battery", "storage", "bess", "backup"],
    "Mention commercial installation scalability":        ["commercial", "industrial", "rooftop"],
    "Mention support for larger-scale project execution": ["utility", "scale", "megawatt", "project execution"],
    "Mention support for growing installation teams":     ["growing", "team", "hiring", "install"],
    "Mention support for residential solar operations":   ["residential", "home", "homeowner"],
}

_PLACEHOLDER_RE = re.compile(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}")
_ALL_CAPS_RE    = re.compile(r"\b[A-Z]{4,}\b")

# Quantified claims that require supporting evidence (e.g. "reduce... by 30%", "3x faster").
# Matches: bare percentages, ROI mentions, reduction/increase/save + number, multipliers.
_QUANTIFIED_CLAIM_RE = re.compile(
    r"\d+\s*%"
    r"|\d+\s*percent"
    r"|\b(roi|return on investment)\b"
    r"|reduce\w*\s+\w+\s+by\s+\d"
    r"|increase\w*\s+\w+\s+by\s+\d"
    r"|save\w*\s+\d"
    r"|\d+x\s+(faster|more|better|cheaper)",
    re.IGNORECASE,
)


def _has_placeholder(text: str) -> bool:
    return bool(_PLACEHOLDER_RE.search(text))


def score_personalization(record: dict) -> int:
    score = 60
    company  = record.get("company_name", "").lower()
    subject  = record.get("subject", "").lower()
    opening  = record.get("opening_line", "").lower()
    body     = record.get("email_body", "").lower()
    kp_name  = record.get("kp_name", "").strip()
    c_type   = record.get("company_type", "").lower()
    market   = record.get("market_focus", "").lower()

    if company and (company in subject or company in opening):
        score += 15

    if kp_name and kp_name.lower() in body:
        score += 10

    if opening and (c_type and c_type in opening) or (market and market in opening):
        if not (company and opening.startswith(company)):
            score += 10

    if any(pat in opening for pat in GENERIC_OPENINGS):
        score -= 20

    if subject and not company in subject and not any(
        kw in subject for kws in ANGLE_KEYWORDS.values() for kw in kws
    ):
        score -= 15

    if company and body.count(company) >= 3:
        score -= 10

    full_text = subject + " " + body
    if _has_placeholder(full_text):
        score -= 10

    return max(0, min(100, score))


def score_relevance(record: dict) -> int:
    score = 60
    angle  = record.get("email_angle", "")
    body   = record.get("email_body", "").lower()
    c_type = record.get("company_type", "").lower()

    angle_kws = ANGLE_KEYWORDS.get(angle, [])
    if angle_kws:
        if any(kw in body for kw in angle_kws):
            score += 30
        else:
            score -= 20

    if c_type and any(word in body for word in c_type.split()):
        score += 10

    if c_type == "residential" and ("utility" in body or "megawatt" in body):
        score -= 10

    if c_type == "utility" and ("residential" in body or "homeowner" in body):
        score -= 10

    return max(0, min(100, score))


def score_spam_risk(record: dict) -> int:
    score  = 0
    subject = record.get("subject", "").lower()
    body    = record.get("email_body", "").lower()
    full    = subject + " " + body

    spam_hits = sum(1 for w in SPAM_WORDS if w in full)
    score += min(spam_hits * 15, 45)

    buzz_hits = sum(1 for w in BUZZWORDS if w in full)
    score += min(buzz_hits * 10, 30)

    if len(subject.split()) > 8:
        score += 10

    body_wc = len(body.split())
    if body_wc > 180:
        score += 10
    if body_wc < 40:
        score += 15

    if body.count("!") > 1:
        score += 5

    raw_full = record.get("subject", "") + " " + record.get("email_body", "")
    if _ALL_CAPS_RE.search(raw_full):
        score += 10

    return max(0, min(100, score))


def compute_overall_score(personalization: int, relevance: int, spam_risk: int) -> int:
    return round(personalization * 0.4 + relevance * 0.4 + (100 - spam_risk) * 0.2)


def determine_approval_status(
    overall: int, spam_risk: int, record: dict
) -> tuple[str, list[str]]:
    notes: list[str] = []
    hard_reject = False

    kp_email = record.get("kp_email", "")
    subject  = record.get("subject", "")
    body     = record.get("email_body", "")
    full     = subject + " " + body

    if not kp_email or "@" not in kp_email:
        notes.append("Missing contact email")
        hard_reject = True
    if not subject.strip():
        notes.append("Empty subject line")
        hard_reject = True
    if not body.strip():
        notes.append("Empty email body")
        hard_reject = True
    if _has_placeholder(full):
        notes.append("Unresolved placeholder detected")
        hard_reject = True

    if hard_reject:
        return "rejected", notes

    # Quantified claims require supporting evidence — force manual review even if score is good
    if _QUANTIFIED_CLAIM_RE.search(full):
        notes.append("unsupported_quantified_claim")

    if overall < 60:
        notes.append(f"Low overall score ({overall})")
    if spam_risk > 55:
        notes.append(f"High spam risk ({spam_risk})")

    p_score = compute_overall_score.__doc__  # unused; access scores via caller
    body_wc = len(body.split())
    if body_wc < 40:
        notes.append("Email body too short")
    if body_wc > 180:
        notes.append("Email body too long")

    # Derive personalization/relevance to add context notes
    pers = record.get("_personalization_score", 0)
    rel  = record.get("_relevance_score", 0)
    if pers < 50:
        notes.append("Opening too generic")
    if rel < 50:
        notes.append("Body mismatched with email angle")

    if overall < 60 or spam_risk > 55 or hard_reject:
        return "rejected", notes
    if overall < 75 or spam_risk > 35 or "unsupported_quantified_claim" in notes:
        return "manual_review", notes
    return "approved", notes


def rule_score_email(record: dict) -> dict:
    pers  = score_personalization(record)
    rel   = score_relevance(record)
    spam  = score_spam_risk(record)
    overall = compute_overall_score(pers, rel, spam)

    # Temporarily attach sub-scores so determine_approval_status can use them
    augmented = dict(record)
    augmented["_personalization_score"] = pers
    augmented["_relevance_score"]       = rel

    status, notes = determine_approval_status(overall, spam, augmented)

    return {
        "personalization_score": pers,
        "relevance_score":       rel,
        "spam_risk_score":       spam,
        "overall_score":         overall,
        "approval_status":       status,
        "review_notes":          ";".join(notes),
    }
