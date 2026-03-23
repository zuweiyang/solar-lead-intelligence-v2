# Workflow 7.4 — Deliverability Breakers: Metric-Based Threshold Rules
#
# evaluate_sender_health() inspects a SenderHealth instance and returns a
# list of (scope, reason_code) tuples for every threshold that is breached.
#
# Callers (e.g. a health-update job) use this list to activate the
# appropriate breakers via breaker_state.set_*_breaker().
#
# Scopes: "sender" | "domain" | "campaign"
# (Global breaker is managed manually; no automatic metric rule trips it.)
from __future__ import annotations

from config.settings import (
    BREAKER_HARD_BOUNCE_RATE,
    BREAKER_INVALID_RATE,
    BREAKER_PROVIDER_FAILURE_RATE,
    BREAKER_SPAM_RATE_CRITICAL,
    BREAKER_SPAM_RATE_WARNING,
    BREAKER_UNSUBSCRIBE_RATE,
)
from src.workflow_7_4_deliverability.sender_health import SenderHealth

# ---------------------------------------------------------------------------
# Reason codes (auditable strings)
# ---------------------------------------------------------------------------

REASON_HARD_BOUNCE_EXCEEDED      = "hard_bounce_rate_exceeded"
REASON_INVALID_RATE_EXCEEDED     = "invalid_rate_exceeded"
REASON_PROVIDER_FAILURE_EXCEEDED = "provider_failure_rate_exceeded"
REASON_UNSUBSCRIBE_EXCEEDED      = "unsubscribe_rate_exceeded"
REASON_SPAM_WARNING              = "spam_rate_warning"
REASON_SPAM_CRITICAL             = "spam_rate_critical"


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_sender_health(health: SenderHealth) -> list[tuple[str, str]]:
    """
    Evaluate which breakers should be activated based on current health metrics.

    Returns a list of (scope, reason_code) pairs for every breached threshold.
    Returns an empty list when all metrics are within limits.

    Rules:
      hard_bounce_rate > BREAKER_HARD_BOUNCE_RATE         → sender breaker
      invalid_rate > BREAKER_INVALID_RATE                 → campaign breaker
      provider_send_failure_rate > BREAKER_PROVIDER_FAILURE_RATE → sender breaker
      unsubscribe_rate > BREAKER_UNSUBSCRIBE_RATE         → sender + campaign breakers
      spam_rate > BREAKER_SPAM_RATE_CRITICAL              → domain breaker (critical)
      spam_rate > BREAKER_SPAM_RATE_WARNING (and ≤ critical) → domain breaker (warning)
    """
    trips: list[tuple[str, str]] = []

    if health.hard_bounce_rate > BREAKER_HARD_BOUNCE_RATE:
        trips.append(("sender", REASON_HARD_BOUNCE_EXCEEDED))

    if health.invalid_rate > BREAKER_INVALID_RATE:
        trips.append(("campaign", REASON_INVALID_RATE_EXCEEDED))

    if health.provider_send_failure_rate > BREAKER_PROVIDER_FAILURE_RATE:
        trips.append(("sender", REASON_PROVIDER_FAILURE_EXCEEDED))

    if health.unsubscribe_rate > BREAKER_UNSUBSCRIBE_RATE:
        trips.append(("sender",   REASON_UNSUBSCRIBE_EXCEEDED))
        trips.append(("campaign", REASON_UNSUBSCRIBE_EXCEEDED))

    if health.spam_rate > BREAKER_SPAM_RATE_CRITICAL:
        trips.append(("domain", REASON_SPAM_CRITICAL))
    elif health.spam_rate > BREAKER_SPAM_RATE_WARNING:
        trips.append(("domain", REASON_SPAM_WARNING))

    return trips
