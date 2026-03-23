# Workflow 7.4 — Deliverability Breakers: Sender Health Model
#
# SenderHealth holds all identity, metric, and breaker-state fields for a
# single sending address.  Domain-scope and campaign-scope breaker state
# lives in the campaign_breakers DB table (see breaker_state.py).
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SenderHealth:
    # --- Identity ---
    sender_email: str                       # UNIQUE key (lower-cased)
    sending_domain: str        = ""         # domain part of sender_email
    provider: str              = ""         # "gmail_api" | "smtp" | ""
    active: bool               = True       # False = administratively disabled

    # --- Health metrics (rates: 0.0–1.0, e.g. 0.03 = 3%) ---
    hard_bounce_rate:           float = 0.0
    invalid_rate:               float = 0.0
    provider_send_failure_rate: float = 0.0
    unsubscribe_rate:           float = 0.0
    spam_rate:                  float = 0.0

    # --- Health metadata ---
    last_health_updated_at: str = ""
    health_source:          str = ""   # "send_logs" | "postmaster" | "manual"
    health_note:            str = ""

    # --- Sender-scope breaker state ---
    sender_breaker_active: bool = False
    sender_breaker_reason: str  = ""
