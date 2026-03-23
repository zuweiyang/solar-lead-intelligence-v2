"""
Test suite — Ticket 4: P0 Deliverability Breakers
==================================================

Groups:
  A  SenderHealth dataclass fields
  B  breaker_rules.evaluate_sender_health() — one rule per threshold
  C  DB: sender_health upsert + get
  D  DB: campaign_breakers upsert + get
  E  breaker_state — all four scope query functions
  F  check_email_eligibility — E0 block (Ticket 3 integration)
  G  check_global_breaker — active / inactive / conn=None
  H  check_domain_breaker — active / inactive / conn=None
  I  check_sender_breaker — active / inactive / conn=None
  J  check_campaign_breaker — active / inactive / conn=None
  K  send_guard.run_checks() — breaker checks integrated in correct order
  L  send_pipeline counters — breaker_blocked tracked
  M  Resilience — conn=None passes all breaker checks through
  N  Reason codes are explicit auditable strings
"""
from __future__ import annotations

import sqlite3
import sys
import traceback
import unittest
from pathlib import Path
from unittest import mock

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    """Return an in-memory DB with sender_health and campaign_breakers tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sender_health (
            id                         INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_email               TEXT    NOT NULL UNIQUE,
            sending_domain             TEXT,
            provider                   TEXT,
            active                     INTEGER NOT NULL DEFAULT 1,
            hard_bounce_rate           REAL    NOT NULL DEFAULT 0.0,
            invalid_rate               REAL    NOT NULL DEFAULT 0.0,
            provider_send_failure_rate REAL    NOT NULL DEFAULT 0.0,
            unsubscribe_rate           REAL    NOT NULL DEFAULT 0.0,
            spam_rate                  REAL    NOT NULL DEFAULT 0.0,
            last_health_updated_at     TEXT,
            health_source              TEXT,
            health_note                TEXT,
            sender_breaker_active      INTEGER NOT NULL DEFAULT 0,
            sender_breaker_reason      TEXT,
            created_at                 TEXT    NOT NULL DEFAULT (datetime('now')),
            updated_at                 TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS campaign_breakers (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            scope          TEXT    NOT NULL,
            scope_key      TEXT    NOT NULL,
            breaker_active INTEGER NOT NULL DEFAULT 0,
            breaker_reason TEXT,
            activated_at   TEXT,
            updated_at     TEXT    NOT NULL DEFAULT (datetime('now')),
            UNIQUE(scope, scope_key)
        );
    """)
    conn.commit()
    return conn


def _minimal_record(**kwargs) -> dict:
    """Return a minimal approved record dict for guard tests."""
    rec = {
        "kp_email":        "test@example.com",
        "subject":         "Test Subject",
        "email_body":      "Test body",
        "approval_status": "approved",
        "send_eligibility": "",
        "email_confidence_tier": "",
    }
    rec.update(kwargs)
    return rec


# ===========================================================================
# Group A — SenderHealth dataclass
# ===========================================================================

class TestSenderHealthDataclass(unittest.TestCase):
    def test_a1_required_field_sender_email(self):
        from src.workflow_7_4_deliverability.sender_health import SenderHealth
        h = SenderHealth(sender_email="test@example.com")
        self.assertEqual(h.sender_email, "test@example.com")

    def test_a2_defaults(self):
        from src.workflow_7_4_deliverability.sender_health import SenderHealth
        h = SenderHealth(sender_email="x@y.com")
        self.assertTrue(h.active)
        self.assertEqual(h.hard_bounce_rate, 0.0)
        self.assertEqual(h.invalid_rate, 0.0)
        self.assertEqual(h.provider_send_failure_rate, 0.0)
        self.assertEqual(h.unsubscribe_rate, 0.0)
        self.assertEqual(h.spam_rate, 0.0)
        self.assertFalse(h.sender_breaker_active)
        self.assertEqual(h.sender_breaker_reason, "")

    def test_a3_all_fields_settable(self):
        from src.workflow_7_4_deliverability.sender_health import SenderHealth
        h = SenderHealth(
            sender_email="sender@domain.com",
            sending_domain="domain.com",
            provider="gmail_api",
            active=True,
            hard_bounce_rate=0.02,
            invalid_rate=0.01,
            provider_send_failure_rate=0.03,
            unsubscribe_rate=0.002,
            spam_rate=0.0005,
            last_health_updated_at="2026-03-01T00:00:00Z",
            health_source="send_logs",
            health_note="all good",
            sender_breaker_active=False,
            sender_breaker_reason="",
        )
        self.assertEqual(h.sending_domain, "domain.com")
        self.assertEqual(h.provider, "gmail_api")
        self.assertAlmostEqual(h.hard_bounce_rate, 0.02)


# ===========================================================================
# Group B — breaker_rules.evaluate_sender_health()
# ===========================================================================

class TestBreakerRules(unittest.TestCase):
    def _health(self, **kwargs):
        from src.workflow_7_4_deliverability.sender_health import SenderHealth
        defaults = dict(
            sender_email="s@d.com",
            hard_bounce_rate=0.0,
            invalid_rate=0.0,
            provider_send_failure_rate=0.0,
            unsubscribe_rate=0.0,
            spam_rate=0.0,
        )
        defaults.update(kwargs)
        return SenderHealth(**defaults)

    def test_b1_clean_health_no_trips(self):
        from src.workflow_7_4_deliverability.breaker_rules import evaluate_sender_health
        trips = evaluate_sender_health(self._health())
        self.assertEqual(trips, [])

    def test_b2_hard_bounce_trips_sender(self):
        from src.workflow_7_4_deliverability.breaker_rules import evaluate_sender_health
        trips = evaluate_sender_health(self._health(hard_bounce_rate=0.04))  # > 3%
        scopes = [t[0] for t in trips]
        self.assertIn("sender", scopes)

    def test_b3_hard_bounce_below_threshold_no_trip(self):
        from src.workflow_7_4_deliverability.breaker_rules import evaluate_sender_health
        trips = evaluate_sender_health(self._health(hard_bounce_rate=0.02))  # < 3%
        scopes = [t[0] for t in trips]
        self.assertNotIn("sender", scopes)

    def test_b4_invalid_rate_trips_campaign(self):
        from src.workflow_7_4_deliverability.breaker_rules import evaluate_sender_health
        trips = evaluate_sender_health(self._health(invalid_rate=0.03))  # > 2%
        scopes = [t[0] for t in trips]
        self.assertIn("campaign", scopes)

    def test_b5_provider_failure_trips_sender(self):
        from src.workflow_7_4_deliverability.breaker_rules import evaluate_sender_health
        trips = evaluate_sender_health(self._health(provider_send_failure_rate=0.06))  # > 5%
        scopes = [t[0] for t in trips]
        self.assertIn("sender", scopes)

    def test_b6_unsubscribe_trips_sender_and_campaign(self):
        from src.workflow_7_4_deliverability.breaker_rules import evaluate_sender_health
        trips = evaluate_sender_health(self._health(unsubscribe_rate=0.01))  # > 0.5%
        scopes = [t[0] for t in trips]
        self.assertIn("sender", scopes)
        self.assertIn("campaign", scopes)

    def test_b7_spam_warning_trips_domain(self):
        from src.workflow_7_4_deliverability.breaker_rules import evaluate_sender_health
        trips = evaluate_sender_health(self._health(spam_rate=0.002))  # > 0.1% but < 0.3%
        scopes = [t[0] for t in trips]
        reasons = [t[1] for t in trips]
        self.assertIn("domain", scopes)
        self.assertIn("spam_rate_warning", reasons)

    def test_b8_spam_critical_trips_domain_critical(self):
        from src.workflow_7_4_deliverability.breaker_rules import evaluate_sender_health
        trips = evaluate_sender_health(self._health(spam_rate=0.005))  # > 0.3%
        scopes = [t[0] for t in trips]
        reasons = [t[1] for t in trips]
        self.assertIn("domain", scopes)
        self.assertIn("spam_rate_critical", reasons)
        self.assertNotIn("spam_rate_warning", reasons)  # only critical, not both

    def test_b9_multiple_thresholds_exceeded(self):
        from src.workflow_7_4_deliverability.breaker_rules import evaluate_sender_health
        trips = evaluate_sender_health(self._health(
            hard_bounce_rate=0.05,
            invalid_rate=0.03,
            unsubscribe_rate=0.01,
        ))
        scopes = [t[0] for t in trips]
        self.assertIn("sender", scopes)
        self.assertIn("campaign", scopes)
        self.assertGreaterEqual(scopes.count("sender"), 1)
        self.assertGreaterEqual(scopes.count("campaign"), 1)


# ===========================================================================
# Group C — DB: sender_health upsert + get
# ===========================================================================

class TestDbSenderHealth(unittest.TestCase):
    def setUp(self):
        self.conn = _make_db()

    def tearDown(self):
        self.conn.close()

    def test_c1_upsert_and_get(self):
        from src.database.db_utils import get_sender_health, upsert_sender_health
        from src.workflow_7_4_deliverability.sender_health import SenderHealth
        h = SenderHealth(
            sender_email="s@example.com",
            sending_domain="example.com",
            hard_bounce_rate=0.01,
        )
        upsert_sender_health(self.conn, h)
        row = get_sender_health(self.conn, "s@example.com")
        self.assertIsNotNone(row)
        self.assertAlmostEqual(row["hard_bounce_rate"], 0.01)
        self.assertEqual(row["sending_domain"], "example.com")

    def test_c2_upsert_is_idempotent(self):
        from src.database.db_utils import get_sender_health, upsert_sender_health
        from src.workflow_7_4_deliverability.sender_health import SenderHealth
        h = SenderHealth(sender_email="s@x.com", hard_bounce_rate=0.01)
        upsert_sender_health(self.conn, h)
        h2 = SenderHealth(sender_email="s@x.com", hard_bounce_rate=0.05)
        upsert_sender_health(self.conn, h2)
        row = get_sender_health(self.conn, "s@x.com")
        self.assertAlmostEqual(row["hard_bounce_rate"], 0.05)

    def test_c3_get_nonexistent_returns_none(self):
        from src.database.db_utils import get_sender_health
        self.assertIsNone(get_sender_health(self.conn, "nobody@nowhere.com"))

    def test_c4_email_normalised_to_lowercase(self):
        from src.database.db_utils import get_sender_health, upsert_sender_health
        from src.workflow_7_4_deliverability.sender_health import SenderHealth
        h = SenderHealth(sender_email="Upper@EXAMPLE.COM")
        upsert_sender_health(self.conn, h)
        row = get_sender_health(self.conn, "upper@example.com")
        self.assertIsNotNone(row)

    def test_c5_sender_breaker_persisted(self):
        from src.database.db_utils import get_sender_health, upsert_sender_health
        from src.workflow_7_4_deliverability.sender_health import SenderHealth
        h = SenderHealth(
            sender_email="s@x.com",
            sender_breaker_active=True,
            sender_breaker_reason="hard_bounce_rate_exceeded",
        )
        upsert_sender_health(self.conn, h)
        row = get_sender_health(self.conn, "s@x.com")
        self.assertEqual(row["sender_breaker_active"], 1)
        self.assertEqual(row["sender_breaker_reason"], "hard_bounce_rate_exceeded")


# ===========================================================================
# Group D — DB: campaign_breakers upsert + get
# ===========================================================================

class TestDbCampaignBreakers(unittest.TestCase):
    def setUp(self):
        self.conn = _make_db()

    def tearDown(self):
        self.conn.close()

    def test_d1_upsert_campaign_breaker(self):
        from src.database.db_utils import get_campaign_breaker_row, upsert_campaign_breaker
        upsert_campaign_breaker(self.conn, "campaign", "camp-001", True, "invalid_rate_exceeded")
        row = get_campaign_breaker_row(self.conn, "campaign", "camp-001")
        self.assertIsNotNone(row)
        self.assertEqual(row["breaker_active"], 1)
        self.assertEqual(row["breaker_reason"], "invalid_rate_exceeded")

    def test_d2_upsert_global_breaker(self):
        from src.database.db_utils import get_campaign_breaker_row, upsert_campaign_breaker
        upsert_campaign_breaker(self.conn, "global", "global", True, "manual_kill_switch")
        row = get_campaign_breaker_row(self.conn, "global", "global")
        self.assertIsNotNone(row)
        self.assertEqual(row["breaker_active"], 1)

    def test_d3_upsert_domain_breaker(self):
        from src.database.db_utils import get_campaign_breaker_row, upsert_campaign_breaker
        upsert_campaign_breaker(self.conn, "domain", "example.com", True, "spam_rate_critical")
        row = get_campaign_breaker_row(self.conn, "domain", "example.com")
        self.assertIsNotNone(row)
        self.assertEqual(row["scope"], "domain")
        self.assertEqual(row["scope_key"], "example.com")

    def test_d4_clear_breaker(self):
        from src.database.db_utils import get_campaign_breaker_row, upsert_campaign_breaker
        upsert_campaign_breaker(self.conn, "campaign", "c1", True, "reason")
        upsert_campaign_breaker(self.conn, "campaign", "c1", False, "")
        row = get_campaign_breaker_row(self.conn, "campaign", "c1")
        self.assertEqual(row["breaker_active"], 0)
        self.assertIsNone(row["activated_at"])

    def test_d5_get_nonexistent_returns_none(self):
        from src.database.db_utils import get_campaign_breaker_row
        self.assertIsNone(get_campaign_breaker_row(self.conn, "campaign", "no-such-id"))


# ===========================================================================
# Group E — breaker_state: all four scope functions
# ===========================================================================

class TestBreakerState(unittest.TestCase):
    def setUp(self):
        self.conn = _make_db()

    def tearDown(self):
        self.conn.close()

    def test_e1_get_sender_breaker_inactive_by_default(self):
        from src.workflow_7_4_deliverability.breaker_state import get_sender_breaker
        active, reason = get_sender_breaker(self.conn, "nobody@x.com")
        self.assertFalse(active)
        self.assertEqual(reason, "")

    def test_e2_set_and_get_sender_breaker(self):
        from src.workflow_7_4_deliverability.breaker_state import (
            get_sender_breaker,
            set_sender_breaker,
        )
        set_sender_breaker(self.conn, "s@x.com", True, "hard_bounce_rate_exceeded")
        active, reason = get_sender_breaker(self.conn, "s@x.com")
        self.assertTrue(active)
        self.assertEqual(reason, "hard_bounce_rate_exceeded")

    def test_e3_clear_sender_breaker(self):
        from src.workflow_7_4_deliverability.breaker_state import (
            get_sender_breaker,
            set_sender_breaker,
        )
        set_sender_breaker(self.conn, "s@x.com", True, "reason")
        set_sender_breaker(self.conn, "s@x.com", False, "")
        active, _ = get_sender_breaker(self.conn, "s@x.com")
        self.assertFalse(active)

    def test_e4_set_and_get_domain_breaker(self):
        from src.workflow_7_4_deliverability.breaker_state import (
            get_domain_breaker,
            set_domain_breaker,
        )
        set_domain_breaker(self.conn, "badomain.com", True, "spam_rate_critical")
        active, reason = get_domain_breaker(self.conn, "badomain.com")
        self.assertTrue(active)
        self.assertEqual(reason, "spam_rate_critical")

    def test_e5_get_domain_breaker_inactive_by_default(self):
        from src.workflow_7_4_deliverability.breaker_state import get_domain_breaker
        active, reason = get_domain_breaker(self.conn, "clean.com")
        self.assertFalse(active)

    def test_e6_set_and_get_campaign_breaker(self):
        from src.workflow_7_4_deliverability.breaker_state import (
            get_campaign_breaker,
            set_campaign_breaker,
        )
        set_campaign_breaker(self.conn, "camp-abc", True, "invalid_rate_exceeded")
        active, reason = get_campaign_breaker(self.conn, "camp-abc")
        self.assertTrue(active)
        self.assertEqual(reason, "invalid_rate_exceeded")

    def test_e7_get_campaign_breaker_inactive_by_default(self):
        from src.workflow_7_4_deliverability.breaker_state import get_campaign_breaker
        active, _ = get_campaign_breaker(self.conn, "no-such-campaign")
        self.assertFalse(active)

    def test_e8_set_and_get_global_breaker(self):
        from src.workflow_7_4_deliverability.breaker_state import (
            get_global_breaker,
            set_global_breaker,
        )
        set_global_breaker(self.conn, True, "manual_kill_switch")
        active, reason = get_global_breaker(self.conn)
        self.assertTrue(active)
        self.assertEqual(reason, "manual_kill_switch")

    def test_e9_get_global_breaker_inactive_by_default(self):
        from src.workflow_7_4_deliverability.breaker_state import get_global_breaker
        active, _ = get_global_breaker(self.conn)
        self.assertFalse(active)

    def test_e10_clear_global_breaker(self):
        from src.workflow_7_4_deliverability.breaker_state import (
            get_global_breaker,
            set_global_breaker,
        )
        set_global_breaker(self.conn, True, "reason")
        set_global_breaker(self.conn, False, "")
        active, _ = get_global_breaker(self.conn)
        self.assertFalse(active)


# ===========================================================================
# Group F — check_email_eligibility (Ticket 3 E0 integration)
# ===========================================================================

class TestCheckEmailEligibility(unittest.TestCase):
    def test_f1_block_status_blocks(self):
        from src.workflow_7_email_sending.send_guard import check_email_eligibility
        rec = _minimal_record(send_eligibility="block", email_confidence_tier="E0")
        result = check_email_eligibility(rec)
        self.assertIsNotNone(result)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["decision"], "blocked")
        self.assertIn("blocked_e0_email", result["reason"])

    def test_f2_allow_eligibility_passes(self):
        from src.workflow_7_email_sending.send_guard import check_email_eligibility
        for eligibility in ("allow", "allow_limited", "hold", "generic_pool_only", ""):
            rec = _minimal_record(send_eligibility=eligibility)
            result = check_email_eligibility(rec)
            self.assertIsNone(result, f"Expected None for send_eligibility={eligibility!r}")

    def test_f3_block_reason_contains_tier(self):
        from src.workflow_7_email_sending.send_guard import check_email_eligibility
        rec = _minimal_record(send_eligibility="block", email_confidence_tier="E0")
        result = check_email_eligibility(rec)
        self.assertIn("E0", result["reason"])

    def test_f4_missing_eligibility_passes(self):
        from src.workflow_7_email_sending.send_guard import check_email_eligibility
        rec = {"kp_email": "x@y.com"}  # no send_eligibility key
        self.assertIsNone(check_email_eligibility(rec))


# ===========================================================================
# Group G — check_global_breaker
# ===========================================================================

class TestCheckGlobalBreaker(unittest.TestCase):
    def setUp(self):
        self.conn = _make_db()

    def tearDown(self):
        self.conn.close()

    def test_g1_active_global_breaker_blocks(self):
        from src.workflow_7_4_deliverability.breaker_state import set_global_breaker
        from src.workflow_7_email_sending.send_guard import check_global_breaker
        set_global_breaker(self.conn, True, "manual_kill_switch")
        result = check_global_breaker(self.conn)
        self.assertIsNotNone(result)
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_global_breaker", result["reason"])

    def test_g2_inactive_global_breaker_passes(self):
        from src.workflow_7_email_sending.send_guard import check_global_breaker
        result = check_global_breaker(self.conn)
        self.assertIsNone(result)

    def test_g3_conn_none_passes(self):
        from src.workflow_7_email_sending.send_guard import check_global_breaker
        result = check_global_breaker(None)
        self.assertIsNone(result)

    def test_g4_reason_preserved_in_block(self):
        from src.workflow_7_4_deliverability.breaker_state import set_global_breaker
        from src.workflow_7_email_sending.send_guard import check_global_breaker
        set_global_breaker(self.conn, True, "emergency_pause")
        result = check_global_breaker(self.conn)
        self.assertIn("emergency_pause", result["reason"])


# ===========================================================================
# Group H — check_domain_breaker
# ===========================================================================

class TestCheckDomainBreaker(unittest.TestCase):
    def setUp(self):
        self.conn = _make_db()

    def tearDown(self):
        self.conn.close()

    def test_h1_active_domain_breaker_blocks(self):
        from src.workflow_7_4_deliverability.breaker_state import set_domain_breaker
        from src.workflow_7_email_sending.send_guard import check_domain_breaker
        set_domain_breaker(self.conn, "spammydomain.com", True, "spam_rate_critical")
        result = check_domain_breaker(self.conn, "spammydomain.com")
        self.assertIsNotNone(result)
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_domain_breaker", result["reason"])

    def test_h2_inactive_domain_passes(self):
        from src.workflow_7_email_sending.send_guard import check_domain_breaker
        result = check_domain_breaker(self.conn, "clean.com")
        self.assertIsNone(result)

    def test_h3_conn_none_passes(self):
        from src.workflow_7_email_sending.send_guard import check_domain_breaker
        result = check_domain_breaker(None, "anydomain.com")
        self.assertIsNone(result)

    def test_h4_empty_domain_passes(self):
        from src.workflow_7_email_sending.send_guard import check_domain_breaker
        result = check_domain_breaker(self.conn, "")
        self.assertIsNone(result)

    def test_h5_different_domain_not_blocked(self):
        from src.workflow_7_4_deliverability.breaker_state import set_domain_breaker
        from src.workflow_7_email_sending.send_guard import check_domain_breaker
        set_domain_breaker(self.conn, "bad.com", True, "reason")
        result = check_domain_breaker(self.conn, "good.com")
        self.assertIsNone(result)


# ===========================================================================
# Group I — check_sender_breaker
# ===========================================================================

class TestCheckSenderBreaker(unittest.TestCase):
    def setUp(self):
        self.conn = _make_db()

    def tearDown(self):
        self.conn.close()

    def test_i1_active_sender_breaker_blocks(self):
        from src.workflow_7_4_deliverability.breaker_state import set_sender_breaker
        from src.workflow_7_email_sending.send_guard import check_sender_breaker
        set_sender_breaker(self.conn, "sender@mydomain.com", True, "hard_bounce_rate_exceeded")
        result = check_sender_breaker(self.conn, "sender@mydomain.com")
        self.assertIsNotNone(result)
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_sender_breaker", result["reason"])

    def test_i2_inactive_sender_passes(self):
        from src.workflow_7_email_sending.send_guard import check_sender_breaker
        result = check_sender_breaker(self.conn, "clean@domain.com")
        self.assertIsNone(result)

    def test_i3_conn_none_passes(self):
        from src.workflow_7_email_sending.send_guard import check_sender_breaker
        result = check_sender_breaker(None, "any@domain.com")
        self.assertIsNone(result)

    def test_i4_empty_sender_passes(self):
        from src.workflow_7_email_sending.send_guard import check_sender_breaker
        result = check_sender_breaker(self.conn, "")
        self.assertIsNone(result)

    def test_i5_reason_preserved(self):
        from src.workflow_7_4_deliverability.breaker_state import set_sender_breaker
        from src.workflow_7_email_sending.send_guard import check_sender_breaker
        set_sender_breaker(self.conn, "s@d.com", True, "provider_failure_rate_exceeded")
        result = check_sender_breaker(self.conn, "s@d.com")
        self.assertIn("provider_failure_rate_exceeded", result["reason"])


# ===========================================================================
# Group J — check_campaign_breaker
# ===========================================================================

class TestCheckCampaignBreaker(unittest.TestCase):
    def setUp(self):
        self.conn = _make_db()

    def tearDown(self):
        self.conn.close()

    def test_j1_active_campaign_breaker_blocks(self):
        from src.workflow_7_4_deliverability.breaker_state import set_campaign_breaker
        from src.workflow_7_email_sending.send_guard import check_campaign_breaker
        set_campaign_breaker(self.conn, "campaign-xyz", True, "invalid_rate_exceeded")
        result = check_campaign_breaker(self.conn, "campaign-xyz")
        self.assertIsNotNone(result)
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_campaign_breaker", result["reason"])

    def test_j2_inactive_campaign_passes(self):
        from src.workflow_7_email_sending.send_guard import check_campaign_breaker
        result = check_campaign_breaker(self.conn, "safe-campaign")
        self.assertIsNone(result)

    def test_j3_conn_none_passes(self):
        from src.workflow_7_email_sending.send_guard import check_campaign_breaker
        result = check_campaign_breaker(None, "any-campaign")
        self.assertIsNone(result)

    def test_j4_empty_campaign_id_passes(self):
        from src.workflow_7_email_sending.send_guard import check_campaign_breaker
        result = check_campaign_breaker(self.conn, "")
        self.assertIsNone(result)

    def test_j5_different_campaign_not_blocked(self):
        from src.workflow_7_4_deliverability.breaker_state import set_campaign_breaker
        from src.workflow_7_email_sending.send_guard import check_campaign_breaker
        set_campaign_breaker(self.conn, "bad-campaign", True, "reason")
        result = check_campaign_breaker(self.conn, "other-campaign")
        self.assertIsNone(result)


# ===========================================================================
# Group K — send_guard.run_checks() integration
# ===========================================================================

class TestRunChecksIntegration(unittest.TestCase):
    """
    Verify that all breaker checks fire in the right order within run_checks().
    Uses a real in-memory DB; patches SMTP_FROM_EMAIL to a known test value.
    """
    def setUp(self):
        self.conn = _make_db()
        self.logs: list[dict] = []

    def tearDown(self):
        self.conn.close()

    def _run(self, record=None, **kwargs):
        from src.workflow_7_email_sending.send_guard import run_checks
        rec = record or _minimal_record()
        # Patch config.settings.SMTP_FROM_EMAIL so the import inside run_checks resolves correctly
        with mock.patch("config.settings.SMTP_FROM_EMAIL", "sender@testdomain.com"):
            return run_checks(
                rec, self.logs, send_mode="dry_run",
                conn=self.conn, campaign_id=kwargs.get("campaign_id", ""),
            )

    def test_k1_all_clear_allows(self):
        result = self._run()
        self.assertTrue(result["allowed"])

    def test_k2_e0_block_fires_before_breakers(self):
        """E0 eligibility check (index 3) fires before breaker checks (index 4+)."""
        from src.workflow_7_4_deliverability.breaker_state import set_global_breaker
        # Even with global breaker active, E0 reason should appear when send_eligibility=block
        set_global_breaker(self.conn, True, "global_kill")
        rec = _minimal_record(send_eligibility="block", email_confidence_tier="E0")
        result = self._run(record=rec)
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_e0_email", result["reason"])

    def test_k3_global_breaker_blocks(self):
        from src.workflow_7_4_deliverability.breaker_state import set_global_breaker
        set_global_breaker(self.conn, True, "kill_switch")
        result = self._run()
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_global_breaker", result["reason"])

    def test_k4_domain_breaker_blocks(self):
        from src.workflow_7_4_deliverability.breaker_state import set_domain_breaker
        set_domain_breaker(self.conn, "testdomain.com", True, "spam_rate_critical")
        result = self._run()
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_domain_breaker", result["reason"])

    def test_k5_sender_breaker_blocks(self):
        from src.workflow_7_4_deliverability.breaker_state import set_sender_breaker
        set_sender_breaker(self.conn, "sender@testdomain.com", True, "hard_bounce_rate_exceeded")
        result = self._run()
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_sender_breaker", result["reason"])

    def test_k6_campaign_breaker_blocks(self):
        from src.workflow_7_4_deliverability.breaker_state import set_campaign_breaker
        set_campaign_breaker(self.conn, "camp-001", True, "invalid_rate_exceeded")
        result = self._run(campaign_id="camp-001")
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_campaign_breaker", result["reason"])

    def test_k7_conn_none_all_pass(self):
        """With conn=None, all breaker checks are skipped — no DB required."""
        from src.workflow_7_email_sending.send_guard import run_checks
        rec = _minimal_record()
        result = run_checks(rec, [], send_mode="dry_run", conn=None, campaign_id="any")
        self.assertTrue(result["allowed"])

    def test_k8_global_breaker_fires_before_domain(self):
        """Global breaker (index 4) should fire before domain breaker (index 5)."""
        from src.workflow_7_4_deliverability.breaker_state import (
            set_domain_breaker,
            set_global_breaker,
        )
        set_global_breaker(self.conn, True, "global_reason")
        set_domain_breaker(self.conn, "testdomain.com", True, "domain_reason")
        result = self._run()
        self.assertIn("blocked_global_breaker", result["reason"])

    def test_k9_existing_checks_still_work(self):
        """Duplicate check still fires after all breaker checks pass."""
        existing_log = {
            "kp_email": "test@example.com",
            "subject":  "Test Subject",
            "send_status": "sent",
            "timestamp": "2099-01-01T00:00:00+00:00",
        }
        from src.workflow_7_email_sending.send_guard import run_checks
        rec = _minimal_record()
        result = run_checks(
            rec, [existing_log], send_mode="dry_run",
            conn=self.conn, campaign_id="",
        )
        self.assertFalse(result["allowed"])
        self.assertIn("Duplicate", result["reason"])


# ===========================================================================
# Group L — send_pipeline counters: breaker_blocked
# ===========================================================================

class TestPipelineBreakeredCounts(unittest.TestCase):
    def test_l1_empty_summary_has_breaker_blocked_key(self):
        from src.workflow_7_email_sending.send_pipeline import _empty_summary
        summary = _empty_summary()
        self.assertIn("breaker_blocked", summary)
        self.assertEqual(summary["breaker_blocked"], 0)

    def test_l2_is_breaker_block_detects_all_prefixes(self):
        from src.workflow_7_email_sending.send_guard import is_breaker_block
        self.assertTrue(is_breaker_block("blocked_e0_email: tier=E0"))
        self.assertTrue(is_breaker_block("blocked_global_breaker: kill"))
        self.assertTrue(is_breaker_block("blocked_domain_breaker: spam"))
        self.assertTrue(is_breaker_block("blocked_sender_breaker: bounce"))
        self.assertTrue(is_breaker_block("blocked_campaign_breaker: invalid"))

    def test_l3_is_breaker_block_rejects_other_reasons(self):
        from src.workflow_7_email_sending.send_guard import is_breaker_block
        self.assertFalse(is_breaker_block("Duplicate email+subject within 24h"))
        self.assertFalse(is_breaker_block("Approval status not sendable: pending"))
        self.assertFalse(is_breaker_block("Missing required field: kp_email"))
        self.assertFalse(is_breaker_block(""))

    def test_l4_breaker_blocked_counted_in_pipeline_logic(self):
        """
        Verify that a breaker-blocked record increments breaker_blocked counter.
        Simulates the pipeline counting logic directly.
        """
        from src.workflow_7_email_sending.send_guard import is_breaker_block
        from src.workflow_7_email_sending.send_pipeline import _empty_summary

        counters = _empty_summary()
        counters["total"] = 2

        guard_results = [
            {"allowed": False, "decision": "blocked", "reason": "blocked_global_breaker: kill"},
            {"allowed": False, "decision": "blocked", "reason": "Duplicate email+subject within 24h"},
        ]
        for guard in guard_results:
            if not guard["allowed"]:
                counters[guard["decision"]] += 1
                if is_breaker_block(guard["reason"]):
                    counters["breaker_blocked"] += 1

        self.assertEqual(counters["blocked"], 2)
        self.assertEqual(counters["breaker_blocked"], 1)  # only the breaker block


# ===========================================================================
# Group M — Resilience: conn=None passes all breaker checks
# ===========================================================================

class TestBreakerResilienceNoConn(unittest.TestCase):
    def test_m1_check_global_breaker_no_conn(self):
        from src.workflow_7_email_sending.send_guard import check_global_breaker
        self.assertIsNone(check_global_breaker(None))

    def test_m2_check_domain_breaker_no_conn(self):
        from src.workflow_7_email_sending.send_guard import check_domain_breaker
        self.assertIsNone(check_domain_breaker(None, "anydomain.com"))

    def test_m3_check_sender_breaker_no_conn(self):
        from src.workflow_7_email_sending.send_guard import check_sender_breaker
        self.assertIsNone(check_sender_breaker(None, "any@domain.com"))

    def test_m4_check_campaign_breaker_no_conn(self):
        from src.workflow_7_email_sending.send_guard import check_campaign_breaker
        self.assertIsNone(check_campaign_breaker(None, "any-campaign"))

    def test_m5_run_checks_no_conn_allows_valid_record(self):
        from src.workflow_7_email_sending.send_guard import run_checks
        rec = _minimal_record()
        result = run_checks(rec, [], send_mode="dry_run", conn=None, campaign_id="c1")
        self.assertTrue(result["allowed"])

    def test_m6_db_exception_does_not_block_send(self):
        """If a DB query raises unexpectedly, the check passes through (non-fatal)."""
        from src.workflow_7_email_sending.send_guard import check_global_breaker
        bad_conn = mock.MagicMock()
        bad_conn.execute.side_effect = RuntimeError("DB error")
        result = check_global_breaker(bad_conn)
        self.assertIsNone(result)


# ===========================================================================
# Group N — Reason codes are explicit auditable strings
# ===========================================================================

class TestReasonCodes(unittest.TestCase):
    def test_n1_reason_code_constants_exist(self):
        from src.workflow_7_4_deliverability.breaker_rules import (
            REASON_HARD_BOUNCE_EXCEEDED,
            REASON_INVALID_RATE_EXCEEDED,
            REASON_PROVIDER_FAILURE_EXCEEDED,
            REASON_SPAM_CRITICAL,
            REASON_SPAM_WARNING,
            REASON_UNSUBSCRIBE_EXCEEDED,
        )
        self.assertEqual(REASON_HARD_BOUNCE_EXCEEDED, "hard_bounce_rate_exceeded")
        self.assertEqual(REASON_INVALID_RATE_EXCEEDED, "invalid_rate_exceeded")
        self.assertEqual(REASON_PROVIDER_FAILURE_EXCEEDED, "provider_failure_rate_exceeded")
        self.assertEqual(REASON_UNSUBSCRIBE_EXCEEDED, "unsubscribe_rate_exceeded")
        self.assertEqual(REASON_SPAM_WARNING, "spam_rate_warning")
        self.assertEqual(REASON_SPAM_CRITICAL, "spam_rate_critical")

    def test_n2_breaker_prefixes_are_strings(self):
        from src.workflow_7_email_sending.send_guard import _BREAKER_PREFIXES
        for prefix in _BREAKER_PREFIXES:
            self.assertIsInstance(prefix, str)
            self.assertTrue(prefix.startswith("blocked_"))

    def test_n3_evaluate_returns_known_reason_codes(self):
        from src.workflow_7_4_deliverability.breaker_rules import (
            REASON_HARD_BOUNCE_EXCEEDED,
            REASON_INVALID_RATE_EXCEEDED,
            REASON_PROVIDER_FAILURE_EXCEEDED,
            REASON_SPAM_CRITICAL,
            REASON_SPAM_WARNING,
            REASON_UNSUBSCRIBE_EXCEEDED,
            evaluate_sender_health,
        )
        from src.workflow_7_4_deliverability.sender_health import SenderHealth
        known_codes = {
            REASON_HARD_BOUNCE_EXCEEDED,
            REASON_INVALID_RATE_EXCEEDED,
            REASON_PROVIDER_FAILURE_EXCEEDED,
            REASON_UNSUBSCRIBE_EXCEEDED,
            REASON_SPAM_WARNING,
            REASON_SPAM_CRITICAL,
        }
        # Extreme health — trips all rules
        h = SenderHealth(
            sender_email="s@d.com",
            hard_bounce_rate=0.9,
            invalid_rate=0.9,
            provider_send_failure_rate=0.9,
            unsubscribe_rate=0.9,
            spam_rate=0.9,
        )
        trips = evaluate_sender_health(h)
        for _, code in trips:
            self.assertIn(code, known_codes, f"Unknown reason code: {code!r}")

    def test_n4_guard_block_reasons_contain_breaker_prefix(self):
        """Every blocked_*_breaker reason must start with a known prefix."""
        from src.workflow_7_email_sending.send_guard import _BREAKER_PREFIXES
        # Verify all reason strings used in guard functions start with a known prefix
        test_reasons = [
            "blocked_e0_email: send_eligibility=block tier=E0",
            "blocked_global_breaker: manual_kill_switch",
            "blocked_domain_breaker: spam_rate_critical",
            "blocked_sender_breaker: hard_bounce_rate_exceeded",
            "blocked_campaign_breaker: invalid_rate_exceeded",
        ]
        for reason in test_reasons:
            matched = any(reason.startswith(p) for p in _BREAKER_PREFIXES)
            self.assertTrue(matched, f"Reason does not start with a known prefix: {reason!r}")


# ===========================================================================
# Runner
# ===========================================================================

def _run_group(group_prefix: str, loader) -> tuple[int, int]:
    suite = unittest.TestSuite()
    for name, obj in loader.getTestCaseNames(group_prefix):
        suite.addTest(obj(name))
    runner = unittest.TextTestRunner(verbosity=0, stream=open("/dev/null", "w"))
    result = runner.run(suite)
    return result.testsRun, len(result.failures) + len(result.errors)


if __name__ == "__main__":
    GROUPS = [
        ("A", "SenderHealth dataclass",              TestSenderHealthDataclass),
        ("B", "breaker_rules.evaluate_sender_health", TestBreakerRules),
        ("C", "DB sender_health upsert+get",          TestDbSenderHealth),
        ("D", "DB campaign_breakers upsert+get",      TestDbCampaignBreakers),
        ("E", "breaker_state four scopes",            TestBreakerState),
        ("F", "check_email_eligibility (E0)",         TestCheckEmailEligibility),
        ("G", "check_global_breaker",                 TestCheckGlobalBreaker),
        ("H", "check_domain_breaker",                 TestCheckDomainBreaker),
        ("I", "check_sender_breaker",                 TestCheckSenderBreaker),
        ("J", "check_campaign_breaker",               TestCheckCampaignBreaker),
        ("K", "run_checks integration",               TestRunChecksIntegration),
        ("L", "pipeline breaker_blocked counter",     TestPipelineBreakeredCounts),
        ("M", "resilience conn=None",                 TestBreakerResilienceNoConn),
        ("N", "reason codes are auditable strings",   TestReasonCodes),
    ]

    total_run  = 0
    total_fail = 0
    group_results: list[tuple[str, str, int, int]] = []

    loader = unittest.TestLoader()

    for letter, desc, cls in GROUPS:
        suite = loader.loadTestsFromTestCase(cls)
        stream = open("/dev/null", "w") if sys.platform != "win32" else open("nul", "w")
        runner = unittest.TextTestRunner(verbosity=0, stream=stream)
        try:
            result = runner.run(suite)
        except Exception as exc:
            print(f"Group {letter} CRASHED: {exc}")
            traceback.print_exc()
            group_results.append((letter, desc, 0, 1))
            total_fail += 1
            continue
        finally:
            stream.close()

        run  = result.testsRun
        fail = len(result.failures) + len(result.errors)
        total_run  += run
        total_fail += fail
        status = "PASS" if fail == 0 else "FAIL"
        group_results.append((letter, desc, run, fail))
        symbol = "OK" if fail == 0 else "XX"
        print(f"  [{status}] Group {letter}: {desc} ({run} tests) {symbol}")
        if fail > 0:
            for f in result.failures + result.errors:
                print(f"    → {f[0]}")
                print(f"      {f[1][:200]}")

    print()
    print(f"  Total: {total_run} tests — {total_fail} failures")
    sys.exit(0 if total_fail == 0 else 1)
