"""
Ticket 3 — P0 Email Verification Gateway: Test Suite

Tests cover:
  A. Tier constants, eligibility mapping, pool mapping (verification_models)
  B. Generic mailbox detection (email_verifier.is_generic_mailbox)
  C. Tier normalization from raw provider responses (_normalize_to_tier)
  D. MockVerificationProvider deterministic rules
  E. HunterVerificationProvider JSON → RawVerificationResponse mapping
  F. get_provider() factory (live=False always returns Mock)
  G. verify_email() full round-trip with Mock provider
  H. E0 contact skip in merge_leads (email_merge integration)
  I. Verified file preference in load_enriched_leads
  J. Persistence: upsert_email_verification + get_verification_by_email
  K. Pipeline resilience: missing input file, empty input, no-email contacts
  L. Pipeline output CSV contains all expected fields

Run:  python scripts/test_email_verification.py
All assertions print PASS/FAIL per group.
"""
from __future__ import annotations

import csv
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

# --- ensure project root is on sys.path ---
sys.path.insert(0, str(Path(__file__).parent.parent))


# ===========================================================================
# Group A — Tier / eligibility / pool constants
# ===========================================================================

def test_group_a():
    from src.workflow_5_9_email_verification.verification_models import (
        ALL_ELIGIBILITIES, ALL_POOLS, ALL_TIERS,
        ELIGIBILITY_ALLOW, ELIGIBILITY_BLOCK, ELIGIBILITY_GENERIC_POOL,
        TIER_E0, TIER_E1, TIER_E2, TIER_E3, TIER_E4,
        TIER_TO_ELIGIBILITY, TIER_TO_POOL,
        POOL_BLOCKED, POOL_GENERIC, POOL_LIMITED, POOL_PRIMARY, POOL_RISK,
    )

    assert len(ALL_TIERS) == 5,            "A1: ALL_TIERS should have 5 entries"
    assert len(ALL_ELIGIBILITIES) == 5,    "A2: ALL_ELIGIBILITIES should have 5 entries"
    assert len(ALL_POOLS) == 5,            "A3: ALL_POOLS should have 5 entries"

    assert TIER_TO_ELIGIBILITY[TIER_E0] == ELIGIBILITY_BLOCK,         "A4: E0 → block"
    assert TIER_TO_ELIGIBILITY[TIER_E1] == ELIGIBILITY_ALLOW,         "A5: E1 → allow"
    assert TIER_TO_ELIGIBILITY[TIER_E4] == ELIGIBILITY_GENERIC_POOL,  "A6: E4 → generic_pool_only"

    assert TIER_TO_POOL[TIER_E0] == POOL_BLOCKED,  "A7: E0 → blocked_pool"
    assert TIER_TO_POOL[TIER_E1] == POOL_PRIMARY,  "A8: E1 → primary_pool"
    assert TIER_TO_POOL[TIER_E2] == POOL_LIMITED,  "A9: E2 → limited_pool"
    assert TIER_TO_POOL[TIER_E3] == POOL_RISK,     "A10: E3 → risk_pool"
    assert TIER_TO_POOL[TIER_E4] == POOL_GENERIC,  "A11: E4 → generic_pool"

    print("Group A PASS — tier/eligibility/pool constants")


# ===========================================================================
# Group B — Generic mailbox detection
# ===========================================================================

def test_group_b():
    from src.workflow_5_9_email_verification.email_verifier import is_generic_mailbox

    # Positive matches (role prefixes)
    for addr in [
        "info@example.com",
        "INFO@EXAMPLE.COM",   # case-insensitive
        "sales@company.co.uk",
        "contact@firm.com",
        "office@solar.com",
        "admin@grid.org",
        "hello@startup.io",
        "support@provider.net",
        "service@corp.com",
        "procurement@enterprise.com",
        "purchasing@buyer.net",
    ]:
        assert is_generic_mailbox(addr), f"B: expected generic for {addr!r}"

    # Negative — named contacts
    for addr in [
        "john.smith@example.com",
        "j.doe@company.co.uk",
        "ceo@startup.io",
        "manager@corp.com",
        "inform@example.com",        # "inform" ≠ "info"
        "salesforce@company.com",    # "salesforce" ≠ "sales"
        "@nodomain.com",             # malformed — empty local
        "nodomain",                  # no @
    ]:
        assert not is_generic_mailbox(addr), f"B: expected NOT generic for {addr!r}"

    print("Group B PASS — generic mailbox detection")


# ===========================================================================
# Group C — Tier normalization from raw responses
# ===========================================================================

def test_group_c():
    from src.workflow_5_9_email_verification.email_verifier import _normalize_to_tier
    from src.workflow_5_9_email_verification.verification_models import (
        TIER_E0, TIER_E1, TIER_E2, TIER_E3, TIER_E4,
    )
    from src.workflow_5_9_email_verification.verification_provider import (
        RawVerificationResponse,
    )

    def _resp(**kwargs):
        defaults = dict(
            deliverable=False, risky=False, undeliverable=False,
            accept_all=False, is_webmail=False, is_block=False,
            smtp_check=False, result="unknown", provider_name="test",
        )
        defaults.update(kwargs)
        return RawVerificationResponse(**defaults)

    # E0 — undeliverable
    assert _normalize_to_tier(_resp(undeliverable=True), False) == TIER_E0, "C1"
    # E0 — error with no delivery signal
    assert _normalize_to_tier(_resp(error="timeout"), False) == TIER_E0, "C2"
    # E4 — generic mailbox (even if deliverable)
    assert _normalize_to_tier(_resp(deliverable=True, smtp_check=True), True) == TIER_E4, "C3"
    # E3 — accept_all / catch-all
    assert _normalize_to_tier(_resp(accept_all=True, result="risky"), False) == TIER_E3, "C4"
    # E3 — unknown result
    assert _normalize_to_tier(_resp(result="unknown"), False) == TIER_E3, "C5"
    # E2 — deliverable + webmail
    assert _normalize_to_tier(_resp(deliverable=True, is_webmail=True, result="risky"), False) == TIER_E2, "C6"
    # E2 — deliverable + risky flag
    assert _normalize_to_tier(_resp(deliverable=True, risky=True, result="risky"), False) == TIER_E2, "C7"
    # E2 — deliverable without smtp_check
    assert _normalize_to_tier(_resp(deliverable=True, result="deliverable"), False) == TIER_E2, "C8"
    # E1 — clean deliverable with smtp_check
    assert _normalize_to_tier(_resp(deliverable=True, smtp_check=True, result="deliverable"), False) == TIER_E1, "C9"
    # E0 wins over generic when undeliverable
    assert _normalize_to_tier(_resp(undeliverable=True), True) == TIER_E0, "C10"

    print("Group C PASS — tier normalization")


# ===========================================================================
# Group D — MockVerificationProvider deterministic rules
# ===========================================================================

def test_group_d():
    from src.workflow_5_9_email_verification.verification_provider import (
        MockVerificationProvider,
    )

    provider = MockVerificationProvider()

    # Undeliverable prefix
    r = provider.verify("bounce_test@example.com")
    assert r.undeliverable and not r.deliverable,  "D1: bounce_ → undeliverable"
    assert r.provider_name == "mock",              "D2"

    # Invalid domain
    r = provider.verify("someone@invalid.domain.com")
    assert r.undeliverable,                        "D3: invalid domain → undeliverable"

    # Catch-all domain
    r = provider.verify("user@catchall.example.com")
    assert r.accept_all and r.risky,               "D4: catchall domain → accept_all"

    # Webmail
    r = provider.verify("john@gmail.com")
    assert r.is_webmail and r.risky,               "D5: gmail → webmail + risky"

    r = provider.verify("jane@yahoo.com")
    assert r.is_webmail,                           "D6: yahoo → webmail"

    # Clean deliverable
    r = provider.verify("ceo@solarcorp.com")
    assert r.deliverable and r.smtp_check,         "D7: normal domain → deliverable"
    assert not r.is_webmail,                       "D8: not webmail"

    print("Group D PASS — MockVerificationProvider")


# ===========================================================================
# Group E — HunterVerificationProvider JSON parsing
# ===========================================================================

def test_group_e():
    """
    Test Hunter provider's JSON → RawVerificationResponse mapping.
    We monkey-patch urlopen to return a canned response — no network call.
    """
    import json
    import unittest.mock as mock

    from src.workflow_5_9_email_verification.verification_provider import (
        HunterVerificationProvider,
    )

    hunter = HunterVerificationProvider(api_key="test_key_e")

    def _make_response(payload: dict):
        raw = json.dumps(payload).encode()
        ctx = mock.MagicMock()
        ctx.__enter__ = mock.Mock(return_value=ctx)
        ctx.__exit__ = mock.Mock(return_value=False)
        ctx.read.return_value = raw
        return ctx

    # E1 — deliverable, smtp_check=True
    payload = {"data": {
        "result": "deliverable", "accept_all": False, "webmail": False,
        "block": False, "smtp_check": True,
    }}
    with mock.patch("urllib.request.urlopen", return_value=_make_response(payload)):
        r = hunter.verify("ceo@solarcorp.com")
    assert r.deliverable and r.smtp_check,  "E1: hunter deliverable mapping"
    assert r.provider_name == "hunter",     "E2"

    # E3 — accept_all / risky
    payload = {"data": {
        "result": "risky", "accept_all": True, "webmail": False,
        "block": False, "smtp_check": False,
    }}
    with mock.patch("urllib.request.urlopen", return_value=_make_response(payload)):
        r = hunter.verify("info@catchall.com")
    assert r.accept_all and r.risky,        "E3: accept_all mapping"

    # E0 — undeliverable
    payload = {"data": {
        "result": "undeliverable", "accept_all": False, "webmail": False,
        "block": True, "smtp_check": False,
    }}
    with mock.patch("urllib.request.urlopen", return_value=_make_response(payload)):
        r = hunter.verify("fake@bounce.io")
    assert r.undeliverable,                 "E4: undeliverable mapping"

    # Network error → error field populated
    with mock.patch("urllib.request.urlopen", side_effect=OSError("timeout")):
        r = hunter.verify("any@example.com")
    assert r.error and not r.deliverable,   "E5: network error surfaced via error field"
    assert r.result == "unknown",           "E6: result='unknown' on error"

    print("Group E PASS — HunterVerificationProvider JSON parsing")


# ===========================================================================
# Group F — get_provider() factory
# ===========================================================================

def test_group_f():
    from src.workflow_5_9_email_verification.verification_provider import (
        MockVerificationProvider,
        get_provider,
    )

    # live=False always returns Mock
    p = get_provider(provider_name="hunter", live=False)
    assert isinstance(p, MockVerificationProvider), "F1: live=False → Mock"

    p = get_provider(provider_name="mock", live=False)
    assert isinstance(p, MockVerificationProvider), "F2: mock + live=False → Mock"

    # live=True + mock → Mock
    p = get_provider(provider_name="mock", live=True)
    assert isinstance(p, MockVerificationProvider), "F3: mock + live=True → Mock"

    # live=True + unknown provider → ValueError
    try:
        get_provider(provider_name="nonexistent", live=True)
        assert False, "F4: should raise ValueError"
    except ValueError:
        pass

    print("Group F PASS — get_provider factory")


# ===========================================================================
# Group G — verify_email() full round-trip
# ===========================================================================

def test_group_g():
    from src.workflow_5_9_email_verification.email_verifier import verify_email
    from src.workflow_5_9_email_verification.verification_models import (
        TIER_E0, TIER_E1, TIER_E2, TIER_E3, TIER_E4,
        ELIGIBILITY_ALLOW, ELIGIBILITY_BLOCK, ELIGIBILITY_GENERIC_POOL,
        ELIGIBILITY_HOLD, ELIGIBILITY_ALLOW_LIMITED,
        POOL_PRIMARY, POOL_BLOCKED, POOL_GENERIC, POOL_RISK, POOL_LIMITED,
    )
    from src.workflow_5_9_email_verification.verification_provider import (
        MockVerificationProvider,
    )

    provider = MockVerificationProvider()

    # E1 — named clean contact
    r = verify_email("ceo@solarcorp.com", provider, source_mode="mock")
    assert r.email_confidence_tier == TIER_E1,      "G1: E1 tier"
    assert r.send_eligibility == ELIGIBILITY_ALLOW,  "G2: allow"
    assert r.send_pool == POOL_PRIMARY,              "G3: primary_pool"
    assert not r.is_generic_mailbox,                 "G4"
    assert r.source_mode == "mock",                  "G5"
    assert r.kp_email == "ceo@solarcorp.com",        "G6: normalised to lower"

    # E0 — undeliverable
    r = verify_email("bounce_test@example.com", provider, source_mode="mock")
    assert r.email_confidence_tier == TIER_E0,       "G7: E0 tier"
    assert r.send_eligibility == ELIGIBILITY_BLOCK,  "G8: block"
    assert r.send_pool == POOL_BLOCKED,              "G9: blocked_pool"

    # E2 — webmail (gmail)
    r = verify_email("someone@gmail.com", provider, source_mode="mock")
    assert r.email_confidence_tier == TIER_E2,               "G10: E2 tier (webmail)"
    assert r.send_eligibility == ELIGIBILITY_ALLOW_LIMITED,  "G11"
    assert r.send_pool == POOL_LIMITED,                      "G12"

    # E3 — catch-all domain
    r = verify_email("user@catchall.domain.com", provider, source_mode="mock")
    assert r.email_confidence_tier == TIER_E3,       "G13: E3 tier (catch-all)"
    assert r.send_eligibility == ELIGIBILITY_HOLD,   "G14"
    assert r.send_pool == POOL_RISK,                 "G15"

    # E4 — generic mailbox prefix
    r = verify_email("info@solarcorp.com", provider, source_mode="mock")
    assert r.email_confidence_tier == TIER_E4,                "G16: E4 tier (generic)"
    assert r.send_eligibility == ELIGIBILITY_GENERIC_POOL,    "G17"
    assert r.send_pool == POOL_GENERIC,                       "G18"
    assert r.is_generic_mailbox,                              "G19"

    # Case normalisation
    r = verify_email("CEO@SolarCorp.COM", provider, source_mode="mock")
    assert r.kp_email == "ceo@solarcorp.com",  "G20: input normalised to lower-case"

    print("Group G PASS — verify_email round-trip")


# ===========================================================================
# Group H — E0 skip in merge_leads
# ===========================================================================

def test_group_h():
    """
    Patch load_enriched_leads to return an E0 row alongside a normal row.
    Confirm merge_leads excludes the E0 row.
    """
    import unittest.mock as mock

    from src.workflow_6_email_generation import email_merge

    fake_leads = [
        {
            "company_name": "BlockedCo", "website": "", "place_id": "P1",
            "company_type": "epc", "market_focus": "", "services_detected": "",
            "confidence_score": "0.9", "lead_score": "80", "target_tier": "A",
            "kp_name": "Alice", "kp_title": "CEO", "kp_email": "alice@blocked.com",
            "enrichment_source": "apollo", "skip_reason": "",
            "email_confidence_tier": "E0",  # must be skipped
            "send_eligibility": "block", "send_pool": "blocked_pool",
        },
        {
            "company_name": "AllowedCo", "website": "", "place_id": "P2",
            "company_type": "epc", "market_focus": "", "services_detected": "",
            "confidence_score": "0.9", "lead_score": "85", "target_tier": "A",
            "kp_name": "Bob", "kp_title": "CTO", "kp_email": "bob@allowed.com",
            "enrichment_source": "apollo", "skip_reason": "",
            "email_confidence_tier": "E1",  # must pass through
            "send_eligibility": "allow", "send_pool": "primary_pool",
        },
    ]

    with (
        mock.patch.object(email_merge, "load_enriched_leads", return_value=fake_leads),
        mock.patch.object(email_merge, "load_research_signals", return_value=[]),
        mock.patch.object(email_merge, "load_company_openings", return_value={}),
    ):
        result = email_merge.merge_leads()

    assert len(result) == 1,                    "H1: E0 contact must be excluded"
    assert result[0]["company_name"] == "AllowedCo", "H2: allowed contact present"
    assert result[0]["email_confidence_tier"] == "E1", "H3: tier propagated to merged record"
    assert result[0]["send_eligibility"] == "allow",   "H4: eligibility propagated"
    assert result[0]["send_pool"] == "primary_pool",   "H5: pool propagated"

    print("Group H PASS — E0 skip in merge_leads")


# ===========================================================================
# Group I — Verified file preference in load_enriched_leads
# ===========================================================================

def test_group_i():
    """
    Confirm load_enriched_leads() prefers VERIFIED_ENRICHED_LEADS_FILE
    when it exists, falling back to ENRICHED_LEADS_FILE otherwise.
    """
    import unittest.mock as mock

    from src.workflow_6_email_generation import email_merge

    with tempfile.TemporaryDirectory() as tmpdir:
        verified_path = Path(tmpdir) / "verified_enriched_leads.csv"
        enriched_path = Path(tmpdir) / "enriched_leads.csv"

        # Write distinct content to each file
        verified_path.write_text(
            "kp_email,company_name\nverified@a.com,VerifiedCo\n", encoding="utf-8"
        )
        enriched_path.write_text(
            "kp_email,company_name\nenriched@b.com,EnrichedCo\n", encoding="utf-8"
        )

        # When verified file exists → should read verified file
        with (
            mock.patch("src.workflow_6_email_generation.email_merge.VERIFIED_ENRICHED_LEADS_FILE", verified_path),
            mock.patch("src.workflow_6_email_generation.email_merge.ENRICHED_LEADS_FILE", enriched_path),
        ):
            rows = email_merge.load_enriched_leads()

        assert len(rows) == 1,                         "I1: one row from verified file"
        assert rows[0]["kp_email"] == "verified@a.com", "I2: verified file was preferred"

        # When verified file does NOT exist → should fall back to enriched file
        verified_path.unlink()
        with (
            mock.patch("src.workflow_6_email_generation.email_merge.VERIFIED_ENRICHED_LEADS_FILE", verified_path),
            mock.patch("src.workflow_6_email_generation.email_merge.ENRICHED_LEADS_FILE", enriched_path),
        ):
            rows = email_merge.load_enriched_leads()

        assert len(rows) == 1,                          "I3: one row from enriched file"
        assert rows[0]["kp_email"] == "enriched@b.com", "I4: fell back to enriched file"

    print("Group I PASS — verified file preference in load_enriched_leads")


# ===========================================================================
# Group J — Persistence: upsert_email_verification + get_verification_by_email
# ===========================================================================

def test_group_j():
    from src.database.db_schema import _DDL_EMAIL_VERIFICATION
    from src.database.db_utils import (
        get_verification_by_email,
        upsert_email_verification,
    )
    from src.workflow_5_9_email_verification.verification_models import VerificationResult

    # Use a minimal in-memory DB with only email_verification to avoid reserved-keyword
    # issues with the pre-existing reply_events table (references column).
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_DDL_EMAIL_VERIFICATION)
    conn.commit()

    result = VerificationResult(
        kp_email              = "ceo@solarcorp.com",
        email_confidence_tier = "E1",
        send_eligibility      = "allow",
        send_pool             = "primary_pool",
        is_generic_mailbox    = False,
        provider_result       = "deliverable",
        provider_name         = "mock",
        verified_at           = "2026-03-19T10:00:00Z",
        source_mode           = "mock",
        error                 = "",
    )

    upsert_email_verification(conn, result)
    row = get_verification_by_email(conn, "ceo@solarcorp.com")

    assert row is not None,                            "J1: row inserted"
    assert row["kp_email"] == "ceo@solarcorp.com",    "J2: email stored"
    assert row["email_confidence_tier"] == "E1",      "J3: tier stored"
    assert row["send_eligibility"] == "allow",        "J4: eligibility stored"
    assert row["send_pool"] == "primary_pool",        "J5: pool stored"
    assert row["is_generic_mailbox"] == 0,            "J6: is_generic_mailbox=False stored as 0"
    assert row["provider_result"] == "deliverable",   "J7"
    assert row["source_mode"] == "mock",              "J8"

    # Upsert update — re-verify with different tier
    result2 = VerificationResult(
        kp_email              = "ceo@solarcorp.com",
        email_confidence_tier = "E2",
        send_eligibility      = "allow_limited",
        send_pool             = "limited_pool",
        is_generic_mailbox    = False,
        provider_result       = "risky",
        provider_name         = "mock",
        verified_at           = "2026-03-19T11:00:00Z",
        source_mode           = "mock",
    )
    upsert_email_verification(conn, result2)
    row2 = get_verification_by_email(conn, "ceo@solarcorp.com")

    assert row2["email_confidence_tier"] == "E2",    "J9: upsert updates tier"
    assert row2["send_eligibility"] == "allow_limited", "J10"

    # Case-insensitive lookup
    row3 = get_verification_by_email(conn, "CEO@SolarCorp.COM")
    assert row3 is not None,                          "J11: case-insensitive lookup"

    # Not-found returns None
    assert get_verification_by_email(conn, "nobody@nowhere.com") is None, "J12"

    conn.close()
    print("Group J PASS — persistence upsert + lookup")


# ===========================================================================
# Group K — Pipeline resilience
# ===========================================================================

def test_group_k():
    """
    Test verification_pipeline.run() under adverse conditions.
    """
    from src.workflow_5_9_email_verification import verification_pipeline

    with tempfile.TemporaryDirectory() as tmpdir:
        enriched_path = Path(tmpdir) / "enriched_leads.csv"
        verified_path = Path(tmpdir) / "verified_enriched_leads.csv"

        import unittest.mock as mock

        # K1 — missing input file
        missing_path = Path(tmpdir) / "nonexistent.csv"
        with (
            mock.patch("src.workflow_5_9_email_verification.verification_pipeline.ENRICHED_LEADS_FILE", missing_path),
            mock.patch("src.workflow_5_9_email_verification.verification_pipeline.VERIFIED_ENRICHED_LEADS_FILE", verified_path),
        ):
            result = verification_pipeline.run()
        assert result.get("error") == "no_input_file",  "K1: missing file → no_input_file"

        # K2 — empty input file
        enriched_path.write_text("", encoding="utf-8")
        with (
            mock.patch("src.workflow_5_9_email_verification.verification_pipeline.ENRICHED_LEADS_FILE", enriched_path),
            mock.patch("src.workflow_5_9_email_verification.verification_pipeline.VERIFIED_ENRICHED_LEADS_FILE", verified_path),
        ):
            result = verification_pipeline.run()
        assert result.get("error") == "empty_input",    "K2: empty file → empty_input"

        # K3 — row with no kp_email → counted as no_email, not crash
        enriched_path.write_text(
            "company_name,kp_email\nNoEmailCo,\n",
            encoding="utf-8",
        )
        with (
            mock.patch("src.workflow_5_9_email_verification.verification_pipeline.ENRICHED_LEADS_FILE", enriched_path),
            mock.patch("src.workflow_5_9_email_verification.verification_pipeline.VERIFIED_ENRICHED_LEADS_FILE", verified_path),
        ):
            # Pass provider_name + live directly so no need to patch module-level settings
            result = verification_pipeline.run(provider_name="mock", live=False)
        assert result.get("no_email") == 1,   "K3: no_email counter incremented"
        assert result.get("e0_blocked") == 1, "K4: no-email rows counted as E0 blocked"
        assert verified_path.exists(),         "K5: output CSV written even for no-email rows"

    print("Group K PASS — pipeline resilience")


# ===========================================================================
# Group L — Pipeline output CSV fields
# ===========================================================================

def test_group_l():
    """
    Run verification_pipeline on a minimal CSV and verify the output CSV
    contains all VERIFICATION_EXTRA_FIELDS alongside the input fields.
    """
    from src.workflow_5_9_email_verification import verification_pipeline
    from src.workflow_5_9_email_verification.verification_models import VERIFICATION_EXTRA_FIELDS

    import unittest.mock as mock

    with tempfile.TemporaryDirectory() as tmpdir:
        enriched_path = Path(tmpdir) / "enriched_leads.csv"
        verified_path = Path(tmpdir) / "verified_enriched_leads.csv"

        enriched_path.write_text(
            "company_name,kp_email\n"
            "SolarCo,ceo@solarcorp.com\n"
            "GenericCo,info@generic.com\n"
            "InvalidCo,bounce_test@example.com\n",
            encoding="utf-8",
        )

        with (
            mock.patch("src.workflow_5_9_email_verification.verification_pipeline.ENRICHED_LEADS_FILE", enriched_path),
            mock.patch("src.workflow_5_9_email_verification.verification_pipeline.VERIFIED_ENRICHED_LEADS_FILE", verified_path),
        ):
            # Pass provider_name + live directly so no need to patch module-level settings
            summary = verification_pipeline.run(provider_name="mock", live=False)

        assert verified_path.exists(),  "L1: verified_enriched_leads.csv created"

        with open(str(verified_path), newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 3, "L2: all 3 input rows written to output"

        header = list(rows[0].keys())
        for field in VERIFICATION_EXTRA_FIELDS:
            assert field in header, f"L3: field {field!r} missing from output CSV"

        # SolarCo → E1 (clean named contact)
        solar = next(r for r in rows if r["company_name"] == "SolarCo")
        assert solar["email_confidence_tier"] == "E1",  "L4: SolarCo → E1"
        assert solar["send_eligibility"] == "allow",    "L5"
        assert solar["send_pool"] == "primary_pool",    "L6"

        # GenericCo → E4 (info@ prefix)
        generic = next(r for r in rows if r["company_name"] == "GenericCo")
        assert generic["email_confidence_tier"] == "E4",            "L7: GenericCo → E4"
        assert generic["send_eligibility"] == "generic_pool_only",  "L8"
        assert generic["is_generic_mailbox"] == "true",             "L9"

        # InvalidCo → E0 (bounce_ prefix)
        invalid = next(r for r in rows if r["company_name"] == "InvalidCo")
        assert invalid["email_confidence_tier"] == "E0",   "L10: InvalidCo → E0"
        assert invalid["send_eligibility"] == "block",     "L11"
        assert invalid["send_pool"] == "blocked_pool",     "L12"

        # Summary counters
        assert summary["e0_blocked"] >= 1,  "L13: e0_blocked counted"
        assert summary["verified"] >= 2,    "L14: verified counted"

    print("Group L PASS — pipeline output CSV fields and tier assignment")


# ===========================================================================
# Main runner
# ===========================================================================

if __name__ == "__main__":
    failures = []

    groups = [
        ("A", test_group_a),
        ("B", test_group_b),
        ("C", test_group_c),
        ("D", test_group_d),
        ("E", test_group_e),
        ("F", test_group_f),
        ("G", test_group_g),
        ("H", test_group_h),
        ("I", test_group_i),
        ("J", test_group_j),
        ("K", test_group_k),
        ("L", test_group_l),
    ]

    for name, fn in groups:
        try:
            fn()
        except Exception as exc:
            print(f"Group {name} FAIL — {exc}")
            failures.append(name)

    print()
    if failures:
        print(f"FAILED groups: {', '.join(failures)}")
        sys.exit(1)
    else:
        total = sum(1 for _ in groups)
        print(f"All {total} groups passed.")
