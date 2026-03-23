"""
Smoke test for Workflow 6.2 — Signal-based Personalization.

Run from the project root:
    py scripts/test_signal_personalization.py

Tests:
  1. Signal ranker unit tests
  2. Signal-to-opening converter unit tests
  3. Full pipeline on synthetic data
  4. Integration check: company_openings.json written correctly
"""

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_6_2_signal_personalization.signal_ranker import rank_signals
from src.workflow_6_2_signal_personalization.signal_to_opening import signal_to_opening_line
from src.workflow_6_2_signal_personalization.signal_pipeline import generate_personalized_openings


# ---------------------------------------------------------------------------
# Synthetic test data
# ---------------------------------------------------------------------------

SYNTHETIC_SIGNALS = [
    {
        "company_name": "VREC Solar",
        "signals": [
            "Installed 150kW rooftop system in Burnaby",
            "Now offering Tesla Powerwall installations",
            "Hiring solar electricians",
        ],
    },
    {
        "company_name": "Ready Solar",
        "signals": [
            "Now offering Tesla Powerwall installations",
            "Completed residential solar project for 20 homes",
        ],
    },
    {
        "company_name": "Shift Energy Group",
        "signals": [
            "Expanding commercial solar operations",
            "Hired 5 new project managers",
            "Working on 2MW solar farm development",
        ],
    },
    {
        "company_name": "DC Power Group",
        "signals": [
            "Specialises in battery storage integration",
        ],
    },
    {
        "company_name": "Pure Solar Contracting",
        "signals": [],   # no signals — should get fallback
    },
]

EXPECTED_OPENINGS = {
    "VREC Solar":            "I saw your recent",      # installation signal wins
    "Ready Solar":           "I noticed",               # powerwall or completed project
    "Shift Energy Group":    "I noticed",               # solar farm signal
    "DC Power Group":        "I noticed",               # battery storage
    "Pure Solar Contracting": "I came across",          # fallback
}


def _write_temp_signals(data: list[dict]) -> Path:
    p = Path(tempfile.mktemp(suffix="_signals.json"))
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return p


def _write_empty(suffix: str) -> Path:
    p = Path(tempfile.mktemp(suffix=suffix))
    p.write_text("[]", encoding="utf-8")
    return p


def main():
    print("=" * 60)
    print("Workflow 6.2 Smoke Test — Signal-based Personalization")
    print("=" * 60)

    errors = 0

    # ------------------------------------------------------------------
    # 1 — Ranker unit tests
    # ------------------------------------------------------------------
    print("\n[1] Signal ranker...")

    # Installation/project should beat hiring
    best = rank_signals([
        "Hiring solar electricians",
        "Installed 150kW rooftop system in Burnaby",
    ])
    assert "150kW" in (best or "") or "rooftop" in (best or "").lower(), \
        f"FAIL: project signal should rank first, got: {best}"

    # Storage should beat generic
    best2 = rank_signals([
        "Company newsletter published",
        "Now offering Tesla Powerwall installations",
    ])
    assert best2 and "powerwall" in best2.lower(), \
        f"FAIL: storage signal should rank first, got: {best2}"

    # Empty list → None
    assert rank_signals([]) is None, "FAIL: empty list should return None"

    print("    OK — ranker produces correct priority order.")

    # ------------------------------------------------------------------
    # 2 — Signal-to-opening unit tests
    # ------------------------------------------------------------------
    print("\n[2] Signal-to-opening converter...")

    cases = [
        ("Installed 150kW rooftop system in Burnaby", "VREC Solar",
         "I saw your recent"),
        ("Now offering Tesla Powerwall installations", "Ready Solar",
         "Powerwall"),
        ("Hiring solar electricians", "Shift Energy",
         "expanding"),
        ("", "Pure Solar",
         "I came across"),
    ]

    for signal, company, expected_fragment in cases:
        result = signal_to_opening_line(signal, company)
        wc = len(result.split())
        assert expected_fragment.lower() in result.lower(), \
            f"FAIL: expected '{expected_fragment}' in opening for signal='{signal}'\n  got: {result}"
        assert wc <= 18, f"FAIL: opening too long ({wc} words): {result}"
        print(f"    OK [{company[:20]:<20}]  {result}")

    # ------------------------------------------------------------------
    # 3 — Full pipeline on synthetic data
    # ------------------------------------------------------------------
    print("\n[3] Full pipeline on 5 synthetic companies...")

    sig_path = _write_temp_signals(SYNTHETIC_SIGNALS)
    empty_fallback = _write_empty("_noresearch.json")
    empty_leads    = _write_empty("_noleads.csv")
    out_path = Path(tempfile.mktemp(suffix="_openings.json"))

    openings = generate_personalized_openings(
        signals_path=sig_path,
        fallback_path=empty_fallback,
        leads_path=empty_leads,
        output_path=out_path,
    )

    # Cleanup temp inputs
    sig_path.unlink(missing_ok=True)
    empty_fallback.unlink(missing_ok=True)
    empty_leads.unlink(missing_ok=True)

    assert len(openings) == 5, f"FAIL: expected 5 openings, got {len(openings)}"

    print(f"\n[4] Verifying openings ({len(openings)} companies):")
    for r in openings:
        company = r["company_name"]
        opening = r["opening_line"]
        fragment = EXPECTED_OPENINGS.get(company, "")
        ok = fragment.lower() in opening.lower() if fragment else True
        status = "OK" if ok else "FAIL"
        if not ok:
            errors += 1
        print(f"    {status} Company : {company}")
        print(f"         Opening : {opening}")
        print(f"         Signal  : {r.get('best_signal', '(none)')[:70]}")
        print()

    # ------------------------------------------------------------------
    # 5 — Verify output file
    # ------------------------------------------------------------------
    print("[5] Verifying output file...")
    if not out_path.exists():
        print("    FAIL: output file not created.")
        errors += 1
    else:
        with open(out_path, encoding="utf-8") as f:
            saved = json.load(f)
        assert len(saved) == 5, f"FAIL: expected 5 rows in JSON, got {len(saved)}"
        for r in saved:
            assert "company_name" in r and "opening_line" in r, \
                f"FAIL: missing fields in {r}"
        print(f"    OK — company_openings.json: {len(saved)} rows, required fields present.")
        out_path.unlink(missing_ok=True)

    if errors:
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Workflow 6.2 smoke test passed.")
    print(f"  Companies processed    : {len(openings)}")
    print(f"  Signal-based openings  : {sum(1 for r in openings if r['best_signal'])}")
    print(f"  Fallback openings      : {sum(1 for r in openings if not r['best_signal'])}")
    print("=" * 60)


if __name__ == "__main__":
    main()
