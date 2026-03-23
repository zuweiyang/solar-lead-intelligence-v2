"""
Smoke test for Workflow 9 — Campaign Runner.

Run from the project root:
    py scripts/test_campaign_runner.py

Tests:
  1. Config validation (valid + invalid)
  2. State initialization, update, and load
  3. Logger append and load
  4. Partial pipeline run (run_until=search_tasks) — no API calls
  5. Resume logic (skips already-done steps)
  6. File creation verification
  7. Log row inspection
  8. State status is "running" during execution (set at start)
  9. last_completed_step matches run_until after run
 10. State transitions correctly: initialized → running → completed
 11. Metro expansion: base_only / recommended / custom + task attribution
"""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_9_campaign_runner.campaign_config import (
    CampaignConfig,
    validate_config,
    get_effective_keywords,
    get_effective_location,
    DEFAULT_KEYWORDS,
)
from src.workflow_9_campaign_runner.campaign_state import (
    initialize_campaign_state,
    load_campaign_state,
    save_campaign_state,
    update_campaign_state,
    STATUS_COMPLETED,
    STATUS_RUNNING,
)
from src.workflow_9_campaign_runner.campaign_logger import (
    append_campaign_log,
    load_campaign_logs,
)
from src.workflow_9_campaign_runner.campaign_runner import run_campaign
from config.settings import CAMPAIGN_RUN_STATE_FILE, CAMPAIGN_RUNNER_LOGS_FILE


def main() -> None:
    print("=" * 60)
    print("Workflow 9 Smoke Test — Campaign Runner")
    print("=" * 60)

    errors = 0

    # ------------------------------------------------------------------
    # 1 — Config validation
    # ------------------------------------------------------------------
    print("\n[1] Config validation...")

    valid_cfg = CampaignConfig(city="Vancouver", country="Canada", run_until="search_tasks")
    errs = validate_config(valid_cfg)
    assert not errs, f"FAIL: valid config should have no errors, got: {errs}"

    bad_cfg = CampaignConfig(run_until="nonexistent_step")
    errs = validate_config(bad_cfg)
    assert errs, "FAIL: bad run_until should produce validation errors"

    loc = get_effective_location(CampaignConfig(city="Vancouver", region="British Columbia", country="Canada"))
    assert "Vancouver" in loc, f"FAIL: location should contain city, got: {loc}"

    kws = get_effective_keywords(CampaignConfig(keyword_mode="default"))
    assert kws == DEFAULT_KEYWORDS, "FAIL: default keywords mismatch"

    custom_kws = get_effective_keywords(
        CampaignConfig(keyword_mode="custom", keywords=["rooftop installer"])
    )
    assert custom_kws == ["rooftop installer"], f"FAIL: custom keywords wrong: {custom_kws}"

    print("    OK — config validation passes.")

    # ------------------------------------------------------------------
    # 2 — State management (using temp file)
    # ------------------------------------------------------------------
    print("\n[2] Campaign state management...")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        state_path = Path(f.name)

    try:
        cfg = CampaignConfig(city="TestCity", country="Canada")

        state = initialize_campaign_state(cfg, path=state_path)
        assert state["status"] == "initialized", f"FAIL: initial status wrong: {state['status']}"
        assert state["last_completed_step"] is None

        loaded = load_campaign_state(state_path)
        assert loaded is not None, "FAIL: state file not loadable"
        assert loaded["campaign_id"] == state["campaign_id"]

        update_campaign_state("search_tasks", "completed", path=state_path)
        updated = load_campaign_state(state_path)
        assert updated["last_completed_step"] == "search_tasks"
        assert updated["status"] == "running"

        update_campaign_state("scrape", "failed", error_message="API timeout", path=state_path)
        failed = load_campaign_state(state_path)
        assert failed["status"] == "failed"
        assert failed["error_message"] == "API timeout"

        print("    OK — state init / update / load works correctly.")
    finally:
        state_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # 3 — Logger
    # ------------------------------------------------------------------
    print("\n[3] Campaign logger...")

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        log_path = Path(f.name)
    log_path.unlink(missing_ok=True)  # let logger create it fresh

    append_campaign_log("test01", "search_tasks", "started",   "beginning",  path=log_path)
    append_campaign_log("test01", "search_tasks", "completed", "done",       path=log_path)
    append_campaign_log("test01", "scrape",       "failed",    "no API key", path=log_path)

    rows = load_campaign_logs("test01", path=log_path)
    assert len(rows) == 3, f"FAIL: expected 3 log rows, got {len(rows)}"
    assert rows[0]["step_name"] == "search_tasks"
    assert rows[2]["status"] == "failed"

    log_path.unlink(missing_ok=True)
    print("    OK — logger writes and reads correctly.")

    # ------------------------------------------------------------------
    # 4 — Partial pipeline run: only search_tasks (no API calls needed)
    # ------------------------------------------------------------------
    print("\n[4] Partial pipeline run (run_until=search_tasks)...")

    config = CampaignConfig(
        city      = "TestVancouver",
        country   = "Canada",
        run_until = "search_tasks",
        dry_run   = True,
    )

    result = run_campaign(config)
    campaign_id = result["campaign_id"]

    assert result["status"] == "completed", f"FAIL: expected completed, got: {result['status']}"
    assert "search_tasks" in result["completed_steps"], \
        f"FAIL: search_tasks should be in completed: {result['completed_steps']}"
    assert result["last_completed_step"] == "search_tasks", \
        f"FAIL: last step wrong: {result['last_completed_step']}"
    assert result["error"] is None, f"FAIL: unexpected error: {result['error']}"

    print(f"    OK — partial run completed. campaign_id={campaign_id}")

    # ------------------------------------------------------------------
    # 5 — Resume logic: re-run with resume=True, search_tasks should be skipped
    # ------------------------------------------------------------------
    print("\n[5] Resume logic...")

    resume_config = CampaignConfig(
        city      = "TestVancouver",
        country   = "Canada",
        run_until = "search_tasks",
        resume    = True,
        dry_run   = True,
    )
    resume_result = run_campaign(resume_config)
    assert resume_result["status"] == "completed", \
        f"FAIL: resume should complete, got: {resume_result['status']}"

    logs = load_campaign_logs(resume_result["campaign_id"])
    skipped = [r for r in logs if r["status"] == "skipped"]
    assert len(skipped) >= 1, f"FAIL: resume should skip already-done steps, got logs: {logs}"
    print(f"    OK — resume skipped {len(skipped)} already-done step(s).")

    # ------------------------------------------------------------------
    # 6 — Verify output files exist
    # ------------------------------------------------------------------
    print("\n[6] Verifying output files...")

    assert CAMPAIGN_RUN_STATE_FILE.exists(), \
        f"FAIL: campaign_run_state.json not found at {CAMPAIGN_RUN_STATE_FILE}"

    with open(CAMPAIGN_RUN_STATE_FILE, encoding="utf-8") as f:
        state_data = json.load(f)
    assert "campaign_id" in state_data
    assert "last_completed_step" in state_data
    assert "status" in state_data
    print(f"    OK — campaign_run_state.json: status={state_data['status']}, "
          f"last={state_data['last_completed_step']}")

    assert CAMPAIGN_RUNNER_LOGS_FILE.exists(), \
        f"FAIL: campaign_runner_logs.csv not found at {CAMPAIGN_RUNNER_LOGS_FILE}"
    print(f"    OK — campaign_runner_logs.csv exists.")

    # ------------------------------------------------------------------
    # 7 — Print sample log rows
    # ------------------------------------------------------------------
    print("\n[7] Sample log rows (last 5):")
    all_logs = load_campaign_logs()
    for row in all_logs[-5:]:
        print(f"    [{row['timestamp']}] {row['campaign_id']} | {row['step_name']:20} | "
              f"{row['status']:10} | {row['message'][:60]}")

    # ------------------------------------------------------------------
    # 8 — Verify last_completed_step matches run_until
    # ------------------------------------------------------------------
    print("\n[8] Verify last_completed_step matches run_until...")

    import json as _json
    with open(CAMPAIGN_RUN_STATE_FILE, encoding="utf-8") as f:
        final_state = _json.load(f)

    assert final_state["last_completed_step"] == "search_tasks", (
        f"FAIL: expected last_completed_step='search_tasks', "
        f"got '{final_state['last_completed_step']}'"
    )
    assert final_state["status"] == "completed", (
        f"FAIL: expected status='completed', got '{final_state['status']}'"
    )
    print(f"    OK — last_completed_step='{final_state['last_completed_step']}', "
          f"status='{final_state['status']}'")

    # ------------------------------------------------------------------
    # 9 — Verify state transitions: STATUS_RUNNING set before loop,
    #     STATUS_COMPLETED set after loop
    # ------------------------------------------------------------------
    print("\n[9] State transitions via temp state file...")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        state_path2 = Path(f.name)

    try:
        cfg2 = CampaignConfig(city="TransitionCity", country="Canada")
        s = initialize_campaign_state(cfg2, path=state_path2)
        assert s["status"] == "initialized"

        # Simulate what the runner now does: set running immediately
        s["status"] = "running"
        save_campaign_state(s, path=state_path2)
        mid = load_campaign_state(state_path2)
        assert mid["status"] == "running", f"FAIL: expected running, got {mid['status']}"

        # Simulate per-step update
        update_campaign_state("search_tasks", "completed", path=state_path2)
        after_step = load_campaign_state(state_path2)
        assert after_step["last_completed_step"] == "search_tasks"
        # run_until defaults to "campaign_status", so "search_tasks" != run_until → running
        assert after_step["status"] == "running", (
            f"FAIL: non-final step should keep status=running, got {after_step['status']}"
        )

        # Simulate final step matching run_until
        s2 = load_campaign_state(state_path2)
        s2["config"]["run_until"] = "search_tasks"
        save_campaign_state(s2, state_path2)
        update_campaign_state("search_tasks", "completed", path=state_path2)
        final2 = load_campaign_state(state_path2)
        assert final2["status"] == "completed", (
            f"FAIL: step==run_until should set status=completed, got {final2['status']}"
        )
        print("    OK — state transitions: initialized -> running -> completed correct.")
    finally:
        state_path2.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # 10 — Verify city validation blocks empty city in UI layer
    # ------------------------------------------------------------------
    print("\n[10] City validation in UI layer...")

    from src.workflow_9_5_streamlit_control_panel.ui_config import build_campaign_config

    bad_city, city_errors = build_campaign_config({
        "country": "Canada", "region": "", "city": "",
        "keyword_mode": "default", "keywords": "",
        "company_limit": 10, "crawl_limit": 10, "enrich_limit": 10,
        "send_mode": "dry_run", "run_until": "search_tasks", "dry_run": True,
    })
    assert bad_city is None and city_errors, \
        f"FAIL: empty city should produce validation error, got errors={city_errors}"
    assert any("city" in e.lower() for e in city_errors), \
        f"FAIL: error should mention city, got: {city_errors}"

    good_city, good_errs = build_campaign_config({
        "country": "Canada", "region": "", "city": "Vancouver",
        "keyword_mode": "default", "keywords": "",
        "company_limit": 10, "crawl_limit": 10, "enrich_limit": 10,
        "send_mode": "dry_run", "run_until": "search_tasks", "dry_run": True,
    })
    assert good_city is not None and not good_errs, \
        f"FAIL: valid city should pass, got errors={good_errs}"
    print("    OK — empty city blocked, non-empty city accepted.")

    # ------------------------------------------------------------------
    # 11 — Metro expansion: get_effective_search_cities + task attribution
    # ------------------------------------------------------------------
    print("\n[11] Metro expansion — get_effective_search_cities + task generation...")

    from src.workflow_9_campaign_runner.campaign_config import get_effective_search_cities

    # base_only: only the base city
    cfg_base = CampaignConfig(city="Vancouver", country="Canada",
                              region="British Columbia", metro_mode="base_only")
    cities_base = get_effective_search_cities(cfg_base)
    assert cities_base == ["Vancouver"], \
        f"FAIL: base_only should produce ['Vancouver'], got {cities_base}"

    # recommended: base + sub-cities from location_data
    cfg_rec = CampaignConfig(city="Vancouver", country="Canada",
                             region="British Columbia", metro_mode="recommended")
    cities_rec = get_effective_search_cities(cfg_rec)
    assert "Vancouver" in cities_rec, "FAIL: recommended must include base city"
    assert len(cities_rec) > 1, \
        f"FAIL: recommended should include sub-cities, got {cities_rec}"
    assert "Burnaby" in cities_rec or "Surrey" in cities_rec, \
        f"FAIL: expected known Vancouver suburbs, got {cities_rec}"
    print(f"    recommended: {cities_rec}")

    # custom: base + explicitly chosen sub-cities
    cfg_custom = CampaignConfig(city="Vancouver", country="Canada",
                                region="British Columbia", metro_mode="custom",
                                metro_sub_cities=["Richmond", "Burnaby"])
    cities_custom = get_effective_search_cities(cfg_custom)
    assert cities_custom == ["Vancouver", "Richmond", "Burnaby"], \
        f"FAIL: custom should be [Vancouver, Richmond, Burnaby], got {cities_custom}"

    # pre-computed search_cities takes priority over metro_mode
    cfg_precomp = CampaignConfig(city="Vancouver", country="Canada",
                                 metro_mode="recommended",
                                 search_cities=["Vancouver", "Langley"])
    cities_precomp = get_effective_search_cities(cfg_precomp)
    assert cities_precomp == ["Vancouver", "Langley"], \
        f"FAIL: pre-computed search_cities should take priority, got {cities_precomp}"

    # Task generation expands across multiple cities with attribution
    import json as _json2
    from config.settings import SEARCH_TASKS_FILE
    from src.workflow_9_campaign_runner.campaign_steps import run_step_1_search_tasks

    multi_cfg = CampaignConfig(
        city             = "Vancouver",
        base_city        = "Vancouver",
        country          = "Canada",
        region           = "British Columbia",
        metro_mode       = "custom",
        metro_sub_cities = ["Richmond"],
        search_cities    = ["Vancouver", "Richmond"],
        keyword_mode     = "default",
        run_until        = "search_tasks",
        dry_run          = True,
    )
    tasks = run_step_1_search_tasks(multi_cfg)
    assert isinstance(tasks, list) and len(tasks) > 0, \
        f"FAIL: expected non-empty tasks list, got {tasks}"

    # Every task must have base_city and search_city
    for t in tasks:
        assert "base_city"   in t, f"FAIL: task missing base_city: {t}"
        assert "search_city" in t, f"FAIL: task missing search_city: {t}"

    # Tasks should reference both cities
    task_search_cities = {t["search_city"] for t in tasks}
    assert "Vancouver" in task_search_cities, "FAIL: Vancouver should appear in tasks"
    assert "Richmond"  in task_search_cities, "FAIL: Richmond should appear in tasks"

    # base_city is always the campaign base city
    for t in tasks:
        assert t["base_city"] == "Vancouver", \
            f"FAIL: base_city should be 'Vancouver', got {t['base_city']}"

    print(f"    OK — {len(tasks)} tasks generated across {task_search_cities}")
    print(f"    OK — all tasks carry base_city='Vancouver'")
    print("    OK — metro expansion tests pass (base_only / recommended / custom / attribution).")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    if errors:
        print(f"\n{errors} test(s) FAILED.")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Workflow 9 smoke test completed successfully (11 sections).")
    print(f"  campaign_id          : {campaign_id}")
    print(f"  completed_steps      : {result['completed_steps']}")
    print(f"  last_completed_step  : {result['last_completed_step']}")
    print(f"  final status         : {result['status']}")
    print(f"  state file           : {CAMPAIGN_RUN_STATE_FILE}")
    print(f"  log file             : {CAMPAIGN_RUNNER_LOGS_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
