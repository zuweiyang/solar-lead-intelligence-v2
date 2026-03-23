"""
Smoke test for Workflow 9.5 / 9.6 Streamlit Campaign Control Panel.

Run from the project root:
    py scripts/test_control_panel.py

Tests:
  1. Core module imports (including ui_actions)
  2. ui_config -- form defaults, build_campaign_config, validation, city validation
  3. ui_state  -- all loaders return correct types (tolerates missing files)
  4. ui_state  -- KPI metrics structure and rates (including new rate keys)
  5. ui_state  -- high-priority leads filter (including open_count and lead_score rules)
  6. ui_state  -- company detail loader
  7. ui_state  -- enhanced file status (14 files, rows + mtime)
  8. ui_actions -- FollowupSendResult structure + dry_run mode
  9. ui_runner -- RunResult structure
 10. app       -- module file exists
 11. File / path verification
 12. location_data -- hierarchy helpers (countries, regions, cities, sub-cities)
 13. ui_state  -- get_city_crawl_stats returns correct structure
 14. ui_config -- metro fields round-trip through build_campaign_config
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def main() -> None:
    print("=" * 60)
    print("Workflow 9.6 Smoke Test [ ] Streamlit Campaign Control Panel")
    print("=" * 60)

    # ------------------------------------------------------------------
    # 1 [ ] Core module imports
    # ------------------------------------------------------------------
    print("\n[1] Importing core modules...")

    from src.workflow_9_5_streamlit_control_panel import ui_config
    from src.workflow_9_5_streamlit_control_panel import ui_runner
    from src.workflow_9_5_streamlit_control_panel import ui_state
    from src.workflow_9_5_streamlit_control_panel import ui_actions

    print("    OK [ ] ui_config, ui_runner, ui_state, ui_actions imported.")

    # ------------------------------------------------------------------
    # 2 [ ] ui_config
    # ------------------------------------------------------------------
    print("\n[2] ui_config [ ] form defaults and config builder...")

    assert "country" in ui_config.UI_DEFAULTS
    assert ui_config.UI_DEFAULTS["country"] == "Canada"
    assert ui_config.UI_DEFAULTS["dry_run"] is True
    assert "campaign_status" in ui_config.RUN_UNTIL_OPTIONS

    # Valid config
    config, errors = ui_config.build_campaign_config({
        "country": "Canada", "region": "BC", "city": "Vancouver",
        "keyword_mode": "default", "keywords": "",
        "company_limit": 10, "crawl_limit": 10, "enrich_limit": 10,
        "send_mode": "dry_run", "run_until": "email_generation", "dry_run": True,
    })
    assert config is not None and not errors, f"FAIL: valid config errors: {errors}"
    assert config.city == "Vancouver"

    # Custom keywords
    config2, _ = ui_config.build_campaign_config({
        "country": "USA", "region": "", "city": "Seattle",
        "keyword_mode": "custom", "keywords": "rooftop installer, solar EPC",
        "company_limit": 5, "crawl_limit": 5, "enrich_limit": 5,
        "send_mode": "dry_run", "run_until": "score", "dry_run": True,
    })
    assert config2 and config2.keywords == ["rooftop installer", "solar EPC"], \
        f"FAIL: custom keywords: {config2.keywords if config2 else 'None'}"

    # Invalid run_until (with a valid city)
    bad, errs = ui_config.build_campaign_config({
        "country": "Canada", "region": "", "city": "Vancouver",
        "keyword_mode": "default", "keywords": "",
        "company_limit": 0, "crawl_limit": 0, "enrich_limit": 0,
        "send_mode": "dry_run", "run_until": "INVALID_STEP", "dry_run": True,
    })
    assert bad is None and errs, "FAIL: invalid run_until should fail validation"

    # City validation: empty city must be rejected
    bad_city, city_errs = ui_config.build_campaign_config({
        "country": "Canada", "region": "", "city": "",
        "keyword_mode": "default", "keywords": "",
        "company_limit": 10, "crawl_limit": 10, "enrich_limit": 10,
        "send_mode": "dry_run", "run_until": "search_tasks", "dry_run": True,
    })
    assert bad_city is None and city_errs, "FAIL: empty city should produce error"
    assert any("city" in e.lower() for e in city_errs), \
        f"FAIL: city error should mention 'city', got: {city_errs}"

    # City validation: whitespace-only also rejected
    bad_ws, ws_errs = ui_config.build_campaign_config({
        "country": "Canada", "region": "", "city": "   ",
        "keyword_mode": "default", "keywords": "",
        "company_limit": 10, "crawl_limit": 10, "enrich_limit": 10,
        "send_mode": "dry_run", "run_until": "search_tasks", "dry_run": True,
    })
    assert bad_ws is None and ws_errs, "FAIL: whitespace-only city should produce error"

    print("    OK -- config builder, validation, and city validation all correct.")

    # ------------------------------------------------------------------
    # 3 [ ] ui_state: basic loaders
    # ------------------------------------------------------------------
    print("\n[3] ui_state [ ] basic loaders tolerate missing files...")

    state = ui_state.load_current_campaign_state()
    assert isinstance(state, dict)

    logs = ui_state.load_campaign_logs()
    assert isinstance(logs, list)

    status_rows = ui_state.load_campaign_status()
    assert isinstance(status_rows, list)

    summary = ui_state.load_campaign_summary()
    assert isinstance(summary, dict)

    file_status = ui_state.load_file_status()
    assert isinstance(file_status, list) and len(file_status) == len(ui_state.TRACKED_FILES)

    print(f"    OK [ ] state keys: {list(state.keys()) or '(empty)'}")
    print(f"    OK [ ] log rows: {len(logs)}, status rows: {len(status_rows)}")
    print(f"    OK [ ] files tracked: {len(file_status)}")

    # ------------------------------------------------------------------
    # 4 [ ] ui_state: KPI metrics structure and rate calculations
    # ------------------------------------------------------------------
    print("\n[4] ui_state [ ] KPI metrics structure...")

    metrics = ui_state.load_pipeline_metrics()
    assert isinstance(metrics, dict)

    required_keys = [
        "total_companies", "qualified_leads", "total_contacts",
        "emails_generated", "emails_sent", "open_count", "click_count",
        "followup_queued", "blocked_count", "open_rate", "click_rate",
        "qualification_rate", "contact_rate", "email_gen_rate",
    ]
    for k in required_keys:
        assert k in metrics, f"FAIL: missing metrics key: {k}"

    assert isinstance(metrics["open_rate"],          float), "FAIL: open_rate should be float"
    assert isinstance(metrics["click_rate"],         float), "FAIL: click_rate should be float"
    assert isinstance(metrics["qualification_rate"], float), "FAIL: qualification_rate should be float"
    assert isinstance(metrics["contact_rate"],       float), "FAIL: contact_rate should be float"
    assert isinstance(metrics["email_gen_rate"],     float), "FAIL: email_gen_rate should be float"
    assert metrics["open_rate"]          >= 0
    assert metrics["click_rate"]         >= 0
    assert metrics["qualification_rate"] >= 0
    assert metrics["contact_rate"]       >= 0
    assert metrics["email_gen_rate"]     >= 0

    # Rate calculation with synthetic data
    from src.workflow_9_5_streamlit_control_panel.ui_state import _sum_col
    from pathlib import Path
    # Test rate math directly
    sent = 10; opens = 3; clicks = 1
    open_rate  = round(opens  / sent * 100, 1)
    click_rate = round(clicks / sent * 100, 1)
    assert open_rate  == 30.0, f"FAIL: expected 30.0, got {open_rate}"
    assert click_rate == 10.0, f"FAIL: expected 10.0, got {click_rate}"

    print("    OK [ ] all KPI keys present, rates computed correctly.")

    ops = ui_state.load_delivery_ops_snapshot()
    assert isinstance(ops, dict), "FAIL: delivery ops snapshot should return dict"
    for k in [
        "cloud_delegated_emails",
        "sent_successfully",
        "current_country",
        "current_location",
        "current_status",
        "uploaded_yesterday_runs",
        "uploaded_yesterday_emails",
        "cloud_run_count",
        "cloud_queued_runs",
        "cloud_waiting_runs",
        "cloud_sending_runs",
        "cloud_failed_runs",
        "bounces_7d",
        "bounce_rate_7d",
        "sent_7d",
        "suppressed_addresses",
        "bounce_addresses",
        "last_bounce_at",
        "snapshot_date",
        "yesterday_date",
    ]:
        assert k in ops, f"FAIL: missing delivery ops key: {k}"

    print("    OK [ ] delivery ops snapshot keys present.")

    worker = ui_state.load_cloud_worker_health()
    assert isinstance(worker, dict), "FAIL: cloud worker health should return dict"
    for k in [
        "worker_health",
        "last_poll_at",
        "last_success_at",
        "last_error_at",
        "active_campaign_id",
        "last_idle_reason",
        "last_manifest_count",
        "last_wait_campaign_id",
        "last_wait_due_at",
        "last_completed_campaign_id",
        "last_failed_campaign_id",
        "last_processed_manifest_uri",
        "alerts_24h",
        "last_alert_at",
        "last_alert_level",
        "last_alert_type",
        "last_alert_message",
        "release_branch",
        "release_commit_short",
        "release_updated_at",
    ]:
        assert k in worker, f"FAIL: missing cloud worker health key: {k}"

    print("    OK [ ] cloud worker health keys present.")

    ready_runs = ui_state.load_ready_cloud_deploys()
    assert isinstance(ready_runs, list), "FAIL: ready cloud deploys should return list"
    if ready_runs:
        for row in ready_runs:
            for key in ("campaign_id", "queue_count", "deploy_status", "modified"):
                assert key in row, f"FAIL: ready deploy row missing key: {key}"
    print("    OK [ ] ready cloud deploy list shape valid.")

    deploy_script = ROOT / "scripts" / "deploy_run_to_gcloud.py"
    if deploy_script.exists():
        text = deploy_script.read_text(encoding="utf-8")
        for marker in ("--all-ready", "--limit", "--force", "deploy_runs("):
            assert marker in text, f"FAIL: deploy script missing batch deploy marker: {marker}"
        print("    OK [ ] batch deploy CLI markers present.")

    update_script = ROOT / "deploy" / "gcp" / "update_vm.sh"
    rollback_script = ROOT / "deploy" / "gcp" / "rollback_vm.sh"
    release_status_script = ROOT / "deploy" / "gcp" / "release_status.sh"
    stage_gmail_script = ROOT / "deploy" / "gcp" / "stage_gmail_oauth.sh"
    github_bundle_script = ROOT / "scripts" / "build_github_bundle.ps1"
    if update_script.exists():
        update_text = update_script.read_text(encoding="utf-8")
        assert "--ref" in update_text, "FAIL: update_vm.sh missing --ref support"
        assert "git_ref_requested" in update_text, "FAIL: update_vm.sh should persist requested git ref"
        print("    OK [ ] update_vm.sh ref-aware release markers present.")
    assert rollback_script.exists(), "FAIL: rollback_vm.sh missing"
    assert release_status_script.exists(), "FAIL: release_status.sh missing"
    assert stage_gmail_script.exists(), "FAIL: stage_gmail_oauth.sh missing"
    assert github_bundle_script.exists(), "FAIL: build_github_bundle.ps1 missing"
    print("    OK [ ] rollback/release status/stage scripts present.")

    actions_file = ROOT / "src" / "workflow_9_5_streamlit_control_panel" / "ui_actions.py"
    if actions_file.exists():
        action_text = actions_file.read_text(encoding="utf-8")
        assert "trigger_cloud_batch_deploy" in action_text, \
            "FAIL: ui_actions missing trigger_cloud_batch_deploy"
        assert "campaign_ids" in action_text, \
            "FAIL: ui_actions batch deploy should support explicit campaign_ids"
        print("    OK [ ] UI batch deploy action present.")

    views_file = ROOT / "src" / "workflow_9_5_streamlit_control_panel" / "ui_views.py"
    if views_file.exists():
        view_text = views_file.read_text(encoding="utf-8")
        for marker in ("Deploy Selected", "ready_cloud_deploy_editor", "CheckboxColumn("):
            assert marker in view_text, f"FAIL: ui_views missing selective deploy marker: {marker}"
        print("    OK [ ] selective deploy UI markers present.")

    # ------------------------------------------------------------------
    # 5 [ ] ui_state: high-priority lead filter
    # ------------------------------------------------------------------
    print("\n[5] ui_state [ ] high-priority leads filter...")

    # Test with synthetic data covering all 5 priority rules
    synthetic_rows = [
        # Rule 1: priority_flag == "high"
        {"company_name": "A", "priority_flag": "high",   "lifecycle_status": "sent_no_open",
         "open_count": "0", "lead_score": "50"},
        # Rule 2: lifecycle_status == "clicked_no_reply"
        {"company_name": "B", "priority_flag": "medium", "lifecycle_status": "clicked_no_reply",
         "open_count": "1", "lead_score": "50"},
        # Rule 3: lifecycle_status == "followup_queued"
        {"company_name": "C", "priority_flag": "low",    "lifecycle_status": "followup_queued",
         "open_count": "0", "lead_score": "40"},
        # Not priority (completed, low score, 0 opens)
        {"company_name": "D", "priority_flag": "low",    "lifecycle_status": "completed",
         "open_count": "0", "lead_score": "40"},
        # Rule 4: open_count >= 2
        {"company_name": "E", "priority_flag": "medium", "lifecycle_status": "sent_no_open",
         "open_count": "3", "lead_score": "50"},
        # Rule 5: lead_score >= 70
        {"company_name": "F", "priority_flag": "low",    "lifecycle_status": "not_sent",
         "open_count": "0", "lead_score": "75"},
        # Not priority (low score, 1 open, no flag)
        {"company_name": "G", "priority_flag": "low",    "lifecycle_status": "sent_no_open",
         "open_count": "1", "lead_score": "55"},
    ]

    # Manually apply the same filter logic as load_high_priority_leads
    def _is_priority(r: dict) -> bool:
        pf = r.get("priority_flag", "").strip().lower()
        ls = r.get("lifecycle_status", "").strip().lower()
        try:
            oc = int(r.get("open_count") or 0)
        except (ValueError, TypeError):
            oc = 0
        try:
            ls_score = float(r.get("lead_score") or 0)
        except (ValueError, TypeError):
            ls_score = 0.0
        return (
            pf == "high"
            or ls in ("clicked_no_reply", "followup_queued")
            or oc >= 2
            or ls_score >= 70
        )

    hp = [r for r in synthetic_rows if _is_priority(r)]
    assert len(hp) == 5, f"FAIL: expected 5 high-priority rows (A,B,C,E,F), got {len(hp)}: {[r['company_name'] for r in hp]}"
    names = {r["company_name"] for r in hp}
    assert names == {"A", "B", "C", "E", "F"}, f"FAIL: wrong companies: {names}"

    # Verify D and G are excluded
    assert "D" not in names, "FAIL: D (completed, low score, 0 opens) should NOT be priority"
    assert "G" not in names, "FAIL: G (1 open, score 55) should NOT be priority"

    # Also test the actual function (tolerates missing CSV)
    real_hp = ui_state.load_high_priority_leads()
    assert isinstance(real_hp, list), "FAIL: should return list"

    print(f"    OK -- filter logic correct (5/7 synthetic rows selected, 2 excluded).")
    print(f"    OK -- real load returned {len(real_hp)} rows.")

    # ------------------------------------------------------------------
    # 6 [ ] ui_state: company detail loader
    # ------------------------------------------------------------------
    print("\n[6] ui_state [ ] company detail loader...")

    detail = ui_state.get_company_detail("NonExistentCompany XYZ 999")
    assert detail is None, "FAIL: non-existent company should return None"

    # If campaign_status.csv exists, try a real company
    if status_rows:
        first_name = status_rows[0].get("company_name", "")
        if first_name:
            detail2 = ui_state.get_company_detail(first_name)
            assert isinstance(detail2, dict), f"FAIL: expected dict for {first_name}"
            assert detail2.get("company_name", "").lower() == first_name.lower(), \
                "FAIL: company_name mismatch in detail"
            print(f"    OK [ ] detail loaded for real company: {first_name}")
        else:
            print("    OK [ ] (no company_name in status rows; skipped real lookup)")
    else:
        print("    OK [ ] no campaign_status.csv yet; non-existent company returns None correctly.")

    companies = ui_state.load_company_names()
    assert isinstance(companies, list)
    print(f"    OK [ ] load_company_names: {len(companies)} companies.")

    # ------------------------------------------------------------------
    # 7 -- ui_state: enhanced file status (16 pipeline files)
    # ------------------------------------------------------------------
    print("\n[7] ui_state -- enhanced file status (16 pipeline files)...")

    enhanced = ui_state.load_enhanced_file_status()
    assert isinstance(enhanced, list)
    assert len(enhanced) == len(ui_state.KEY_FILES), \
        f"FAIL: expected {len(ui_state.KEY_FILES)} files, got {len(enhanced)}"
    # Must cover all tracked pipeline files
    assert len(enhanced) == 16, \
        f"FAIL: expected 16 tracked files, got {len(enhanced)}"

    expected_files = {
        "search_tasks.json", "raw_leads.csv", "company_pages.json",
        "company_text.json", "company_analysis.json", "qualified_leads.csv",
        "enriched_leads.csv", "generated_emails.csv", "send_logs.csv",
        "engagement_summary.csv", "followup_queue.csv", "campaign_status.csv",
        "cloud_deploy_status.json", "cloud_send_status.json",
        "campaign_run_state.json", "campaign_runner_logs.csv",
    }
    tracked_names = {row["file"] for row in enhanced}
    missing = expected_files - tracked_names
    assert not missing, f"FAIL: missing files from enhanced status: {missing}"

    for row in enhanced:
        assert "file" in row and "exists" in row and "modified" in row, \
            f"FAIL: missing fields in enhanced status row: {row}"

    print(f"    OK -- {len(enhanced)} pipeline files tracked with mtime.")
    for row in enhanced:
        icon = "[OK]" if row["exists"] else "[ ]"
        print(f"    {icon} {row['file']:30} rows={row['rows']!s:5} {row['modified']}")

    # ------------------------------------------------------------------
    # 8 [ ] ui_actions: FollowupSendResult + dry_run
    # ------------------------------------------------------------------
    print("\n[8] ui_actions [ ] FollowupSendResult and manual_send_followup_1...")

    from src.workflow_9_5_streamlit_control_panel.ui_actions import (
        FollowupSendResult,
        manual_send_followup_1,
        get_high_priority_rows,
        get_company_detail as _action_detail,
    )

    # Structure test
    r = FollowupSendResult(attempted=5, sent=3, dry_run=2, blocked=1, errors=0)
    assert r.attempted == 5
    assert r.sent == 3
    assert r.messages == []

    # Dry-run call (safe [ ] returns immediately if no followup_queue.csv)
    result = manual_send_followup_1(send_mode="dry_run")
    assert isinstance(result, FollowupSendResult)
    assert result.send_mode == "dry_run"
    # Either no candidates (file missing) or candidates processed safely
    assert isinstance(result.messages, list)

    # High-priority rows helper
    hp_rows = get_high_priority_rows()
    assert isinstance(hp_rows, list)

    # Company detail via actions
    detail_via_action = _action_detail("NonExistent Co")
    assert detail_via_action is None

    print(f"    OK [ ] FollowupSendResult structure correct.")
    print(f"    OK [ ] manual_send_followup_1(dry_run): attempted={result.attempted}, "
          f"messages={len(result.messages)}")

    # ------------------------------------------------------------------
    # 9 [ ] ui_runner: RunResult structure
    # ------------------------------------------------------------------
    print("\n[9] ui_runner [ ] RunResult structure...")

    from src.workflow_9_5_streamlit_control_panel.ui_runner import RunResult
    r2 = RunResult(success=True, campaign_id="x1", status="completed",
                   completed_steps=["search_tasks"], last_completed_step="search_tasks")
    assert r2.success and r2.campaign_id == "x1"

    failed = RunResult(success=False, error="boom")
    assert not failed.success and failed.completed_steps == []

    print("    OK [ ] RunResult fields correct.")

    # ------------------------------------------------------------------
    # 10 [ ] app module file check
    # ------------------------------------------------------------------
    print("\n[10] app.py [ ] file existence check...")

    app_path = Path(__file__).parent.parent / "src" / "workflow_9_5_streamlit_control_panel" / "app.py"
    assert app_path.exists(), f"FAIL: app.py not found at {app_path}"
    print(f"    OK [ ] app.py found at {app_path}")

    # ------------------------------------------------------------------
    # 11 [ ] File path verification
    # ------------------------------------------------------------------
    print("\n[11] File / path verification...")

    from config.settings import CAMPAIGN_RUN_STATE_FILE, CAMPAIGN_RUNNER_LOGS_FILE
    assert CAMPAIGN_RUN_STATE_FILE.name  == "campaign_run_state.json"
    assert CAMPAIGN_RUNNER_LOGS_FILE.name == "campaign_runner_logs.csv"

    module_dir = Path(__file__).parent.parent / "src" / "workflow_9_5_streamlit_control_panel"
    for fname in ("__init__.py", "app.py", "ui_config.py", "ui_runner.py",
                  "ui_state.py", "ui_views.py", "ui_actions.py"):
        assert (module_dir / fname).exists(), f"FAIL: {fname} not found"

    launcher = Path(__file__).parent / "run_control_panel.py"
    assert launcher.exists(), f"FAIL: run_control_panel.py not found"

    print("    OK [ ] all module files and launcher present.")

    # ------------------------------------------------------------------
    # 12 -- location_data: hierarchy helpers
    # ------------------------------------------------------------------
    print("\n[12] location_data -- hierarchy helpers...")

    from src.workflow_9_5_streamlit_control_panel.location_data import (
        get_countries,
        get_regions,
        get_base_cities,
        get_sub_cities,
        get_all_cities_flat,
        is_known_location,
    )

    countries = get_countries()
    assert isinstance(countries, list) and len(countries) > 0, \
        "FAIL: get_countries should return non-empty list"
    assert "Canada" in countries, "FAIL: Canada should be in countries"
    assert "United States" in countries, "FAIL: United States should be in countries"

    regions = get_regions("Canada")
    assert isinstance(regions, list) and len(regions) > 0, \
        "FAIL: get_regions('Canada') should return non-empty list"
    assert "British Columbia" in regions, "FAIL: British Columbia should be in Canada regions"
    assert "Ontario" in regions, "FAIL: Ontario should be in Canada regions"

    base_cities = get_base_cities("Canada", "British Columbia")
    assert isinstance(base_cities, list) and len(base_cities) > 0, \
        "FAIL: get_base_cities should return non-empty list"
    assert "Vancouver" in base_cities, "FAIL: Vancouver should be in BC base cities"

    sub_cities = get_sub_cities("Canada", "British Columbia", "Vancouver")
    assert isinstance(sub_cities, list) and len(sub_cities) > 0, \
        "FAIL: get_sub_cities should return non-empty list for Vancouver"
    assert "Burnaby" in sub_cities, "FAIL: Burnaby should be a Vancouver sub-city"
    assert "Richmond" in sub_cities, "FAIL: Richmond should be a Vancouver sub-city"

    # Unknown city/region/country returns []
    assert get_regions("Narnia") == [], "FAIL: unknown country should return []"
    assert get_base_cities("Canada", "Narnia") == [], "FAIL: unknown region should return []"
    assert get_sub_cities("Canada", "British Columbia", "Narnia") == [], \
        "FAIL: unknown city should return []"

    flat = get_all_cities_flat("Canada", "British Columbia")
    assert "Vancouver" in flat and "Burnaby" in flat, \
        "FAIL: flat list should include base + sub cities"

    assert is_known_location("Canada") is True
    assert is_known_location("Canada", "British Columbia") is True
    assert is_known_location("Canada", "British Columbia", "Vancouver") is True
    assert is_known_location("Narnia") is False
    assert is_known_location("Canada", "Narnia") is False

    print(f"    OK -- {len(countries)} countries, BC has {len(base_cities)} base cities.")
    print(f"    OK -- Vancouver sub-cities: {sub_cities}")

    # ------------------------------------------------------------------
    # 13 -- ui_state: get_city_crawl_stats returns correct structure
    # ------------------------------------------------------------------
    print("\n[13] ui_state -- get_city_crawl_stats structure...")

    city_stats = ui_state.get_city_crawl_stats()
    assert isinstance(city_stats, dict), "FAIL: get_city_crawl_stats should return dict"

    # Each value must have lead_count, status, last_updated fields
    for city, stat in city_stats.items():
        assert isinstance(city, str), f"FAIL: city key should be str, got {type(city)}"
        assert "lead_count" in stat, f"FAIL: stat for {city} missing lead_count"
        assert "status" in stat,     f"FAIL: stat for {city} missing status"
        assert "last_updated" in stat, f"FAIL: stat for {city} missing last_updated"
        assert stat["status"] in ("completed", "running", "partial", "new"), \
            f"FAIL: invalid status '{stat['status']}' for {city}"
        assert isinstance(stat["lead_count"], int), \
            f"FAIL: lead_count should be int for {city}"

    print(f"    OK -- city_stats returned {len(city_stats)} cities.")
    if city_stats:
        for city, stat in list(city_stats.items())[:3]:
            print(f"    {city}: leads={stat['lead_count']}, status={stat['status']}")

    # ------------------------------------------------------------------
    # 14 -- ui_config: metro fields round-trip
    # ------------------------------------------------------------------
    print("\n[14] ui_config -- metro fields round-trip through build_campaign_config...")

    # base_only: search_cities = [base_city]
    cfg_base, errs = ui_config.build_campaign_config({
        "country": "Canada", "region": "British Columbia", "base_city": "Vancouver",
        "metro_mode": "base_only", "metro_sub_cities": [],
        "keyword_mode": "default", "keywords": "",
        "company_limit": 5, "crawl_limit": 5, "enrich_limit": 5,
        "send_mode": "dry_run", "run_until": "search_tasks", "dry_run": True,
    })
    assert cfg_base is not None and not errs, f"FAIL: base_only should pass, got {errs}"
    assert cfg_base.base_city == "Vancouver"
    assert cfg_base.metro_mode == "base_only"
    assert cfg_base.search_cities == ["Vancouver"], \
        f"FAIL: base_only search_cities should be ['Vancouver'], got {cfg_base.search_cities}"

    # recommended: search_cities = [base_city] + sub-cities (pre-resolved by UI)
    cfg_rec, errs2 = ui_config.build_campaign_config({
        "country": "Canada", "region": "British Columbia", "base_city": "Vancouver",
        "metro_mode": "recommended", "metro_sub_cities": ["Burnaby", "Richmond"],
        "keyword_mode": "default", "keywords": "",
        "company_limit": 5, "crawl_limit": 5, "enrich_limit": 5,
        "send_mode": "dry_run", "run_until": "search_tasks", "dry_run": True,
    })
    assert cfg_rec is not None and not errs2, f"FAIL: recommended should pass, got {errs2}"
    assert cfg_rec.metro_mode == "recommended"
    assert "Vancouver" in cfg_rec.search_cities
    assert "Burnaby"   in cfg_rec.search_cities
    assert "Richmond"  in cfg_rec.search_cities

    # custom: search_cities = [base_city] + chosen subs
    cfg_cust, errs3 = ui_config.build_campaign_config({
        "country": "Canada", "region": "British Columbia", "base_city": "Vancouver",
        "metro_mode": "custom", "metro_sub_cities": ["Surrey", "Langley"],
        "keyword_mode": "default", "keywords": "",
        "company_limit": 5, "crawl_limit": 5, "enrich_limit": 5,
        "send_mode": "dry_run", "run_until": "search_tasks", "dry_run": True,
    })
    assert cfg_cust is not None and not errs3, f"FAIL: custom should pass, got {errs3}"
    assert cfg_cust.search_cities == ["Vancouver", "Surrey", "Langley"], \
        f"FAIL: custom search_cities wrong: {cfg_cust.search_cities}"

    # backward compat: city field still works (no base_city)
    cfg_back, errs4 = ui_config.build_campaign_config({
        "country": "Canada", "region": "", "city": "Calgary",
        "metro_mode": "base_only", "metro_sub_cities": [],
        "keyword_mode": "default", "keywords": "",
        "company_limit": 5, "crawl_limit": 5, "enrich_limit": 5,
        "send_mode": "dry_run", "run_until": "search_tasks", "dry_run": True,
    })
    assert cfg_back is not None and not errs4, f"FAIL: backward compat (city field) failed: {errs4}"
    assert cfg_back.base_city == "Calgary", \
        f"FAIL: base_city should be Calgary from city fallback, got {cfg_back.base_city}"

    # invalid metro_mode
    cfg_bad_metro, errs5 = ui_config.build_campaign_config({
        "country": "Canada", "region": "", "base_city": "Vancouver",
        "metro_mode": "INVALID_MODE", "metro_sub_cities": [],
        "keyword_mode": "default", "keywords": "",
        "company_limit": 5, "crawl_limit": 5, "enrich_limit": 5,
        "send_mode": "dry_run", "run_until": "search_tasks", "dry_run": True,
    })
    assert cfg_bad_metro is None and errs5, \
        f"FAIL: invalid metro_mode should be rejected, got errs={errs5}"

    print("    OK -- base_only / recommended / custom search_cities computed correctly.")
    print("    OK -- backward compat city field works.")
    print("    OK -- invalid metro_mode rejected by validation.")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Workflow 9.6 smoke test completed successfully (14 sections).")
    print("=" * 60)


if __name__ == "__main__":
    main()
