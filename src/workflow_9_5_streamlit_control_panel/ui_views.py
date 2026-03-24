"""
Workflow 9.5 / 9.6 — Streamlit Campaign Control Panel: View Helpers

Reusable Streamlit rendering functions.  Each function owns one UI section.

9.6 additions:
- render_dry_run_explanation()        — visible dry_run mode explanation
- render_logs_view()                  — now includes Refresh button
- render_kpi_dashboard()              — enhanced KPI with rates
- render_high_priority_leads_view()   — filtered high-priority table
- render_company_detail_view()        — company selector + lifecycle detail
- render_manual_followup_action()     — Send followup_1 button
- render_enhanced_file_status_view()  — rows + modification times
"""
from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd
from streamlit import column_config

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.workflow_9_5_streamlit_control_panel.debug_log import log

from src.workflow_9_5_streamlit_control_panel.ui_config import (
    UI_DEFAULTS,
    KEYWORD_MODE_OPTIONS,
    SEND_MODE_OPTIONS,
    RUN_UNTIL_OPTIONS,
    METRO_MODE_OPTIONS,
    METRO_MODE_LABELS,
)
from src.workflow_9_5_streamlit_control_panel.ui_state import (
    load_current_campaign_state,
    load_campaign_logs,
    load_campaign_status,
    load_campaign_summary,
    load_cloud_worker_health,
    load_delivery_ops_snapshot,
    load_pipeline_metrics,
    load_ready_cloud_deploys,
    load_cloud_deploy_reconciliation,
    load_manual_review_queue,
    load_multi_run_comparison,
    load_file_status,
    load_enhanced_file_status,
    load_high_priority_leads,
    get_company_detail,
    load_company_names,
    get_city_crawl_stats,
)
from src.workflow_9_5_streamlit_control_panel.location_data import (
    get_continents,
    get_countries_by_continent,
    get_countries,
    get_regions,
    get_base_cities,
    get_sub_cities,
    get_all_cities_flat,
)
from src.workflow_9_5_streamlit_control_panel.ui_runner import (
    RunResult,
    run_campaign_from_ui,
    resume_campaign_from_ui,
)
from src.workflow_9_5_streamlit_control_panel.ui_actions import (
    refresh_dashboard_state,
    manual_send_followup_1,
    trigger_cloud_batch_deploy,
)
from src.workflow_9_campaign_runner.campaign_config import PIPELINE_STEPS
from src.workflow_9_campaign_runner.campaign_runner import is_campaign_running


# ---------------------------------------------------------------------------
# Section 0 — Header
# ---------------------------------------------------------------------------

def render_header() -> None:
    log.ui("render_header()")
    st.title("☀️ Solar Lead Intelligence — Campaign Control Panel")
    st.caption("Workflow 9.5 / 9.6 — Visual interface for Workflow 9 Campaign Runner")
    st.divider()


# ---------------------------------------------------------------------------
# Section 1a — Dry-run Explanation
# ---------------------------------------------------------------------------

def render_dry_run_explanation() -> None:
    """Visible explanation of dry_run mode for operators."""
    log.ui("render_dry_run_explanation()")
    with st.expander("ℹ️ What is Dry Run mode?", expanded=False):
        st.markdown(
            """
**Dry Run** is controlled by **Send Mode**.

If **Send Mode = `dry_run`**, the pipeline executes normally, but **no real emails are sent** to external recipients.

What happens in dry_run mode:
- All pipeline steps run as normal (scraping, analysis, enrichment, generation, quality, repair)
- Emails go through generation, quality scoring, and the send pipeline logic
- Send logs and campaign status are updated
- The `send_status` is recorded as `dry_run` instead of `sent`
- **No SMTP connection is opened** — no real emails reach any inbox

Use dry_run when:
- Testing the pipeline on a new region
- Checking that emails look correct before going live
- Running the pipeline without SMTP credentials

To send real emails: set **Send Mode** to `smtp` or `gmail_api`.
            """
        )


def _lookup_city_stat(
    city_stats: dict[str, dict],
    base_city: str,
    region: str = "",
    country: str = "",
) -> dict | None:
    """Match a UI-selected city against stored source_location-style keys."""
    city = (base_city or "").strip()
    if not city:
        return None

    exact = city_stats.get(city)
    if exact:
        return exact

    candidates = []
    if region and country:
        candidates.append(f"{city}, {region}, {country}")
    if country:
        candidates.append(f"{city}, {country}")
    if region:
        candidates.append(f"{city}, {region}")

    for key in candidates:
        stat = city_stats.get(key)
        if stat:
            return stat

    city_lower = city.lower()
    for key, stat in city_stats.items():
        key_lower = key.lower()
        if key_lower == city_lower or key_lower.startswith(f"{city_lower},"):
            return stat

    return None


# ---------------------------------------------------------------------------
# Section 1b — Campaign Configuration Form
# ---------------------------------------------------------------------------

def render_campaign_form() -> dict:
    """Render the campaign configuration form. Returns raw form values dict."""
    log.ui("render_campaign_form()")
    st.header("Campaign Configuration")

    # Read run_mode from the previous render so the Location section can
    # conditionally show metro expansion (Single) or city multiselect (Multiple)
    # before the radio widget itself is rendered above it.
    current_run_mode = st.session_state.get("run_mode", "Single Run")

    # ---- Pipeline controls --------------------------------------------------
    st.subheader("Pipeline Controls")

    run_mode = st.radio(
        "Run Mode",
        options=["Single Run", "Multiple Run"],
        index=0 if current_run_mode == "Single Run" else 1,
        horizontal=True,
        key="run_mode",
        help=(
            "Single Run: run one city immediately via the Run Campaign button. "
            "Multiple Run: add selected cities to the scheduler queue."
        ),
    )

    col9, col10, col11 = st.columns(3)
    with col9:
        run_until = st.selectbox(
            "Run Until",
            options=RUN_UNTIL_OPTIONS,
            index=RUN_UNTIL_OPTIONS.index(UI_DEFAULTS["run_until"]),
            help="Stop the pipeline after this step.",
        )
    with col10:
        send_mode = st.selectbox(
            "Send Mode",
            options=SEND_MODE_OPTIONS,
            index=SEND_MODE_OPTIONS.index(UI_DEFAULTS["send_mode"]),
            help="'dry_run' simulates sending. 'smtp' sends real emails.",
        )
    with col11:
        dry_run = send_mode == "dry_run"
        dry_run = st.checkbox(
            "Dry Run (derived from Send Mode)",
            value=dry_run,
            disabled=True,
            help="This reflects the current Send Mode. Use Send Mode to switch between dry_run and real sending.",
        )

    auto_cloud_deploy_default = bool(UI_DEFAULTS.get("auto_cloud_deploy", False))
    auto_cloud_deploy = st.checkbox(
        "Auto Upload To Cloud After Completion",
        value=False if send_mode == "dry_run" else auto_cloud_deploy_default,
        disabled=send_mode == "dry_run",
        help=(
            "When enabled, a live-send campaign that reaches `campaign_status` with a non-empty "
            "`final_send_queue.csv` will auto-handoff to cloud send. Disable this to keep the run local "
            "until you manually deploy it from Ready To Deploy."
        ),
    )
    if send_mode == "dry_run":
        st.caption("Cloud auto-upload is disabled in `dry_run` mode.")

    # ---- Location -----------------------------------------------------------
    st.subheader("Location")

    manual_entry = st.checkbox(
        "Enter location manually",
        value=False,
        key="manual_location_entry",
        help="Check this to type a location that is not in the built-in hierarchy.",
    )

    if manual_entry:
        col1, col2, col3 = st.columns(3)
        with col1:
            country = st.text_input("Country", value=UI_DEFAULTS["country"])
        with col2:
            region = st.text_input("Region / Province", value=UI_DEFAULTS["region"],
                                   placeholder="e.g. British Columbia")
        with col3:
            base_city = st.text_input("City", value=UI_DEFAULTS["base_city"],
                                      placeholder="e.g. Vancouver")
    else:
        # Row 1: Continent → Country (cascading)
        continents = get_continents()
        default_country = UI_DEFAULTS["country"]

        # Find which continent contains the default country
        default_continent = "North America"
        for cont, clist in zip(continents, [get_countries_by_continent(c) for c in continents]):
            if default_country in clist:
                default_continent = cont
                break

        col0, col1 = st.columns([1, 2])
        with col0:
            continent = st.selectbox(
                "Continent",
                options=continents,
                index=continents.index(default_continent),
                key="loc_continent",
            )
        with col1:
            countries_in_continent = get_countries_by_continent(continent)
            country_idx = (
                countries_in_continent.index(default_country)
                if default_country in countries_in_continent else 0
            )
            country = st.selectbox(
                "Country",
                options=countries_in_continent,
                index=country_idx,
                key="loc_country",
            )

        # Row 2: Region → City (cascading, fallback to text if no hierarchy)
        col2, col3 = st.columns(2)
        with col2:
            regions = get_regions(country)
            if regions:
                region_idx = (
                    regions.index(UI_DEFAULTS["region"])
                    if UI_DEFAULTS["region"] in regions else 0
                )
                region = st.selectbox("Region / Province", options=regions,
                                      index=region_idx, key="loc_region")
            else:
                region = st.text_input("Region / Province", value="",
                                       placeholder="e.g. State / Province",
                                       key="loc_region_txt")
        with col3:
            base_cities = get_base_cities(country, region) if region else []
            if base_cities:
                bc_default = UI_DEFAULTS.get("base_city", "")
                bc_idx = base_cities.index(bc_default) if bc_default in base_cities else 0
                base_city = st.selectbox("Base City", options=base_cities,
                                         index=bc_idx, key="loc_base_city")
            else:
                base_city = st.text_input("Base City", value="",
                                          placeholder="e.g. City name",
                                          key="loc_base_city_txt")

    # ---- Single Run: city badge + metro expansion ---------------------------
    metro_mode      = "base_only"
    metro_sub_cities: list[str] = []
    selected_cities: list[str] = []

    if current_run_mode == "Single Run":
        # City crawl status badge
        if base_city:
            try:
                city_stats = get_city_crawl_stats()
                stat = _lookup_city_stat(city_stats, base_city, region=region, country=country)
                if stat:
                    _STATUS_ICON = {"completed": "✅", "running": "🔄", "partial": "⚠️", "new": "🆕"}
                    icon   = _STATUS_ICON.get(stat["status"], "❓")
                    status = stat["status"].capitalize()
                    leads  = stat["lead_count"]
                    st.caption(f"City status: {icon} {status}  |  Leads on record: {leads}")
                else:
                    st.caption(f"City status: 🆕 No data yet for **{base_city}**")
            except Exception as exc:
                log.warn("get_city_crawl_stats() failed", exc=exc)

        # Metro Expansion
        st.subheader("Metro Expansion")
        metro_labels = [METRO_MODE_LABELS[m] for m in METRO_MODE_OPTIONS]
        default_metro_idx = METRO_MODE_OPTIONS.index(UI_DEFAULTS.get("metro_mode", "base_only"))
        metro_label = st.radio(
            "Metro Mode",
            options=metro_labels,
            index=default_metro_idx,
            horizontal=True,
            key="metro_mode_radio",
            help=(
                "base_only: search only the selected city. "
                "recommended: auto-expand to known suburbs. "
                "custom: choose your own sub-cities."
            ),
        )
        metro_mode = METRO_MODE_OPTIONS[metro_labels.index(metro_label)]

        if metro_mode == "recommended":
            sub_cities = get_sub_cities(country, region, base_city) if (base_city and not manual_entry) else []
            if sub_cities:
                st.info(
                    f"Recommended expansion: **{base_city}** + "
                    + ", ".join(sub_cities[:5])
                    + (f" +{len(sub_cities) - 5} more" if len(sub_cities) > 5 else "")
                )
                metro_sub_cities = sub_cities
            else:
                st.warning(
                    f"No recommended sub-cities found for **{base_city}**. "
                    "Falling back to base city only."
                )
        elif metro_mode == "custom":
            candidate_subs = get_sub_cities(country, region, base_city) if (base_city and not manual_entry) else []
            metro_sub_cities = st.multiselect(
                "Sub-cities (custom selection)",
                options=candidate_subs,
                default=[],
                key="metro_sub_cities_select",
                help="Select the satellite/suburban cities to search in addition to the base city.",
            )

        if base_city:
            effective_cities = [base_city] if metro_mode == "base_only" else list(dict.fromkeys([base_city] + metro_sub_cities))
            if len(effective_cities) == 1:
                st.caption(f"Search will cover: **{effective_cities[0]}** (1 city)")
            else:
                st.caption(f"Search will cover: **{', '.join(effective_cities)}** ({len(effective_cities)} cities)")

    else:
        # ---- Multiple Run: city multiselect ---------------------------------
        st.subheader("Select Cities to Queue")
        available_cities = get_all_cities_flat(country, region) if region else get_base_cities(country, "")
        if not available_cities and not manual_entry:
            # Fallback: gather base cities across all regions in this country
            from src.workflow_9_5_streamlit_control_panel.location_data import LOCATION_HIERARCHY  # noqa: PLC0415
            all_regions = LOCATION_HIERARCHY.get(country, {})
            for reg_data in all_regions.values():
                for bc, subs in reg_data.items():
                    if bc not in available_cities:
                        available_cities.append(bc)
                    available_cities.extend(s for s in subs if s not in available_cities)

        if available_cities:
            add_all = st.checkbox(
                f"Add all {len(available_cities)} cities in this region",
                value=False,
                key="multi_add_all",
            )
            # Only force-fill on the rising edge (False → True).
            # If add_all was already True on the previous render, the user may
            # have clicked X to remove a city — we must NOT overwrite their
            # edit on the next render or the removed city gets re-added.
            _was_add_all = st.session_state.get("_multi_add_all_prev", False)
            if add_all and not _was_add_all:
                st.session_state["multi_cities"] = available_cities
            st.session_state["_multi_add_all_prev"] = add_all
            selected_cities = st.multiselect(
                "Cities to queue (1 job per city)",
                options=available_cities,
                key="multi_cities",
                help="Each selected city becomes one independent queue job.",
            )
            if selected_cities:
                st.caption(f"{len(selected_cities)} job(s) will be added to the queue: {', '.join(selected_cities)}")
        else:
            st.info("No city list found for this country/region. Switch to Single Run or use manual entry.")
            selected_cities = []

    # ---- Keywords & Limits --------------------------------------------------
    st.subheader("Keywords & Limits")

    col4, col5 = st.columns([1, 2])
    with col4:
        keyword_mode = st.selectbox(
            "Keyword Mode",
            options=KEYWORD_MODE_OPTIONS,
            index=KEYWORD_MODE_OPTIONS.index(UI_DEFAULTS["keyword_mode"]),
            help="'default' uses the built-in solar keyword set; 'custom' lets you specify your own.",
        )
    with col5:
        keywords_disabled = keyword_mode != "custom"
        keywords = st.text_input(
            "Custom Keywords (comma-separated)",
            value=UI_DEFAULTS["keywords"],
            placeholder="commercial solar installer, solar project developer, battery storage installer",
            disabled=keywords_disabled,
            help="Only used when Keyword Mode is 'custom'.",
        )

    col6, col7, col8 = st.columns(3)
    with col6:
        company_limit = st.number_input(
            "Company Limit", min_value=0, value=UI_DEFAULTS["company_limit"],
            help="Max companies to analyse / score. 0 = no limit.",
        )
    with col7:
        crawl_limit = st.number_input(
            "Crawl Limit", min_value=0, value=UI_DEFAULTS["crawl_limit"],
            help="Max websites to crawl. 0 = no limit.",
        )
    with col8:
        enrich_limit = st.number_input(
            "Enrich Limit", min_value=0, value=UI_DEFAULTS["enrich_limit"],
            help="Max leads to enrich / generate emails for. 0 = no limit.",
        )

    return {
        "country":          country,
        "region":           region,
        "base_city":        base_city,
        "city":             base_city,   # backward-compat alias
        "metro_mode":       metro_mode,
        "metro_sub_cities": metro_sub_cities,
        "keyword_mode":     keyword_mode,
        "keywords":         keywords,
        "company_limit":    int(company_limit),
        "crawl_limit":      int(crawl_limit),
        "enrich_limit":     int(enrich_limit),
        "run_until":        run_until,
        "send_mode":        send_mode,
        "auto_cloud_deploy": bool(auto_cloud_deploy),
        "dry_run":          dry_run,
        "run_mode":         run_mode,
        "selected_cities":  selected_cities,
    }


# ---------------------------------------------------------------------------
# Queue helper utilities (used by both runner controls and queue panel)
# ---------------------------------------------------------------------------

#: Statuses that count as "active" — an already-queued job at these
#: statuses acts as a duplicate guard.
_ACTIVE_QUEUE_STATUSES = frozenset(["pending", "running", "paused"])


def _detect_dup_jobs(
    cities: list[str],
    form_values: dict,
    active_jobs: list[dict],
) -> tuple[list[str], list[str]]:
    """
    Split *cities* into (duplicates, unique) against *active_jobs*.

    Duplicate key: (location, country, region, send_mode, run_until, auto_cloud_deploy)
    — location/country/region compared case-insensitively.
    Historical completed/failed jobs do NOT block re-queuing.
    """
    def _key(loc: str) -> tuple:
        return (
            loc.lower().strip(),
            form_values.get("country", "").lower().strip(),
            form_values.get("region",  "").lower().strip(),
            form_values.get("send_mode",  "dry_run"),
            form_values.get("run_until",  "campaign_status"),
            bool(form_values.get("auto_cloud_deploy", False)),
        )

    active_keys = {
        (
            j["location"].lower().strip(),
            j["country"].lower().strip(),
            j.get("region", "").lower().strip(),
            j["send_mode"],
            j["run_until"],
            bool(j.get("auto_cloud_deploy", False)),
        )
        for j in active_jobs
    }

    duplicates, unique = [], []
    for city in cities:
        (duplicates if _key(city) in active_keys else unique).append(city)
    return duplicates, unique


def _add_jobs_to_queue(cities: list[str], form_values: dict) -> list[str]:
    """
    Call add_job() once per city with the shared form settings.
    Returns a list of human-readable success strings (one per job).
    """
    import inspect  # noqa: PLC0415
    from src.workflow_9_queue_scheduler.queue_store import add_job, update_job  # noqa: PLC0415

    raw_kw = form_values.get("keywords", "") or ""
    keywords_list: list[str] = (
        [k.strip() for k in raw_kw.split(",") if k.strip()]
        if isinstance(raw_kw, str)
        else list(raw_kw)
    )

    added = []
    add_job_params = set(inspect.signature(add_job).parameters.keys())
    for city in cities:
        job_kwargs = {
            "location": city,
            "country": form_values.get("country", ""),
            "region": form_values.get("region", ""),
            "send_mode": form_values.get("send_mode", "dry_run"),
            "run_until": form_values.get("run_until", "campaign_status"),
            "company_limit": int(form_values.get("company_limit", 0) or 0),
            "crawl_limit": int(form_values.get("crawl_limit", 0) or 0),
            "enrich_limit": int(form_values.get("enrich_limit", 0) or 0),
            "keyword_mode": form_values.get("keyword_mode", "default"),
            "keywords": keywords_list,
            "metro_mode": "base_only",
        }
        desired_auto_cloud_deploy = bool(form_values.get("auto_cloud_deploy", False))
        add_job_supports_auto_cloud = "auto_cloud_deploy" in add_job_params
        if add_job_supports_auto_cloud:
            job_kwargs["auto_cloud_deploy"] = desired_auto_cloud_deploy
        job = add_job(**job_kwargs)
        if not add_job_supports_auto_cloud:
            update_job(job["job_id"], auto_cloud_deploy=desired_auto_cloud_deploy)
            job["auto_cloud_deploy"] = desired_auto_cloud_deploy
        added.append(f"{job['location']}, {job['country']} (job {job['job_id']})")
    return added


def _render_dup_confirmation() -> None:
    """
    Render the duplicate-confirmation UI.

    Reads / writes st.session_state["_queue_dup_pending"].
    Three outcomes:
      - "Skip duplicates" → add only the unique cities
      - "Add all anyway"  → add everything, duplicates included
      - "Cancel"          → discard without adding anything
    """
    pending    = st.session_state["_queue_dup_pending"]
    duplicates = pending["duplicates"]
    unique     = pending["unique"]
    all_cities = pending["cities"]
    saved_form = pending["form_values"]

    st.warning(
        f"**{len(duplicates)} city/cities already active in the queue** "
        f"(pending / running / paused):\n\n"
        + "  ".join(f"• **{c}**" for c in duplicates)
    )
    if unique:
        st.info(f"Not yet queued ({len(unique)}): {', '.join(unique)}")
    else:
        st.info("All selected cities are already in the active queue.")

    col_skip, col_add_all, col_cancel = st.columns(3)

    with col_skip:
        skip_label = (
            f"Skip duplicates — add {len(unique)} new"
            if unique else "Nothing new to add"
        )
        if st.button(
            skip_label,
            type="primary",
            disabled=not unique,
            width="stretch",
            key="dup_skip",
        ):
            added = _add_jobs_to_queue(unique, saved_form)
            del st.session_state["_queue_dup_pending"]
            st.session_state["_queue_add_result"] = (
                f"Added {len(added)} new job(s):\n" + "\n".join(f"- {a}" for a in added)
                if added else "No new jobs added — all selected cities were duplicates."
            )
            st.rerun()

    with col_add_all:
        if st.button(
            f"Add all {len(all_cities)} anyway",
            width="stretch",
            key="dup_add_all",
        ):
            added = _add_jobs_to_queue(all_cities, saved_form)
            del st.session_state["_queue_dup_pending"]
            st.session_state["_queue_add_result"] = (
                f"Added {len(added)} job(s) (including duplicates):\n"
                + "\n".join(f"- {a}" for a in added)
            )
            st.rerun()

    with col_cancel:
        if st.button("Cancel", width="stretch", key="dup_cancel"):
            del st.session_state["_queue_dup_pending"]
            st.rerun()


# Compute paths directly — avoids importing from config.settings at module load
# time, which can fail if Streamlit serves a stale __pycache__ for settings.py.
_DATA_DIR = Path(__file__).parent.parent.parent / "data"
_SCHEDULER_PID_FILE = _DATA_DIR / "scheduler.pid"
_SCHEDULER_LOG_FILE = _DATA_DIR / "queue_runner.log"
_SCHEDULER_LOG_PREV_FILE = _DATA_DIR / "queue_runner.previous.log"


def _is_scheduler_pid_alive() -> bool:
    """Return True if the PID stored in scheduler.pid belongs to a live process."""
    import os
    import subprocess
    if not _SCHEDULER_PID_FILE.exists():
        return False
    try:
        pid = int(_SCHEDULER_PID_FILE.read_text().strip())
        if sys.platform == "win32":
            # os.kill(pid, 0) is unreliable on some Windows/Python builds and can
            # raise WinError 87 / SystemError. Use tasklist for a safe existence probe.
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                check=False,
            )
            stdout = (result.stdout or "").strip()
            return bool(stdout) and "No tasks are running" not in stdout
        os.kill(pid, 0)   # signal 0 = existence probe; raises if dead
        return True
    except (OSError, ValueError, ProcessLookupError):
        return False


def _rotate_scheduler_log() -> None:
    """
    Rotate the previous queue runner log before starting a new scheduler process.

    Keeps one backup copy:
      data/queue_runner.log           -> current run
      data/queue_runner.previous.log  -> previous run
    """
    if not _SCHEDULER_LOG_FILE.exists():
        return
    try:
        if _SCHEDULER_LOG_PREV_FILE.exists():
            _SCHEDULER_LOG_PREV_FILE.unlink()
        _SCHEDULER_LOG_FILE.replace(_SCHEDULER_LOG_PREV_FILE)
    except OSError:
        # Non-fatal: if rotation fails, fall back to appending to the existing log.
        pass


def _start_scheduler_process() -> tuple[bool, str]:
    """Launch run_queue_scheduler.py as a fully detached background process, save PID.

    On Windows we pass CREATE_NO_WINDOW so the child does not inherit
    Streamlit's console handles — without this the WebSocket disconnects
    briefly when the new process starts.
    On Unix we use start_new_session=True for the same isolation effect.
    stdin is redirected to DEVNULL on all platforms so the child never
    tries to read from the parent's terminal.

    stdout/stderr are written to data/queue_runner.log so operators can
    monitor multi-run progress from a terminal without attaching the child
    process to Streamlit's console. The previous run is rotated to
    data/queue_runner.previous.log before each new start.
    """
    import os
    import subprocess
    from src.workflow_9_queue_scheduler.queue_store import is_queue_paused, resume_queue
    script = Path(__file__).parent.parent.parent / "scripts" / "run_queue_scheduler.py"
    log_handle = None
    try:
        # Inherit the current environment and force UTF-8 mode so that
        # open() calls without an explicit encoding throughout the entire
        # pipeline default to UTF-8 instead of the system codec (e.g. GBK
        # on Chinese Windows), which would crash on Arabic/CJK text.
        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        queue_was_paused = is_queue_paused()
        if queue_was_paused:
            resume_queue()
            log.action("Queue auto-resumed on Start Runner")

        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        _rotate_scheduler_log()
        log_handle = open(_SCHEDULER_LOG_FILE, "a", encoding="utf-8")

        kwargs: dict = {
            "stdin":  subprocess.DEVNULL,
            "stdout": log_handle,
            "stderr": log_handle,
            "env":    env,
        }
        if sys.platform == "win32":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        else:
            kwargs["start_new_session"] = True

        # -X utf8 forces UTF-8 mode at the interpreter level — equivalent to
        # PYTHONUTF8=1 but applied before any module code runs, which is the
        # most reliable way to ensure print() and open() use UTF-8 on Windows.
        proc = subprocess.Popen([sys.executable, "-X", "utf8", str(script)], **kwargs)
        _SCHEDULER_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
        _SCHEDULER_PID_FILE.write_text(str(proc.pid))
        log_handle.close()
        message = (
            f"Queue runner started (PID {proc.pid}). "
            f"Logs: {_SCHEDULER_LOG_FILE}"
        )
        if queue_was_paused:
            message += " Queue was resumed automatically."
        return True, message
    except Exception as exc:
        if log_handle is not None and not log_handle.closed:
            log_handle.close()
        return False, f"Failed to start scheduler: {exc}"


def _stop_scheduler_process() -> str:
    """Send SIGTERM to the PID in scheduler.pid and remove the file."""
    import os
    import signal as _signal
    if not _SCHEDULER_PID_FILE.exists():
        return "No scheduler PID file found — scheduler may already be stopped."
    try:
        pid = int(_SCHEDULER_PID_FILE.read_text().strip())
        os.kill(pid, _signal.SIGTERM)
        _SCHEDULER_PID_FILE.unlink(missing_ok=True)
        return f"Stop signal sent to PID {pid}."
    except (OSError, ValueError, ProcessLookupError) as exc:
        _SCHEDULER_PID_FILE.unlink(missing_ok=True)
        return f"Scheduler was not running (cleaned up stale PID): {exc}"


_QUEUE_PANEL_REFRESH_INTERVAL = 4
_UI_INTERACTION_PAUSE_SECONDS = 8


def _pause_ui_autorefresh(seconds: int = _UI_INTERACTION_PAUSE_SECONDS) -> None:
    """Temporarily suspend fragment-driven auto-refresh after table interactions."""
    st.session_state["_ui_autorefresh_paused_until"] = time.time() + max(seconds, 0)


def _is_ui_autorefresh_paused() -> bool:
    paused_until = float(st.session_state.get("_ui_autorefresh_paused_until", 0) or 0)
    return paused_until > time.time()


def _format_local_timestamp(value: str) -> str:
    """Render queue timestamps in the operator machine's local timezone."""
    text = str(value or "").strip()
    if not text:
        return ""

    candidates = [
        text.replace(" UTC", "+00:00"),
        text.replace(" UTC", ""),
        text,
    ]
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                return dt.strftime("%Y-%m-%d %H:%M")
            return dt.astimezone().strftime("%Y-%m-%d %H:%M")
        except ValueError:
            continue
    return text[:16]


def _get_active_campaign_id() -> str:
    """
    Return the campaign_id of the currently running campaign by reading
    campaign_run_state.json.  The scheduler only writes campaign_id back to
    the queue job *after* the run completes, so this is the only reliable
    source during execution.
    """
    try:
        import json  # noqa: PLC0415
        from config.settings import CAMPAIGN_RUN_STATE_FILE  # noqa: PLC0415
        state_path = Path(str(CAMPAIGN_RUN_STATE_FILE))
        if not state_path.exists():
            return ""
        with open(state_path, encoding="utf-8") as f:
            return json.load(f).get("campaign_id", "")
    except Exception:
        return ""


def _get_running_job_step(campaign_id: str) -> str | None:
    """
    Return the most recently logged step name for *campaign_id*.
    Reads only the last row of the campaign runner log — minimal I/O.
    """
    try:
        from config.settings import RUNS_DIR  # noqa: PLC0415
        log_file = RUNS_DIR / campaign_id / "campaign_runner_logs.csv"
        if not log_file.exists():
            return None
        import csv  # noqa: PLC0415
        with open(log_file, encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        return rows[-1].get("step_name") if rows else None
    except Exception:
        return None


def _get_scheduler_status(summary: dict) -> dict:
    """
    Infer scheduler process state.

    Priority order:
      1. Pause flag set                    → paused
      2. Lock file or running job          → active (job in progress)
      3. PID file + process alive          → active (idle between jobs)
      4. Pending jobs but no live PID      → not_running
      5. Empty queue, no live PID          → unknown

    Returns a dict with keys:
      state  — "active" | "paused" | "not_running" | "unknown"
      detail — human-readable string
      pid_alive — bool (True if PID check confirmed a live process)
    """
    if summary.get("paused_flag"):
        return {
            "state":     "paused",
            "detail":    "Queue is paused. Click Resume Queue to continue processing.",
            "pid_alive": _is_scheduler_pid_alive(),
        }

    lock_active = is_campaign_running()
    running_job = summary.get("running_job")
    pid_alive   = _is_scheduler_pid_alive()

    if lock_active or running_job:
        if running_job:
            detail = (
                f"Processing **{running_job['location']}, {running_job['country']}** "
                f"(job {running_job['job_id']})"
            )
        else:
            detail = "Campaign lock active — a job is starting or cleaning up."
        return {"state": "active", "detail": detail, "pid_alive": pid_alive}

    if pid_alive:
        return {
            "state":     "active",
            "detail":    "Scheduler is running and waiting for jobs.",
            "pid_alive": True,
        }

    if summary.get("pending", 0) > 0:
        return {
            "state":     "not_running",
            "detail":    f"{summary['pending']} pending job(s) waiting. Start the queue runner:",
            "pid_alive": False,
        }

    return {
        "state":     "unknown",
        "detail":    "No pending jobs. Add cities via Multiple Run above, then start the runner.",
        "pid_alive": False,
    }


def _describe_queue_runner_phase(summary: dict, sched: dict) -> tuple[str, str]:
    """
    Return a human-readable phase label + explanation for the queue runner.

    This is intentionally more operator-friendly than `_get_scheduler_status()`.
    It focuses on the question the user actually cares about:
    "Did my click do anything, and what is the runner doing right now?"
    """
    running_job = summary.get("running_job")
    next_job = summary.get("next_job")
    pending = int(summary.get("pending") or 0)

    if sched["state"] == "paused":
        return (
            "Queue paused",
            "The scheduler will not claim new jobs until you click Resume Queue.",
        )

    if running_job:
        campaign_id = running_job.get("campaign_id") or _get_active_campaign_id()
        step = _get_running_job_step(campaign_id) if campaign_id else None
        if step and step in PIPELINE_STEPS:
            return (
                "Running workflow steps",
                f"Job {running_job['job_id']} is actively executing step `{step}`.",
            )
        return (
            "Job claimed, pipeline starting",
            f"Job {running_job['job_id']} has been claimed by the runner and is waiting for the first workflow step to be logged.",
        )

    if sched.get("pid_alive") and pending > 0:
        if next_job:
            return (
                "Runner alive, waiting to claim next job",
                f"The runner is up and the next pending job is {next_job['job_id']} ({next_job['location']}, {next_job['country']}).",
            )
        return (
            "Runner alive, waiting to claim next job",
            "The runner process is alive and waiting for the next pending job to be claimed.",
        )

    if sched.get("pid_alive"):
        return (
            "Runner idle",
            "The runner process is alive, but there are no pending jobs to claim right now.",
        )

    if pending > 0:
        return (
            "Pending jobs are waiting for runner start",
            "Jobs are queued, but the background runner is not currently alive.",
        )

    return (
        "Queue idle",
        "There are no pending jobs and no active queue runner work at the moment.",
    )


# ---------------------------------------------------------------------------
# Section 1c — Runner Controls
# ---------------------------------------------------------------------------

def render_runner_controls(form_values: dict) -> None:
    """Run Campaign / Resume Campaign buttons (Single Run) or Add to Queue (Multiple Run)."""
    log.ui("render_runner_controls()", run_mode=form_values.get("run_mode"))
    st.subheader("Run Controls")

    run_mode = form_values.get("run_mode", "Single Run")

    # ---- Multiple Run -------------------------------------------------------
    if run_mode == "Multiple Run":
        selected_cities = form_values.get("selected_cities", [])
        if not selected_cities:
            st.info("Select at least one city in Campaign Configuration above to add jobs to the queue.")
            return

        # Show result from a previous add (post-rerun)
        if "_queue_add_result" in st.session_state:
            result_msg = st.session_state.pop("_queue_add_result")
            st.success(result_msg)
            st.caption("Jobs added — scroll down to the Campaign Queue section to start the runner.")

        # Duplicate confirmation flow (shown instead of the normal button)
        if "_queue_dup_pending" in st.session_state:
            _render_dup_confirmation()
            return

        st.write(f"**{len(selected_cities)} city/cities selected** — each will become one scheduler job.")

        if st.button(
            f"Add {len(selected_cities)} city/cities to Queue",
            type="primary",
            width="stretch",
        ):
            try:
                from src.workflow_9_queue_scheduler.queue_store import list_jobs  # noqa: PLC0415
            except ImportError as exc:
                st.error(f"Queue module not available: {exc}")
                return

            active_jobs = [j for j in list_jobs() if j.get("status") in _ACTIVE_QUEUE_STATUSES]
            duplicates, unique = _detect_dup_jobs(selected_cities, form_values, active_jobs)

            log.action("Add to Queue clicked", cities=len(selected_cities), duplicates=len(duplicates), unique=len(unique))

            if duplicates:
                # Enter confirmation state and re-render
                st.session_state["_queue_dup_pending"] = {
                    "duplicates": duplicates,
                    "unique":     unique,
                    "cities":     selected_cities,
                    "form_values": dict(form_values),
                }
                st.rerun()
            else:
                added = _add_jobs_to_queue(selected_cities, form_values)
                st.success(f"Added {len(added)} job(s):\n" + "\n".join(f"- {a}" for a in added))
                st.caption("Jobs added — use the ▶ Start Runner button in the Campaign Queue section below.")
        return

    # ---- Single Run ---------------------------------------------------------
    # Display result from a previous run (persisted across the rerun)
    if "_run_result" in st.session_state:
        _display_run_result(st.session_state.pop("_run_result"))

    # Lock guard — disable buttons while another run is active
    locked = is_campaign_running()
    if locked:
        st.warning(
            "⏳ A campaign is currently running. "
            "Buttons are disabled until it completes. "
            "If this is stale, delete `data/campaign_run.lock` to force-unlock."
        )

    col_run, col_resume = st.columns(2)

    with col_run:
        if st.button("▶ Run Campaign", type="primary",
                     width="stretch", disabled=locked):
            log.action("▶ Run Campaign clicked",
                       city=form_values.get("base_city"),
                       country=form_values.get("country"),
                       send_mode=form_values.get("send_mode"),
                       run_until=form_values.get("run_until"),
                       dry_run=form_values.get("dry_run"))
            with st.spinner("Running campaign via Workflow 9…"):
                result = run_campaign_from_ui(form_values)
            log.action("Run Campaign finished",
                       success=result.success,
                       campaign_id=result.campaign_id,
                       status=result.status,
                       last_step=result.last_completed_step,
                       error=result.error[:200] if result.error else "")
            refresh_dashboard_state()
            st.session_state["_run_result"] = result
            st.rerun()

    with col_resume:
        if st.button("⟳ Resume Campaign",
                     width="stretch", disabled=locked):
            log.action("⟳ Resume Campaign clicked")
            with st.spinner("Resuming last campaign…"):
                result = resume_campaign_from_ui()
            log.action("Resume Campaign finished",
                       success=result.success,
                       campaign_id=result.campaign_id,
                       status=result.status,
                       last_step=result.last_completed_step,
                       error=result.error[:200] if result.error else "")
            refresh_dashboard_state()
            st.session_state["_run_result"] = result
            st.rerun()


def _display_run_result(result: RunResult) -> None:
    if result.success:
        st.success(
            f"Campaign **{result.campaign_id}** completed.  "
            f"Last step: `{result.last_completed_step}`  |  "
            f"Steps done: {len(result.completed_steps)}"
        )
    else:
        st.error(f"Campaign failed.\n\n```\n{result.error}\n```")
        if result.last_completed_step:
            st.info(f"Last successful step: `{result.last_completed_step}`")


# ---------------------------------------------------------------------------
# Section 2 — Current Campaign State
# ---------------------------------------------------------------------------

def render_campaign_state_view() -> None:
    log.ui("render_campaign_state_view()")
    st.header("Current Campaign State")
    state = load_current_campaign_state()

    if not state:
        log.state("No campaign state file found")
        st.info("No campaign state found. Run a campaign to populate this section.")
        return

    run_status = state.get("status", "")
    cloud_status = state.get("cloud_deploy_status") or "not_enabled"
    cloud_updated = state.get("cloud_deploy_updated_at") or "—"
    log.state("Campaign state loaded",
              campaign_id=state.get("campaign_id"),
              status=run_status,
              last_step=state.get("last_completed_step"),
              error=str(state.get("error_message", ""))[:120])

    # Only show full live details when a campaign is actively running.
    # Stale completed/failed states from previous sessions are collapsed so the
    # dashboard looks clean when opened with no active run.
    if run_status == "running":
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Campaign ID",  state.get("campaign_id", "—"))
        col2.metric("Status",       run_status)
        col3.metric("Last Step",    state.get("last_completed_step") or "—")
        col4.metric("Cloud Deploy", cloud_status)

        col5, col6, col7 = st.columns(3)
        col5.metric("Started",       state.get("started_at", "—"))
        col6.metric("Updated",       state.get("updated_at", "—"))
        col7.metric("Cloud Updated", cloud_updated)

        if state.get("cloud_deploy_error"):
            st.warning(f"Cloud deploy error: {state['cloud_deploy_error']}")

        cfg = state.get("config", {})
        if cfg:
            with st.expander("Campaign Config", expanded=False):
                st.json(cfg)
    else:
        # No active run — show a neutral message and collapse past-run details
        st.info("No campaign is currently running.")

        label = "Last run"
        if run_status == "completed":
            label = f"Last run — {state.get('campaign_id', '')} (completed)"
        elif run_status == "failed":
            label = f"Last run — {state.get('campaign_id', '')} (failed)"

        with st.expander(label, expanded=False):
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Campaign ID",  state.get("campaign_id", "—"))
            col2.metric("Status",       run_status or "—")
            col3.metric("Last Step",    state.get("last_completed_step") or "—")
            col4.metric("Cloud Deploy", cloud_status)

            col5, col6, col7 = st.columns(3)
            col5.metric("Started",       state.get("started_at", "—"))
            col6.metric("Updated",       state.get("updated_at", "—"))
            col7.metric("Cloud Updated", cloud_updated)

            if state.get("error_message"):
                st.error(f"Error: {state['error_message']}")
            if state.get("cloud_deploy_error"):
                st.warning(f"Cloud deploy error: {state['cloud_deploy_error']}")

            cfg = state.get("config", {})
            if cfg:
                st.json(cfg)


# ---------------------------------------------------------------------------
# Section 3 — KPI Dashboard (enhanced in 9.6)
# ---------------------------------------------------------------------------

def render_kpi_dashboard() -> None:
    """Enhanced KPI panel with rates and pipeline conversion metrics."""
    log.ui("render_kpi_dashboard()")
    st.header("KPI Dashboard")
    m = load_pipeline_metrics()
    state = load_current_campaign_state()
    ops = load_delivery_ops_snapshot()
    worker = load_cloud_worker_health()
    log.data("KPI metrics loaded",
             companies=m.get("total_companies"),
             emails_sent=m.get("emails_sent"),
             opens=m.get("open_count"))

    current_country = ops.get("current_country") or "Idle"
    current_location = ops.get("current_location") or "No active city"
    current_status = ops.get("current_status") or "idle"
    yesterday_date = ops.get("yesterday_date") or "yesterday"

    st.subheader("Send Ops Snapshot")
    s1, s2, s3, s4 = st.columns(4)
    s1.metric(
        "Cloud Delegated Emails",
        ops.get("cloud_delegated_emails", 0),
        delta=f"{ops.get('cloud_run_count', 0)} cloud runs",
        help="Emails already handed off to cloud sending runs in gmail_api mode.",
    )
    s2.metric(
        "Sent Successfully",
        ops.get("sent_successfully", 0),
        help="Successful cloud sends recorded in send_batch_summary.json.",
    )
    s3.metric(
        "Current Country",
        current_country,
        delta=f"{current_location} | {current_status}",
        help="Current sending country from the live queue or the latest run state.",
    )
    s4.metric(
        "Uploaded Yesterday",
        ops.get("uploaded_yesterday_emails", 0),
        delta=f"{ops.get('uploaded_yesterday_runs', 0)} runs on {yesterday_date}",
        help="Cloud-uploaded email volume from the previous day.",
    )

    st.caption(
        "Quick answer for operations: how many emails are already in cloud sending, "
        "how many actually sent, which country is active now, and how much was uploaded yesterday."
    )
    st.caption(
        "Cloud queue metrics prefer live cloud state: local run folders are reconciled against "
        "GCS run status and the worker mirror before counts are shown."
    )

    q1, q2, q3, q4 = st.columns(4)
    q1.metric(
        "Queued in Cloud",
        ops.get("cloud_queued_runs", 0),
        help="Runs uploaded to cloud send but not yet synced by the worker.",
    )
    q2.metric(
        "Waiting Window",
        ops.get("cloud_waiting_runs", 0),
        help="Runs synced by the worker and waiting for the target-market send window.",
    )
    q3.metric(
        "Sending Now",
        ops.get("cloud_sending_runs", 0),
        help="Runs currently in the cloud worker send stage.",
    )
    q4.metric(
        "Cloud Failures",
        ops.get("cloud_failed_runs", 0),
        help="Runs that entered an explicit cloud send failed state and need operator recovery.",
    )

    st.subheader("Cloud Worker Health")
    w1, w2, w3, w4 = st.columns(4)
    w1.metric("Worker Health", worker.get("worker_health", "unknown"))
    w2.metric("Active Campaign", worker.get("active_campaign_id") or "Idle")
    w3.metric("Alerts 24h", worker.get("alerts_24h", 0))
    release_label = worker.get("release_commit_short") or "unknown"
    if worker.get("release_branch"):
        release_label = f"{release_label} ({worker['release_branch']})"
    w4.metric("Deployed Commit", release_label)

    st.caption(
        f"Last poll: {worker.get('last_poll_at') or 'n/a'} | "
        f"Last success: {worker.get('last_success_at') or 'n/a'} | "
        f"Last error: {worker.get('last_error_at') or 'n/a'} | "
        f"Idle reason: {worker.get('last_idle_reason') or 'n/a'} | "
        f"Poll result: {worker.get('last_poll_result') or 'n/a'}"
    )
    st.caption(
        f"Bucket: {worker.get('worker_bucket') or 'n/a'} | "
        f"Manifest prefix: {worker.get('worker_manifests_prefix') or 'n/a'} | "
        f"Manifest count: {worker.get('last_manifest_count') or 0} | "
        f"Inflight count: {worker.get('last_inflight_count') or 0} | "
        f"Actionable candidates: {worker.get('last_candidate_count') or 0}"
    )
    if worker.get("last_sync_campaign_id") or worker.get("last_reconciled_campaign_id"):
        st.caption(
            f"Last synced campaign: {worker.get('last_sync_campaign_id') or 'n/a'} | "
            f"Last reconciled campaign: {worker.get('last_reconciled_campaign_id') or 'n/a'}"
        )
    if worker.get("last_selected_campaign_id") or worker.get("last_selected_due_at"):
        st.caption(
            f"Selected campaign: {worker.get('last_selected_campaign_id') or 'n/a'} | "
            f"Selected due: {worker.get('last_selected_due_at') or 'n/a'}"
        )
    if worker.get("claimed_campaign_id") or worker.get("claimed_manifest_uri"):
        st.caption(
            f"Claimed campaign: {worker.get('claimed_campaign_id') or 'n/a'} | "
            f"Claimed manifest: {worker.get('claimed_manifest_uri') or 'n/a'}"
        )
    if worker.get("last_candidate_campaign_ids"):
        candidate_sample = worker.get("last_candidate_campaign_ids") or []
        st.caption("Candidate sample: " + " | ".join(candidate_sample[:5]))
    if worker.get("last_manifest_sample"):
        sample = worker.get("last_manifest_sample") or []
        st.caption("Manifest sample: " + " | ".join(sample[:3]))
    if worker.get("last_inflight_sample"):
        sample = worker.get("last_inflight_sample") or []
        st.caption("Inflight sample: " + " | ".join(sample[:3]))
    if worker.get("last_alert_message"):
        st.caption(
            f"Last alert: {worker.get('last_alert_at') or 'n/a'} | "
            f"{worker.get('last_alert_level') or 'n/a'} / {worker.get('last_alert_type') or 'n/a'} | "
            f"{worker.get('last_alert_message')}"
        )
    if worker.get("worker_config_issue"):
        st.error(f"Cloud worker config issue: {worker['worker_config_issue']}")
    if worker.get("worker_health") in {"stalled", "offline", "misconfigured"}:
        st.warning(
            "Cloud worker health is derived from mirrored VM state. Short GCS sync delay is possible, "
            "so confirm VM service health before assuming cloud send is stalled or offline."
        )

    with st.expander("Current Cloud Handoff Detail", expanded=False):
        d1, d2, d3 = st.columns(3)
        d1.metric("Local Run", state.get("status") or "unknown")
        d2.metric("Cloud Deploy", state.get("cloud_deploy_status") or "not_enabled")
        d3.metric("Cloud Send", state.get("cloud_send_status") or "not_queued")

        d4, d5, d6 = st.columns(3)
        d4.metric("Cloud Updated", state.get("cloud_send_updated_at") or "-")
        d5.metric("Due At", state.get("cloud_send_due_at") or "-")
        d6.metric("Market", state.get("cloud_send_market") or "-")

        if state.get("cloud_deploy_error"):
            st.warning(f"Cloud deploy error: {state['cloud_deploy_error']}")
        if state.get("cloud_send_error"):
            st.warning(f"Cloud send error: {state['cloud_send_error']}")
        if state.get("cloud_send_failed_stage"):
            st.caption(f"Cloud send failed stage: {state['cloud_send_failed_stage']}")
        if state.get("cloud_send_processed_manifest_uri"):
            st.caption(f"Processed manifest: {state['cloud_send_processed_manifest_uri']}")
        if state.get("cloud_send_failed_manifest_uri"):
            st.caption(f"Failed manifest: {state['cloud_send_failed_manifest_uri']}")
        if state.get("cloud_deploy_file_count") or state.get("cloud_deploy_elapsed_seconds"):
            st.caption(
                "Deploy upload stats: "
                f"{state.get('cloud_deploy_file_count') or 0} files | "
                f"{state.get('cloud_deploy_bytes') or 0} bytes | "
                f"{state.get('cloud_deploy_elapsed_seconds') or 0}s | "
                f"{state.get('cloud_deploy_upload_mode') or 'n/a'}"
            )
        if state.get("cloud_send_uploaded_file_count") or state.get("cloud_send_upload_elapsed_seconds"):
            st.caption(
                "Cloud send sync-back stats: "
                f"{state.get('cloud_send_uploaded_file_count') or 0} files | "
                f"{state.get('cloud_send_uploaded_bytes') or 0} bytes | "
                f"{state.get('cloud_send_upload_elapsed_seconds') or 0}s | "
                f"{state.get('cloud_send_upload_mode') or 'n/a'}"
            )

    st.subheader("Ready To Deploy")
    ready_runs = load_ready_cloud_deploys(limit=12)
    if "_cloud_batch_deploy_result" in st.session_state:
        result = st.session_state.pop("_cloud_batch_deploy_result")
        if result.success:
            st.success(
                f"Batch deploy finished. Requested={result.requested} "
                f"Deployed={result.deployed} Skipped={result.skipped} Failed={result.failed}"
            )
        else:
            st.error(
                f"Batch deploy failed. Requested={result.requested} "
                f"Deployed={result.deployed} Skipped={result.skipped} Failed={result.failed}"
            )
        for msg in result.messages[:8]:
            st.caption(msg)

    a1, a2 = st.columns(2)
    with a1:
        if st.button("Deploy Top 5", key="deploy_top_5", disabled=not ready_runs):
            batch_result = trigger_cloud_batch_deploy(limit=5)
            refresh_dashboard_state()
            st.session_state["_cloud_batch_deploy_result"] = batch_result
            st.rerun()
    with a2:
        if st.button("Deploy All Ready", key="deploy_all_ready", disabled=not ready_runs):
            batch_result = trigger_cloud_batch_deploy(limit=0)
            refresh_dashboard_state()
            st.session_state["_cloud_batch_deploy_result"] = batch_result
            st.rerun()

    if ready_runs:
        st.caption(
            "Completed non-dry-run campaigns that are either not yet handed off to cloud send "
            "or look stale and need a manifest re-deploy."
        )
        st.caption(
            "Some `gmail_api` runs may already be handed off automatically when Workflow 9 "
            "finishes at `campaign_status`, so a run can show local cloud-deploy metadata "
            "even if you did not click a Deploy button here."
        )
        ready_df = pd.DataFrame(ready_runs)
        ready_df["location"] = ready_df.apply(
            lambda row: f"{row.get('location', '')}, {row.get('country', '')}".strip(", "),
            axis=1,
        )
        ready_df["select"] = False
        ready_df = ready_df.rename(columns={
            "campaign_id": "Campaign",
            "location": "Location",
            "queue_count": "Queue",
            "send_mode": "Send Mode",
            "run_until": "Run Until",
            "deploy_status": "Deploy Status",
            "cloud_send_status": "Cloud Send",
            "recovery_reason": "Recovery Reason",
            "modified": "Modified",
        })
        edited_ready = st.data_editor(
            ready_df[[
                "select", "Campaign", "Location", "Queue",
                "Send Mode", "Run Until", "Deploy Status", "Cloud Send",
                "Recovery Reason", "Modified",
            ]],
            width="stretch",
            hide_index=True,
            key="ready_cloud_deploy_editor",
            column_config={
                "select": column_config.CheckboxColumn(
                    "Select",
                    help="Select one or more ready runs for targeted cloud deploy.",
                    default=False,
                ),
            },
            disabled=[
                "Campaign", "Location", "Queue", "Send Mode",
                "Run Until", "Deploy Status", "Cloud Send",
                "Recovery Reason", "Modified",
            ],
        )
        selected_campaigns = edited_ready.loc[edited_ready["select"] == True, "Campaign"].tolist()
        selected_signature = tuple(sorted(selected_campaigns))
        previous_signature = tuple(st.session_state.get("_ready_deploy_selected_campaigns", ()))
        if selected_signature != previous_signature:
            st.session_state["_ready_deploy_selected_campaigns"] = list(selected_signature)
            _pause_ui_autorefresh()
        if st.button("Deploy Selected", key="deploy_selected_ready", disabled=not selected_campaigns):
            batch_result = trigger_cloud_batch_deploy(campaign_ids=selected_campaigns)
            refresh_dashboard_state()
            st.session_state["_cloud_batch_deploy_result"] = batch_result
            st.rerun()
        if _is_ui_autorefresh_paused():
            st.caption(
                "Auto-refresh is temporarily paused for a few seconds so row selection stays stable while you pick campaigns."
            )
        st.caption(
            "Rows marked `stale_handoff_redeploy` mean the run reached cloud deploy completed "
            "but never advanced into cloud send, so re-deploying will re-queue its manifest."
        )
    else:
        st.info("No completed runs are currently waiting for cloud deploy.")

    st.subheader("Cloud Deploy History / Reconciliation")
    recon_rows = load_cloud_deploy_reconciliation(limit=20)
    if recon_rows:
        recon_df = pd.DataFrame(recon_rows)
        recon_df["location"] = recon_df.apply(
            lambda row: f"{row.get('location', '')}, {row.get('country', '')}".strip(", "),
            axis=1,
        )
        recon_df = recon_df.rename(columns={
            "campaign_id": "Campaign",
            "location": "Location",
            "final_send_queue": "Emails In Final Queue",
            "cloud_handoff": "Cloud Handoff",
            "deploy_status": "Deploy Status",
            "cloud_send_status": "Cloud Send",
            "reconciliation": "Reconciliation",
            "note": "Note",
            "modified": "Modified",
        })
        st.caption(
            "Recent live-send handoffs reconciled across local run files, queue metadata, "
            "and cloud worker state. Use this to distinguish manual deploys, auto-on-complete, "
            "active waiting campaigns, failures, and historical mismatches."
        )
        st.caption(
            "Cloud send lifecycle: `queued in cloud manifest backlog` -> "
            "`claimed by cloud worker, waiting for send window` -> `sending` -> `completed`."
        )
        st.dataframe(
            recon_df[[
                "Campaign", "Location", "Emails In Final Queue", "Cloud Handoff",
                "Deploy Status", "Cloud Send", "Reconciliation",
                "Note", "Modified",
            ]],
            width="stretch",
            hide_index=True,
        )
    else:
        st.caption("No recent live-send cloud handoff history found.")

    r1, r2, r3, r4 = st.columns(4)
    r1.metric(
        "Bounces 7d",
        ops.get("bounces_7d", 0),
        delta=f"{ops.get('bounce_addresses', 0)} addresses",
        help="Bounce events recorded in engagement logs over the last 7 days.",
    )
    r2.metric(
        "Bounce Rate 7d",
        f"{ops.get('bounce_rate_7d', 0.0)}%",
        delta=f"{ops.get('sent_7d', 0)} sent",
        help="Bounces divided by true sent emails over the last 7 days.",
    )
    r3.metric(
        "Suppressed Addresses",
        ops.get("suppressed_addresses", 0),
        help="Unique emails currently suppressed by reply / bounce handling.",
    )
    r4.metric(
        "Last Bounce",
        ops.get("last_bounce_at") or "None",
        help="Most recent bounce timestamp seen in engagement logs.",
    )

    # Row 1 — pipeline volumes
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Companies",        m.get("total_companies",  0))
    c2.metric("Qualified Leads",  m.get("qualified_leads",  0))
    c3.metric("Contacts",         m.get("total_contacts",   0))
    c4.metric("Emails Generated", m.get("emails_generated", 0))
    c5.metric("Emails Sent",      m.get("emails_sent",      0))

    # Row 2 — engagement + conversion rates
    c6, c7, c8, c9, c10 = st.columns(5)
    c6.metric("Opens",             m.get("open_count",   0))
    c7.metric("Clicks",            m.get("click_count",  0))
    c8.metric("Open Rate",         f"{m.get('open_rate',  0.0)}%")
    c9.metric("Click Rate",        f"{m.get('click_rate', 0.0)}%")
    c10.metric("Follow-up Queued", m.get("followup_queued", 0))

    # Row 3 — pipeline conversion rates
    c11, c12, c13, c14, c15 = st.columns(5)
    c11.metric("Qualification Rate",   f"{m.get('qualification_rate', 0.0)}%",
               help="qualified_leads / companies")
    c12.metric("Contact Rate",         f"{m.get('contact_rate', 0.0)}%",
               help="contacts / companies")
    c13.metric("Email Gen Rate",       f"{m.get('email_gen_rate', 0.0)}%",
               help="emails_generated / contacts")
    c14.metric("Send Review Queue", m.get("review_required", 0),
               help="Rows tagged for manual confirmation in Workflow 7.")
    c15.metric("Hard Send Blocks", m.get("send_hard_blocked", 0),
               help="True send-stage hard blocks, excluding review-required rows.")

    if m.get("blocked_count", 0) > 0:
        st.caption(f"Follow-up blocked contacts in current campaign view: {m['blocked_count']}")

    summary = load_campaign_summary()
    if summary:
        with st.expander("Campaign Status Summary", expanded=False):
            st.json(summary)


# ---------------------------------------------------------------------------
# legacy alias kept for app.py compatibility
def render_metrics_view() -> None:
    return render_kpi_dashboard()


def render_multi_run_comparison_view() -> None:
    log.ui("render_multi_run_comparison_view()")
    st.header("Multi-Run Comparison")
    st.caption(
        "Recent completed queue jobs compared side by side: funnel volume, dedup loss, "
        "repair lift, and generic-contact dependency."
    )

    rows = load_multi_run_comparison(limit=8)
    if not rows:
        st.info("No completed multi-run jobs found yet.")
        return

    df = pd.DataFrame(rows)
    for col in (
        "dedup_rate_pct",
        "final_queue_rate",
        "generic_only_pct",
        "review_required_pct",
        "delivery_ready_rate",
    ):
        if col not in df.columns:
            df[col] = 0.0
    for col in ("review_required", "hard_blocked", "delivery_ready"):
        if col not in df.columns:
            df[col] = 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cities Compared", len(df))
    c2.metric("Avg Dedup Loss", f"{df['dedup_rate_pct'].mean():.1f}%")
    c3.metric("Avg Delivery-Ready", f"{df['delivery_ready_rate'].mean():.1f}%")
    c4.metric("Avg Review Queue", f"{df['review_required_pct'].mean():.1f}%")

    display = df.rename(columns={
        "completed_at": "Completed",
        "location": "City",
        "campaign_id": "Campaign",
        "raw_leads": "Raw",
        "dedup_skipped": "Dedup Skipped",
        "dedup_rate_pct": "Dedup %",
        "qualified": "Qualified",
        "qualification_pct": "Qualify %",
        "contacts": "Contacts",
        "generated_emails": "Generated",
        "send_queue_initial": "Initial Queue",
        "final_send_queue": "Final Queue",
        "delivery_ready": "Delivery Ready",
        "delivery_ready_rate": "Delivery-Ready %",
        "review_required": "Review Required",
        "review_required_pct": "Review %",
        "hard_blocked": "Hard Blocked",
        "repair_lift": "Repair Lift",
        "final_rejected": "Rejected",
        "generic_only_pct": "Generic-Only %",
    })

    st.dataframe(display, width="stretch", hide_index=True)
    st.caption(
        "Reading guide: high `Dedup %` means overlap with recent nearby runs; high "
        "`Review %` means more records were tagged for operator confirmation instead "
        "of auto-send; high `Generic-Only %` means the city relied heavily on generic mailboxes."
    )


def render_manual_review_queue_view() -> None:
    log.ui("render_manual_review_queue_view()")
    st.header("Manual Review Queue")
    st.caption(
        "Soft-risk records from the send step. These were tagged for operator review "
        "instead of being auto-blocked."
    )

    rows = load_manual_review_queue(limit=200)
    if not rows:
        st.info("No manual-review records found for the current campaign view.")
        return

    df = pd.DataFrame(rows)
    if "review_reason" not in df.columns:
        df["review_reason"] = ""
    if "review_tags" not in df.columns:
        df["review_tags"] = ""

    c1, c2, c3 = st.columns(3)
    c1.metric("Review Items", len(df))
    c2.metric(
        "Unique Companies",
        df["company_name"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()
        if "company_name" in df.columns else 0,
    )
    c3.metric(
        "Unique Reasons",
        df["review_reason"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique(),
    )

    preferred = [
        "company_name", "kp_name", "kp_email",
        "subject", "review_reason", "review_tags",
        "send_policy_action", "send_policy_reason",
        "overall_score", "lead_score",
    ]
    display_cols = [c for c in preferred if c in df.columns]
    st.dataframe(df[display_cols] if display_cols else df, width="stretch", hide_index=True)
    st.caption(f"{len(df)} record(s) require manual review.")


# ---------------------------------------------------------------------------
# Section 4 — Runner Logs (with Refresh button — 9.6)
# ---------------------------------------------------------------------------

def render_logs_view() -> None:
    log.ui("render_logs_view()")
    st.header("Runner Logs")

    col_hdr, col_btn = st.columns([5, 1])
    with col_hdr:
        st.subheader("Recent Steps")
    with col_btn:
        if st.button("🔄 Refresh", key="refresh_logs"):
            log.action("Refresh Logs clicked")
            refresh_dashboard_state()
            st.rerun()

    logs = load_campaign_logs(limit=50)
    log.data("Campaign logs loaded", count=len(logs))
    if not logs:
        st.info("No runner logs yet. Run a campaign to see step-by-step execution logs.")
        return

    df = pd.DataFrame(logs)
    # Ensure expected columns exist
    for col in ("timestamp", "campaign_id", "step_name", "status", "message"):
        if col not in df.columns:
            df[col] = ""

    df = df[["timestamp", "campaign_id", "step_name", "status", "message"]]

    _ICONS = {"completed": "✅", "started": "▶", "skipped": "⏭", "failed": "❌"}
    df["status"] = df["status"].apply(lambda s: _ICONS.get(s, s))

    st.dataframe(df, width="stretch", hide_index=True)
    st.caption(f"Showing last {len(df)} log rows (most recent first)")


# ---------------------------------------------------------------------------
# Section 5 — Campaign Status Table (existing, unchanged)
# ---------------------------------------------------------------------------

def render_status_table_view() -> None:
    log.ui("render_status_table_view()")
    st.header("Campaign Status Table")
    rows = load_campaign_status()
    log.data("Campaign status loaded", rows=len(rows))

    if not rows:
        st.info("campaign_status.csv not found. Run a full pipeline to populate this table.")
        return

    df = pd.DataFrame(rows)
    preferred = [
        "company_name", "kp_email", "lifecycle_status",
        "next_action", "priority_flag", "open_count", "click_count",
        "latest_followup_stage",
    ]
    display_cols = [c for c in preferred if c in df.columns]
    df_display = df[display_cols].fillna("") if display_cols else df.fillna("")

    with st.expander("Filters", expanded=False):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            status_options = (
                ["All"] + sorted(df["lifecycle_status"].dropna().unique().tolist())
                if "lifecycle_status" in df.columns else ["All"]
            )
            status_filter = st.selectbox("Lifecycle Status", status_options, key="st_filter")
        with fc2:
            pf_options = (
                ["All"] + sorted(df["priority_flag"].dropna().unique().tolist())
                if "priority_flag" in df.columns else ["All"]
            )
            priority_filter = st.selectbox("Priority Flag", pf_options, key="pf_filter")
        with fc3:
            search = st.text_input("Search company name", key="cn_search")

    filtered = df_display.copy()
    if status_filter != "All" and "lifecycle_status" in filtered.columns:
        filtered = filtered[filtered["lifecycle_status"] == status_filter]
    if priority_filter != "All" and "priority_flag" in filtered.columns:
        filtered = filtered[filtered["priority_flag"] == priority_filter]
    if search and "company_name" in filtered.columns:
        filtered = filtered[
            filtered["company_name"].str.contains(search, case=False, na=False)
        ]

    st.dataframe(filtered, width="stretch", hide_index=True)
    st.caption(f"Showing {len(filtered)} of {len(df)} contacts")


# ---------------------------------------------------------------------------
# Section 6 — Output File Status (basic, retained for compatibility)
# ---------------------------------------------------------------------------

def render_file_status_view() -> None:
    st.header("Output File Status")
    st.caption("Shows which pipeline steps have produced output files.")

    file_statuses = load_file_status()
    df = pd.DataFrame(file_statuses)
    df["present"] = df["exists"].apply(lambda x: "✅" if x else "—")
    df["size"] = df.apply(lambda r: f"{r['size_kb']} KB" if r["exists"] else "", axis=1)
    display = df[["file", "present", "size"]].rename(columns={
        "file": "File", "present": "Present", "size": "Size",
    })
    st.dataframe(display, width="stretch", hide_index=True)


# ---------------------------------------------------------------------------
# Section 7 — High Priority Leads (9.6)
# ---------------------------------------------------------------------------

def render_high_priority_leads_view() -> None:
    log.ui("render_high_priority_leads_view()")
    st.header("High Priority Leads")
    st.caption(
        "Shown if any: priority_flag=high | lifecycle=clicked_no_reply or followup_queued "
        "| open_count >= 2 | lead_score >= 70."
    )

    rows = load_high_priority_leads()
    if not rows:
        from config.settings import CAMPAIGN_STATUS_FILE
        if not CAMPAIGN_STATUS_FILE.exists():
            st.info(
                "campaign_status.csv not found. "
                "Run the pipeline through the campaign_status step to populate this view."
            )
        else:
            st.info("No high-priority leads match the current filter criteria.")
        return

    df = pd.DataFrame(rows)
    preferred = [
        "company_name", "kp_name", "kp_email",
        "lifecycle_status", "next_action", "priority_flag",
        "open_count", "click_count", "latest_followup_stage",
    ]
    display_cols = [c for c in preferred if c in df.columns]
    df_display = df[display_cols].fillna("") if display_cols else df.fillna("")

    st.dataframe(df_display, width="stretch", hide_index=True)
    st.caption(f"{len(rows)} high-priority contacts")


# ---------------------------------------------------------------------------
# Section 8 — Company Lifecycle Detail (9.6)
# ---------------------------------------------------------------------------

def render_company_detail_view() -> None:
    log.ui("render_company_detail_view()")
    st.header("Company Lifecycle Detail")

    from config.settings import CAMPAIGN_STATUS_FILE
    if not CAMPAIGN_STATUS_FILE.exists():
        st.info(
            "campaign_status.csv not found. "
            "Run the pipeline through the **campaign_status** step to populate lifecycle detail."
        )
        return

    company_names = load_company_names()
    if not company_names:
        st.info("campaign_status.csv exists but contains no company records.")
        return

    selected = st.selectbox(
        "Select a company",
        options=["— select —"] + company_names,
        key="company_detail_select",
    )

    if selected == "— select —":
        st.caption("Choose a company above to see its full lifecycle detail.")
        return

    detail = get_company_detail(selected)
    if not detail:
        st.warning(f"No data found for: {selected}")
        return

    # Identity
    st.subheader("Identity")
    id_col1, id_col2 = st.columns(2)
    id_col1.markdown(f"**Company:** {detail.get('company_name', '—')}")
    id_col1.markdown(f"**Website:** {detail.get('website', '—')}")
    id_col2.markdown(f"**Contact:** {detail.get('kp_name', '—')}")
    id_col2.markdown(f"**Email:** {detail.get('kp_email', '—')}")

    # Context
    st.subheader("Lead Context")
    ctx1, ctx2, ctx3 = st.columns(3)
    ctx1.metric("Company Type",  detail.get("company_type",  "—"))
    ctx2.metric("Market Focus",  detail.get("market_focus",  "—"))
    ctx3.metric("Lead Score",    detail.get("lead_score",    "—"))

    # Send state
    st.subheader("Send State")
    s1, s2, s3 = st.columns(3)
    s1.metric("Initial Send",       detail.get("initial_sent",        "—"))
    s2.metric("Latest Send Status", detail.get("latest_send_status",  "—"))
    s3.metric("Send Attempts",      detail.get("total_send_attempts", "—"))

    # Engagement
    st.subheader("Engagement")
    e1, e2, e3, e4 = st.columns(4)
    e1.metric("Opens",  detail.get("open_count",  0))
    e2.metric("Clicks", detail.get("click_count", 0))
    e3.metric("First Open",  detail.get("first_open_time",  "—"))
    e4.metric("Last Click",  detail.get("last_click_time",  "—"))

    # Follow-up
    st.subheader("Follow-up")
    f1, f2, f3 = st.columns(3)
    f1.metric("Latest Stage",  detail.get("latest_followup_stage",  "—"))
    f2.metric("Stage Status",  detail.get("latest_followup_status", "—"))
    f3.metric("Next Action",   detail.get("next_action",            "—"))

    # Latest email (if enriched)
    if detail.get("latest_subject"):
        st.subheader("Latest Generated Email")
        st.markdown(f"**Subject:** {detail['latest_subject']}")
        body = detail.get("latest_body", "")
        if body:
            with st.expander("Email Body", expanded=False):
                st.text(body[:2000])


# ---------------------------------------------------------------------------
# Section 9 — Manual Send followup_1 (9.6)
# ---------------------------------------------------------------------------

def render_manual_followup_action() -> None:
    log.ui("render_manual_followup_action()")
    st.header("Manual Action: Send followup_1")
    st.caption(
        "Manually trigger a followup_1 send for contacts currently in the follow-up queue. "
        "All existing safety checks (dedup, business hours, guard rules) still apply."
    )

    from src.workflow_9_5_streamlit_control_panel.ui_state import load_followup_1_candidates
    candidates = load_followup_1_candidates()

    if not candidates:
        st.info(
            "No followup_1 candidates found in followup_queue.csv. "
            "Run Workflow 8 (Follow-up Automation) to generate the queue."
        )
        return

    st.write(f"**{len(candidates)}** followup_1 candidate(s) ready in queue.")

    col_mode, col_btn = st.columns([2, 1])
    with col_mode:
        action_mode = st.selectbox(
            "Send Mode for this action",
            options=["dry_run", "smtp"],
            key="followup_send_mode",
            help="dry_run logs results without sending real emails.",
        )
    with col_btn:
        st.write("")  # vertical spacing
        send_clicked = st.button(
            "📤 Send followup_1 now",
            type="primary",
            width="stretch",
            key="manual_followup_btn",
        )

    if send_clicked:
        log.action("📤 Send followup_1 clicked", mode=action_mode, candidates=len(candidates))
        with st.spinner(f"Processing {len(candidates)} followup_1 candidates ({action_mode})…"):
            result = manual_send_followup_1(send_mode=action_mode)
        log.action("Send followup_1 finished",
                   attempted=result.attempted, sent=result.sent,
                   dry_run=result.dry_run, blocked=result.blocked, errors=result.errors)
        refresh_dashboard_state()

        if action_mode == "dry_run":
            st.success(
                f"Dry-run complete. Attempted: {result.attempted}  |  "
                f"Dry-run logged: {result.dry_run}  |  "
                f"Blocked: {result.blocked}  |  Errors: {result.errors}"
            )
        else:
            st.success(
                f"Send complete. Attempted: {result.attempted}  |  "
                f"Sent: {result.sent}  |  Blocked: {result.blocked}  |  "
                f"Errors: {result.errors}"
            )

        if result.messages:
            with st.expander("Send Details", expanded=True):
                for msg in result.messages:
                    st.text(msg)


# ---------------------------------------------------------------------------
# Section 10 — Enhanced File Status (9.6)
# ---------------------------------------------------------------------------

def render_enhanced_file_status_view() -> None:
    log.ui("render_enhanced_file_status_view()")
    st.header("Pipeline File Status")
    st.caption("All pipeline output files — existence, row count, size, and last modified time.")

    col_hdr, col_btn = st.columns([5, 1])
    with col_btn:
        if st.button("🔄 Refresh", key="refresh_files"):
            refresh_dashboard_state()
            st.rerun()

    statuses = load_enhanced_file_status()
    df = pd.DataFrame(statuses)
    df["present"] = df["exists"].apply(lambda x: "✅" if x else "—")
    df["size"] = df.apply(lambda r: f"{r['size_kb']} KB" if r["exists"] else "", axis=1)
    # Coerce rows to str to avoid mixed int/"—" Arrow serialization error
    df["rows"] = df["rows"].apply(lambda v: str(v) if v != "" else "")

    display = df[["file", "present", "rows", "size", "modified"]].rename(columns={
        "file":     "File",
        "present":  "Present",
        "rows":     "Rows",
        "size":     "Size",
        "modified": "Last Modified",
    })
    st.dataframe(display, width="stretch", hide_index=True)


# ---------------------------------------------------------------------------
# Section 10 — Campaign Queue Panel
# ---------------------------------------------------------------------------

def _render_queue_panel_content() -> None:
    """
    Render the multi-city campaign queue panel.

    Shows:
    - Live status metrics (running / pending / completed / failed)
    - Current running job and next queued job
    - Pause / Resume queue control
    - Full jobs table with status badges
    - Per-job actions (remove / retry)

    To add jobs, select Multiple Run in Campaign Configuration above and click
    "Add N cities to Queue".
    """
    log.ui("render_queue_panel()")
    st.header("Campaign Queue")
    st.caption("To add jobs, use **Multiple Run** in Campaign Configuration above.")

    try:
        from src.workflow_9_queue_scheduler.queue_store import (  # noqa: PLC0415
            queue_summary, list_jobs, remove_job, requeue_job,
            pause_queue, resume_queue, is_queue_paused,
            STATUS_PENDING, STATUS_RUNNING, STATUS_COMPLETED, STATUS_FAILED,
        )
    except ImportError as exc:
        log.error("Queue module import failed", exc=exc)
        st.error(f"Queue module not available: {exc}")
        return

    summary = queue_summary()
    log.data("Queue summary",
             running=summary.get("running"),
             pending=summary.get("pending"),
             completed=summary.get("completed"),
             failed=summary.get("failed"),
             total=summary.get("total"))

    # ---- Auto-stop: kill the scheduler when all jobs are finished -----------
    # The scheduler loops indefinitely after the queue empties, so pid_alive
    # would stay True forever.  When there are no pending or running jobs but
    # the process is still alive, stop it automatically so the button flips
    # back to "Start Runner" without requiring a manual page interaction.
    pid_alive = _is_scheduler_pid_alive()
    queue_fully_done = (
        pid_alive
        and summary["pending"] == 0
        and summary["running"] == 0
        and summary["total"] > 0          # queue existed (not just empty on first load)
    )
    if queue_fully_done:
        _stop_scheduler_process()
        pid_alive = False
        st.toast("All jobs finished — queue runner stopped automatically.")

    # ---- Scheduler status + Start / Stop ------------------------------------
    sched = _get_scheduler_status(summary)
    # Override pid_alive in sched with the already-computed value (avoids a
    # second os.kill probe after we may have just stopped the process).
    sched["pid_alive"] = pid_alive

    status_col, btn_col = st.columns([4, 1])

    with status_col:
        if sched["state"] == "active":
            st.success(f"**Scheduler — Active** | {sched['detail']}")
        elif sched["state"] == "paused":
            st.warning(f"**Scheduler — Paused** | {sched['detail']}")
        elif sched["state"] == "not_running":
            st.error(f"**Scheduler — Not Running** | {sched['detail']}")
        else:
            st.info(f"**Scheduler — Idle** | {sched['detail']}")

        phase_title, phase_detail = _describe_queue_runner_phase(summary, sched)
        st.caption(
            "Runner lifecycle: "
            "`Start Runner` -> `queued in scheduler` -> "
            "`claimed job / waiting for first workflow step` -> "
            "`running workflow step` -> `completed`"
        )
        st.caption(f"**Now:** {phase_title}. {phase_detail}")

    with btn_col:
        # Don't allow Start while a job is actively running (lock present)
        job_running = sched["state"] == "active" and is_campaign_running()

        if pid_alive:
            if st.button("■ Stop Runner", width="stretch", key="sched_stop"):
                log.action("■ Stop Runner clicked")
                msg = _stop_scheduler_process()
                log.action("Stop result", detail=msg)
                st.toast(msg)
                st.rerun()
        else:
            if st.button(
                "▶ Start Runner",
                type="primary",
                width="stretch",
                key="sched_start",
                disabled=job_running,
            ):
                log.action("▶ Start Runner clicked")
                ok, msg = _start_scheduler_process()
                log.action("Start result", ok=ok, detail=msg)
                if ok:
                    st.toast(msg)
                else:
                    st.error(msg)
                st.rerun()

    # ---- Metrics row --------------------------------------------------------
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Running",   summary["running"])
    col2.metric("Pending",   summary["pending"])
    col3.metric("Completed", summary["completed"])
    col4.metric("Failed",    summary["failed"])
    col5.metric("Total",     summary["total"])

    # ---- Running + Next job -------------------------------------------------
    info_col, ctrl_col = st.columns([3, 1])

    with info_col:
        running_job = summary["running_job"]
        next_job    = summary["next_job"]

        if running_job:
            st.success(
                f"**Running:** {running_job['location']}, {running_job['country']} "
                f"(job {running_job['job_id']}) — started {running_job.get('started_at','?')}"
            )
            # Progress bar — read last logged step from campaign runner logs.
            # campaign_id is only written to the queue job after the run
            # completes, so fall back to campaign_run_state.json which always
            # holds the active campaign_id during execution.
            cid = running_job.get("campaign_id") or _get_active_campaign_id()
            if cid:
                step = _get_running_job_step(cid)
                if step and step in PIPELINE_STEPS:
                    idx = PIPELINE_STEPS.index(step)
                    progress = (idx + 1) / len(PIPELINE_STEPS)
                    st.progress(progress, text=f"Step {idx + 1}/{len(PIPELINE_STEPS)}: `{step}`")
                else:
                    st.progress(
                        0.02,
                        text="Job claimed by runner. Waiting for the first workflow step to be logged...",
                    )
                    st.caption(
                        "This short gap is normal: the queue job has started, but `campaign_runner_logs.csv` has not written its first step yet."
                    )
        else:
            if sched.get("pid_alive") and summary["pending"] > 0:
                st.info("Runner is alive and waiting to claim the next pending job.")
                st.caption(
                    "If this message persists for more than a few refresh cycles, check `data/queue_runner.log` for the handoff details."
                )
            elif sched.get("pid_alive"):
                st.info("No job is currently running. The runner process is alive and idle.")
            else:
                st.info("No job currently running.")

        if next_job:
            st.info(
                f"**Next up:** {next_job['location']}, {next_job['country']} "
                f"(job {next_job['job_id']} | priority {next_job['priority']})"
            )

    with ctrl_col:
        paused = is_queue_paused()
        if paused:
            st.warning("Queue is PAUSED")
            if st.button("Resume Queue", key="queue_resume"):
                log.action("Resume Queue clicked")
                resume_queue()
                st.rerun()
        else:
            if st.button("Pause Queue", key="queue_pause"):
                log.action("Pause Queue clicked")
                pause_queue()
                st.rerun()

    st.divider()

    # ---- Jobs table ---------------------------------------------------------
    all_jobs = list_jobs()
    if not all_jobs:
        st.caption("Queue is empty.")
        return

    _STATUS_BADGE = {
        STATUS_PENDING:   "pending",
        STATUS_RUNNING:   "running",
        STATUS_COMPLETED: "completed",
        STATUS_FAILED:    "failed",
        "paused":         "paused",
    }

    rows = []
    for j in all_jobs:
        rows.append({
            "select":    False,
            "job_id":    j["job_id"],
            "status":    _STATUS_BADGE.get(j["status"], j["status"]),
            "location":  f"{j['location']}, {j['country']}",
            "priority":  j["priority"],
            "send_mode": j["send_mode"],
            "cloud_handoff": (
                "disabled" if j["send_mode"] == "dry_run"
                else (
                    "auto" if j.get("auto_cloud_deploy") is True
                    else "manual" if j.get("auto_cloud_deploy") is False
                    else "legacy"
                )
            ),
            "run_until": j["run_until"],
            "campaign":  j.get("campaign_id", ""),
            "started":   _format_local_timestamp(j.get("started_at", "")),
            "finished":  _format_local_timestamp(j.get("completed_at", "")),
            "error":     (j.get("error") or "")[:60],
        })

    df = pd.DataFrame(rows)
    edited_df = st.data_editor(
        df,
        hide_index=True,
        width="stretch",
        key="queue_jobs_editor",
        column_config={
            "select": column_config.CheckboxColumn(
                "Select",
                help="Select one or more jobs for quick remove or re-queue actions.",
                default=False,
            ),
            "job_id": column_config.TextColumn("job_id", width="small"),
            "status": column_config.TextColumn("status", width="small"),
            "location": column_config.TextColumn("location", width="medium"),
            "cloud_handoff": column_config.TextColumn("cloud_handoff", width="small"),
            "error": column_config.TextColumn("error", width="large"),
        },
        disabled=["job_id", "status", "location", "priority", "send_mode", "cloud_handoff", "run_until", "campaign", "started", "finished", "error"],
    )

    selected_ids = (
        edited_df.loc[edited_df["select"], "job_id"].tolist()
        if "select" in edited_df.columns
        else []
    )
    selected_jobs = [j for j in all_jobs if j["job_id"] in selected_ids]

    st.caption("Tip: tick rows in the table, then use the quick actions below.")

    action_col1, action_col2 = st.columns(2)
    with action_col1:
        if st.button("Remove selected", key="queue_remove_selected", disabled=not selected_ids):
            running_selected = [j["job_id"] for j in selected_jobs if j.get("status") == STATUS_RUNNING]
            if running_selected:
                st.warning(
                    "Running jobs cannot be removed while they are in progress: "
                    + ", ".join(running_selected)
                )
            else:
                removed = 0
                for job_id in selected_ids:
                    if remove_job(job_id):
                        removed += 1
                log.action("Remove selected jobs", count=removed, selected=len(selected_ids))
                st.toast(f"Removed {removed} job(s).")
                st.rerun()

    with action_col2:
        if st.button("Re-queue selected", key="queue_requeue_selected", disabled=not selected_ids):
            running_selected = [j["job_id"] for j in selected_jobs if j.get("status") == STATUS_RUNNING]
            if running_selected:
                st.warning(
                    "Running jobs cannot be re-queued while they are in progress: "
                    + ", ".join(running_selected)
                )
            else:
                requeued = 0
                for job_id in selected_ids:
                    if requeue_job(job_id):
                        requeued += 1
                log.action("Re-queue selected jobs", count=requeued, selected=len(selected_ids))
                st.toast(f"Re-queued {requeued} job(s).")
                st.rerun()


if hasattr(st, "fragment"):
    @st.fragment(run_every=_QUEUE_PANEL_REFRESH_INTERVAL)
    def _render_queue_panel_fragment() -> None:
        _render_queue_panel_content()
else:
    def _render_queue_panel_fragment() -> None:
        _render_queue_panel_content()


def render_queue_panel() -> None:
    """
    Render the multi-city campaign queue panel.

    Uses Streamlit fragment auto-refresh when available so the queue state and
    progress bar update without relying on browser-side rerun hacks.
    """
    if _is_ui_autorefresh_paused():
        _render_queue_panel_content()
        return
    _render_queue_panel_fragment()
