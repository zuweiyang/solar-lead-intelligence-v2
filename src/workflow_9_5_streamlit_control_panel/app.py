"""
Workflow 9.5 / 9.6 — Streamlit Campaign Control Panel

Main application entry point.

Run:
    streamlit run src/workflow_9_5_streamlit_control_panel/app.py

Or via the convenience launcher:
    py scripts/run_control_panel.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow imports from project root when launched by Streamlit
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st

from src.workflow_9_5_streamlit_control_panel.debug_log import log

log.app("═══ Streamlit Control Panel loading ═══")

from src.workflow_9_5_streamlit_control_panel.ui_views import (
    render_header,
    render_dry_run_explanation,
    render_campaign_form,
    render_runner_controls,
    render_campaign_state_view,
    render_kpi_dashboard,
    render_manual_review_queue_view,
    render_multi_run_comparison_view,
    render_logs_view,
    render_status_table_view,
    render_high_priority_leads_view,
    render_company_detail_view,
    render_manual_followup_action,
    render_enhanced_file_status_view,
    render_queue_panel,
)

# ---------------------------------------------------------------------------
# Page config — must be the first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Campaign Control Panel",
    page_icon="🎛️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------


def main() -> None:
    log.app("── main() render cycle start ──")

    # ── Header ──────────────────────────────────────────────────────────────
    render_header()

    # ── Section: Dry-run explanation ─────────────────────────────────────────
    render_dry_run_explanation()

    # ── Section 1 — Campaign Configuration + Run Controls ───────────────────
    form_values = render_campaign_form()
    st.divider()
    render_runner_controls(form_values)

    st.divider()

    # ── Section 2 — Campaign Queue ────────────────────────────────────────────
    # Placed immediately after Run Controls so the user can add cities and
    # start the queue runner without scrolling.
    render_queue_panel()

    st.divider()

    # ── Section 3 — KPI Dashboard ────────────────────────────────────────────
    render_kpi_dashboard()

    st.divider()

    # ── Section 4 — Current Campaign State ───────────────────────────────────
    render_campaign_state_view()

    st.divider()

    # ── Section 5 — Runner Logs (with refresh) ───────────────────────────────
    render_multi_run_comparison_view()

    st.divider()

    render_manual_review_queue_view()

    st.divider()

    render_logs_view()

    st.divider()

    # ── Section 6 — High Priority Leads ──────────────────────────────────────
    render_high_priority_leads_view()

    st.divider()

    # ── Section 7 — Company Lifecycle Detail ─────────────────────────────────
    render_company_detail_view()

    st.divider()

    # ── Section 8 — Manual Send followup_1 ───────────────────────────────────
    render_manual_followup_action()

    st.divider()

    # ── Section 9 — Campaign Status Table ────────────────────────────────────
    render_status_table_view()

    st.divider()

    # ── Section 10 — Enhanced File Status ─────────────────────────────────────
    render_enhanced_file_status_view()


if __name__ == "__main__":
    log.app("__main__ entry point")
    main()
    log.app("── main() render cycle complete ──")
