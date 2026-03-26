"""
Workflow 9.5 / 9.6 - Streamlit Campaign Control Panel

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
from src.workflow_9_queue_scheduler.control_panel_heartbeat import (
    write_control_panel_heartbeat,
)

log.app("=== Streamlit Control Panel loading ===")

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


st.set_page_config(
    page_title="Campaign Control Panel",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def main() -> None:
    log.app("-- main() render cycle start --")
    write_control_panel_heartbeat("streamlit_main")

    render_header()
    render_dry_run_explanation()

    form_values = render_campaign_form()
    st.divider()
    render_runner_controls(form_values)

    st.divider()
    render_queue_panel()

    st.divider()
    show_kpi_dashboard = st.toggle(
        "Show KPI Dashboard",
        value=False,
        key="show_kpi_dashboard",
        help="Hidden by default to keep the control panel faster. Turn this on only when you need KPI details.",
    )
    if show_kpi_dashboard:
        render_kpi_dashboard()
        st.divider()

    render_campaign_state_view()

    st.divider()
    render_multi_run_comparison_view()

    show_advanced_panels = st.toggle(
        "Show Advanced Panels",
        value=False,
        key="show_advanced_panels",
        help="Hidden by default to keep the control panel lighter. Turn this on only when you need review queues, logs, lifecycle detail, or file-status diagnostics.",
    )
    if show_advanced_panels:
        st.divider()
        render_manual_review_queue_view()

        st.divider()
        render_logs_view()

        st.divider()
        render_high_priority_leads_view()

        st.divider()
        render_company_detail_view()

        st.divider()
        render_manual_followup_action()

        st.divider()
        render_status_table_view()

        st.divider()
        render_enhanced_file_status_view()


if __name__ == "__main__":
    log.app("__main__ entry point")
    main()
    log.app("-- main() render cycle complete --")
