"""
Solar Lead Intelligence — Campaign Dashboard
Run:  streamlit run src/dashboard/dashboard.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import streamlit as st
import pandas as pd

# Allow imports from project root when run via streamlit
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.database.db_connection import get_db_connection
from config.settings import DATABASE_FILE

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Solar Lead Intelligence",
    page_icon="☀️",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Data loaders (cached per session)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def load_overview() -> dict:
    if not DATABASE_FILE.exists():
        return {}
    conn = get_db_connection()
    def q(sql):
        row = conn.execute(sql).fetchone()
        return row[0] if row else 0

    data = {
        "total_companies": q("SELECT COUNT(*) FROM companies"),
        "total_contacts":  q("SELECT COUNT(*) FROM contacts"),
        "emails_generated": q("SELECT COUNT(*) FROM emails"),
        "emails_sent":     q("SELECT COUNT(*) FROM email_sends WHERE send_status IN ('sent','dry_run')"),
        "open_count":      q("SELECT COUNT(*) FROM engagement WHERE event_type = 'open'"),
        "click_count":     q("SELECT COUNT(*) FROM engagement WHERE event_type = 'click'"),
        "reply_count":     q("SELECT COUNT(*) FROM engagement WHERE event_type = 'reply'"),
    }
    conn.close()
    return data


@st.cache_data(ttl=30)
def load_lead_table() -> pd.DataFrame:
    if not DATABASE_FILE.exists():
        return pd.DataFrame()
    conn = get_db_connection()
    sql = """
        SELECT
            co.company_name,
            co.website,
            ct.email          AS contact_email,
            ca.lead_score,
            ca.company_type,
            ca.market_focus,
            MAX(CASE WHEN es.send_status IN ('sent','dry_run') THEN 1 ELSE 0 END) AS email_sent,
            COUNT(DISTINCT CASE WHEN en.event_type = 'open'  THEN en.id END) AS open_count,
            COUNT(DISTINCT CASE WHEN en.event_type = 'click' THEN en.id END) AS click_count,
            MAX(fu.stage)     AS followup_stage
        FROM companies co
        LEFT JOIN company_analysis ca ON ca.company_id = co.id
        LEFT JOIN contacts ct         ON ct.company_id = co.id
        LEFT JOIN emails em           ON em.company_id = co.id
        LEFT JOIN email_sends es      ON es.email_id   = em.id
        LEFT JOIN engagement  en      ON en.email_id   = em.id
        LEFT JOIN followups   fu      ON fu.contact_id = ct.id
        GROUP BY co.id, ct.id
        ORDER BY ca.lead_score DESC NULLS LAST
    """
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df


@st.cache_data(ttl=30)
def load_engagement_rates() -> dict:
    if not DATABASE_FILE.exists():
        return {}
    conn = get_db_connection()
    sent   = conn.execute("SELECT COUNT(*) FROM email_sends WHERE send_status IN ('sent','dry_run')").fetchone()[0]
    opens  = conn.execute("SELECT COUNT(DISTINCT email_id) FROM engagement WHERE event_type = 'open'").fetchone()[0]
    clicks = conn.execute("SELECT COUNT(DISTINCT email_id) FROM engagement WHERE event_type = 'click'").fetchone()[0]
    replies= conn.execute("SELECT COUNT(DISTINCT email_id) FROM engagement WHERE event_type = 'reply'").fetchone()[0]
    conn.close()

    def rate(n): return round(n / sent * 100, 1) if sent else 0.0

    return {
        "emails_sent": sent,
        "open_rate":   rate(opens),
        "click_rate":  rate(clicks),
        "reply_rate":  rate(replies),
    }


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------

def _metric_card(label: str, value, delta=None):
    st.metric(label=label, value=value, delta=delta)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.title("☀️ Solar Lead Intelligence — Campaign Dashboard")

    if not DATABASE_FILE.exists():
        st.warning(
            f"Database not found at `{DATABASE_FILE}`.  "
            "Run `py scripts/init_database.py` first."
        )
        st.stop()

    # ── Overview metrics ────────────────────────────────────────────────────
    st.header("Overview")
    ov = load_overview()
    cols = st.columns(7)
    metrics = [
        ("🏢 Companies",    ov.get("total_companies", 0)),
        ("👤 Contacts",     ov.get("total_contacts",  0)),
        ("✉️ Generated",    ov.get("emails_generated", 0)),
        ("📤 Sent",         ov.get("emails_sent",     0)),
        ("👁️ Opens",        ov.get("open_count",      0)),
        ("🖱️ Clicks",       ov.get("click_count",     0)),
        ("💬 Replies",      ov.get("reply_count",     0)),
    ]
    for col, (label, val) in zip(cols, metrics):
        with col:
            st.metric(label, val)

    st.divider()

    # ── Engagement rates ────────────────────────────────────────────────────
    st.header("Engagement Metrics")
    er = load_engagement_rates()
    if er.get("emails_sent", 0) == 0:
        st.info("No emails sent yet.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Emails Sent",  er["emails_sent"])
        c2.metric("Open Rate",    f"{er['open_rate']}%")
        c3.metric("Click Rate",   f"{er['click_rate']}%")
        c4.metric("Reply Rate",   f"{er['reply_rate']}%")

    st.divider()

    # ── Lead table ──────────────────────────────────────────────────────────
    st.header("Lead Table")

    df = load_lead_table()
    if df.empty:
        st.info("No leads in the database yet. Run `py scripts/init_database.py --sync`.")
    else:
        # Filters
        with st.expander("Filters", expanded=False):
            f_col1, f_col2, f_col3 = st.columns(3)
            with f_col1:
                min_score = st.slider("Min lead score", 0, 100, 0)
            with f_col2:
                sent_filter = st.selectbox("Email sent?", ["All", "Yes", "No"])
            with f_col3:
                company_search = st.text_input("Search company name")

        filtered = df.copy()
        if min_score > 0:
            filtered = filtered[filtered["lead_score"].fillna(0) >= min_score]
        if sent_filter == "Yes":
            filtered = filtered[filtered["email_sent"] == 1]
        elif sent_filter == "No":
            filtered = filtered[filtered["email_sent"] != 1]
        if company_search:
            filtered = filtered[
                filtered["company_name"].str.contains(company_search, case=False, na=False)
            ]

        # Display columns
        display_cols = [
            "company_name", "website", "contact_email", "lead_score",
            "company_type", "email_sent", "open_count", "click_count", "followup_stage",
        ]
        display_cols = [c for c in display_cols if c in filtered.columns]

        st.dataframe(
            filtered[display_cols].fillna(""),
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"Showing {len(filtered)} of {len(df)} leads")


if __name__ == "__main__":
    main()
