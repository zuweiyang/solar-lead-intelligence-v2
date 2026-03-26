import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- API Keys ---
OPENAI_API_KEY      = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
APOLLO_API_KEY      = os.getenv("APOLLO_API_KEY", "")
HUNTER_API_KEY      = os.getenv("HUNTER_API_KEY", "")
OPENROUTER_API_KEY       = os.getenv("OPENROUTER_API_KEY", "")
LLM_PROVIDER             = os.getenv("LLM_PROVIDER", "")
LLM_MODEL                = os.getenv("LLM_MODEL", "")
EMAIL_GEN_PROVIDER       = os.getenv("EMAIL_GEN_PROVIDER",       "openrouter")
EMAIL_GEN_MODEL          = os.getenv("EMAIL_GEN_MODEL",          "anthropic/claude-3.5-haiku")
EMAIL_GEN_FALLBACK_MODEL = os.getenv("EMAIL_GEN_FALLBACK_MODEL", "openai/gpt-4o-mini")

# --- Email Settings (legacy — kept for backward compatibility) ---
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
SENDER_NAME = os.getenv("SENDER_NAME", "")
SENDER_TITLE = os.getenv("SENDER_TITLE", "")

# --- Workflow 7 — Send Settings ---
EMAIL_SEND_MODE  = os.getenv("EMAIL_SEND_MODE", "dry_run")   # "dry_run" | "smtp" | "gmail_api"
SMTP_HOST        = os.getenv("SMTP_HOST",        os.getenv("EMAIL_HOST",     "smtp.gmail.com"))
SMTP_PORT        = int(os.getenv("SMTP_PORT",    os.getenv("EMAIL_PORT",     "587")))
SMTP_USERNAME    = os.getenv("SMTP_USERNAME",    os.getenv("EMAIL_ADDRESS",  ""))
SMTP_PASSWORD    = os.getenv("SMTP_PASSWORD",    os.getenv("EMAIL_PASSWORD", ""))
SMTP_USE_TLS     = os.getenv("SMTP_USE_TLS",     "true").lower() == "true"
SMTP_FROM_EMAIL  = os.getenv("SMTP_FROM_EMAIL",  os.getenv("EMAIL_ADDRESS",  ""))
SMTP_FROM_NAME   = os.getenv("SMTP_FROM_NAME",   os.getenv("SENDER_NAME",    ""))
REPLY_TO_EMAIL   = os.getenv("REPLY_TO_EMAIL",   "")

# --- Gmail API OAuth2 paths ---
_CONFIG_DIR             = Path(__file__).parent
GMAIL_CLIENT_SECRET_FILE = _CONFIG_DIR / "gmail_client_secret.json"
GMAIL_TOKEN_FILE         = _CONFIG_DIR / "gmail_token.json"

# --- Gmail API rate-limit / retry settings ---
GMAIL_API_MIN_SEND_INTERVAL_SECONDS = float(os.getenv("GMAIL_API_MIN_SEND_INTERVAL_SECONDS", "2.0"))
GMAIL_API_MAX_RETRIES               = int(os.getenv("GMAIL_API_MAX_RETRIES",               "5"))
GMAIL_API_BACKOFF_BASE_SECONDS      = float(os.getenv("GMAIL_API_BACKOFF_BASE_SECONDS",    "1.0"))
GMAIL_API_BACKOFF_MAX_SECONDS       = float(os.getenv("GMAIL_API_BACKOFF_MAX_SECONDS",     "32.0"))
GMAIL_API_ENABLE_JITTER             = os.getenv("GMAIL_API_ENABLE_JITTER", "true").lower() == "true"
SEND_PACING_MIN_SECONDS             = float(os.getenv("SEND_PACING_MIN_SECONDS", "45"))
SEND_PACING_MAX_SECONDS             = float(os.getenv("SEND_PACING_MAX_SECONDS", "180"))
SEND_HOURLY_LIMIT                   = int(os.getenv("SEND_HOURLY_LIMIT", "20"))
SEND_WINDOW_START = int(os.getenv("SEND_WINDOW_START", "8"))   # 08:00 local
SEND_WINDOW_END   = int(os.getenv("SEND_WINDOW_END",   "18"))  # 18:00 local
SEND_WINDOW_SLOTS = os.getenv("SEND_WINDOW_SLOTS", "09:00-12:00,14:00-16:00")

# --- Pipeline Settings ---
DAILY_EMAIL_LIMIT = int(os.getenv("DAILY_EMAIL_LIMIT", "100"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
SCRAPE_DELAY_SECONDS = float(os.getenv("SCRAPE_DELAY_SECONDS", "1.0"))
CRAWL_DELAY_SECONDS = float(os.getenv("CRAWL_DELAY_SECONDS", "1.0"))

# --- Cloud Send Deployment Settings ---
CLOUD_SEND_ENABLED = os.getenv("CLOUD_SEND_ENABLED", "false").lower() == "true"
CLOUD_AUTO_DEPLOY_ON_COMPLETE = os.getenv("CLOUD_AUTO_DEPLOY_ON_COMPLETE", "false").lower() == "true"
CLOUD_ENVIRONMENT = os.getenv("CLOUD_ENVIRONMENT", "local")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
GCS_RUNS_PREFIX = os.getenv("GCS_RUNS_PREFIX", "runs")
GCS_MANIFESTS_PREFIX = os.getenv("GCS_MANIFESTS_PREFIX", "manifests")
GCS_INFLIGHT_PREFIX = os.getenv("GCS_INFLIGHT_PREFIX", "inflight")
GCS_PROCESSED_PREFIX = os.getenv("GCS_PROCESSED_PREFIX", "processed")
GCS_FAILED_PREFIX = os.getenv("GCS_FAILED_PREFIX", "failed")
GCS_STATUS_PREFIX = os.getenv("GCS_STATUS_PREFIX", "ops")
CLOUD_WORKER_POLL_SECONDS = float(os.getenv("CLOUD_WORKER_POLL_SECONDS", "60"))
CLOUD_WORKER_ALERT_WEBHOOK = os.getenv("CLOUD_WORKER_ALERT_WEBHOOK", "").strip()
CLOUD_WORKER_ALERT_EMAIL_TO = os.getenv("CLOUD_WORKER_ALERT_EMAIL_TO", "").strip()
CLOUD_WORKER_ALERT_EMAIL_MODE = os.getenv("CLOUD_WORKER_ALERT_EMAIL_MODE", "gmail_api").strip().lower()
CLOUD_WORKER_ALERT_EMAIL_FROM = os.getenv("CLOUD_WORKER_ALERT_EMAIL_FROM", "").strip()
CLOUD_WORKER_ALERT_SUBJECT_PREFIX = os.getenv("CLOUD_WORKER_ALERT_SUBJECT_PREFIX", "[CloudWorker]").strip()
CLOUD_SEND_INBOX_DAILY_LIMIT = int(
    os.getenv("CLOUD_SEND_INBOX_DAILY_LIMIT", os.getenv("DAILY_EMAIL_LIMIT", "100"))
)
CLOUD_SEND_INBOX_HOURLY_LIMIT = int(
    os.getenv("CLOUD_SEND_INBOX_HOURLY_LIMIT", os.getenv("SEND_HOURLY_LIMIT", "20"))
)
CLOUD_SEND_SKIP_WEEKENDS = os.getenv("CLOUD_SEND_SKIP_WEEKENDS", "true").lower() == "true"
CLOUD_SEND_CAP_TIMEZONE = os.getenv("CLOUD_SEND_CAP_TIMEZONE", "UTC").strip() or "UTC"
REPLY_INTELLIGENCE_HOURS_BACK = int(os.getenv("REPLY_INTELLIGENCE_HOURS_BACK", "168"))
REPLY_INTELLIGENCE_MAX_RESULTS = int(os.getenv("REPLY_INTELLIGENCE_MAX_RESULTS", "100"))
REPLY_INTELLIGENCE_OUR_EMAIL = os.getenv("REPLY_INTELLIGENCE_OUR_EMAIL", "").strip()

# --- Base Directories ---
BASE_DIR     = Path(__file__).parent.parent
DATA_DIR     = BASE_DIR / "data"
RUNS_DIR     = DATA_DIR / "runs"    # campaign-scoped artifacts live here
CRM_DIR      = DATA_DIR / "crm"     # global CRM files that span all campaigns
PROMPTS_DIR  = Path(__file__).parent / "prompts"

# ---------------------------------------------------------------------------
# Path proxy classes
#
# _RunPath  — resolves to  data/runs/<active_campaign_id>/<filename>
#             Falls back to data/<filename> when accessed outside a run.
# _CrmPath  — always resolves to  data/crm/<filename>
#
# Both classes implement the Path-like protocol so workflow files require
# zero changes: open(path, ...), path.exists(), path.parent.mkdir(), etc.
# ---------------------------------------------------------------------------

class _RunPath:
    """
    Lazy path proxy that resolves to data/runs/<campaign_id>/<filename> at
    access time.  When no run is active (e.g. direct script invocations or
    the Streamlit UI before a campaign starts) it falls back to DATA_DIR so
    old behaviour is preserved.
    """
    __slots__ = ("_filename",)

    def __init__(self, filename: str) -> None:
        self._filename = filename

    def _resolve(self) -> Path:
        from config.run_context import get_active_campaign_id
        cid = get_active_campaign_id()
        if cid:
            run_dir = RUNS_DIR / cid
            run_dir.mkdir(parents=True, exist_ok=True)
            return run_dir / self._filename
        # Fallback: legacy DATA_DIR path so callers outside a run still work
        return DATA_DIR / self._filename

    # ---- path-like protocol ----
    def __fspath__(self) -> str:
        return str(self._resolve())

    def __str__(self) -> str:
        return str(self._resolve())

    def __repr__(self) -> str:
        return f"_RunPath({self._filename!r})"

    def __truediv__(self, other):
        return self._resolve() / other

    def __rtruediv__(self, other):
        return Path(other) / self._resolve()

    def __eq__(self, other):
        if isinstance(other, _RunPath):
            return self._filename == other._filename
        return self._resolve() == other

    def __hash__(self):
        return hash(self._filename)

    def __getattr__(self, name: str):
        # Delegate any Path attribute/method to the resolved Path
        return getattr(self._resolve(), name)


class _CrmPath:
    """
    Lazy path proxy that always resolves to data/crm/<filename>.
    Used for global CRM files that are append-only and span all campaigns
    (send_logs.csv, engagement_logs.csv, followup_logs.csv).
    """
    __slots__ = ("_filename",)

    def __init__(self, filename: str) -> None:
        self._filename = filename

    def _resolve(self) -> Path:
        CRM_DIR.mkdir(parents=True, exist_ok=True)
        return CRM_DIR / self._filename

    # ---- path-like protocol ----
    def __fspath__(self) -> str:
        return str(self._resolve())

    def __str__(self) -> str:
        return str(self._resolve())

    def __repr__(self) -> str:
        return f"_CrmPath({self._filename!r})"

    def __truediv__(self, other):
        return self._resolve() / other

    def __rtruediv__(self, other):
        return Path(other) / self._resolve()

    def __eq__(self, other):
        if isinstance(other, _CrmPath):
            return self._filename == other._filename
        return self._resolve() == other

    def __hash__(self):
        return hash(self._filename)

    def __getattr__(self, name: str):
        return getattr(self._resolve(), name)


# ---------------------------------------------------------------------------
# Process-level paths — fixed locations, independent of active campaign
# ---------------------------------------------------------------------------

DATABASE_FILE             = DATA_DIR / "solar_leads.db"
CAMPAIGN_RUN_STATE_FILE   = DATA_DIR / "campaign_run_state.json"
CAMPAIGN_LOCK_FILE        = DATA_DIR / "campaign_run.lock"
CAMPAIGN_QUEUE_FILE       = DATA_DIR / "campaign_queue.json"
CAMPAIGN_QUEUE_PAUSE_FLAG = DATA_DIR / "queue_pause.flag"
SCHEDULER_PID_FILE        = DATA_DIR / "scheduler.pid"
CONTROL_PANEL_HEARTBEAT_FILE = DATA_DIR / "control_panel_heartbeat.json"
CONTROL_PANEL_HEARTBEAT_TIMEOUT_SECONDS = float(
    os.getenv("CONTROL_PANEL_HEARTBEAT_TIMEOUT_SECONDS", "20")
)

# ---------------------------------------------------------------------------
# Campaign-scoped paths — resolve under data/runs/<campaign_id>/ per run
# ---------------------------------------------------------------------------

SEARCH_TASKS_FILE     = _RunPath("search_tasks.json")
RAW_LEADS_FILE        = _RunPath("raw_leads.csv")
COMPANY_PAGES_FILE    = _RunPath("company_pages.json")
COMPANY_TEXT_FILE     = _RunPath("company_text.json")
COMPANY_ANALYSIS_FILE = _RunPath("company_analysis.json")
BUYER_FILTER_FILE     = _RunPath("buyer_filter.json")
COMPANY_CONTENT_FILE  = _RunPath("company_content.json")
COMPANY_PROFILES_FILE = _RunPath("company_profiles.json")
QUALIFIED_LEADS_FILE  = _RunPath("qualified_leads.csv")
ENRICHED_LEADS_FILE          = _RunPath("enriched_leads.csv")
ENRICHED_CONTACTS_FILE       = _RunPath("enriched_contacts.csv")
SCORED_CONTACTS_FILE         = _RunPath("scored_contacts.csv")       # P1-2B output
VERIFIED_ENRICHED_LEADS_FILE = _RunPath("verified_enriched_leads.csv")
QUEUE_POLICY_FILE            = _RunPath("queue_policy.csv")           # P1-3A output
POLICY_SUMMARY_FILE          = _RunPath("policy_summary.json")         # P1-3C — queue-stage counts
RESEARCH_SIGNAL_RAW_FILE = _RunPath("research_signal_raw.json")
RESEARCH_SIGNALS_FILE    = _RunPath("research_signals.json")
GENERATED_EMAILS_FILE = _RunPath("generated_emails.csv")
SCORED_EMAILS_FILE    = _RunPath("scored_emails.csv")
SEND_QUEUE_FILE       = _RunPath("send_queue.csv")
REJECTED_EMAILS_FILE  = _RunPath("rejected_emails.csv")
REPAIRED_EMAILS_FILE  = _RunPath("repaired_emails.csv")
RESCORED_EMAILS_FILE  = _RunPath("rescored_emails.csv")
FINAL_SEND_QUEUE_FILE = _RunPath("final_send_queue.csv")
FINAL_REJECTED_FILE   = _RunPath("final_rejected_emails.csv")
MANUAL_REVIEW_QUEUE_FILE = _RunPath("manual_review_queue.csv")
EMAIL_TEMPLATES_FILE  = _RunPath("email_templates.json")
EMAIL_LOGS_FILE       = _RunPath("email_logs.csv")
SEND_BATCH_SUMMARY    = _RunPath("send_batch_summary.json")
ENGAGEMENT_SUMMARY_FILE = _RunPath("engagement_summary.csv")
CLOUD_SEND_STATUS_FILE = _RunPath("cloud_send_status.json")
CLOUD_SEND_RESULT_FILE = _RunPath("cloud_send_result.json")

# --- Workflow 7 tracking ---
TRACKING_BASE_URL = os.getenv("TRACKING_BASE_URL", "http://localhost:5000")

# --- Workflow 8 — Follow-up ---
FOLLOWUP_MAX_STAGE       = int(os.getenv("FOLLOWUP_MAX_STAGE",    "3"))
FOLLOWUP_1_DELAY_DAYS    = int(os.getenv("FOLLOWUP_1_DELAY_DAYS", "3"))
FOLLOWUP_2_DELAY_DAYS    = int(os.getenv("FOLLOWUP_2_DELAY_DAYS", "7"))
FOLLOWUP_3_DELAY_DAYS    = int(os.getenv("FOLLOWUP_3_DELAY_DAYS", "14"))
COMPANY_OPENINGS_FILE    = _RunPath("company_openings.json")
COMPANY_SIGNALS_FILE     = _RunPath("company_signals.json")
FOLLOWUP_CANDIDATES_FILE = _RunPath("followup_candidates.csv")
FOLLOWUP_QUEUE_FILE      = _RunPath("followup_queue.csv")
FOLLOWUP_BLOCKED_FILE    = _RunPath("followup_blocked.csv")

# --- Workflow 8.5 — Campaign Status ---
CAMPAIGN_STATUS_FILE    = _RunPath("campaign_status.csv")
CAMPAIGN_STATUS_SUMMARY = _RunPath("campaign_status_summary.json")
CLOUD_DEPLOY_STATUS_FILE = _RunPath("cloud_deploy_status.json")

# --- Workflow 5.9 — Email Verification ---
EMAIL_VERIFIER_PROVIDER = os.getenv("EMAIL_VERIFIER_PROVIDER", "hunter")
EMAIL_VERIFIER_LIVE     = os.getenv("EMAIL_VERIFIER_LIVE", "false").lower() == "true"

# --- Workflow 7.4 — Deliverability Breaker Thresholds ---
# Rates are expressed as fractions (0.0–1.0), e.g. 0.03 = 3%.
BREAKER_HARD_BOUNCE_RATE      = float(os.getenv("BREAKER_HARD_BOUNCE_RATE",      "0.03"))   # 3%  → sender breaker
BREAKER_INVALID_RATE          = float(os.getenv("BREAKER_INVALID_RATE",          "0.02"))   # 2%  → campaign breaker
BREAKER_PROVIDER_FAILURE_RATE = float(os.getenv("BREAKER_PROVIDER_FAILURE_RATE", "0.05"))   # 5%  → sender breaker
BREAKER_UNSUBSCRIBE_RATE      = float(os.getenv("BREAKER_UNSUBSCRIBE_RATE",      "0.005"))  # 0.5% → sender + campaign breaker
BREAKER_SPAM_RATE_WARNING     = float(os.getenv("BREAKER_SPAM_RATE_WARNING",     "0.001"))  # 0.1% → domain breaker (warning)
BREAKER_SPAM_RATE_CRITICAL    = float(os.getenv("BREAKER_SPAM_RATE_CRITICAL",    "0.003"))  # 0.3% → domain breaker (critical)

# --- Workflow 9 — Campaign Runner ---
CAMPAIGN_RUNNER_LOGS_FILE = _RunPath("campaign_runner_logs.csv")

# --- Workflow 6.7 — Email Repair ---
EMAIL_REPAIR_ERRORS_FILE = _RunPath("email_repair_errors.csv")

# --- Workflow 5 — Lead Scoring ---
DISQUALIFIED_LEADS_FILE = _RunPath("disqualified_leads.csv")
DEDUP_SKIPPED_FILE      = _RunPath("dedup_skipped.csv")

# ---------------------------------------------------------------------------
# Global CRM paths — always resolve under data/crm/, span all campaigns
# ---------------------------------------------------------------------------

SEND_LOGS_FILE            = _CrmPath("send_logs.csv")
ENGAGEMENT_LOGS_FILE      = _CrmPath("engagement_logs.csv")
FOLLOWUP_LOGS_FILE        = _CrmPath("followup_logs.csv")
CRM_DATABASE_FILE         = _CrmPath("crm_database.csv")
CLASSIFICATION_CACHE_FILE = _CrmPath("classification_cache.json")
REPLY_LOGS_FILE           = _CrmPath("reply_logs.csv")

# ---------------------------------------------------------------------------
# Prompt paths
# ---------------------------------------------------------------------------

COMPANY_RESEARCH_PROMPT  = PROMPTS_DIR / "company_research.txt"
LEAD_QUALIFICATION_PROMPT= PROMPTS_DIR / "lead_qualification.txt"
EMAIL_GENERATION_PROMPT  = PROMPTS_DIR / "email_generation.txt"
