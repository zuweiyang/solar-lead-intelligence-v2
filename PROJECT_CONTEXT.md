# PROJECT_CONTEXT.md
# This file exists so that Claude Code can recover full architectural context
# after context compaction. Read this file before making any architectural decisions.
#
# NOTE: Claude Code must read PROJECT_CONTEXT.md before making major architectural
# changes or implementing a new workflow.

---

## PROJECT OVERVIEW

**Project Name:** solar-lead-intelligence

**Purpose:**
An automated AI-driven B2B lead generation system for the solar and battery storage industry.

The system automatically:
1. discovers companies
2. scrapes company data
3. crawls company websites
4. analyzes companies using AI
5. scores qualified leads
6. generates outreach emails
7. schedules follow-ups

The goal is to build a fully automated outbound sales pipeline.

---

## PIPELINE ARCHITECTURE

### Workflow 0 — Lead Database Layer

| | |
|---|---|
| **Location** | `src/database/` (db layer) + `src/dashboard/` (UI) |
| **Files** | `db_connection.py`, `db_schema.py`, `db_utils.py`, `csv_sync.py`, `dashboard.py` |
| **Database** | `data/solar_leads.db` (SQLite) |
| **Init script** | `py scripts/init_database.py [--sync]` |
| **Dashboard** | `streamlit run src/dashboard/dashboard.py` |

Central SQLite database that becomes the Single Source of Truth for all pipeline data. CSV files remain the primary inter-workflow transport; this layer imports them and enables querying across all stages.

**Tables:**
- `companies` — companies from Google Maps; deduped by `place_id`
- `company_analysis` — classification output from Workflow 4
- `contacts` — enriched contacts (Apollo / Hunter / website / guessed)
- `company_signals` — signals from Workflow 5.8
- `emails` — generated email drafts from Workflow 6
- `email_sends` — send logs from Workflow 7
- `engagement` — open / click / reply / bounce / unsubscribe events from Workflow 7.5
- `followups` — follow-up stages 1–3 from Workflow 8

**db_connection.py** — `get_db_connection()` returns a `sqlite3.Connection` with `row_factory=sqlite3.Row` and foreign keys enabled.

**db_schema.py** — `create_all_tables(conn)` creates all tables and indexes; idempotent.

**db_utils.py** — CRUD helpers: `insert_company()`, `get_company_by_place_id()`, `insert_contact()`, `insert_email()`, `log_email_send()`, `log_engagement_event()`. All accept an open connection and return the row id.

**csv_sync.py** — `sync_all(conn)` imports existing CSV files in dependency order: raw_leads → enriched_leads → generated_emails → send_logs → engagement_logs → followup_logs. Each function is safe to re-run (deduplication guards).

**dashboard.py** — Streamlit UI with three sections:
1. Overview metrics (companies, contacts, emails generated/sent, opens, clicks, replies)
2. Engagement rates (open %, click %, reply %)
3. Lead table with filters (score, sent status, company name search)

**Integration rule for future workflows:**
- Read from database first; write results back to database; export CSV only when needed.
- Workflow 2: `INSERT company` if `place_id` not present.
- Workflow 6: `SELECT companies + contacts` → `INSERT email`.
- Workflow 7: `INSERT email_sends`.
- Workflow 7.5: `INSERT engagement events`.
- Workflow 8: `INSERT followups`.

---

### Workflow 1 — Search Task Generation

| | |
|---|---|
| **Location** | `src/workflow_1_lead_generation/` |
| **Input** | keywords + locations |
| **Output** | `data/search_tasks.json` |

Generates combinations of keywords and locations used to search companies.
Keywords are defined in `keyword_generator.py`. The campaign-level keyword list is in `campaign_config.py` (`DEFAULT_KEYWORDS`). Current default list (updated to target commercial EPC leads):

```
commercial solar installer, industrial solar contractor,
solar project developer, rooftop solar contractor,
solar farm developer, renewable energy developer,
commercial property developer, battery storage installer
```

Removed from earlier list: `solar installer` (too broad/residential), `solar EPC` (surfaces competitors), `solar contractor` (too generic), `solar panel installer` (duplicate signal), `solar energy company` (too vague).

---

### Workflow 2 — Google Maps Scraping

| | |
|---|---|
| **Location** | `src/workflow_2_data_scraping/` |
| **Files** | `google_maps_scraper.py`, `data_cleaner.py` |
| **Input** | `data/search_tasks.json` |
| **Output** | `data/raw_leads.csv` |

Uses the Google Places API (key in `.env` as `GOOGLE_MAPS_API_KEY`).

**Steps:**
1. Places Text Search API — finds companies, auto-paginates (up to 3 pages × 20 results)
2. Places Details API — fetches `website`, `phone` **only when missing** from Text Search (reduces API cost ~70%)

**Output fields:**
```
company_name, address, website, phone, rating, category,
place_id, source_keyword, source_location
```

**Deduplication:** always use `place_id` (primary), fall back to normalised `website` URL.

---

### Workflow 3 — Website Crawling

| | |
|---|---|
| **Location** | `src/workflow_3_web_crawler/` |
| **Files** | `website_crawler.py`, `content_extractor.py` |
| **Input** | `data/raw_leads.csv` |
| **Outputs** | `data/company_pages.json`, `data/company_text.json` |

**Steps:**
1. `website_crawler.py` — crawls homepage + `/about`, `/services`, `/projects`, `/products` (max 5 pages/site, 1 s delay between domains, dedup by root domain via `tldextract`)
2. `content_extractor.py` — strips scripts/styles/nav with BeautifulSoup, returns clean text ≤ 5000 chars per company

---

### Workflow 4 — AI Company Analysis

| | |
|---|---|
| **Location** | `src/workflow_4_company_analysis/` |
| **Files** | `company_classifier.py` |
| **Input** | `data/company_text.json` |
| **Output** | `data/company_analysis.json` |

Uses Anthropic `claude-haiku-4-5` (cheapest model). Falls back to keyword rules when `ANTHROPIC_API_KEY` is not set.

Classifications: `solar installer`, `solar EPC`, `solar contractor`, `solar developer`, `solar energy company`, `solar panel installer`, `solar farm developer`, `battery storage installer`, `BESS integrator`.

Output fields: `company_name`, `website`, `place_id`, `company_type`, `market_focus`, `services_detected`, `confidence_score`, `classification_method`.

---

### Workflow 4.5 — Buyer Filter / Value Chain Classification  (P1-1A)

| | |
|---|---|
| **Location** | `src/workflow_4_5_buyer_filter/` |
| **Files** | `buyer_filter_models.py`, `value_chain_classifier.py`, `buyer_filter_rules.py`, `buyer_filter_pipeline.py` |
| **Input** | `data/runs/<campaign_id>/company_analysis.json` (required), `data/runs/<campaign_id>/company_text.json` (optional — text signals) |
| **Output** | `data/runs/<campaign_id>/buyer_filter.json` (`BUYER_FILTER_FILE`) |
| **Campaign step** | `buyer_filter` (registered between `analyze` and `score`) |
| **Test** | `scripts/test_buyer_filter.py` (Groups A–M, 13 groups) |

Deterministic (zero AI calls), fully auditable buyer classification layer that enriches every `company_analysis.json` record with structured buyer-fit fields before lead scoring.

**Value chain roles** (field: `value_chain_role`):
- `installer` — solar installer, panel installer, battery storage installer
- `epc_or_contractor` — solar EPC, solar contractor, BESS integrator
- `developer` — solar developer, solar farm developer
- `distributor` — solar component distributor
- `manufacturer` — detected via text keywords (factory, OEM, manufacture)
- `consultant` — detected via text keywords when role=unclear
- `media_or_directory` — detected via text keywords when role=unclear
- `association_or_nonbuyer` — not yet mapped
- `unclear` — fallback

**Scored fields (0–10):**
- `buyer_likelihood_score` — top-line composite (60% procurement + 40% market_fit)
- `procurement_relevance_score` — will they buy our mounting/racking systems?
- `market_fit_score` — commercial/utility ICP fit
- `project_signal_strength` — keyword evidence of real project delivery

**Signal strength fields (0–10):** `commercial_signal_strength`, `utility_signal_strength`, `installer_signal_strength`, `developer_signal_strength`, `distributor_signal_strength`

**Negative targeting flags (bool):**
- `competitor_flag` — manufacturer detected (also sets `manufacturer_flag`)
- `manufacturer_flag` — factory/OEM text signals
- `consultant_flag` — advisory/feasibility text signals
- `media_or_directory_flag` — news/blog/directory text signals
- `negative_residential_flag` — market_focus=residential OR homeowner text

**Score caps:** manufacturer/competitor → bls ≤ 3; media → bls ≤ 2; consultant → prs ≤ 3.

**Backward compatibility:** Workflow 5 (lead_scorer.py) still reads `company_analysis.json` directly. P1-1B will wire buyer_filter scores into the scoring formula. `buyer_filter.json` contains all original `company_analysis.json` fields plus the new buyer-fit fields.

**`load_buyer_filter_results(limit=0)`** — convenience loader in `buyer_filter_pipeline.py` for downstream use (P1-1B).

---

### Workflow 5 — Lead Scoring  (P1-1B: Dual-Axis Qualification)

| | |
|---|---|
| **Location** | `src/workflow_5_lead_scoring/` |
| **Files** | `lead_scorer.py` |
| **Input** | `data/runs/<campaign_id>/buyer_filter.json` (preferred, P1-1A) OR `data/runs/<campaign_id>/company_analysis.json` (fallback) |
| **Output** | `data/runs/<campaign_id>/qualified_leads.csv`, `data/runs/<campaign_id>/disqualified_leads.csv` |
| **Threshold** | `QUALIFIED_THRESHOLD = 45` |
| **Test** | `scripts/test_lead_scorer_p1_1b.py` (Groups A–M, 13 groups) |

**P1-1B dual-axis scoring model** — deterministic, auditable, backward-compatible:

**Axis 1: Solar Relevance** (unchanged from v1, same numeric values):
```
solar installer / solar EPC / solar panel installer  +40
solar developer / solar contractor                   +35
battery storage installer / BESS integrator          +30
solar farm developer / distributor                   +25
solar energy company                                 +20
utility-scale market                                 +25
commercial market                                    +20
mixed market                                         +10
residential market                                    -5
website present                                      +10
consulting / marketing / training (legacy)      -20 to -30
low confidence (<0.50)                                -8
```

**Axis 2: Buyer Relevance** (new, from P1-1A buyer_filter.json):
```
buyer_relevance_component = min(20, round(PRS × 1.5 + PSS × 0.5))
```
where PRS = procurement_relevance_score (0–10), PSS = project_signal_strength (0–10).

**Value chain role adjustment** (new, from value_chain_role):
```
installer:              0
epc_or_contractor:     +3
developer:              0
distributor:            0
manufacturer:          -20
consultant:            -20
media_or_directory:    -25
association_or_nonbuyer:-20
unclear:                -5
```

**Negative targeting penalties** (new, from flags):
```
negative_residential_flag:      0  (informational only — residential installers ARE valid targets)
competitor_flag (role ≠ manufacturer): -10 extra
```

**Hard score caps** (safety net after all additive logic):
```
manufacturer_flag OR competitor_flag  → max score = 30  (below threshold)
consultant_flag                       → max score = 30
media_or_directory_flag               → max score = 25
```

**Threshold: 45** (unchanged from v1). The wider distribution makes 45 the right cut:
- Strong commercial EPC → 80–95; residential installer → ~30–36; manufacturer/consultant/media → ≤ 30.

**Input priority:**
1. `buyer_filter.json` (P1-1A) — triggers `scoring_version="v2_with_buyer_filter"`
2. `company_analysis.json` — fallback, triggers `scoring_version="v1_solar_only"` (identical to pre-P1-1B behavior)

**New output fields in `qualified_leads.csv`** (added in P1-1B, backwards-compatible):
- `qualification_status` — "qualified" / "rejected"
- `qualification_reason_summary` — human-readable single line explaining the decision
- `solar_relevance_component`, `buyer_relevance_component`, `value_chain_adjustment`, `negative_targeting_penalty` — score component breakdown
- `scoring_version` — "v2_with_buyer_filter" or "v1_solar_only"
- `value_chain_role`, `buyer_likelihood_score`, `procurement_relevance_score`, `market_fit_score`, `project_signal_strength` — buyer filter pass-throughs
- `negative_residential_flag`, `competitor_flag`, `manufacturer_flag`, `consultant_flag`, `media_or_directory_flag` — negative targeting flag pass-throughs
- `buyer_filter_reason` — human-readable from P1-1A

**TODO for later P1 tickets:**
- P1-1C: Wire `buyer_filter_reason` into Workflow 5.5 enrichment targeting
- P1-2: Use `procurement_relevance_score` to prioritise send queue ordering
- P1-3: Feed historical send outcomes back to score weighting (requires P1-1C feedback loop)
- DB: Add P1-1B scoring component fields to `company_analysis` / `contacts` tables

---

### Workflow 5.5 — Lead Enrichment (P1-2A: Multi-Contact Output)

| | |
|---|---|
| **Location** | `src/workflow_5_5_lead_enrichment/` |
| **Files** | `enricher.py` |
| **Input** | `data/runs/<id>/qualified_leads.csv` |
| **Outputs** | `data/runs/<id>/enriched_leads.csv` (primary contact, backward-compat) + `data/runs/<id>/enriched_contacts.csv` (all contacts, up to 3 per company) |

**P1-2A upgrade**: enrichment now extracts **up to 3 contacts per company** using slot-filling across all sources, instead of collapsing to one.

#### Multi-contact waterfall (slot-filling order)

1. **Apollo.io** People Search — buyer-persona contacts first (CEO, Founder, Owner, etc.), then any relevant title. Tag: `apollo`.
2. **Hunter.io** Domain Search — fills remaining slots; sorted by persona match then confidence. Tag: `hunter`.
3. **Website** — site_emails scraped by Workflow 3, one contact per email. Tag: `website`.
4. **Mock** — deterministic dummy data when both API keys absent (smoke-test). Tag: `mock`.
5. **Guessed** — role-address patterns (`info@`, `sales@`) as last resort in live runs. Tag: `guessed`.

Slots are filled in order; duplicate emails are deduplicated across sources.

#### Output files

| File | Description |
|---|---|
| `enriched_leads.csv` | **Unchanged format** — one row per company, rank=1 (primary) contact only. Fields: `ENRICHED_FIELDS`. Consumed by Workflow 5.9 and 6. |
| `enriched_contacts.csv` | **New** — one row per contact (up to 3 per company). Fields: all `ENRICHED_FIELDS` + `contact_rank` + `is_generic_mailbox`. |

#### Contact metadata fields (enriched_contacts.csv only)

| Field | Type | Values |
|---|---|---|
| `contact_rank` | int | 1=primary, 2=backup, 3=tertiary |
| `is_generic_mailbox` | str | `"true"` when local-part is `info`, `sales`, `contact`, `admin`, etc. |

#### Generic mailbox detection

`_is_generic_mailbox(email)` checks the local-part against `_GENERIC_LOCAL_PARTS` (frozenset of ~30 alias patterns). Generic contacts are still included but ranked lower and flagged for downstream routing.

#### DB persistence

`contacts` table gains two columns via migration (`_MIGRATIONS_CONTACTS`):
- `contact_rank INTEGER NOT NULL DEFAULT 1`
- `is_generic_mailbox INTEGER NOT NULL DEFAULT 0`

`csv_sync.sync_enriched_contacts()` syncs `enriched_contacts.csv` → contacts table (deduped by email+company_id). `sync_all()` calls it after `sync_enriched_leads()`.

#### Backward compatibility

- `ENRICHED_FIELDS` constant unchanged — Workflow 5.9 (verification_pipeline) import is safe.
- `enrich_lead()` function unchanged — still returns a single contact dict.
- `save_enriched_leads()` unchanged — `extrasaction="ignore"` drops multi-contact fields.
- `run()` now calls `enrich_lead_multi()` internally and writes both output files.

Keys: `APOLLO_API_KEY`, `HUNTER_API_KEY` in `.env`.

---

### Workflow 5.8 — Company Signal Research

| | |
|---|---|
| **Location** | `src/workflow_5_8_signal_research/` |
| **Files** | `signal_collector.py`, `signal_summarizer.py` |
| **Input** | `data/enriched_leads.csv` |
| **Outputs** | `data/research_signal_raw.json`, `data/research_signals.json` |

Lightweight, rule-based (no AI) pre-email research layer that collects recent activity signals from company websites and social pages, then classifies them into structured outreach outputs.

**signal_collector.py** — per company:
1. Fetches homepage + up to 5 signal pages (`/`, `/news`, `/blog`, `/projects`, `/case-studies`, `/careers`)
2. Discovers social links (LinkedIn, Facebook, Instagram, YouTube) from homepage
3. Extracts headlines (`h1`/`h2`/`h3`) and meta descriptions from each page
4. Saves raw structured signals to `research_signal_raw.json`

**signal_summarizer.py** — per company:
1. Scans all collected text for keyword categories: `battery`, `commercial`, `utility`, `expansion`, `residential`
2. Produces `research_summary` (deterministic text) and `email_angle` (outreach recommendation)
3. Saves final structured output to `research_signals.json`

**Constraints:** no API keys required, max 5 pages/company, max 3 social links, timeout 10 s, fail gracefully.

**Output fields per company:** `company_name`, `website`, `place_id`, `recent_signals`, `research_summary`, `email_angle`.

---

### Workflow 6.2 — Signal-based Personalization

| | |
|---|---|
| **Location** | `src/workflow_6_2_signal_personalization/` |
| **Files** | `signal_loader.py`, `signal_ranker.py`, `signal_to_opening.py`, `signal_pipeline.py` |
| **Inputs** | `data/company_signals.json` (primary) or `data/research_signals.json` (fallback) + `data/enriched_leads.csv` |
| **Output** | `data/company_openings.json` |

Converts raw company signals into personalized cold email opening lines. Pure rule-based — no AI required.

**signal_loader.py** — loads signal data:
- Prefers `company_signals.json` (native format: `{company_name, signals: [...]}`)
- Falls back to `research_signals.json` (uses `recent_signals` field)
- Returns list of `{company_name, signals}` dicts

**signal_ranker.py** — selects the single best signal per company:
- Tier 0 (project): `install`, `rooftop`, `solar farm`, `solar project`, `completed`, `commission`, `deploy`, `kw`, `mw`, `megawatt`
- Tier 1 (storage): `battery`, `powerwall`, `storage`, `bess`, `backup power`, `energy storage`, `tesla`, `enphase`
- Tier 2 (expansion): `hiring`, `hire`, `expanding`, `expansion`, `join our team`, `growing`, `new office`, `new location`, `now serving`
- Filters out signals with < 3 words; tiebreaker = total keyword hits (richest signal wins)

**signal_to_opening.py** — converts best signal → opening sentence (≤18 words):
- Pattern cascade: install+size → "I saw your recent…"; completed project → "I noticed your team completed…"; rooftop → location-aware; offering/Powerwall → "I noticed your team is offering…"; battery/BESS → "I noticed your team specialises in…"; expansion/hiring → "It looks like your team is expanding…"; solar farm → utility-scale mention
- Fallback: `"I came across {company_name} while looking at solar installers."`

**signal_pipeline.py** — orchestrator:
1. Loads signals + enriched lead names
2. Deduplicates company list (signals first, then leads not already covered)
3. Ranks signals and converts to opening lines
4. Writes `company_openings.json` as `[{company_name, best_signal, opening_line}]`

**Integration with Workflow 6:** `email_merge.py` loads `company_openings.json` via `load_company_openings()` and attaches `opening_line` to each merged record. `email_generator.py` uses it as `'Use this exact opening sentence: "{opening_line}"'` in the AI prompt.

---

### Workflow 6 — Email Generation

| | |
|---|---|
| **Location** | `src/workflow_6_email_generation/` |
| **Files** | `email_merge.py`, `email_templates.py`, `email_generator.py` |
| **Inputs** | `data/enriched_leads.csv` + `data/research_signals.json` (optional) |
| **Output** | `data/generated_emails.csv` |

Generates one cold email draft per enriched lead via OpenRouter LLM. Falls back to rule-based templates if AI fails.

**email_merge.py** — merges enriched leads with research signals:
- Primary key: `place_id`; fallbacks: normalized `website`, `company_name`
- Derives `email_angle` from `company_type`: battery/BESS/storage → `storage`; installer → `installation`; else → `supply`
- Prefers signal-derived angle when `research_signals.json` exists and has a non-generic value
- Handles missing `research_signals.json` gracefully

**email_templates.py** — rule-based fallback generation:
- Greeting: `Hi {first_name},` if contact name known; else `Hello {Company} team,`
- Subject lines keyed on angle: `storage` / `installation` / `supply`
- `trim_to_limit(text, max_words=180)` enforces word count cap
- Returns both `body` (canonical) and `email_body` (legacy) keys

**email_generator.py** — OpenRouter AI with rule-based fallback:
- Primary provider: **OpenRouter** (`anthropic/claude-3.5-haiku` by default)
- Configured via `EMAIL_GEN_PROVIDER`, `EMAIL_GEN_MODEL`, `OPENROUTER_API_KEY`
- JSON output: `subject`, `body`
- Greeting enforced: never "Hi there," — uses first name or "Hello {Company} team,"
- Role confusion prevention: system prompt explicitly forbids "At {company} we..."
- Hard cap: 120 words per email
- Fallback: rule-based template on any AI error

**email_angle values:**
- `storage` — company handles battery/BESS/storage work
- `installation` — solar installer or solar panel installer
- `supply` — all other types (EPC, contractor, energy company)

**Output columns:** `company_name`, `kp_name`, `kp_email`, `subject`, `body`, `lead_score`, `email_angle`, `generation_source`

---

### Workflow 6.5 — Email Quality Scoring

| | |
|---|---|
| **Location** | `src/workflow_6_5_email_quality/` |
| **Files** | `quality_merge.py`, `quality_rules.py`, `email_quality_scorer.py` |
| **Input** | `data/generated_emails.csv` |
| **Outputs** | `data/scored_emails.csv`, `data/send_queue.csv`, `data/rejected_emails.csv` |

Quality gate between Workflow 6 (Email Generation) and Workflow 7 (Email Sending).

**Scoring dimensions:**
- Personalization (40%) — company name usage, KP name, opening line quality
- Relevance (40%) — email angle keyword match, company type alignment
- Spam risk inverted (20%) — spam words, buzzwords, length, caps

**Approval thresholds:**
- `approved` — overall_score ≥ 75 AND spam_risk ≤ 35
- `manual_review` — overall_score 60–74 OR spam_risk 36–55
- `rejected` — overall_score < 60 OR spam_risk > 55 OR hard failures (missing email, empty subject/body, unresolved placeholders)

**AI scoring mode:** OpenRouter → Anthropic → OpenAI → rule-based fallback
**scoring_mode field:** `"ai"` or `"rule"`

---

### Workflow 6.7 — Email Repair Loop

| | |
|---|---|
| **Location** | `src/workflow_6_7_email_repair/` |
| **Files** | `repair_selector.py`, `email_rewriter.py`, `repair_pipeline.py` |
| **Input** | `data/scored_emails.csv` |
| **Outputs** | `data/repaired_emails.csv`, `data/rescored_emails.csv`, `data/final_send_queue.csv`, `data/final_rejected_emails.csv` |

Quality repair gate between Workflow 6.5 (Email Quality Scoring) and Workflow 7 (Email Sending).

**repair_selector.py** — identifies repairable emails:
- Loads `scored_emails.csv`
- Selects `manual_review` or `rejected` records with `overall_score ≥ 45`
- Skips hard failures: missing `kp_email`, empty subject/body, unresolved placeholders
- Returns `(repairable, already_approved)` lists

**email_rewriter.py** — rewrites weak emails:
- Provider waterfall: OpenRouter → Anthropic → OpenAI → rule fallback
- Builds repair prompt including `review_notes` to target specific issues
- Falls back to `build_rule_based_email()` from email_templates when AI unavailable

**repair_pipeline.py** — orchestrates full repair flow:
1. Selects repairable emails via `repair_selector`
2. Rewrites each via `email_rewriter`
3. Rescores using `score_email()` from Workflow 6.5
4. Applies repair thresholds: `approved_after_repair` ≥72 & spam≤35; `manual_review_after_repair` 60–71; `rejected_final` <60
5. Writes `repaired_emails.csv`, `rescored_emails.csv`
6. `final_send_queue.csv` = originally approved + newly approved after repair

**Extra output fields:** `repair_mode`, `repair_source`, `original_score`, `original_status`

---

### Workflow 7 — Email Sending + Send Logging

| | |
|---|---|
| **Location** | `src/workflow_7_email_sending/` |
| **Files** | `send_loader.py`, `send_guard.py`, `email_sender.py`, `send_logger.py`, `send_pipeline.py` |
| **Input** | `data/final_send_queue.csv` |
| **Outputs** | `data/send_logs.csv`, `data/send_batch_summary.json` |

First real outbound execution layer. Reads approved emails from Workflow 6.7's final send queue, enforces all safety rules, sends via configurable provider, and logs every attempt.

**send_loader.py** — loads and validates the final send queue:
- Requires columns: `company_name`, `kp_email`, `subject`, `email_body`
- Only processes records with `approval_status` ∈ `{approved, approved_after_repair}`
- Exits clearly if file missing or columns wrong

**send_guard.py** — enforces six safety checks in order:
1. Required fields — block if `kp_email`, `subject`, or `email_body` missing
2. Email format — block malformed addresses (regex `@` check)
3. Approval status — block anything not in approved set
4. Business hours — defer if outside Mon–Fri 08:00–18:00 local time (configurable via `SEND_WINDOW_START`/`SEND_WINDOW_END`)
5. Duplicate protection — block if same `kp_email + subject` already sent/dry-run within 24h
6. Company throttle — block if same `place_id` (or `company_name` fallback) already contacted within 24h

Returns `{"allowed": bool, "decision": "send"|"blocked"|"deferred", "reason": "..."}`.

**email_sender.py** — sends one email per call:
- `EMAIL_SEND_MODE=dry_run` (default) — simulates send, logs `dry_run`, no SMTP connection
- `EMAIL_SEND_MODE=smtp` — sends via smtplib with optional TLS; supports `Reply-To`
- Returns `{"send_status", "provider", "provider_message_id", "error_message"}`
- Never raises — all exceptions caught, returned as `failed` status

**send_logger.py** — persistent append-only log:
- Writes to `data/send_logs.csv`; creates with header if missing
- Logs ALL outcomes: `sent`, `dry_run`, `failed`, `blocked`, `deferred`
- `load_recent_logs(hours=24)` — used by send_guard for dedup
- `sent_recently(kp_email, subject)` — helper for external queries
- `company_sent_recently(place_id, company_name)` — helper for external queries

**send_pipeline.py** — orchestrator:
- Loads queue → runs guards → sends → logs → enforces `DAILY_EMAIL_LIMIT`
- In-memory log extended each send so same-batch dedup works without file re-reads
- Writes `send_batch_summary.json` at end

**Send log columns:** `timestamp`, `company_name`, `place_id`, `kp_name`, `kp_email`, `subject`, `send_decision`, `send_status`, `decision_reason`, `provider`, `provider_message_id`, `error_message`

**Config variables (`.env`):**
```
EMAIL_SEND_MODE=dry_run       # "dry_run" | "smtp"
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=
SMTP_PASSWORD=
SMTP_USE_TLS=true
SMTP_FROM_EMAIL=
SMTP_FROM_NAME=
REPLY_TO_EMAIL=
SEND_WINDOW_START=8
SEND_WINDOW_END=18
```

---

### Workflow 7.5 — Open / Click Tracking

| | |
|---|---|
| **Location** | `src/workflow_7_5_engagement_tracking/` |
| **Files** | `tracking_id_manager.py`, `email_tracking_injector.py`, `engagement_logger.py`, `engagement_aggregator.py`, `tracking_server.py` |
| **Inputs** | `data/send_logs.csv` (metadata lookup) |
| **Outputs** | `data/engagement_logs.csv`, `data/engagement_summary.csv` |

Engagement tracking layer between Workflow 7 (Email Sending) and Workflow 8 (Follow-up Automation). Enables answering: was the email opened? was a link clicked? how many times?

**tracking_id_manager.py** — generates unique, URL-safe IDs:
- `generate_tracking_id(record)` → `{place_id}_{timestamp}_{random4}`
- `generate_message_id(record)` → `msg_{sha1hex12}`
- Both IDs attached to send_logs.csv rows by Workflow 7

**email_tracking_injector.py** — prepares tracked HTML:
- `build_html_email(body)` — wraps plain text in minimal valid HTML
- `inject_open_tracking(html, tid, base_url)` — 1×1 transparent pixel before `</body>`
- `rewrite_click_links(html, tid, base_url)` — rewrites `http/https` hrefs to click-tracking URLs
- `prepare_tracked_email(body, tid, base_url)` → `{plain_text, html_body, tracking_id, tracked_links_count}`

**engagement_logger.py** — append-only event store (`engagement_logs.csv`):
- `event_type`: `open` or `click`
- Columns: `timestamp`, `tracking_id`, `message_id`, `company_name`, `kp_email`, `event_type`, `target_url`, `ip`, `user_agent`

**engagement_aggregator.py** — per-email summary (`engagement_summary.csv`):
- Groups events by `tracking_id`; counts opens/clicks; records first/last timestamps
- `run()` re-aggregates from scratch (fully derived data)

**tracking_server.py** — lightweight Flask server:
- `GET /health` → `{"status": "ok"}`
- `GET /track/open/<tracking_id>` → logs open event, returns 1×1 transparent GIF
- `GET /track/click/<tracking_id>?url=<encoded>` → logs click, redirects 302 to target
- Enriches events with company_name/kp_email from send_logs.csv lookup
- Run: `py -m src.workflow_7_5_engagement_tracking.tracking_server`

**⚠ OPEN TRACKING LIMITATIONS (document these):**
- Apple Mail Privacy Protection (MPP) pre-fetches pixels → inflated open counts
- Gmail and other clients may proxy image loads on behalf of recipients
- Corporate email security gateways may scan all links on delivery
- **Open counts are approximate signals only** — not hard metrics
- **Click tracking is significantly more reliable** than open tracking
- Reply tracking is NOT part of this workflow; belongs to Workflow 8

**Config variables (`.env`):**
```
TRACKING_BASE_URL=http://localhost:5000
```

---

### Workflow 8 — Follow-up Automation

| | |
|---|---|
| **Location** | `src/workflow_8_followup/` |
| **Files** | `followup_selector.py`, `followup_stop_rules.py`, `followup_scheduler.py`, `followup_generator.py`, `followup_pipeline.py` |
| **Inputs** | `data/send_logs.csv`, `data/engagement_summary.csv`, `data/followup_logs.csv` |
| **Outputs** | `data/followup_candidates.csv`, `data/followup_queue.csv`, `data/followup_blocked.csv`, `data/followup_logs.csv` |

Behavior-aware follow-up scheduling layer. Reads send history and engagement signals to decide which contacts are due for follow-up, generates drafts, and outputs a follow-up queue. Does NOT send emails — that is handled by a future send step.

**followup_selector.py** — identifies candidates:
- Loads `send_logs.csv`, keeps `sent` and `dry_run` records
- Groups by `kp_email`, keeps latest send per contact
- Joins `engagement_summary.csv` by `kp_email` (aggregates open/click counts)
- Reads `followup_logs.csv` to count prior follow-ups per contact → determines stage
- Returns candidate list with `followup_stage`, `open_count`, `click_count`

**followup_stop_rules.py** — blocks unsafe follow-ups:
- Missing or malformed `kp_email` → blocked
- Invalid/exhausted follow-up stage → blocked
- `suppressed` flag set → blocked
- `classify_engagement(open_count, click_count)` → `no_open` / `opened_no_click` / `multi_open_no_click` / `clicked_no_reply`

**followup_scheduler.py** — computes due dates:
- `followup_1` → 3 days after last send
- `followup_2` → 7 days after last touch
- `followup_3` → 14 days after last touch
- `build_followup_schedule(candidate, now)` → `{due_date, is_due, scheduled_action}`
- `scheduled_action`: `queue_now` / `wait` / `blocked`

**followup_generator.py** — generates follow-up drafts:
- Provider waterfall: OpenRouter → Anthropic → OpenAI → fallback templates
- Prompt adapts to `engagement_status` without mentioning opens/clicks or tracking
- Stage-appropriate length: followup_1 (50–90w), followup_2 (60–100w), followup_3 (50–80w)
- Never says "just checking in", "circling back", or references engagement signals
- Fallback templates keyed on `followup_stage` × `engagement_status`

**followup_pipeline.py** — orchestrator:
- Selects → classifies engagement → stop rules → schedule → generate
- Max 3 follow-up stages; stage determined by count in `followup_logs.csv`
- `followup_queue.csv` = contacts due now with generated drafts
- `followup_blocked.csv` = blocked/deferred with reasons
- `followup_logs.csv` = append-only record of all decisions (used for stage tracking in future runs)

**Follow-up schedule:**
```
Initial send     → Day 0  (Workflow 7)
followup_1       → Day 3  after initial send
followup_2       → Day 7  after followup_1
followup_3       → Day 14 after followup_2
```

**Engagement-aware generation rules:**
- `no_open` → short light reminder, possibly adjusted subject
- `opened_no_click` → clearer value proposition
- `multi_open_no_click` → more concrete relevance clarification
- `clicked_no_reply` → practical next step, slightly more concrete CTA

**Config variables (`.env`):**
```
FOLLOWUP_MAX_STAGE=3
FOLLOWUP_1_DELAY_DAYS=3
FOLLOWUP_2_DELAY_DAYS=7
FOLLOWUP_3_DELAY_DAYS=14
```

---

### Workflow 8.5 — Campaign Status Aggregator

| | |
|---|---|
| **Location** | `src/workflow_8_5_campaign_status/` |
| **Files** | `status_loader.py`, `status_merger.py`, `status_classifier.py`, `status_pipeline.py` |
| **Inputs** | `data/send_logs.csv`, `data/engagement_summary.csv`, `data/followup_logs.csv`, `data/followup_queue.csv`, `data/followup_blocked.csv` (+ optional: `data/final_send_queue.csv`, `data/enriched_leads.csv`) |
| **Outputs** | `data/campaign_status.csv`, `data/campaign_status_summary.json` |

Read-only aggregator — does not send emails or modify any other pipeline files. Produces a single per-contact operational status view.

**status_loader.py** — loads and normalises all input tables into dicts keyed by the best available join key (`pid:{place_id}` → `email:{kp_email}` → `name:{company_name}`).

**status_merger.py** — joins all tables into one flat record per contact. Join order: send_logs as universe → engagement by `tracking_id` → followup tables by `kp_email`.

**status_classifier.py** — assigns `lifecycle_status`, `next_action`, `priority_flag`:
- Evaluated in priority order: `completed` → `followup_sent` → `followup_queued` → `followup_deferred` → `followup_blocked` → `clicked_no_reply` → `opened_no_click` → `sent_no_open` → `not_sent`
- Priority flags: `high` (clicked / queued with click), `medium` (opened / queued without click / sent_no_open), `low` (blocked / deferred / completed / not_sent)

**status_pipeline.py** — orchestrator: load → merge → classify → write outputs.

**Lifecycle statuses:**
```
not_sent           — no initial send on record
sent_no_open       — sent, no open recorded
opened_no_click    — opened but no click
clicked_no_reply   — clicked but no reply
followup_queued    — follow-up ready to send in queue
followup_deferred  — follow-up scheduled but not yet due
followup_blocked   — follow-up blocked by stop rules
followup_sent      — at least one follow-up has been sent
completed          — all 3 follow-up stages exhausted
```

---

### Workflow 9 — Campaign Runner

| | |
|---|---|
| **Location** | `src/workflow_9_campaign_runner/` |
| **Files** | `campaign_config.py`, `campaign_state.py`, `campaign_logger.py`, `campaign_steps.py`, `campaign_runner.py` |
| **CLI** | `py scripts/run_campaign.py [options]` |
| **Test** | `py scripts/test_campaign_runner.py` |
| **Process-level outputs** | `data/campaign_run_state.json`, `data/campaign_run.lock` (fixed paths) |
| **Per-run outputs** | `data/runs/<campaign_id>/campaign_runner_logs.csv` and all pipeline artifacts |

Top-level orchestration layer that runs the full outbound pipeline from a single command. Workflow 9 is **not** a business-logic workflow — it wraps Workflows 1–8.5 in a thin, resumable execution layer. It does not duplicate any workflow logic.

**Why it exists:** Without Workflow 9, running the pipeline requires manually invoking 15+ separate workflow scripts in the correct order. Workflow 9 makes the whole system operable from one command, with state persistence for safe resumption.

**campaign_config.py** — `CampaignConfig` dataclass with all run parameters:
- Geographic targeting: `country`, `region`, `city`
- Keywords: `keyword_mode` (`"default"` | `"custom"`), `keywords`
- Limits: `company_limit`, `crawl_limit`, `enrich_limit` (all 0 = no limit)
- Send: `send_mode` (`"dry_run"` | `"smtp"`), `dry_run`
- Execution: `run_until` (step name), `resume`
- Helpers: `get_effective_keywords()`, `get_effective_location()`, `validate_config()`

**campaign_state.py** — JSON state persistence (`data/campaign_run_state.json`):
- Fields: `campaign_id`, `started_at`, `updated_at`, `last_completed_step`, `status`, `config`, `error_message`
- Statuses: `initialized` → `running` → `completed` | `failed` | `paused`
- Functions: `initialize_campaign_state()`, `load_campaign_state()`, `save_campaign_state()`, `update_campaign_state()`
- Cloud handoff tracking is now modeled separately from pipeline execution:
  - `cloud_deploy_status`
  - `cloud_deploy_updated_at`
  - `cloud_deploy_error`
- Valid cloud deploy statuses:
  - `not_enabled`
  - `pending`
  - `started`
  - `completed`
  - `failed`
- Per-run handoff state is written to:
  - `data/runs/<campaign_id>/cloud_deploy_status.json`
- Helpers:
  - `save_cloud_deploy_status()`
  - `load_cloud_deploy_status()`
  - `sync_cloud_deploy_status()`
- `save_campaign_state()` now also mirrors the global run state into:
  - `data/runs/<campaign_id>/campaign_run_state.json`
  so run-scoped tooling and cloud deploy code can read stable campaign metadata without depending on the process-level state file.

**campaign_logger.py** — Append-only CSV step log (`data/campaign_runner_logs.csv`):
- Columns: `timestamp`, `campaign_id`, `step_name`, `status`, `message`
- Step statuses: `started` | `completed` | `skipped` | `failed`
- Functions: `append_campaign_log()`, `load_campaign_logs()`

**campaign_steps.py** — Thin wrappers (one per workflow step):
- Translates `CampaignConfig` into each workflow's `run()` parameters
- Injects `send_mode` into environment before calling Workflow 7
- Calls DB sync checkpoint after: scrape, enrich, email generation, send, campaign status
- Raises `RuntimeError` clearly if required input files are missing
- Step wrappers: `run_step_1_search_tasks` through `run_step_8_5_campaign_status`

**campaign_runner.py** — Orchestration engine:
- `run_campaign(config)` — executes steps in order, logs each, updates state; stops at `run_until`
- `resume_campaign()` — loads last state, re-activates run context for that campaign_id, continues from next unfinished step
- On failure: logs failure, persists failed state, returns error summary
- Step registry (`PIPELINE_STEPS`) defines canonical order for all 19 steps:
  `search_tasks → scrape → crawl → analyze → buyer_filter → score → enrich →`
  `contact_scoring → verify → signals → queue_policy → personalization →`
  `email_generation → email_quality → email_repair → send → tracking → followup → campaign_status`
- **Run context lifecycle**: calls `run_context.set_active_run(campaign_id)` immediately after campaign_id is known (before acquiring lock), and `run_context.clear_active_run()` in the `finally` block. This activates `_RunPath` proxies for all steps.
- **No intermediate file cleanup on fresh start**: each fresh run gets its own empty `data/runs/<campaign_id>/` directory. Old runs stay archived in their own directories.
- **Stale lock recovery**: `campaign_runner.is_campaign_running()` now auto-clears a stale `data/campaign_run.lock` when the persisted campaign state is no longer `running`, or when the state heartbeat has been idle for more than 2 hours. If the stale state still says `running`, it is rewritten to `failed` with a recovery message so the UI and queue runner stop misreporting an active campaign.

**Supported run modes (via `run_until`):**
- Research only: `--run-until enrich` or `--run-until signals`
- Generate only: `--run-until email_repair`
- Full pipeline: omit `--run-until` (default = `campaign_status`)

**Queue runner background process / logs:**
- `ui_views.py` starts the multi-run scheduler as a detached background process from the "Start Runner" button
- PID file: `data/scheduler.pid`
- Live stdout/stderr log: `data/queue_runner.log`
- Previous run backup: `data/queue_runner.previous.log`
- On every new scheduler start, the prior `queue_runner.log` is rotated to `queue_runner.previous.log` before the new process begins writing
- PID liveness check is platform-specific: on Windows, `ui_views.py` uses `tasklist` instead of `os.kill(pid, 0)` because some Python/Windows combinations raise `WinError 87` / `SystemError` for signal-0 existence probes
- Queue Panel auto-refresh now prefers Streamlit's native `st.fragment(run_every=...)` mechanism instead of the earlier browser `postMessage({type:"streamlit:rerun"})` hack; this keeps the queue/progress section updating more reliably while the scheduler is active
- `Start Runner` now auto-resumes a paused queue before launching the scheduler process. This matches operator expectation: clicking Start should begin consuming pending jobs, not start an idle process that remains blocked by the pause flag.
- Recommended terminal monitoring command (PowerShell): `Get-Content .\data\queue_runner.log -Wait`

**Compatibility:**
- SQLite sync (Workflow 0) runs automatically after major steps if `solar_leads.db` exists
- Streamlit dashboard (Workflow 0) continues to work without modification
- Each wrapped workflow remains independently runnable as before (path proxies fall back to `DATA_DIR` when no run context is set)

**Example commands:**
```bash
# Full pipeline for Vancouver
py scripts/run_campaign.py --city Vancouver --country Canada

# Stop after email generation
py scripts/run_campaign.py --city Vancouver --country Canada --run-until email_generation

# Limit to 20 companies
py scripts/run_campaign.py --city Seattle --country USA --company-limit 20

# Resume after interruption
py scripts/run_campaign.py --resume

# Send real emails (default is dry_run)
py scripts/run_campaign.py --city Vancouver --send-mode smtp

# Launch Campaign Control Panel (Workflow 9.5)
streamlit run src/workflow_9_5_streamlit_control_panel/app.py
py scripts/run_control_panel.py
```

---

### Workflow 9.5 — Streamlit Campaign Control Panel

| | |
|---|---|
| **Location** | `src/workflow_9_5_streamlit_control_panel/` |
| **Files** | `app.py`, `ui_config.py`, `ui_runner.py`, `ui_state.py`, `ui_views.py` |
| **App** | `streamlit run src/workflow_9_5_streamlit_control_panel/app.py` |
| **Launcher** | `py scripts/run_control_panel.py` |
| **Test** | `py scripts/test_control_panel.py` |

Visual UI layer on top of Workflow 9 — Campaign Runner. Allows a non-technical operator to configure, run, monitor, and resume campaigns without using terminal commands. Does not replace Workflow 9 or the CLI; both coexist.

**What the panel provides:**
1. **Campaign Configuration form** — country, region, city, keyword mode, custom keywords, company/crawl/enrich limits, run-until step, send mode, dry-run toggle
2. **Run Controls** — "Run Campaign" and "Resume Campaign" buttons; both call Workflow 9 directly
3. **Current Campaign State** — reads `campaign_run_state.json`; shows campaign_id, status, last completed step, timestamps, and cloud deploy handoff status
4. **Pipeline Metrics** — top-line counts (companies, contacts, emails generated/sent, opens, clicks, follow-ups queued) derived from CSV outputs
5. **Runner Logs** — last 50 rows of `campaign_runner_logs.csv`; most recent first; status icons
6. **Campaign Status Table** — filterable view of `campaign_status.csv` (lifecycle_status, next_action, priority_flag, open/click counts)
7. **Output File Status** — shows which of the 13 tracked pipeline files currently exist and their sizes

**Module breakdown:**
- `ui_config.py` — `UI_DEFAULTS` dict, `build_campaign_config(form_values)` → `(CampaignConfig | None, errors)`, comma-separated keyword parsing
- `ui_runner.py` — `RunResult` dataclass; `run_campaign_from_ui(form_values)`, `resume_campaign_from_ui()`; wraps Workflow 9 with exception handling
- `ui_state.py` — `load_current_campaign_state()`, `load_campaign_logs()`, `load_campaign_status()`, `load_campaign_summary()`, `load_pipeline_metrics()`, `load_file_status()`; all tolerate missing files. Contains `_activate_display_context()` — called at the start of each read function to point `_RunPath` proxies at the last campaign's directory (reads `campaign_run_state.json`, calls `set_active_run()`); no-op when a run is actively in progress.
- `load_current_campaign_state()` also self-heals stale UI state: if `campaign_run_state.json` still says `running` but `campaign_runner.is_campaign_running()` finds no active lock after stale-lock recovery, the state file is rewritten to `failed` with a recovery message so the control panel does not keep showing a phantom running campaign.
- `load_current_campaign_state()` now also merges `data/runs/<campaign_id>/cloud_deploy_status.json` so the UI distinguishes:
  - local pipeline completion
  - cloud deploy pending / started / completed / failed
- `ui_views.py` — section renderers for the control panel, including the Campaign Queue panel. The queue progress bar no longer keeps its own stale step list; it now reads the canonical Workflow 9 `PIPELINE_STEPS` from `campaign_config.py` so UI progress stays aligned with the real runner order (`buyer_filter`, `contact_scoring`, `verify`, `queue_policy`, etc. included).
- Queue Panel job management UX is now table-first: the jobs table includes a selectable checkbox column and direct `Remove selected` / `Re-queue selected` actions below the table. This replaces the older expander-driven “pick one job_id from a dropdown” flow and is intended to reduce friction when cleaning up multiple queued cities.
- UI/system consistency fixes:
  - The Streamlit `Dry Run` checkbox is now read-only and derived from `Send Mode`; Workflow 7 behavior is controlled by `send_mode`, so the UI no longer implies that an independent checkbox can override real sending.
  - KPI `Contacts` / `Contact Rate` now count true contact rows from `enriched_contacts.csv` instead of company-level `enriched_leads.csv`.
  - The city-status badge in the configuration form now matches both bare city names and full `source_location`-style strings such as `City, Region, Country`, so previously crawled cities are less likely to be shown as “No data yet”.
- The control panel now includes a **Multi-Run Comparison** section that summarizes recent completed queue jobs side by side. Data source is `campaign_queue.json` + each run folder under `data/runs/<campaign_id>/`. Per-city columns include: raw leads, dedup skipped, dedup %, qualified, contacts, generated emails, initial send queue, final send queue, repair lift, rejected, and generic-only %. This is intended to make metro overlap and low-contact-quality cities visible without manual file inspection.
- The Multi-Run Comparison view is defensive against partial/older run data: rate columns such as `final_queue_rate`, `dedup_rate_pct`, and `generic_only_pct` are now always materialized by the loader or defaulted in the view so the page does not crash with a `KeyError` when one run is missing a derived metric column.
- Workflow 7 send-stage soft review path:
  - `generic_only` queue-policy matches and duplicate/company-throttle guard hits are no longer hard-blocked by default.
  - These records now log `send_status=review_required`, are written to `manual_review_queue.csv` in the current run folder, and appear in the control panel's **Manual Review Queue** section for operator inspection.
  - Hard deliverability / suppression risks still remain true blocks (e.g. E0 eligibility, breaker blocks, malformed email, missing required fields).
- `campaign_status_summary.json` send-stage policy section now includes `review_required` so operators can distinguish true hard blocks from soft-review deferrals.
- KPI Dashboard now surfaces send-stage review outcomes explicitly:
  - `Send Review Queue` = count of `send_logs.csv` rows where `send_status=review_required`
  - `Hard Send Blocks` = count of `send_logs.csv` rows where `send_status=blocked`
  - follow-up blocking remains a separate downstream caption rather than being mixed into send-stage metrics
- KPI Dashboard now also includes a **Send Ops Snapshot** block for operator memory / cloud-send visibility:
  - `Cloud Delegated Emails` = total send-stage rows from completed `gmail_api` cloud runs (aggregated from `campaign_queue.json` + each run's `cloud_deploy_status.json` + `send_batch_summary.json`)
  - `Sent Successfully` = total true `sent` rows aggregated from completed cloud runs
  - `Current Country` = currently running queue job country when the queue is active; otherwise falls back to the latest run state
  - `Uploaded Yesterday` = prior-day cloud-uploaded email volume plus run count, so operators can answer "yesterday uploaded how many?" without opening run folders
  - Deliverability risk cards are shown in the same snapshot:
    - `Bounces 7d`
    - `Bounce Rate 7d`
    - `Suppressed Addresses`
    - `Last Bounce`
  - These are aggregated from global CRM files (`send_logs.csv`, `engagement_logs.csv`, `reply_logs.csv`) so the operator sees both throughput and email health on the same screen
  - Loader implementation: `load_delivery_ops_snapshot()` in `ui_state.py`
- Multi-Run Comparison now includes soft-review visibility so city-to-city comparisons reflect the new operating model:
  - `Delivery Ready` / `Delivery-Ready %` = `sent + dry_run` rows from `send_batch_summary.json`
  - `Review Required` / `Review %` = `review_required` rows from `send_batch_summary.json`
  - `Hard Blocked` = true send-stage blocks from `send_batch_summary.json`
- The **Manual Review Queue** panel now shows lightweight operator summary metrics (review item count, unique companies, unique review reasons) above the table to help triage campaigns without opening CSV files manually.
- Cloud send recovery path:
  - `load_ready_cloud_deploys()` no longer hides every `cloud_deploy_status=completed` run.
  - A completed deploy can reappear in **Ready To Deploy** when cloud send never advanced (`cloud_send_status` missing / `not_queued` / `queued`) for at least 30 minutes, or when `cloud_send_status=failed`.
  - These recovery rows are labeled with deploy statuses such as `stale_handoff_redeploy` or `send_failed_redeploy`, and the table now shows both `Cloud Send` and `Recovery Reason`.
  - Control-panel cloud deploy actions now call `deploy_runs(..., force=True)` so operators can re-queue a stale manifest without dropping to the CLI.
- Cloud worker re-deploy bug fix:
  - Root cause of `no_actionable_manifests` with visible manifests: `scripts/cloud_send_worker.py` previously skipped any manifest whose `campaign_id` was already present in the worker's in-memory `completed_campaigns` / `failed_campaigns` sets, even if the operator had re-deployed the same campaign and the fresh run-scoped `cloud_send_status.json` had been reset to `queued`.
  - The worker now relies on `_reconcile_manifest_with_run_state()` plus current run-scoped cloud-send status, instead of blindly short-circuiting on historical completed/failed memory.
  - Effect: a fresh re-deployed manifest for an existing `campaign_id` can re-enter candidate preparation and actually progress to `synced` / `waiting_window` / `sending`, instead of being stranded in `manifests/` while the worker reports `no_actionable_manifests`.
- Market/timezone fallback hardening for cloud send:
  - Root cause of `cloud_send_market=""` / `cloud_send_timezone="UTC"` on some queue-run campaigns: their `final_send_queue.csv` rows did not carry city/country fields, and some run folders lacked `campaign_run_state.json`, so `send_guard._resolve_location()` exhausted its old fallbacks and defaulted to UTC.
  - `src/workflow_7_email_sending/send_guard.py` now resolves market context in this order:
    1. row-level `city` / `country`
    2. row-level `source_location`
    3. run-scoped `data/runs/<campaign_id>/campaign_run_state.json`
    4. global `data/campaign_run_state.json`
    5. `data/campaign_queue.json` match by `campaign_id`
  - Effect: queue-generated cloud-send campaigns that only preserve `campaign_id` can still recover `location/country` and therefore use the correct country/city timezone mapping instead of silently falling back to `UTC`.
- `app.py` — `st.set_page_config()` + calls all render functions in layout order

**Compatibility:**
- Calls Workflow 9 (`run_campaign`, `resume_campaign`) — no business logic duplication
- All CSV pipeline outputs remain intact
- Compatible with SQLite layer (Workflow 0) and existing Streamlit dashboard (`src/dashboard/`)
- CLI (`scripts/run_campaign.py`) continues to work unchanged

---

### Workflow 9.6 — Streamlit Control Panel Enhancements

| | |
|---|---|
| **Location** | `src/workflow_9_5_streamlit_control_panel/` (same package as 9.5) |
| **New file** | `ui_actions.py` |
| **Updated files** | `ui_state.py`, `ui_views.py`, `app.py` |
| **Test** | `py scripts/test_control_panel.py` (rewritten — 14 sections) |

Six enhancements layered on top of Workflow 9.5; all changes are backwards-compatible.

**Enhancement 1 — Refreshable Log View**
- `render_logs_view()` now has a "Refresh" button that calls `refresh_dashboard_state()` + `st.rerun()`
- `refresh_dashboard_state()` (in `ui_actions.py`) calls `st.cache_data.clear()` so all loaders re-read from disk

**Enhancement 2 — KPI Dashboard with Rates**
- `load_pipeline_metrics()` extended with: `open_rate`, `click_rate` (floats, %), `blocked_count`
- `render_kpi_dashboard()` renders 2 rows of 5 metric tiles: companies, qualified leads, contacts, emails generated, sent / open rate%, click rate%, followup queued, blocked
- `open_rate = round(open_count / emails_sent * 100, 1)` — 0.0 when no emails sent

**Enhancement 3 — High-Priority Lead Filter**
- `load_high_priority_leads()` in `ui_state.py`: returns rows where `priority_flag=="high"` OR `lifecycle_status in ("clicked_no_reply", "followup_queued")`
- `render_high_priority_leads_view()` renders the filtered table with badge counts

**Enhancement 4 — Company Lifecycle Detail View**
- `get_company_detail(company_name)` in `ui_state.py`: looks up `campaign_status.csv` row + enriches with `generated_emails.csv` subject/body by `kp_email`
- `render_company_detail_view()`: company selector dropdown + 5 subsections (Identity, Context, Send State, Engagement, Follow-up, Latest Email)
- `load_company_names()` returns sorted unique names from `campaign_status.csv` for the selector

**Enhancement 5 — Manual Send followup_1 Action**
- `manual_send_followup_1(send_mode)` in `ui_actions.py`: loads `followup_queue.csv` filtered to `followup_stage=="followup_1"`, injects `EMAIL_SEND_MODE` env var, calls `send_guard.run_checks()` → `send_one()` → `build_log_row()` → `append_send_log()`; restores env on exit
- Returns `FollowupSendResult(attempted, sent, dry_run, blocked, errors, send_mode, messages)`
- `render_manual_followup_action()`: send mode selector + button; shows per-company SENT/DRY-RUN/BLOCKED/FAILED messages

**Enhancement 6 — Enhanced File Status**
- `KEY_FILES` (6 entries): the most operationally important files (state, logs, status, followup, send_logs, engagement)
- `load_enhanced_file_status()` returns `{file, exists, rows, size_kb, modified}` — `modified` is human-readable `YYYY-MM-DD HH:MM` via `_mtime()`
- `render_enhanced_file_status_view()`: table with rows, size, last-modified columns + Refresh button

**Additional dry-run safety:**
- `render_dry_run_explanation()`: expandable info block shown at top of page explaining dry-run vs live modes

**Module breakdown (updated):**
- `ui_config.py` — unchanged from 9.5
- `ui_runner.py` — unchanged from 9.5; now calls `refresh_dashboard_state()` after run/resume
- `ui_state.py` — extended: `load_pipeline_metrics()` (rates + blocked), `load_high_priority_leads()`, `get_company_detail()`, `load_company_names()`, `load_followup_queue()`, `load_followup_1_candidates()`, `load_enhanced_file_status()`; new helpers `_mtime()`, `_sum_col()`, `KEY_FILES`
- `ui_state.py` — also includes `_parse_dt()` + `load_delivery_ops_snapshot()` for cloud-send / operator memory metrics
- `ui_actions.py` — NEW: `FollowupSendResult`, `manual_send_followup_1()`, `refresh_dashboard_state()`, `get_high_priority_rows()`, `get_company_detail()`
- `ui_views.py` — extended: 6 new `render_*()` functions; refresh button in `render_logs_view()`; `render_metrics_view()` kept as alias for `render_kpi_dashboard()`; `render_kpi_dashboard()` now renders the `Send Ops Snapshot` above the existing KPI rows
- `app.py` — updated section order: header → dry_run_explanation → form → run_controls → kpi_dashboard → campaign_state → logs → high_priority_leads → company_detail → manual_followup → status_table → enhanced_file_status

---

### Stabilization Pass — Bug Fixes (post-9.6)

Applied to: `campaign_runner.py`, `ui_config.py`, `ui_state.py`, `ui_views.py`, `test_campaign_runner.py`, `test_control_panel.py`

**Fix 1 — Campaign State Synchronization (`campaign_runner.py`)**
- Runner now explicitly sets `status=running` immediately after initializing state (fresh run) or loading state (resume), so the UI reflects "running" during active execution.
- Removed the redundant post-loop double `update_campaign_state` call; replaced with a single clean finalization block that always writes `status=completed` and the correct `last_completed_step`.
- Per-step `update_campaign_state` calls remain: correctly sets `status=running` for intermediate steps and `status=completed` when `step_name == run_until`.
- Imported `save_campaign_state` and `STATUS_COMPLETED` at the module level.

**Fix 2 — Campaign Status Reading / Messages (`ui_views.py`)**
- `render_company_detail_view()`: explicitly checks if `campaign_status.csv` exists before loading company names. If missing, shows: *"Run the pipeline through the campaign_status step to populate lifecycle detail."*
- `render_high_priority_leads_view()`: if rows are empty, distinguishes between missing file and empty result.

**Fix 3 — KPI Metrics Normalization (`ui_state.py`)**
- `followup_queued` now counts directly from `followup_queue.csv` (previously counted from `campaign_status.csv` lifecycle_status column).
- Added three pipeline conversion rates: `qualification_rate` (qualified/companies), `contact_rate` (contacts/companies), `email_gen_rate` (emails_generated/contacts). All float, zero-safe.
- All metric sources documented in function docstring.

**Fix 4 — High Priority Leads Filter (`ui_state.py`)**
- Extended `load_high_priority_leads()` with two additional rules: `open_count >= 2` and `lead_score >= 70`. These ensure the view is useful even before sending begins (high-scoring leads visible as soon as enrichment completes). CSV string values parsed safely to int/float.

**Fix 5 — Company Detail View (`ui_views.py`)**
- Explicit existence check for `campaign_status.csv` with helpful message when missing.

**Fix 6 — Run Campaign UX (`ui_config.py`, `ui_views.py`)**
- `build_campaign_config()`: rejects empty or whitespace-only city with a clear error message. Country defaults to Canada if blank.
- `render_runner_controls()`: after run/resume, result is stored in `st.session_state` and `st.rerun()` is called immediately. This forces the entire page to re-render with fresh data before displaying the result. Result is displayed from session state on the next render pass.

**Fix 7 — Expanded File Status (`ui_state.py`)**
- `KEY_FILES` expanded from 6 to 16 entries covering the full pipeline in execution order: `search_tasks.json`, `raw_leads.csv`, `company_pages.json`, `company_text.json`, `company_analysis.json`, `qualified_leads.csv`, `enriched_leads.csv`, `generated_emails.csv`, `send_logs.csv`, `engagement_summary.csv`, `followup_queue.csv`, `campaign_status.csv`, `cloud_deploy_status.json`, `cloud_send_status.json`, `campaign_run_state.json`, `campaign_runner_logs.csv`.
- `TRACKED_FILES` aliased to `KEY_FILES` (they are now identical).
- `COMPANY_PAGES_FILE` added to imports in `ui_state.py`.

**Test updates:**
- `test_campaign_runner.py` — 10 sections (was 7): added §8 last_completed_step == run_until, §9 state transitions initialized→running→completed, §10 city validation in UI layer.
- `test_control_panel.py` — expanded §2 (city validation), §4 (3 new rate metric keys), §5 (7-row synthetic filter with all 5 priority rules), §7 (14-file assertion with name check).

---

### Smart Location & Metro Expansion (post-Stabilization)

Applied to: `location_data.py` (new), `campaign_config.py`, `campaign_steps.py`, `campaign_state.py`, `ui_state.py`, `ui_config.py`, `ui_views.py`, `test_campaign_runner.py`, `test_control_panel.py`

**Core motivation:** Google Maps returns ~60 results per query per city. Distributing keyword × city searches across multiple sub-cities in a metro area runs separate queries for each city, multiplying total lead recall for a single metro.

**New file: `src/workflow_9_5_streamlit_control_panel/location_data.py`**
- `LOCATION_HIERARCHY` dict: Country → Region/State → Base City → List[sub-cities]
- Covers ~80+ countries with full Region → City → Sub-city data. World coverage includes: US (18 metro areas), Canada (BC, ON, AB, QC, MB, SK), Mexico, UK, Germany, France, Italy, Spain, Netherlands, Belgium, Switzerland, Austria, Sweden, Norway, Denmark, Finland, Poland, Portugal, Czech Republic, Greece, Ireland, Hungary, Romania, Ukraine, Russia, China, Japan, India, South Korea, Australia, New Zealand, Singapore, Malaysia, Indonesia, Thailand, Vietnam, Philippines, Pakistan, Bangladesh, Kazakhstan, UAE, Saudi Arabia, Qatar, Kuwait, Bahrain, Oman, Israel, Jordan, Iran, Iraq, Egypt, South Africa, Nigeria, Kenya, Ethiopia, Tanzania, Ghana, Morocco, Tunisia, Algeria, Brazil, Argentina, Chile, Colombia, Peru, Venezuela, Ecuador, Bolivia, Uruguay, Paraguay + more.
- `CONTINENT_COUNTRIES` dict groups all 193 countries into 7 continents (Africa, Asia, Europe, Middle East, North America, Oceania, South America) for the UI Continent selector.
- `ALL_COUNTRIES` = sorted flat list of all 193 countries, derived from `CONTINENT_COUNTRIES`.
- Helper functions: `get_countries()` (returns `ALL_COUNTRIES`), `get_continents()`, `get_countries_by_continent(continent)`, `get_regions(country)`, `get_base_cities(country, region)`, `get_sub_cities(country, region, city)`, `get_all_cities_flat(country, region)`, `is_known_location(country, region, city)`

**`campaign_config.py` additions:**
- `METRO_MODES = ["base_only", "recommended", "custom"]`
- New `CampaignConfig` fields: `base_city` (primary/preferred), `metro_mode`, `metro_sub_cities`, `search_cities` (pre-computed final list)
- `city` kept as backward-compat alias
- `get_effective_search_cities(config)`: priority order — (1) pre-computed `search_cities`, (2) computed from `metro_mode` at runtime. Lazy-imports `location_data` for `recommended` mode.
- `_dedup_ordered()`: deduplicates list preserving insertion order

**`campaign_steps.py` — Step 1 multi-city expansion:**
- `run_step_1_search_tasks()` iterates `get_effective_search_cities(config)`
- Each task carries `base_city` (campaign root city) and `search_city` (specific sub-city for this query) for downstream analytics attribution
- Falls back to `search_task_builder.run()` when no cities configured (CLI / legacy mode)

**`campaign_state.py`:** `_config_to_dict` serializes all metro fields; `resume_campaign` restores them

**`ui_state.py` — `get_city_crawl_stats()`:**
- Returns `dict[city_name, {lead_count, status, last_updated}]`
- Reads from SQLite `companies.source_location` first, falls back to `raw_leads.csv`
- Infers `status` from `campaign_run_state.json`: `running`, `completed`, `partial`, `new`

**`ui_views.py` — `render_campaign_form()` rewrite:**
- Cascading selectboxes: **Continent → Country → Region → Base City** (driven by `CONTINENT_COUNTRIES` and `LOCATION_HIERARCHY`)
- "Enter location manually" checkbox: reverts to free-text inputs for unlisted locations
- City crawl status badge: shows lead count + status icon below the base city selector
- Metro Mode radio: `base only` / `recommended metro expansion` / `custom metro selection`
  - `recommended`: reads `get_sub_cities()` and displays the expansion list (read-only)
  - `custom`: multiselect of known sub-cities
- Search coverage summary: "Search will cover: Vancouver, Burnaby, Richmond (3 cities)"
- Returns `base_city`, `metro_mode`, `metro_sub_cities` in form values dict + `city` alias

**`ui_config.py` additions:**
- `METRO_MODE_OPTIONS`, `METRO_MODE_LABELS` exported for the view layer
- `build_campaign_config()` reads `base_city`/`metro_mode`/`metro_sub_cities`, computes `search_cities`, validates `metro_mode` in `METRO_MODES`
- Backward-compat: `base_city` falls back to `city` if not provided

**Test updates:**
- `test_campaign_runner.py` — 11 sections: §11 tests `base_only`/`recommended`/`custom` city expansion, pre-computed `search_cities` priority, multi-city task generation with `base_city`/`search_city` attribution.
- `test_control_panel.py` — 14 sections: §12 tests all `location_data` helpers (countries, regions, base cities, sub-cities, unknown returns, is_known_location), §13 tests `get_city_crawl_stats()` structure, §14 tests metro form round-trip (base_only/recommended/custom/backward-compat/invalid metro_mode).

---

### Runtime Hardening (post-Smart Location)

Applied to: `email_templates.py`, `quality_merge.py`, `repair_pipeline.py`, `campaign_runner.py`, `ui_views.py`

**Fix 1 — email_templates.py: lead_score empty-string guard**
- `build_email_body()` line: `_CTAS[record.get("lead_score", 0) % len(_CTAS)]`
- When `lead_score` is `""` (as set by `repair_pipeline` when merging CSV rows), `"" % 4` triggers Python's string `%` operator, raising `TypeError: not all arguments converted during string formatting`.
- Fix: `int(record.get("lead_score") or 0) % len(_CTAS)` — coerces `None`, `""`, and `0` to integer `0` before modulo.

**Fix 2 — quality_merge.py: column aliasing**
- `generated_emails.csv` uses column name `body` (not `email_body`) and `generation_source` (not `generation_mode`).
- Reduced `REQUIRED_COLUMNS` to only truly unrecoverable fields: `["company_name", "kp_email", "subject"]`.
- Added `_ALIASES` dict + `_get(row, key)` helper: tries all known aliases before falling back to default. Aliases: `email_body ↔ body`, `opening_line ↔ email_opening`, `generation_mode ↔ generation_source`.

**Fix 3 — repair_pipeline.py: per-record error isolation**
- Previously any unhandled exception in the repair loop crashed the entire `email_repair` step.
- Wrapped the full record processing block (rewrite → rescore) in a per-record `try/except`.
- On error: prints `[Workflow 6.7]   ERROR record N: ...` + full `traceback.format_exc()` output, then `continue`s to the next record.
- Pipeline no longer fails when individual records raise unexpected errors.

**Fix 4 — campaign_runner.py: traceback logging on step failure**
- `except Exception as exc:` block now also calls `traceback.format_exc()` and prints the full traceback via `print()`.
- Streamlit captures `print()` output, making tracebacks visible in the control panel log view.

**Fix 5 — ui_views.py: Streamlit deprecation cleanup**
- All `st.dataframe(..., use_container_width=True)` calls replaced with `st.dataframe(..., width="stretch")`.
- `st.button(..., use_container_width=True)` calls left unchanged (button API is different from dataframe API).

---

### Stabilization Pass 2 — Concurrency, Isolation & Reliability (post-Runtime Hardening)

Applied to: `settings.py`, `campaign_config.py`, `campaign_runner.py`, `ui_views.py`, `signal_loader.py`, `signal_pipeline.py`, `repair_pipeline.py`, `email_rewriter.py`

**Fix 1 — Campaign lock / anti-reentry (`campaign_runner.py`, `settings.py`, `ui_views.py`)**
- `CAMPAIGN_LOCK_FILE = DATA_DIR / "campaign_run.lock"` added to `settings.py`
- `is_campaign_running()` — returns `True` while lock file exists
- `_acquire_lock(campaign_id)` — writes lock file with campaign_id on start
- `_release_lock()` — removes lock file on completion or failure (in `finally` block)
- `run_campaign()` raises `RuntimeError` immediately if lock is already held
- `ui_views.render_runner_controls()` checks `is_campaign_running()` and:
  - Disables both "Run Campaign" and "Resume Campaign" buttons via `disabled=True`
  - Shows a warning banner explaining the lock with instructions to force-delete it

**Fix 2 — Fresh-run intermediate-file cleanup (`campaign_runner.py`)**
- On every non-resume campaign start, `_clear_intermediate_files()` deletes all intermediate pipeline files before the new run begins.
- Files cleared: `search_tasks.json`, `raw_leads.csv`, `company_pages.json`, `company_text.json`, `company_analysis.json`, `qualified_leads.csv`, `enriched_leads.csv`, `research_signal_raw.json`, `research_signals.json`, `company_openings.json`, `generated_emails.csv`, `scored_emails.csv`, `send_queue.csv`, `rejected_emails.csv`, `repaired_emails.csv`, `rescored_emails.csv`, `final_send_queue.csv`, `final_rejected_emails.csv`
- Files NOT cleared on resume: all of the above are preserved for resume continuity.
- Cleared file names are printed to the log for traceability.

**Fix 3 — Workflow 6.2 signal personalization name-matching (`signal_loader.py`, `signal_pipeline.py`)**
- Root cause: company names in `research_signals.json` and `enriched_leads.csv` often differed by punctuation, legal suffixes (`Inc.`, `Ltd.`, `Corp.`), or `&` vs `and`.
- `_normalize_name()` in `signal_loader.py` now applies:
  - lowercase + strip
  - replace `&` → `and`
  - strip common legal suffixes: `inc`, `ltd`, `corp`, `llc`, `l.l.c`, `co`, `limited`, `corporation`, `incorporated`, `plc`
  - remove all non-alphanumeric characters
  - collapse whitespace
- `signal_pipeline.py` re-uses `_normalize_name` from signal_loader (instead of a separate simpler function) so lookups are consistent between the two modules.

**Fix 4 — Streamlit Arrow serialization**
- Previously addressed in Runtime Hardening (rows column coercion).
- No new issues in this pass — all `st.dataframe()` calls already use `width="stretch"` and `.fillna("")`.

**Fix 5 — Workflow 6.7 input normalization + error CSV (`repair_pipeline.py`)**
- `_normalize_record(record)` normalizes all records before repair:
  - Numeric fields (`lead_score`, `overall_score`, `personalization_score`, `relevance_score`, `spam_risk_score`) coerced to `int` (empty string or None → 0)
  - Text fields (`company_name`, `kp_email`, `subject`, `review_notes`, `email_body`, etc.) coerced to `str`, stripped
- `EMAIL_REPAIR_ERRORS_FILE = DATA_DIR / "email_repair_errors.csv"` in settings.py
- `_log_repair_error(record, exc, tb, mode_attempted)` appends one row per failed record with: `company_name`, `kp_email`, `subject`, `original_status`, `original_score`, `exception_message`, `traceback_snippet`, `repair_mode_attempted`
- Error log is append-only; never blocks the pipeline
- Log summary line shows `"Errors logged: N"` and path to `email_repair_errors.csv`

**Fix 6 — Workflow 6.7 identifier logging (`email_rewriter.py`)**
- Rewrite error log now shows `kp_email` as primary identifier, falling back to `company_name`:
  - Before: `Rewrite error (openrouter) for :`
  - After: `Rewrite error (openrouter) for kathryn@vrec.ca: Expecting value: line 1 column 1`

**Updated default keywords (`campaign_config.py`)**
- Reverted to a broader, better-balanced set covering all valid B2B prospect types:
  ```python
  "solar installer", "solar contractor", "commercial solar installer",
  "solar developer", "solar energy company", "solar farm developer",
  "battery storage installer", "energy storage integrator"
  ```

---

### Integration Fix — Crawl → Text Extraction → Analyze dependency chain

Applied to: `campaign_steps.py`

**Root cause:**
`run_step_3_crawl` in `campaign_steps.py` called only `website_crawler.run()`, which saves `company_pages.json`. It never called `content_extractor.run()`, which reads `company_pages.json` and produces `company_text.json`. Step 4 (`analyze`) requires `company_text.json` as its input via `_require_file(COMPANY_TEXT_FILE, "analyze")`, so every fresh pipeline run failed at `analyze` with:

```
Required input file missing: data/company_text.json
```

**Fix:**
`run_step_3_crawl` now calls both sub-steps in sequence:
```python
crawl(limit=config.crawl_limit)   # → company_pages.json
extract_text()                     # → company_text.json  (consumed by Step 4)
```

**Artifact flow (canonical):**
```
raw_leads.csv
  → website_crawler.run()   → data/company_pages.json   (raw page records)
  → content_extractor.run() → data/company_text.json    (clean text, ≤5000 chars/company)
  → company_classifier.run()→ data/company_analysis.json
```

**Regression coverage:**
- `test_campaign_runner.py` §3 (crawl step) implicitly tests this — if `company_text.json` is not produced, §4 (analyze) would fail.
- No new test added; the existing step-sequencing test covers the dependency.

---

### End-to-End Pipeline Validation + Issues 8/9/10 (post-Stabilization Pass 2)

**Pipeline status:** Workflow 9 now runs end-to-end successfully from `search_tasks` through `campaign_status`. Confirmed working in Hamilton, Ontario and Ottawa, Ontario test runs.

---

#### Issue 8 — Places API INVALID_REQUEST during pagination (`google_maps_scraper.py`)

**Root cause:** `_text_search()` waited only 2 s before requesting page 2 via `pagetoken`. Google's Places Text Search API specifies that tokens "sometimes" are not immediately valid, and returns `INVALID_REQUEST` when the token isn't ready yet. The original code broke silently on this status with no retry and no context in the error message. The log showed only "Places API error: INVALID_REQUEST" — no query string, no distinction between initial vs. pagination failures.

Confirmed systemic across many Ottawa-area queries: high result density areas produce more pagination requests, making the timing issue more likely to trigger.

**Fix (Pass 1 — pagination retry):**
- Added `_PAGETOKEN_INITIAL_DELAY = 2` and `_PAGETOKEN_RETRY_DELAY = 4` constants.
- On `INVALID_REQUEST` during a `pagetoken` request: log a WARN, sleep 4 s, retry once. If still failing, log and stop pagination for that query (preserving results from page 1).
- On any non-OK status: log includes the query string, which page failed (initial vs. page N), and `error_message` from the Google response.
- Per-task result log now shows source: `→ N results [Places API (primary)]`.

**Fix (Pass 2 — diagnostics + API key guard):**
- `run()` now raises `RuntimeError` immediately if `GOOGLE_MAPS_API_KEY` is not set (fail fast rather than making 40+ silent API calls that all return `REQUEST_DENIED`).
- On `REQUEST_DENIED`: log adds hint `(check API key, billing, and Places API enable status)`.
- On `OVER_QUERY_LIMIT`: log adds hint `(daily quota exceeded — try again tomorrow or upgrade billing)`.

**Files changed:** `src/workflow_2_data_scraping/google_maps_scraper.py`

**Fallback behavior:** No scraper fallback exists — Places API is the only source. On failure, scraper logs the error and continues to the next task. Retry only applies to pagetoken timing errors.

**Regression coverage:** Log output now distinguishes: initial vs. pagination failures, INVALID_REQUEST (timing) vs. REQUEST_DENIED (key/billing) vs. OVER_QUERY_LIMIT (quota).

---

#### Issue 9 — Repair error summary showed `Errors logged: 0` despite visible rewrite errors (`repair_pipeline.py`, `email_rewriter.py`)

**Root cause:** `rewrite_email()` had its own internal `try/except`. When the OpenRouter call failed (e.g., empty JSON response), the exception was caught *inside* `rewrite_email()`, logged to stdout, and the function fell back to rule-based repair — returning a valid `(draft, "rule", "repair_fallback")` tuple. From `repair_pipeline.py`'s perspective, `rewrite_email()` *succeeded*. The outer `except Exception` block was never reached, so `error_count` stayed 0 and `_log_repair_error()` was never called.

**Fix:**
- `rewrite_email()` now returns a 4-tuple: `(draft, repair_mode, repair_source, ai_error)`. `ai_error` is `""` on success, the exception string when AI failed and fell back to rule.
- `repair_pipeline.py` unpacks the 4-tuple and checks `ai_error`:
  - If non-empty: increments `ai_rewrite_errors`, calls `_log_repair_error()`.
- Summary now shows two distinct counters:
  - `AI rewrite errors : N  (fell back to rule)` — AI called, failed, rule fallback used
  - `Record failures   : N  (record skipped entirely)` — outer try/except triggered
  - `Errors logged     : N  → email_repair_errors.csv` — total rows written
- Return dict includes `ai_rewrite_errors` and `errors_logged` keys.

**Files changed:** `src/workflow_6_7_email_repair/email_rewriter.py`, `src/workflow_6_7_email_repair/repair_pipeline.py`

**Regression coverage:** The rewrite error for `jax.y@sunvibesolar.ca` would now appear in `email_repair_errors.csv` and in the summary as `AI rewrite errors: 1`.

---

#### Issue 10 — Tracking/followup/campaign_status appeared to include historical data from prior runs

**Design decision: GLOBAL CRM VIEW — by design.**

These three steps (`tracking`, `followup`, `campaign_status`) read from append-only global files:
- `send_logs.csv` — all sends across all campaign runs
- `engagement_logs.csv` — all engagement events across all campaigns
- `followup_logs.csv` — all follow-up decisions across all campaigns

This is intentional. The system is a B2B sales CRM. The campaign_status view shows the full lifecycle of every contact ever touched, regardless of which campaign found them. This enables tracking follow-up sequences across campaigns.

**Send_logs filtering — verified correct:**
- `send_logs.csv` records ALL outcomes: `sent`, `dry_run`, `failed`, `blocked`, `deferred`.
- `status_loader.load_send_logs()` and `followup_selector._load_sent_logs()` both filter to `{"sent", "dry_run"}` only.
- `blocked` and `deferred` entries are logged for audit purposes but **never** propagate to follow-up candidates or campaign_status contacts.
- `dry_run` entries ARE included in follow-up candidates — correct by design. A dry_run contact was in-scope for sending; they should receive follow-up emails when the campaign eventually runs in smtp mode.

**Fix (labeling):**
- `run_step_7_5_tracking`, `run_step_8_followup`, `run_step_8_5_campaign_status` in `campaign_steps.py` print explicit global CRM scope notes.
- `status_pipeline.py` Loaded summary now shows `smtp_sent=N, dry_run=N` breakdown instead of just "sent contacts".
- `followup_selector.py` candidate count message now says "(global CRM — all campaigns)".

**Files changed:** `src/workflow_9_campaign_runner/campaign_steps.py`, `src/workflow_8_5_campaign_status/status_pipeline.py`, `src/workflow_8_followup/followup_selector.py`

**Operators should understand:**
- `campaign_status.csv` is a complete outreach history, not a per-campaign snapshot.
- If Hamilton contacts appear during an Ottawa run, that is correct behavior.
- To view per-campaign data, filter `send_logs.csv` by `source_location` in the raw logs.

---

### State Model Cleanup — Send Semantics + Campaign Scope (post-Ottawa validation)

Adopted a formal data semantics specification to resolve send_logs/campaign_status/followup confusion.

---

#### Root Causes

**Why send_logs grew even when Sent = 0:**
`send_pipeline.py` calls `append_send_log()` for EVERY record disposition: `sent`, `dry_run`, `failed`, `blocked`, `deferred`. This is CORRECT (attempt-level logging). The confusion arose because: (a) no `campaign_id` in rows made it impossible to distinguish which campaign produced each row, and (b) the label "email_sends" implied delivered-only.

**Why campaign_status showed historical contacts:**
`status_loader.load_send_logs()` read ALL rows in `send_logs.csv` across all campaigns. There was no `campaign_id` field in send_logs rows, so no campaign-scoped filter was possible. Every campaign_status run showed every contact ever emailed.

---

#### State Model — Canonical Definitions

```
CAMPAIGN-SCOPED VIEW
  Definition : only records with campaign_id == current run's campaign_id
  Used for   : campaign_status.csv, per-run reporting
  Rule       : must never contain contacts from other campaigns

GLOBAL CRM VIEW
  Definition : all records across all campaigns (append-only history)
  Used for   : Workflow 7.5 (engagement), Workflow 8 (followup)
  Rule       : must be explicitly labeled as global wherever it appears

SEND_LOGS SEMANTICS
  Stores     : ALL send attempts — sent, dry_run, failed, blocked, deferred
  NOT        : "successfully sent" only
  To count sent  : filter send_status == 'sent'
  To count sent+dry_run : filter send_status in ('sent', 'dry_run')
```

---

#### Changes Applied

**1. `send_logs.csv` schema extended**
Added two new fields to `LOG_FIELDS` in `send_logger.py`:
- `campaign_id` — stamped on every row from the current campaign run
- `send_mode` — "smtp" or "dry_run", stamped on every row

Every `build_log_row()` call in `send_pipeline.py` receives `campaign_id` and `send_mode`.

**2. `send_pipeline.run()` signature extended**
`run(limit, campaign_id, send_mode)` — caller provides both. Campaign runner passes them from the active state.

**3. `run_step_7_send()` reads campaign_id from state**
`campaign_steps.py` loads `campaign_id` from `campaign_run_state.json` and passes it to `send_pipeline.run()`.

**4. `campaign_status` is now campaign-scoped**
`status_loader.load_send_logs(path, campaign_id="")` — when `campaign_id` is non-empty, only rows with a matching `campaign_id` are included.
`status_loader.load_all(campaign_id="")` — passes filter through.
`status_pipeline.run(campaign_id="")` — accepts campaign_id, passes to `load_all()`, prints scope in header.
`run_step_8_5_campaign_status()` reads campaign_id from state and passes it.

**5. Workflow 8 (followup) remains global CRM**
Follow-up sequences span campaigns by design: a contact from Hamilton campaign is still eligible for follow-up during an Ottawa campaign. The followup_selector explicitly says "(global CRM — all campaigns)" in its output.

**6. Workflow 7.5 (tracking) remains global**
Engagement events (opens, clicks) are keyed by `tracking_id`. Cross-campaign aggregation is correct — there's no "polluting" historical data here, only real engagement events from actually-delivered emails.

**7. DB layer updated**
- `email_sends` table DDL: added `campaign_id TEXT`, `send_mode TEXT`, `send_decision TEXT`
- `db_utils.log_email_send()`: passes all three new fields
- `db_schema.migrate_schema()`: new function — applies ALTER TABLE to add new columns on existing databases (idempotent, silently ignores already-existing columns)
- `db_connection.get_db_connection()`: calls `migrate_schema()` automatically on every connection
- `csv_sync.sync_send_logs()`: now prints attempt-level status breakdown `(sent=N, dry_run=N, deferred=N, blocked=N)`

---

#### Migration Behavior

`send_logs.csv` rows written before this change have empty `campaign_id` and `send_mode` fields.
- `load_send_logs(campaign_id="abc123")` will **exclude** legacy rows (they have `campaign_id=""`)
- Legacy campaigns will not appear in any campaign-scoped `campaign_status` view
- This is correct — you can't retroactively know which campaign produced legacy rows
- Global view (`campaign_id=""`) still loads everything, legacy rows included

---

### Ottawa End-to-End Run + Issues 8-revisited / 11 (post-Hamilton validation)

Confirmed reproducible across Ottawa metro area (high-density query area with many sub-cities).

---

#### Issue 8 (revisited) — Systemic INVALID_REQUEST across many Ottawa queries

**Additional root cause identified:** Issue 11 (see below) was generating malformed location strings for cross-province sub-cities (e.g., `"solar installer Gatineau, Ontario, Canada"`). While this doesn't directly cause `INVALID_REQUEST` (Google would more likely return `ZERO_RESULTS`), it produces incorrect query attribution and wastes API quota. The Issue 11 fix removes this class of bad queries.

**See Issue 8 fix above for diagnostics improvements applied in this pass.**

---

#### Issue 11 — City/province normalization bug: Gatineau tagged as Ontario

**Root cause — two separate bugs:**

1. **Data error in `location_data.py`:** Gatineau was listed as a sub-city of Ottawa under Ontario:
   ```python
   "Ontario": {"Ottawa": ["Kanata", "Nepean", "Orleans", "Gloucester", "Gatineau"]}
   ```
   Gatineau is in Quebec, not Ontario. The Ottawa-Gatineau National Capital Region spans both provinces.

2. **Structural bug in `campaign_steps.py` `run_step_1_search_tasks()`:** The location string builder blindly applied `config.region` (the base city's province) to ALL search cities:
   ```python
   parts = [search_city]
   if config.region:
       parts.append(config.region)  # WRONG: appended "Ontario" to every city including Gatineau
   ```
   Even if the data were correct, this would still fail for any sub-city that crosses provincial/state boundaries.

**Fix:**

1. `location_data.py`:
   - Removed Gatineau from `Ontario / Ottawa` sub-cities.
   - Added Gatineau as a base city under `Quebec` with its own sub-city list: `["Aylmer", "Hull", "Buckingham", "Cantley"]`.

2. `campaign_steps.py`:
   - Added `_get_city_region(country, city)` helper: reverse-looks up any city in `LOCATION_HIERARCHY` to find its correct region; returns `None` for unknown cities.
   - `run_step_1_search_tasks()` now calls `_get_city_region()` per search city and falls back to `config.region` only when the city is not in location_data.

**Result:**
- Ottawa metro: `Ottawa→Ontario`, `Kanata→Ontario`, `Nepean→Ontario`, `Orleans→Ontario`, `Gloucester→Ontario` — correct.
- Gatineau: no longer included in Ottawa's sub-city list. If a user adds Gatineau as a custom city or runs a Gatineau campaign, it will be tagged as `Quebec`.
- Cross-province metros now supported: any sub-city in a different province/state than the base city will use its own correct region.

**Files changed:** `src/workflow_9_5_streamlit_control_panel/location_data.py`, `src/workflow_9_campaign_runner/campaign_steps.py`

**Regression coverage:**
- Ottawa + recommended sub-cities: no Gatineau in the list (removed from data).
- Custom city "Gatineau, Canada" reverse-lookup → "Quebec" → query = "solar installer Gatineau, Quebec, Canada" (correct).
- Unknown custom cities fall back to `config.region` (no regression for existing behavior).

---

### Run-Scoped Architecture Refactor (post-Calgary validation)

Every campaign run now gets its own isolated file directory. Global CRM files that span all campaigns have a dedicated shared directory. The path-proxy pattern requires zero changes to individual workflow files.

---

#### Root cause that triggered the refactor

Calgary run (campaign `0bfe69bd`) confirmed that without run isolation:
- Every campaign overwrote the same `data/raw_leads.csv`, `data/generated_emails.csv`, etc.
- `campaign_status.csv` merged ALL historical contacts across all runs (because `send_logs.csv` had no `campaign_id` column in older code)
- No archival of prior run artifacts — second campaign destroyed first campaign's data

---

#### Directory layout

```
data/
  solar_leads.db                        ← process-level, shared across all runs
  campaign_run_state.json               ← process-level, points to last/active run
  campaign_run.lock                     ← process-level semaphore
  runs/
    <campaign_id>/                      ← all pipeline artifacts for ONE run
      search_tasks.json
      raw_leads.csv
      company_pages.json  company_text.json
      company_analysis.json  buyer_filter.json
      qualified_leads.csv  disqualified_leads.csv
      enriched_leads.csv  enriched_contacts.csv
      scored_contacts.csv                       ← P1-2B contact scoring
      verified_enriched_leads.csv               ← Ticket 3 verification
      research_signal_raw.json  research_signals.json
      queue_policy.csv                          ← P1-3A queue policy
      company_openings.json  company_signals.json
      generated_emails.csv  scored_emails.csv
      send_queue.csv  final_send_queue.csv
      repaired_emails.csv  rescored_emails.csv  rejected_emails.csv
      send_batch_summary.json  engagement_summary.csv
      followup_candidates.csv  followup_queue.csv  followup_blocked.csv
      campaign_status.csv  campaign_status_summary.json
      campaign_runner_logs.csv  email_repair_errors.csv
  crm/
    send_logs.csv           ← global, append-only, spans all campaigns
    engagement_logs.csv     ← global, append-only
    followup_logs.csv       ← global, append-only
    crm_database.csv        ← global CRM export
```

---

#### Path proxy implementation

**`config/run_context.py`** (new) — module-global run context:
- `set_active_run(campaign_id)` — called by `campaign_runner.run_campaign()` after campaign_id is known
- `clear_active_run()` — called in the `finally` block after each run
- `get_active_campaign_id()` — read by `_RunPath._resolve()` at file-access time

**`config/settings.py`** — two proxy classes replace all `DATA_DIR / "file"` constants:

```python
class _RunPath:
    # Resolves to  data/runs/<active_campaign_id>/<filename>  when a run is active
    # Falls back to  data/<filename>  when called outside a run (backward compat)

class _CrmPath:
    # Always resolves to  data/crm/<filename>  regardless of run context
```

Both classes implement the full Path-like protocol (`__fspath__`, `__str__`, `__getattr__` delegation, `__truediv__`) so all existing workflow code (`open(path, ...)`, `path.exists()`, `path.parent.mkdir()`, etc.) works without modification.

**Path constant classification:**

| Category | Class | Examples |
|---|---|---|
| Campaign-scoped pipeline artifacts | `_RunPath` | `RAW_LEADS_FILE`, `GENERATED_EMAILS_FILE`, `CAMPAIGN_STATUS_FILE`, `SEND_BATCH_SUMMARY`, `FINAL_SEND_QUEUE_FILE`, all 26 others |
| Global CRM (cross-campaign) | `_CrmPath` | `SEND_LOGS_FILE`, `ENGAGEMENT_LOGS_FILE`, `FOLLOWUP_LOGS_FILE`, `CRM_DATABASE_FILE` |
| Process-level (fixed) | real `Path` | `DATABASE_FILE`, `CAMPAIGN_LOCK_FILE`, `CAMPAIGN_RUN_STATE_FILE` |

New module-level constants in `settings.py`:
- `RUNS_DIR = DATA_DIR / "runs"` — parent of all run directories
- `CRM_DIR  = DATA_DIR / "crm"`  — parent of all CRM files

---

#### `ui_state.py` display context fix

After `campaign_runner.clear_active_run()` fires, `_RunPath` proxies fall back to `DATA_DIR`. Without an explicit fix, the Streamlit dashboard becomes blind to run outputs after a campaign completes.

**Fix:** `_activate_display_context()` in `ui_state.py` reads `campaign_run_state.json` (process-level path, always accessible) and calls `set_active_run(campaign_id)` so the dashboard reads from the last completed campaign's directory. It is a no-op if a run is actively in progress. Called at the start of every `load_*` and `get_*` function in `ui_state.py`.

---

#### Data migration for existing runs

Any campaign run before the refactor wrote artifacts to `data/` root. Run the migration script once to move them into the new layout:

```bash
py scripts/migrate_legacy_data.py --dry-run   # preview
py scripts/migrate_legacy_data.py             # apply
```

The script reads `campaign_run_state.json` to determine the last campaign_id and moves:
- `data/send_logs.csv` → `data/crm/send_logs.csv` (and engagement/followup logs)
- All pipeline artifacts → `data/runs/<campaign_id>/`

---

#### Regression tests

`tests/test_run_scoped_paths.py` — 39 tests, 8 classes:
- `TestProxyTypes` — all 26 campaign constants are `_RunPath`; 4 CRM constants are `_CrmPath`; 3 process-level are real `Path`
- `TestRunPathProtocol` — `open()`, `csv.DictReader`, `exists()`, `parent.mkdir()`, `unlink()`, `name`, `suffix`, `__fspath__`, `__str__`, `__truediv__`, fallback-to-DATA_DIR
- `TestCrmPathProtocol` — always resolves to `data/crm/`, unaffected by active run
- `TestCampaignIsolation` — campaign A artifacts invisible to campaign B and vice versa
- `TestResumeTargetsCorrectRun` — set_active_run("old-id") immediately redirects all constants
- `TestCrmIsolation` — CRM paths never contain "runs/"; stable across run switches
- `TestNoLegacyDataRootWrites` — no campaign artifact resolves to `DATA_DIR` parent when run is active
- `TestRunContextStateMachine` — set/clear/override lifecycle

Run: `py -m pytest tests/test_run_scoped_paths.py -v`

---

#### Calgary run diagnosis (why legacy paths appeared in live run)

Calgary campaign `0bfe69bd` (2026-03-16) ran with the **old code**. Evidence:
- `data/send_logs.csv` has 58 rows with no `campaign_id` column (old schema)
- All artifacts are in `data/` root; `data/runs/` has only unit-test directories
- `data/crm/` was empty; `send_logs.csv` was at legacy path

**Root cause:** Streamlit caches imported modules in `sys.modules`. Editing `.py` files on disk while Streamlit is running has no effect — the old module objects remain in memory until the process is restarted. The refactored code only takes effect after a full `Ctrl+C → streamlit run` restart.

**Why campaign_status showed 7 historical contacts (Sent=0, dry_run mode):** Old `send_pipeline` wrote no `campaign_id` column → old `status_pipeline` passed `campaign_id=""` → `load_send_logs` filter branch never entered → all 7 `send_status="dry_run"` rows from any prior run appeared in the report.

**Ongoing:** 12 rows in Calgary's `send_logs.csv` have a malformed `send_status` value (an entire dict serialised as a string: `"{'send_status': 'dry_run', ...}"`). These come from a bug in the old send logger. They are benign — `status_loader` excludes them because they don't match `"sent"` or `"dry_run"`.

---

## DATA FLOW

```
[Workflow 9 — Campaign Runner orchestrates all steps below]
[All campaign artifacts write to data/runs/<campaign_id>/]
[Global CRM files write to data/crm/]

data/runs/<campaign_id>/search_tasks.json
        →  (Workflow 2 — Google Maps Scraping)
data/runs/<campaign_id>/raw_leads.csv
        →  (Workflow 3 — Website Crawling)
data/runs/<campaign_id>/company_pages.json
        →  (Workflow 3 — Content Extraction)
data/runs/<campaign_id>/company_text.json
        →  (Workflow 4 — AI Company Analysis)
data/runs/<campaign_id>/company_analysis.json
        →  (Workflow 4.5 — Buyer Filter / Value Chain Classification)
data/runs/<campaign_id>/buyer_filter.json
        →  (Workflow 5 — Lead Scoring)
data/runs/<campaign_id>/qualified_leads.csv
        →  (Workflow 5.5 — Lead Enrichment)
data/runs/<campaign_id>/enriched_leads.csv        ← primary contact per company (backward-compat)
data/runs/<campaign_id>/enriched_contacts.csv     ← all contacts, up to 3 per company (P1-2A)
        →  (Workflow 5.6 — Contact Scoring P1-2B)
data/runs/<campaign_id>/scored_contacts.csv       ← one row per contact, ranked + primary flag
        →  (Workflow 5.9 — Email Verification / Ticket 3)
data/runs/<campaign_id>/verified_enriched_leads.csv  ← send_eligibility per email
        →  (Workflow 5.8 — Company Signal Research)
data/runs/<campaign_id>/research_signal_raw.json
        →  (Workflow 5.8 — Signal Summarizer)
data/runs/<campaign_id>/research_signals.json
        →  (Workflow 6 — Queue Policy Enforcement P1-3A)
data/runs/<campaign_id>/queue_policy.csv          ← one row per company: policy action for primary contact
        →  (Workflow 6.2 — Signal-based Personalization)
data/runs/<campaign_id>/company_openings.json
        →  (Workflow 6 — Email Generation)
data/runs/<campaign_id>/generated_emails.csv
        →  (Workflow 6.5 — Email Quality Scoring)
data/runs/<campaign_id>/scored_emails.csv
data/runs/<campaign_id>/send_queue.csv
        →  (Workflow 6.7 — Email Repair Loop)
data/runs/<campaign_id>/repaired_emails.csv
        →  (Workflow 6.7 — Rescore)
data/runs/<campaign_id>/rescored_emails.csv
data/runs/<campaign_id>/final_send_queue.csv
        →  (Workflow 7 — Email Sending)
data/crm/send_logs.csv                ← GLOBAL CRM, append-only, stamped with campaign_id
data/runs/<campaign_id>/send_batch_summary.json
        →  (Workflow 7.5 — Open / Click Tracking)
data/crm/engagement_logs.csv          ← GLOBAL CRM, append-only
data/runs/<campaign_id>/engagement_summary.csv
        →  (Workflow 8 — Follow-up Automation)
data/runs/<campaign_id>/followup_candidates.csv
data/runs/<campaign_id>/followup_queue.csv
data/runs/<campaign_id>/followup_blocked.csv
data/crm/followup_logs.csv            ← GLOBAL CRM, append-only
        →  (Workflow 8.5 — Campaign Status Aggregator, CAMPAIGN-SCOPED)
data/runs/<campaign_id>/campaign_status.csv
data/runs/<campaign_id>/campaign_status_summary.json
        →  (Workflow 9 — Campaign Runner state, PROCESS-LEVEL)
data/campaign_run_state.json
data/solar_leads.db
```

## KEY FILES

| File | Purpose |
|---|---|
| `data/solar_leads.db` | SQLite database — central data store (Workflow 0) |
| `data/campaign_run_state.json` | Live campaign run state — resume target (Workflow 9) |
| `data/campaign_run.lock` | Campaign lock file — exists only while a run is active; delete to force-unlock |
| `data/runs/<campaign_id>/` | All per-run pipeline artifacts (`_RunPath` constants) |
| `data/crm/` | Global CRM files shared across all campaigns (`_CrmPath` constants) |
| `config/run_context.py` | Module-global run context — `set_active_run()`, `clear_active_run()`, `get_active_campaign_id()` |
| `config/run_paths.py` | `RunPaths` frozen dataclass — explicit, campaign-scoped `Path` objects passed directly to pipeline steps; avoids `_RunPath` proxy for first-batch workflows |
| `config/settings.py` | All file-path constants (`_RunPath`, `_CrmPath`, real `Path`), API keys, pipeline tuning |
| `scripts/run_campaign.py` | Main CLI entry point for running campaigns (Workflow 9) |
| `scripts/migrate_legacy_data.py` | One-time migration: moves legacy `data/` root files into run-scoped layout |
| `src/workflow_9_5_streamlit_control_panel/app.py` | Streamlit campaign control panel (Workflow 9.5) |
| `src/workflow_9_5_streamlit_control_panel/location_data.py` | Location hierarchy (countries → regions → cities → sub-cities) for metro expansion |
| `tests/test_run_scoped_paths.py` | 39 regression tests covering `_RunPath`/`_CrmPath` protocol, isolation, and state machine |
| `scripts/run_control_panel.py` | Convenience launcher for the control panel |
| `main.py` | CLI entry point — runs full pipeline or individual workflows |
| `config/prompts/` | AI prompt templates for company research, scoring, email generation |
| `src/pipeline/orchestrator.py` | Chains all 8 workflows; supports `--workflow N` and `--from-stage N` |
| `src/database/models.py` | Dataclass definitions for all entities |
| `src/database/db_manager.py` | Generic CSV/JSON read-write helpers |
| `.env` | Secret keys — never commit to git |

---

## DEVELOPMENT RULES

1. **Do not break existing workflows** — each workflow must remain independently runnable.
2. **Maintain clear pipeline stages** — one workflow's output is the next workflow's input.
3. **All pipeline outputs live under `data/runs/<campaign_id>/`** — never write campaign artifacts directly to `data/` root.
4. **Use `_RunPath` for campaign-scoped files, `_CrmPath` for global CRM files** — declare all new pipeline file constants in `config/settings.py`; zero per-workflow path logic.
5. **`campaign_runner.py` must always set/clear run context** — call `run_context.set_active_run(campaign_id)` before steps and `run_context.clear_active_run()` in the `finally` block.
6. **All workflows must be independently testable** — test scripts go in `scripts/`; regression tests for path layer go in `tests/test_run_scoped_paths.py`.
7. **Minimise API costs** — e.g. skip Place Details calls when data already exists in Text Search response.
8. **Deduplication must use `place_id`** when available (stable Google identifier); fall back to normalised URL only when `place_id` is absent.
9. **Fail gracefully** — log errors and continue processing remaining records; never crash the full pipeline on a single bad record.
10. **No hardcoded secrets** — all keys and credentials come from `.env` via `config/settings.py`.

---

## RUNNING THE SYSTEM

```bash
# Full pipeline
py main.py --all

# Single workflow
py main.py --workflow 2

# Resume from a specific stage
py main.py --from-stage 4

# Initialise database (Workflow 0)
py scripts/init_database.py           # create tables only
py scripts/init_database.py --sync    # create tables + import CSVs

# Launch dashboard
streamlit run src/dashboard/dashboard.py

# Smoke tests (run in order)
py scripts/test_workflow2_scraper.py
py scripts/test_data_cleaner.py
py scripts/test_workflow3_crawler.py
py scripts/test_company_classifier.py
py scripts/test_lead_scoring.py
py scripts/test_lead_enrichment.py
py scripts/test_enrichment_p1_2a.py
py scripts/test_signal_research.py
py scripts/test_signal_personalization.py
py scripts/test_email_generation.py
py scripts/test_email_quality.py
py scripts/test_email_repair.py
py scripts/test_email_sending.py
py scripts/test_engagement_tracking.py
py scripts/test_followup_workflow.py
py scripts/test_campaign_status.py
py scripts/test_campaign_runner.py
py scripts/test_control_panel.py

# Run a campaign (Workflow 9)
py scripts/run_campaign.py --city Vancouver --country Canada
py scripts/run_campaign.py --city Vancouver --run-until email_generation
py scripts/run_campaign.py --resume

# Migrate legacy data to run-scoped layout (run ONCE after upgrading)
py scripts/migrate_legacy_data.py --dry-run   # preview only
py scripts/migrate_legacy_data.py             # apply migration

# Run-scoped path regression tests
pytest tests/test_run_scoped_paths.py -v
```

---

### Outbound Send Layer — Gmail API + Production Hardening (2026-03-18)

#### Send mode: Gmail API as sole production transport

**Problem:** GFW blocks all outbound SMTP (ports 25, 465, 587) on this network. SMTP is unusable for production sends.

**Solution:** Gmail API over HTTPS (port 443) using OAuth2. Token is cached locally; no browser prompt on subsequent sends.

**`email_sender.py`** — updated to support three modes:
- `EMAIL_SEND_MODE=gmail_api` — **production default**. Calls `service.users().messages().send()` via Google API client.
- `EMAIL_SEND_MODE=smtp` — available for non-GFW environments; not a fallback.
- `EMAIL_SEND_MODE=dry_run` — simulation; no network call.

**New functions in `email_sender.py`:**
- `_get_gmail_service()` — loads `config/gmail_token.json`, refreshes token if expired via `google.auth.transport.requests.Request`, returns authenticated `gmail` service object. Raises `RuntimeError` if token is missing or cannot be refreshed.
- `_send_gmail_api(record)` — builds MIME message, base64url-encodes it, calls Gmail API. Returns `{send_status, provider, provider_message_id, error_message}`. On failure, returns `status=failed` — **no SMTP fallback**.

**`config/settings.py`** additions:
- `GMAIL_CLIENT_SECRET_FILE = config/gmail_client_secret.json`
- `GMAIL_TOKEN_FILE = config/gmail_token.json`
- `EMAIL_SEND_MODE` comment updated to include `gmail_api`

**`.env`:** `EMAIL_SEND_MODE=gmail_api` set as production default.

**New scripts:**
- `scripts/authorize_gmail.py` — one-time OAuth2 authorization. Run when token is missing or expired. Opens browser, saves token to `config/gmail_token.json`.
- `scripts/test_gmail_send.py` — one-shot smoke test. Sends to `yangzuwei@gmail.com` via Gmail API. Separate from campaign sender. **Confirmed working 2026-03-18 — message ID `19cff2dadb400b39`.**

**`send_pipeline.py`** — unchanged. Already calls `send_one()` which routes through `_send_gmail_api()` when mode is `gmail_api`.

**Credential security:**
- `.gitignore` created at project root.
- `config/gmail_client_secret.json` and `config/gmail_token.json` are excluded from git.
- `config/*.json` excluded broadly.
- `data/` and `.env` also excluded.

**OAuth2 credential files:**
- `config/gmail_client_secret.json` — GCP project `gen-lang-client-0754517612` (project number `503580949036`), installed-app OAuth2 client
- `config/gmail_token.json` — cached OAuth2 token, scope: `https://www.googleapis.com/auth/gmail.send`. Auto-refreshed by `_get_gmail_service()`.

---

### Email Quality — Send-tier Stratification + Email Angle Binding (2026-03-18)

These changes were made alongside the Dubai validation run `ffd9f0f7`.

#### `target_tier` added to scorer (`lead_scorer.py`)

`_target_tier(company_type, confidence, method) → "A" / "B" / "C"`:
- **A**: core types (solar installer, solar EPC, solar contractor, solar panel installer) + confidence ≥ 0.65
- **B**: secondary types (solar developer, solar energy company, BESS integrator, etc.) + AI conf ≥ 0.65; OR core types with lower confidence; OR distributor with AI conf ≥ 0.75
- **C**: rules-only classification, low confidence, or distributor without high-confidence AI

All distributors stay in pool — no hard filter. Low-confidence distributors go Tier C (deprioritized, not removed).

`target_tier` and `classification_method` added to `QUALIFIED_FIELDS`.

#### `send_tier` stratification in email merge (`email_merge.py`)

`_send_tier(target_tier, enrichment_source, kp_email) → "A" / "B1" / "B2" / "C"`:
- **A**: target_tier A + named contact (apollo/hunter)
- **B1**: target_tier A + website contact OR target_tier B + named contact
- **B2**: target_tier A + guessed, target_tier B + website, target_tier C + named
- **C**: no valid email, or guessed email on weak type

`send_tier` included in merged record and written to `generated_emails.csv`.

#### 7-angle email system (`email_merge.py` + `email_generator.py`)

`_derive_email_angle()` produces 7 specific angles:
- `project_delivery` — EPC / contractor
- `installation` — solar panel installer / installer
- `storage_integration` — BESS / battery storage
- `distributor_supply` — solar component distributor
- `project_pipeline` — solar developer / farm developer
- `general_solar` — broad solar energy company
- `cautious_outreach` — always for Tier C (no specific personalization)

`_SYSTEM` prompt in `email_generator.py` updated with body guidance per angle and send_tier hints (A/B1 may personalize; B2 light; C conservative).

#### Tier A no-signal path improvement (`email_generator.py`)

For Tier A/B1 leads with no signal-derived opening line, `services_detected` (scraped from company website) is used to enrich the `type_hint` before building the opening instruction. Up to 2 specific services are injected; generic terms ("solar", "energy", "services") are skipped.

---

### Email Personalization — Calculator Tool Integration (2026-03-18)

**Tool:** OmniSol Global Designer at `https://omnisolglobal.com/calculator`
**What it does:** Takes site specs (wind speed, snow load, roof type, pitch, array rows/columns) and outputs structural design specs (max support spacing, cantilever, exclusion zone) plus a full BOM with OmniSol SKUs and quantities.

#### First-touch cold email rules (`email_generator.py` — `_SYSTEM` prompt)

Calculator mention added as secondary signal — **not** primary pitch:
- **Allowed**: `project_delivery`, `installation` angles. One soft closing sentence if email has room: `"We also built a simple mounting sizing tool for early-stage project checks — happy to share it if useful."`
- **Distributor_supply**: only if it fits naturally; omit when in doubt.
- **storage_integration**, **cautious_outreach**: never mention.
- **No URL in first-touch emails.**
- Words "free", "instant", "automated BOM" are prohibited.
- Calculator mention must be secondary to main CTA.

#### Follow-up email rules (`followup_generator.py`)

**New constant:** `_CALCULATOR_BASE_URL = "https://omnisolglobal.com/calculator?utm_source=email&utm_medium=followup&utm_campaign=cold_outreach"`

**`_CALCULATOR_ANGLES = {"project_delivery", "installation"}`** — only these angles get any calculator mention.

**`_calculator_url_for(stage, engagement, email_angle) → str`:**
- Returns UTM URL only when `stage=followup_2` + `engagement=clicked_no_reply` + angle in `_CALCULATOR_ANGLES`
- Returns `""` in all other cases

**`_build_prompt()` additions:** passes `email_angle` and `calculator_url` into the prompt template.

**Prompt rules per stage:**
- `followup_1`: soft sentence only, no URL: `"We also have a simple mounting sizing tool — happy to share it if useful."`
- `followup_2` + `clicked_no_reply` + relevant angle: UTM URL included as concrete next step
- `followup_2` + other engagement: soft sentence only
- `followup_3`: no calculator mention

**`_build_fallback()`** mirrors the same logic for the deterministic (no-AI) fallback path.

**Rule 10 (inbound via calculator):** Any lead who submits "Request Official Quote & Layout" via the calculator should be treated as a warm inbound lead and routed into Workflow 8 follow-up sequence instead of cold outreach. Implementation pending — depends on form submission webhook from `omnisolglobal.com`.

---

### Ticket 4 — Deliverability Breakers (2026-03-19)

**Package:** `src/workflow_7_4_deliverability/`

#### New DB tables

**`sender_health`** — per-sender identity, health metrics, and sender-scope breaker state:
- Identity: `sender_email` (UNIQUE), `sending_domain`, `provider`, `active`
- Metrics (rates 0.0–1.0): `hard_bounce_rate`, `invalid_rate`, `provider_send_failure_rate`, `unsubscribe_rate`, `spam_rate`
- Health metadata: `last_health_updated_at`, `health_source`, `health_note`
- Breaker state: `sender_breaker_active` (0/1), `sender_breaker_reason`

**`campaign_breakers`** — domain/campaign/global-scope breaker state:
- `scope`: `'domain'` | `'campaign'` | `'global'`
- `scope_key`: sending_domain | campaign_id | `'global'`
- `breaker_active` (0/1), `breaker_reason`, `activated_at`
- UNIQUE(scope, scope_key)

#### New modules

**`sender_health.py`** — `SenderHealth` dataclass (all identity + metric + sender-breaker fields)

**`breaker_rules.py`** — `evaluate_sender_health(health) → list[tuple[str, str]]`:
- Returns (scope, reason_code) pairs for every breached threshold
- Rules: hard_bounce > 3% → sender; invalid_rate > 2% → campaign; provider_failure > 5% → sender; unsubscribe > 0.5% → sender+campaign; spam > 0.3% → domain (critical); spam > 0.1% → domain (warning)
- Reason code constants: `REASON_HARD_BOUNCE_EXCEEDED`, `REASON_INVALID_RATE_EXCEEDED`, etc.

**`breaker_state.py`** — four-scope query + update functions:
- `get_sender_breaker(conn, sender_email) → (bool, str)`
- `set_sender_breaker(conn, sender_email, active, reason, sending_domain="")`
- `get_domain_breaker(conn, sending_domain) → (bool, str)`
- `set_domain_breaker(conn, sending_domain, active, reason)`
- `get_campaign_breaker(conn, campaign_id) → (bool, str)`
- `set_campaign_breaker(conn, campaign_id, active, reason)`
- `get_global_breaker(conn) → (bool, str)`
- `set_global_breaker(conn, active, reason)`

#### Thresholds (config/settings.py)

```
BREAKER_HARD_BOUNCE_RATE      = 0.03  # 3%   → sender breaker
BREAKER_INVALID_RATE          = 0.02  # 2%   → campaign breaker
BREAKER_PROVIDER_FAILURE_RATE = 0.05  # 5%   → sender breaker
BREAKER_UNSUBSCRIBE_RATE      = 0.005 # 0.5% → sender + campaign breaker
BREAKER_SPAM_RATE_WARNING     = 0.001 # 0.1% → domain breaker (warning)
BREAKER_SPAM_RATE_CRITICAL    = 0.003 # 0.3% → domain breaker (critical)
```
All overridable via env vars with matching names.

#### Guard chain (send_guard.py)

`run_checks()` new signature: `(record, recent_logs, now, send_mode, conn=None, campaign_id="")`

New check order:
1. required_fields
2. email_format
3. approval_status
4. **email_eligibility** — blocks if `send_eligibility == "block"` (Ticket 3 E0); reason: `blocked_e0_email`
5. **global_breaker** — reason: `blocked_global_breaker`
6. **domain_breaker** — reads SMTP_FROM_EMAIL domain; reason: `blocked_domain_breaker`
7. **sender_breaker** — reads SMTP_FROM_EMAIL; reason: `blocked_sender_breaker`
8. **campaign_breaker** — uses campaign_id param; reason: `blocked_campaign_breaker`
9. [business_hours — skipped in dry_run]
10. duplicate
11. company_throttle

All breaker checks are **non-fatal when conn=None** (pass through silently). DB errors inside checks also pass through.

`is_breaker_block(reason: str) → bool` — returns True for any breaker/E0 reason prefix.

#### Pipeline (send_pipeline.py)

- `run()` opens best-effort DB connection, passes `conn` + `campaign_id` to `run_checks()`
- New counter: `breaker_blocked` — subset of `blocked`, counts deliverability-breaker + E0 blocks
- Batch summary print + JSON include `breaker_blocked`
- `_empty_summary()` includes `breaker_blocked: 0`
- Additional anti-burst pacing has now been added to Workflow 7 real sending:
  - `SEND_HOURLY_LIMIT` (default `20`) caps real sends per rolling hour; when the cap is reached, `send_pipeline.run()` stops the current batch early and records `stopped_hourly_limit=1` in the batch summary
  - `_count_hourly_send_slots()` counts recent successful sends from `send_logs.csv` (`sent` for real modes, `dry_run` for preview mode)
  - The intent is to avoid large same-hour bursts that can trigger provider throttling, temporary deferrals, or reputation damage

#### Real-send pacing (email_sender.py / config.settings)

- Real sending is no longer a rigid fixed-cadence loop.
- New env-configurable pacing knobs:
  - `SEND_PACING_MIN_SECONDS` (default `45`)
  - `SEND_PACING_MAX_SECONDS` (default `180`)
  - `SEND_HOURLY_LIMIT` (default `20`)
- `send_one()` now applies `_apply_real_send_pacing()` for `smtp` and `gmail_api` before each real send.
- `_target_real_send_gap()` uses the larger of:
  - the configured pacing minimum
  - the average gap implied by the hourly cap (`3600 / SEND_HOURLY_LIMIT`)
  - Gmail's provider floor (`GMAIL_API_MIN_SEND_INTERVAL_SECONDS`) for Gmail API mode
- The final gap is randomized between min/max so sends land on a human-like irregular cadence instead of an obviously robotic fixed interval.
- Successful SMTP and Gmail API sends call `_mark_real_send_completed()` so the next send respects the measured real-send gap.
- Existing protections remain in place on top of this pacing layer:
  - business-hour / target-market window enforcement in `send_guard.py`
  - duplicate and company-throttle suppression
  - Gmail API retry + backoff for 429 / 5xx transient failures
  - deliverability breakers for bounce / invalid / provider-failure / spam spikes

#### db_utils.py additions

- `upsert_sender_health(conn, health)` — INSERT OR UPDATE on sender_email
- `get_sender_health(conn, sender_email) → dict | None`
- `upsert_campaign_breaker(conn, scope, scope_key, active, reason)` — INSERT OR UPDATE on (scope, scope_key)
- `get_campaign_breaker_row(conn, scope, scope_key) → dict | None`

#### Tests

`scripts/test_deliverability_breakers.py` — 78 tests in 14 groups A–N, all passing.

---

### Workflow 5.6 — Contact Scoring (P1-2B, 2026-03-20)

| | |
|---|---|
| **Location** | `src/workflow_5_6_contact_scoring/` |
| **Files** | `contact_scoring_rules.py`, `contact_scoring_pipeline.py` |
| **Input** | `data/runs/<id>/enriched_contacts.csv` (from Workflow 5.5) |
| **Output** | `data/runs/<id>/scored_contacts.csv` |
| **Campaign step** | `contact_scoring` (between `enrich` and `verify`) |
| **Test** | `tests/test_contact_scoring.py` (60 tests, all pass) |

**Purpose:** For each company, rank all enriched contacts and select exactly one primary contact. Produces an auditable `scored_contacts.csv` with fit scores, priority ranks, and a `is_primary_contact` flag.

#### Fit Score Components

| Component | Weight | Field |
|---|---|---|
| Title tier (A–D) | up to 40 pts | `kp_title` substring match |
| Email quality | up to 20 pts | `email_confidence_tier` (E1–E4; E0 = 0) |
| Enrichment source | up to 20 pts | `enrichment_source` (apollo > hunter > website > guessed) |
| Generic penalty | −15 pts | applied when `is_generic_mailbox == "true"` |

- **Title tiers:** A = 40 (owner/ceo/procurement/principal), B = 30 (director/VP/GM/partner), C = 20 (manager/supervisor/coordinator), D = 10 (staff/engineer/lead/coordinator)
- Named contacts always ranked above generic contacts regardless of fit score
- Tie-break: alphabetical by `kp_name` (deterministic)

#### Primary Contact Selection Rules

1. Rank all contacts for the company by fit score (desc), then name (asc)
2. Rank 1 → `is_primary_contact = "true"`; all others → `"false"`
3. `is_generic_mailbox == "true"` contacts ranked last, even if fit score is higher than a named contact
4. `contact_priority_rank` field: 1 = primary, 2+ = fallback

#### Output fields (scored_contacts.csv)

Passes through all `enriched_contacts.csv` columns and adds:
```
contact_fit_score, contact_fit_breakdown, contact_selection_reason
contact_priority_rank, is_primary_contact
scoring_version
```

#### Bug fix: substring false-positive in title scoring

`"coo"` was removed from `_TITLE_TIER_B` because it substring-matches inside `"coordinator"` (which is Tier D). `"chief operating"` already covers COO. **Rule: never add abbreviations shorter than 4 chars to title tier lists.**

#### Tests

`tests/test_contact_scoring.py` — 60 tests, 10 classes, all pass.

---

### Workflow 6 — Queue Policy Enforcement (P1-3A, 2026-03-20)

| | |
|---|---|
| **Location** | `src/workflow_6_queue_policy/` |
| **Files** | `queue_policy_models.py`, `queue_policy_rules.py`, `queue_policy_pipeline.py` |
| **Input** | `data/runs/<id>/scored_contacts.csv` (required — P1-2B primary contacts) |
| **Input** | `data/runs/<id>/verified_enriched_leads.csv` (optional — Ticket 3 verification) |
| **Output** | `data/runs/<id>/queue_policy.csv` |
| **Campaign step** | `queue_policy` (between `signals` and `personalization`) |
| **Test** | `tests/test_queue_policy.py` (53 tests, all pass) |

**Purpose:** Translate upstream P1-2B contact selection and Ticket 3 verification data into explicit, auditable queue policy decisions. Output (`queue_policy.csv`) is consumed by P1-3B send-time enforcement. Workflow 6 email generation continues to read `enriched_leads.csv` unchanged.

**Pipeline position:**
```
contact_scoring (5.6) → verify (5.9) → signals (5.8) → queue_policy (6) → personalization (6.2) → email_generation (6)
```

#### Policy action model

| Action | Meaning | Trigger |
|---|---|---|
| `queue_normal` | Safe, enter normal queue | E1 verified (`allow`) |
| `queue_limited` | Queueable but flagged | E2 (`allow_limited`) or named+unverified |
| `hold` | Do not queue; preserve for review | E3 catch-all (`hold`) |
| `generic_only` | Generic mailbox path only | E4 (`generic_pool_only`) or unverified generic |
| `block` | Do not queue | E0 invalid (`block`) or no email address |

#### Policy decision logic (`queue_policy_rules.py`)

`decide_policy(send_eligibility, is_generic, has_verification, has_email) → (action, reason)`:

1. No email address → `block` (`no_email_address`)
2. Verified + eligibility `block` → `block` (`verified_e0_invalid`)
3. Verified + eligibility `hold` → `hold` (`verified_e3_catchall`)
4. Verified + eligibility `generic_pool_only` → `generic_only` (`verified_e4_generic_mailbox`)
5. Verified + eligibility `allow_limited` → `queue_limited` (`verified_e2_limited`)
6. Verified + eligibility `allow` → `queue_normal` (`verified_e1_allow`)
7. No verification + generic mailbox → `generic_only` (`unverified_generic_mailbox`)
8. No verification + named email → `queue_limited` (`unverified_named_email`)

Key invariant: **missing verification is never silently treated as `queue_normal`**.

#### Verification data resolution priority

1. `send_eligibility` already on `scored_contacts.csv` row (from P1-2B's verification enrichment) → source: `scored_contacts`
2. Look up primary contact email in `verified_enriched_leads.csv` index → source: `verified_leads`
3. No verification data found → source: `fallback` (conservative rules apply)

#### Selected primary contact enforcement

- Only `is_primary_contact == "true"` rows from `scored_contacts.csv` are processed (one per company)
- Fallback contacts are never promoted into the queue policy in this phase
- Generic contacts can be primary only when no named contact exists — explicitly labeled with `selected_contact_is_generic`

#### Output fields (queue_policy.csv)

```
company_name, website, place_id, lead_score, qualification_status, target_tier, company_type, market_focus
selected_contact_email, selected_contact_name, selected_contact_title
selected_contact_rank, selected_contact_is_generic, selected_contact_source
contact_fit_score, contact_selection_reason
selected_send_eligibility, selected_send_pool, selected_email_confidence_tier, verification_source
send_policy_action, send_policy_reason
policy_version
```

#### Backward compatibility

- `enriched_leads.csv` is NOT modified (Workflow 6 email generation continues to read it)
- `scored_contacts.csv` is NOT modified
- `queue_policy.csv` is a new additive artifact only
- All existing pipeline steps continue to work unchanged

#### Path constants

- `config/settings.py`: `QUEUE_POLICY_FILE = _RunPath("queue_policy.csv")`
- `config/run_paths.py`: `queue_policy_file: Path` (field in `RunPaths` frozen dataclass)

#### Downstream helpers

- `load_queued_normal(policy_path)` → list of rows with `send_policy_action == "queue_normal"` (for P1-3B)
- `load_queue_policy(policy_path)` → all rows (for reporting/analytics)

#### Summary reporting keys

`run()` returns: `total`, `queue_normal`, `queue_limited`, `hold`, `generic_only`, `block`, `named_primary`, `generic_primary`, `errors`, `output_file`

---

### Workflow 7 Send-Time Policy Enforcement (P1-3B, 2026-03-20)

**Ticket:** P1-3B — Send-Time Policy Enforcement
**Status:** Complete

**Purpose:** Wire the pre-computed `queue_policy.csv` (P1-3A) into actual send execution so that policy decisions are honoured at send time. No re-derivation of eligibility logic — send-time reads the already-computed `send_policy_action`.

#### What Changed

**`src/workflow_7_email_sending/send_logger.py`**
- Added `"send_policy_action"` and `"send_policy_reason"` to `LOG_FIELDS` (appended — backward compatible with existing log rows)
- Added `send_policy_action: str = ""` and `send_policy_reason: str = ""` params to `build_log_row()` and return dict

**`src/workflow_7_email_sending/send_pipeline.py`**
- Imports `QUEUE_POLICY_FILE` from settings and all 5 policy action constants from `queue_policy_models`
- New `_load_policy_indices(policy_path)` helper: loads `queue_policy.csv` → `(by_place_id, by_email, file_found)` where file_found=False means the file was absent
- New `_lookup_policy(record, by_place_id, by_email)` helper: tries place_id first, falls back to kp_email (case-insensitive)
- `run()` now loads the policy index before the send loop; prints explicit WARNING if file is missing
- Per-record policy enforcement (before guards):
  - `block` → logged as `send_decision="policy_blocked"`, `send_status="blocked"`, skipped (guards + send never called)
  - `hold` → logged as `send_decision="policy_held"`, `send_status="held"`, skipped (guards + send never called)
  - `queue_normal / queue_limited / generic_only` → proceed to guards; tracked with their own policy counters
  - Record not in policy file → `policy_action="policy_missing"`, `policy_missing` counter incremented, proceeds to guards
  - No policy file → `policy_action=""`, no policy counters incremented, all records proceed normally (backward compat)
- `send_policy_action` and `send_policy_reason` stamped on EVERY send log row (guard-blocked rows included)
- New counters in `run()` return dict and batch summary JSON:
  - `held` — records skipped by `hold` policy
  - `policy_blocked` — subset of `blocked` caused by policy `block`
  - `policy_held` — count of records held by policy
  - `policy_generic_only` — records with `generic_only` that proceeded to guards
  - `policy_queue_limited` — records with `queue_limited` that proceeded to guards
  - `policy_queue_normal` — records with `queue_normal` that proceeded normally
  - `policy_missing` — records not found in policy file (file existed but record absent)
- `_empty_summary()` updated to include all new keys

#### Policy Contract at Send Time

| `send_policy_action` | Reaches guards? | Reaches `send_one()`? | Counter |
|---|---|---|---|
| `block` | No | No | `policy_blocked` + `blocked` |
| `hold` | No | No | `policy_held` + `held` |
| `generic_only` | Yes | Yes (if guards pass) | `policy_generic_only` |
| `queue_limited` | Yes | Yes (if guards pass) | `policy_queue_limited` |
| `queue_normal` | Yes | Yes (if guards pass) | `policy_queue_normal` |
| `policy_missing` | Yes | Yes (if guards pass) | `policy_missing` |
| `""` (no policy file) | Yes | Yes (if guards pass) | none |

#### Missing Policy File Behavior

When `queue_policy.csv` is absent:
- An explicit WARNING is printed (not silent bypass)
- All records proceed through guards normally (backward compat — dry_run still works)
- No policy counters are incremented

#### Tests

**`tests/test_send_policy_enforcement.py`** — 27 tests, 7 test classes, all pass:
- `TestLoggerPolicyFields` (4) — LOG_FIELDS contains both fields; build_log_row propagates them; append_send_log writes them
- `TestPolicyIndexLoading` (6) — missing file, place_id+email indexes, lowercase normalization, precedence, fallback, no-match
- `TestPolicyBlock` (4) — send_one not called, correct counters, correct log row, guards not called
- `TestPolicyHold` (3) — send_one not called, correct counters, correct log row
- `TestPolicyQueueNormal` (2) — proceeds to send, policy fields stamped in log
- `TestPolicyQueueLimited` (1) — distinct counter, send proceeds, no false block/hold
- `TestPolicyGenericOnly` (1) — distinct counter, send proceeds
- `TestMissingPolicyFile` (2) — send proceeds, WARNING printed
- `TestPolicyMissingRecord` (2) — `policy_missing` counter incremented, record still reaches guards
- `TestBatchSummaryPolicyCounts` (2) — `_empty_summary()` keys, counters written to JSON

---

### Policy Visibility / Reporting / Compatibility (P1-3C, 2026-03-20)

**Ticket:** P1-3C — Policy Visibility / Reporting / Compatibility Completion
**Status:** Complete

**Purpose:** Close the observability loop on the policy pipeline. After P1-3B, policy decisions were enforced at send time but not visible in reporting. P1-3C makes every policy decision traceable from `queue_policy.csv` → `send_logs.csv` → `campaign_status.csv`, and adds cross-stage comparison to the campaign status summary.

#### New Artifact: `policy_summary.json`

Written by `queue_policy_pipeline.run()` at the end of the queue-policy step. Persists queue-stage policy distribution so it can be compared against send-stage outcomes later.

- **Path constant:** `POLICY_SUMMARY_FILE = _RunPath("policy_summary.json")` in `config/settings.py`
- **RunPaths field:** `policy_summary_file: Path` in `config/run_paths.py`; resolved as `run_dir / "policy_summary.json"` in `for_campaign()`
- **Written at both exit points** of `queue_policy_pipeline.run()` (empty input case + normal path)

Example structure:
```json
{
  "generated_at": "2026-03-20T12:34:56.789+00:00",
  "policy_version": "1.0",
  "queue_stage": {
    "total": 42,
    "queue_normal": 30,
    "queue_limited": 5,
    "hold": 3,
    "generic_only": 2,
    "block": 2,
    "named_primary": 35,
    "generic_primary": 7,
    "errors": 0
  }
}
```

#### Cross-Stage Policy Comparison in `campaign_status_summary.json`

`campaign_status_summary.json` now includes a `policy` section with both pipeline stages for audit:

```json
{
  "policy": {
    "queue_stage": { ... },   // from policy_summary.json (written by workflow 6)
    "send_stage":  { ... }    // from send_batch_summary.json (written by workflow 7)
  }
}
```

- `queue_stage`: the full `queue_stage` dict from `policy_summary.json`
- `send_stage`: selected keys from `send_batch_summary.json` — `total`, `sent`, `dry_run`, `failed`, `blocked`, `held`, `deferred`, `breaker_blocked`, `policy_blocked`, `policy_held`, `policy_generic_only`, `policy_queue_limited`, `policy_queue_normal`, `policy_missing`

**Backward compatibility:** if either source file is absent or malformed, the corresponding section is `{}` (not an error).

#### `STATUS_FIELDS` and `campaign_status.csv` Changes

Added to `STATUS_FIELDS` in `status_pipeline.py` (positioned after `"initial_provider"`):
```python
"send_policy_action", "send_policy_reason",
```

Merger (`status_merger.py`) extracts both fields from each send-log row:
```python
rec["send_policy_action"] = send_row.get("send_policy_action", "")
rec["send_policy_reason"] = send_row.get("send_policy_reason", "")
```

Legacy rows without these columns default to `""` (no crash).

#### Full Traceability Chain

A single contact can now be followed across all three audit layers:

| Layer | File | Key | Policy fields |
|---|---|---|---|
| Queue stage | `queue_policy.csv` | `place_id` / email | `send_policy_action`, `send_policy_reason` |
| Send stage | `send_logs.csv` | `place_id` / `kp_email` | `send_policy_action`, `send_policy_reason` |
| Status stage | `campaign_status.csv` | `place_id` / `kp_email` | `send_policy_action`, `send_policy_reason` + `lifecycle_status` |
| Summary | `campaign_status_summary.json` | — | `policy.queue_stage`, `policy.send_stage` |

#### What Changed

**`config/settings.py`**
- Added `POLICY_SUMMARY_FILE = _RunPath("policy_summary.json")`

**`config/run_paths.py`**
- Added `policy_summary_file: Path` field
- Added `policy_summary_file=run_dir / "policy_summary.json"` in `for_campaign()`

**`src/workflow_6_queue_policy/queue_policy_pipeline.py`**
- Added `_save_policy_summary(stats, out_path)` helper — writes `policy_summary.json`
- Called at both exit points of `run()` (empty input and normal path)

**`src/workflow_8_5_campaign_status/status_merger.py`**
- Added extraction of `send_policy_action` and `send_policy_reason` from send-log rows

**`src/workflow_8_5_campaign_status/status_pipeline.py`**
- Added `_load_json_safe(path)` helper — returns `{}` on missing/malformed
- Added `_build_policy_section(policy_summary, send_batch)` — constructs `policy` dict for summary
- Added `POLICY_SUMMARY_FILE`, `SEND_BATCH_SUMMARY` to imports
- Added `"send_policy_action"`, `"send_policy_reason"` to `STATUS_FIELDS`
- Updated `run()` signature with `policy_summary_path` and `send_batch_summary_path` params
- Added `policy` key to summary JSON at both exit points (zero contacts + normal)

**`tests/test_run_paths_architecture.py`, `tests/test_contact_scoring.py`, `tests/test_queue_policy.py`**
- Added `policy_summary_file=run_dir / "policy_summary.json"` to all `_make_run_paths()` helpers (required when adding new RunPaths field)

#### Tests

**`tests/test_policy_visibility.py`** — 29 tests, 8 test classes, all pass:
- `TestRunPathsAndConstants` (4) — policy_summary_file field exists in RunPaths, path under run_dir, constant in settings, for_campaign resolves it
- `TestPolicySummaryJson` (7) — file written by queue_policy run, contains generated_at / policy_version / queue_stage, counts match actual output, written on empty input, written when scored file missing
- `TestStatusFields` (3) — STATUS_FIELDS contains both policy columns, positioned near send fields
- `TestStatusMergerPolicyFields` (3) — merger propagates action+reason, defaults to `""` for legacy rows
- `TestCampaignStatusPolicySection` (6) — summary has `policy` key, `queue_stage` sub-key, `send_stage` sub-key, policy section written to JSON, empty when source files absent, present even with zero contacts
- `TestCrossStageComparison` (1) — queue_stage total and send_stage total are numerically comparable
- `TestPolicyTraceability` (2) — contact traceable by place_id from queue to status, policy_reason alongside lifecycle_status
- `TestBackwardCompatibility` (3) — missing files don't crash, malformed JSON doesn't crash, pre-P1-3B log rows (no policy fields) degrade gracefully

---

## Bug Fix — research_signal_raw.json FileNotFoundError (campaign 98de0467)

### Incident

Campaign `98de0467` (Saudi Arabia / Qatif, `enrich_limit: 20`) failed at the `signals` step with:

```
[Errno 2] No such file or directory: 'data/runs/98de0467/research_signal_raw.json'
```

`enriched_leads.csv` existed but contained 0 data rows (the enrich step completed with no contacts found).
`last_completed_step` was recorded as `"enrich"`; `signals` never completed.

### Root Cause

Two cooperative defects in the **pre-P1-2B code** (before `contact_scoring` and `verify` steps were added):

1. **`signal_collector.py` — no empty-file write on zero leads**
   When `load_enriched_leads()` returned `[]`, the collector returned early without calling `save_raw_signals()`.
   `research_signal_raw.json` was never created.

2. **`signal_summarizer.py` — no exists-guard before open()**
   `summarizer.run()` called `open(raw_path)` unconditionally.
   With the file absent, Python raised `FileNotFoundError`, crashing the step.

### Fix

**Fix 1 — `signal_collector.py`** (empty-leads path, ~lines 214–218):
When `leads` is empty, write an empty `[]` file before returning:

```python
if not leads:
    print("[Workflow 5.8] No enriched leads found — writing empty raw signals file.")
    save_raw_signals([], out_path=paths.research_signal_raw_file)
    return []
```

**Fix 2 — `signal_summarizer.py`** (missing-file guard, ~lines 206–210):
Check `raw_path.exists()` before opening; degrade gracefully:

```python
if not raw_path.exists():
    print("[Workflow 5.8] No raw signals file found — writing empty signals output.")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump([], f)
    return []
```

Both fixes are defence-in-depth: Fix 1 ensures the file is always written; Fix 2 ensures the summarizer never crashes even if Fix 1 were absent.

### Step Contract (post-fix)

| Step | Reads | Writes | Behaviour on empty / absent input |
|---|---|---|---|
| `signal_collector.run(paths)` | `paths.enriched_leads_file` (or `verified_enriched_leads_file` if exists) | `paths.research_signal_raw_file` | Writes `[]` when 0 leads found |
| `signal_summarizer.run(paths)` | `paths.research_signal_raw_file` | `paths.research_signals_file` | Returns `[]` and writes empty file when raw file absent |
| `run_step_5_8_signals` | enriched (or verified) leads file | both JSON files | Calls `_require_file` on input, then calls collector then summarizer |

Both output files (`research_signal_raw.json`, `research_signals.json`) are always present after a successful `signals` step, even when 0 leads were enriched.

### Files Changed

| File | Change |
|---|---|
| `src/workflow_5_8_signal_research/signal_collector.py` | Write empty `[]` file when leads list is empty |
| `src/workflow_5_8_signal_research/signal_summarizer.py` | Guard `open()` with `raw_path.exists()` check |
| `tests/test_signal_research_contract.py` | **NEW** — 19 regression tests (6 test classes) |

### Test Coverage (`tests/test_signal_research_contract.py`, 19 tests)

- `TestCollectorEmptyLeadsWritesFile` (3) — raw file created when 0 leads; content is `[]`; also when limit filters all rows
- `TestCollectorRunScopedPath` (3) — file written under `data/runs/<id>/`; two campaigns do not share files
- `TestSummarizerMissingFileGraceful` (4) — returns `[]` when raw absent; writes empty output; never raises `FileNotFoundError`; handles empty raw file
- `TestSummarizerReadsExistingRawFile` (3) — processes real raw records; writes output; end-to-end collect→summarize with empty leads
- `TestSignalsStepEmptyEnrichedLeads` (4) — step completes without error; both output files present; no `FileNotFoundError`; output is valid JSON
- `TestSignalsStepResumeSafety` (2) — step safe when raw file absent at resume; summarizer safe when called without prior collector

---

## Update - Send Target Routing / Final Queue Integrity (2026-03-22)

### Problem addressed

The pipeline previously allowed a mismatch between upstream contact selection and downstream send artifacts:

- `scored_contacts.csv` / routed contact selection could contain a valid named contact
- later outputs could still surface blank or stale contact fields
- `final_send_queue.csv` was not explicitly modeling whether the queued target was a named contact or a generic mailbox

This made the send queue harder to audit and caused quality reviews to overestimate contact quality when generic mailboxes were present.

### New first-touch routing contract

Workflow 6 email merge now applies **company-level send-target routing** before email generation:

1. If a usable **named** contact exists, first touch uses that named contact only
2. If no usable named contact exists but a usable **generic mailbox** exists, first touch uses the generic mailbox
3. If neither exists, the company is skipped and does not enter the final send queue

#### Named contact definition

A contact is considered usable named contact when:

- `kp_name` is non-empty
- `kp_email` is non-empty
- email is **not** a generic mailbox
- contact is sendable (`email_sendable=true` or eligibility in `allow / allow_limited / generic_pool_only`)

Trusted contacts are preferred when multiple named contacts are available. Title relevance is used to classify `strong_named` vs `weak_named`, but not to suppress a still-usable named contact.

#### Generic mailbox definition

Generic mailbox detection uses local-part patterns such as:

`info`, `sales`, `contato`, `comercial`, `office`, `atendimento`, `support`, `contact`, `hello`

Generic mailboxes are now a **fallback tier**, not the same quality layer as named contacts.

### New routing fields propagated downstream

The following fields are now attached during email-merge routing and carried through generated/scored/repaired/final queue outputs:

- `contact_name`
- `contact_title`
- `contact_email`
- `send_target_type` = `named` / `generic`
- `contact_source` = `kp` / `generic` / `fallback`
- `named_contact_available` = `true` / `false`
- `generic_contact_available` = `true` / `false`
- `contact_quality` = `strong_named` / `weak_named` / `generic_only` / `none`
- `generic_only` = `true` / `false`

Backward compatibility is preserved:

- `kp_name`, `kp_title`, `kp_email` are still populated for older send-time code
- send loader normalizes `contact_* -> kp_*` when needed

### Final queue / send-stage integrity

Workflow 7 send-time handling now assumes a single explicit send target per company:

- `generic_only` policy no longer forces manual review by default
- when policy allows generic-only routing, the generic target proceeds through guards and can be sent
- send-stage duplicate / company suppression no longer creates a `review_required` branch for normal first-touch routing:
  - duplicate email+subject within 24h -> `deferred`
  - same contact / same company suppression inside the throttle window -> `deferred`
  - `dry_run` is intentionally looser and only suppresses exact same contact or same `place_id`
  - domain-wide and company-name-wide suppression remains active for live `smtp` / `gmail_api` sending
- send logs now stamp:
  - `kp_title`
  - `contact_name`
  - `contact_title`
  - `contact_email`
  - `send_target_type`
  - `contact_source`
  - `contact_quality`

Send batch summary now also records:

- `final_named_sends`
- `final_generic_sends`

### Initial send routing + follow-up fallback

The outbound routing model is now:

- initial send:
  - if a usable named contact exists, send only to the named contact
  - if no usable named contact exists but a usable generic mailbox exists, send to the generic mailbox
  - if neither exists, the company does not enter `final_send_queue.csv`
- follow-up fallback:
  - if the initial send target was `named`
  - and the company also has a usable generic mailbox in `scored_contacts.csv`
  - then `followup_1` may be rerouted to that generic mailbox as `generic_fallback_after_named`
  - the original named target is preserved in:
    - `original_contact_email`
    - `original_contact_name`
  - if no generic fallback exists, follow-up stays on the original contact

This behavior is implemented in:

- `src/workflow_6_email_generation/email_merge.py`
- `src/workflow_7_email_sending/send_guard.py`
- `src/workflow_8_followup/followup_selector.py`
- `src/workflow_8_followup/followup_pipeline.py`

### Campaign quality reporting additions

`campaign_status_summary.json` now includes `quality_report` with at least:

- `raw_leads`
- `qualified_leads`
- `named_contact_companies`
- `generic_only_companies`
- `companies_with_any_sendable_contact`
- `final_send_queue_count`
- `final_named_sends_count`
- `final_generic_sends_count`
- `contact_field_completeness_pct`
- `final_queue_contact_integrity_pct`
- `final_queue_contact_name_completeness_pct`

This is intended to separate:

- companies with real named contacts
- companies with generic-only fallback coverage
- final queue rows whose send target is fully auditable

### Lead scoring weighting adjustment (2026-03-22)

Lead scoring was nudged to increase commercial value concentration without collapsing volume:

- `commercial` market focus weight increased
- `mixed` market focus weight reduced
- `solar epc`, `solar contractor`, and `solar component distributor` weights increased
- `solar installer` / `solar panel installer` weights reduced slightly

Intent: keep raw discovery volume broadly stable while making qualified output somewhat more commercial / EPC / distributor heavy.

### Email copy refresh (2026-03-22)

Cold-email and follow-up copy were tightened to sound less templated and more realistic for live Gmail API testing:

- shorter, plainer subject lines
- less "I know your business in detail" language when signals are weak
- more modest value framing: one practical benefit instead of stacked product claims
- softer CTAs such as "Happy to share a few details if useful."
- consistent sign-off driven by `SENDER_NAME` / `SENDER_TITLE`

Updated files:

- `src/workflow_6_email_generation/email_templates.py`
- `src/workflow_6_email_generation/email_generator.py`
- `src/workflow_8_followup/followup_generator.py`

### Target-market send windows (2026-03-22)

Workflow 7 send-time business-hours enforcement now uses the recipient market's local timezone instead of the operator machine's local clock.

- default preferred send slots:
  - `09:00-12:00`
  - `14:00-16:00`
- configured via `SEND_WINDOW_SLOTS`
- fallback remains `SEND_WINDOW_START` / `SEND_WINDOW_END` when slot parsing fails

Location resolution priority for each send record:

1. explicit `city` + `country`
2. parsed `source_location`
3. active campaign state (`campaign_run_state.json`)

Current explicit timezone support includes at least:

- Brazil -> `America/Sao_Paulo`
- Saudi Arabia / KSA -> `Asia/Riyadh`

Business-week support is also market-aware where required:

- default: Monday-Friday
- Saudi Arabia: Sunday-Thursday

Relevant files:

- `src/workflow_7_email_sending/send_guard.py`
- `src/workflow_7_email_sending/send_pipeline.py`
- `src/workflow_7_email_sending/send_logger.py`
- `src/workflow_6_email_generation/email_generator.py`
- `src/workflow_6_5_email_quality/quality_merge.py`
- `src/workflow_6_5_email_quality/email_quality_scorer.py`
- `src/workflow_6_7_email_repair/repair_pipeline.py`

For already-completed runs that should send automatically when the target
market opens, use:

- `scripts/auto_send_runs.py`

This helper watches one or more completed `campaign_id`s, waits until the next
market-local eligible send window, then runs:

- Workflow 7 send in `gmail_api` mode
- Workflow 8.5 campaign status refresh

It is intended for "queue is ready tonight, send automatically tomorrow morning
in the target market" use cases.

### Bounce handling closure (2026-03-23)

Bounce / return processing is now a full closed loop rather than only a future
deliverability-threshold concept.

- Reply Intelligence now classifies DSN / delivery-failure messages as a first-class
  reply type: `bounce`
- Detection sources:
  - DSN / failure phrases such as `delivery status notification`, `mail delivery failed`,
    `undeliverable`, `user unknown`, `recipient address rejected`
  - sender-address heuristic for `mailer-daemon` / `postmaster`
- `bounce` operationally maps to:
  - `suppression_status = suppressed`
  - `followup_paused = True`
  - no automated future follow-up or resend to that address

Relevant implementation files:

- `src/workflow_7_8_reply_intelligence/reply_classifier.py`
- `src/workflow_7_8_reply_intelligence/reply_state_manager.py`
- `src/workflow_8_followup/followup_stop_rules.py`
- `src/workflow_7_email_sending/send_guard.py`
- `src/workflow_7_8_reply_intelligence/reply_pipeline.py`

Send-time protection is now closed as well:

- Workflow 7 send guard reads the reply suppression index before sending
- already-bounced / unsubscribed / hard-no addresses are blocked before `send_one()`
- block reason prefix: `blocked_reply_suppression`
- paused reply states still defer rather than hard-block

Bounce analytics are also persisted:

- when a matched reply is classified as `bounce`, `reply_pipeline.py` appends a
  `bounce` event to global `engagement_logs.csv`
- this allows CRM sync / dashboard reporting / future sender-health jobs to read
  bounce events from the same event stream as opens / clicks / replies

Control-panel visibility added in the Send Ops Snapshot:

- `Bounces 7d`
- `Bounce Rate 7d`
- `Suppressed Addresses`
- `Last Bounce`

These are aggregated from global CRM files:

- `data/crm/send_logs.csv`
- `data/crm/engagement_logs.csv`
- `data/crm/reply_logs.csv`

Longer-term cloud deployment planning for "local run complete -> cloud waits ->
market-local Gmail send" is documented in:

- `GOOGLE_CLOUD_MINIMAL_DEPLOYMENT.md`

The concrete operator runbook and first-pass implementation assets are now:

- `GOOGLE_CLOUD_SETUP_RUNBOOK.md`
- `scripts/deploy_run_to_gcloud.py`
- `scripts/cloud_send_worker.py`
- `deploy/gcp/bootstrap_vm.sh`
- `deploy/gcp/systemd/cloud-send-worker.service`
- `deploy/gcp/.env.gcp.example`

GitHub-readiness / safe-sharing support added:

- `.env.example`
- `scripts/check_github_readiness.py`
- `scripts/prepare_github_repo_export.py`
- `GITHUB_READY_CHECKLIST.md`
- `GITHUB_EXPORT_STRUCTURE.md`
- `data/.gitkeep`
- `.gitignore` expanded to ignore local caches and `node_modules/`

Cloud auto-deploy hook added:

- `src/workflow_9_campaign_runner/campaign_runner.py` now supports an opt-in
  post-run background deploy to GCS after a full successful run
- gated by:
  - `CLOUD_SEND_ENABLED=true`
  - `CLOUD_AUTO_DEPLOY_ON_COMPLETE=true`
- safety rules:
  - only triggers when `run_until == "campaign_status"`
  - only after `campaign_status` completes successfully
  - only if `final_send_queue.csv` exists
  - never triggers for `dry_run` campaigns
  - runs in a detached background subprocess so it does not block the local run
- state flow for cloud handoff is now explicit:
  - runner marks `cloud_deploy_status=pending` before background trigger
  - `scripts/deploy_run_to_gcloud.py` marks:
    - `started` when upload begins
    - `completed` after manifest upload succeeds
    - `failed` if GCS upload errors
- this state is persisted to `data/runs/<campaign_id>/cloud_deploy_status.json`
  and surfaced back into the control panel so operators can distinguish:
  - local pipeline completed
  - cloud upload in progress
  - cloud upload failed
  - cloud upload completed / cloud worker can now take over
- `scripts/deploy_run_to_gcloud.py` also enforces the same rule:
  - `dry_run` campaigns are rejected from manual cloud deploy
  - their `cloud_deploy_status` remains effectively opt-out / not eligible

### Cloud worker state + failure closure (2026-03-23)

The GCP VM worker now writes an explicit cloud-send lifecycle in addition to the
earlier cloud-deploy handoff state.

- New per-run artifact:
  - `data/runs/<campaign_id>/cloud_send_status.json`
- Status flow now supports:
  - `queued`
  - `synced`
  - `waiting_window`
  - `sending`
  - `completed`
  - `failed`
- `scripts/deploy_run_to_gcloud.py` now writes `cloud_send_status=queued` once
  the run and manifest have been uploaded successfully.
- `scripts/cloud_send_worker.py` now updates structured state as it progresses:
  - after local sync: `synced`
  - while waiting for the target-market time window: `waiting_window`
  - when Workflow 7 is actively running: `sending`
  - on success: `completed`
  - on exception: `failed`

Failure / observability additions:

- worker-local state file `data/cloud_send_worker_state.json` now also tracks:
  - `failed_campaigns`
  - `last_poll_at`
  - `last_success_at`
  - `last_error_at`
  - `active_campaign_id`
- failure events are appended to:
  - `data/cloud_worker_alerts.jsonl`
- optional outbound alerting is supported via:
  - `CLOUD_WORKER_ALERT_WEBHOOK`
- failure runs are no longer retried blindly in the same worker loop:
  - once a campaign enters worker `failed`, it is added to `failed_campaigns`
  - this avoids silent duplicate-send risk after partial cloud execution
  - recovery becomes an explicit operator action instead of automatic re-send

Control-panel visibility expanded:

- `load_current_campaign_state()` now merges `cloud_send_status.json` into the
  current run state alongside `cloud_deploy_status.json`
- `Send Ops Snapshot` now also shows:
  - `Queued in Cloud`
  - `Waiting Window`
  - `Sending Now`
  - `Cloud Failures`
- enhanced file-status tracking now includes:
  - `cloud_send_status.json`

### V2 operations hardening: GitHub / secrets / recovery (2026-03-23)

Three V2 engineering gaps now have a concrete in-repo operator path instead of
remaining only as planning items:

1. deployment / version management
2. Gmail secret handling
3. VM / worker recovery

New VM operations assets:

- `deploy/gcp/update_vm.sh`
  - standard GitHub-driven VM update path
  - fetches from the configured remote / branch
  - uses `git pull --ff-only`
  - now also supports `--ref <git-tag-or-commit>` for pinned release deploys
  - refreshes `.venv` and `requirements.txt`
  - reinstalls the systemd unit
  - restarts `cloud-send-worker`
  - writes deployment metadata to `data/deploy_release.json`
  - release metadata now also records:
    - `git_ref_requested`
    - `deploy_mode`
- `deploy/gcp/rollback_vm.sh`
  - standard rollback wrapper around `update_vm.sh --ref ...`
- `deploy/gcp/release_status.sh`
  - prints the current `data/deploy_release.json`

GitHub export automation hardening:

- `scripts/prepare_github_repo_export.py`
  - now supports `--zip`
  - can create a timestamped zip archive after building `_github_export`
- `scripts/build_github_bundle.ps1`
  - Windows one-click wrapper for GitHub handoff
  - runs `check_github_readiness.py`
  - runs `prepare_github_repo_export.py --overwrite --zip`
  - intent: remove repeated manual pre-GitHub steps from the operator workflow

Deployment/version-management current operator entrypoints:

- standard VM update:
  - `bash deploy/gcp/update_vm.sh`
- pinned ref deploy:
  - `bash deploy/gcp/update_vm.sh --ref <git-tag-or-commit>`
- rollback:
  - `bash deploy/gcp/rollback_vm.sh <git-tag-or-commit>`
- inspect deployed release metadata:
  - `bash deploy/gcp/release_status.sh`
- build GitHub-safe handoff bundle on Windows:
  - `powershell -ExecutionPolicy Bypass -File scripts/build_github_bundle.ps1`
- `deploy/gcp/restore_gmail_oauth.sh`
  - standard Gmail OAuth restore / validation script
  - supports:
    - file-based restore via `SOLAR_SECRET_SOURCE_DIR`
    - Secret Manager restore via `SOLAR_GMAIL_CLIENT_SECRET_NAME` and
      `SOLAR_GMAIL_TOKEN_SECRET_NAME`
  - validates final runtime files at:
    - `config/gmail_client_secret.json`
    - `config/gmail_token.json`
- `deploy/gcp/recover_cloud_worker.sh`
  - standard worker recovery flow
  - runs update, secret restore, service restart, and prints logs / alerts

Systemd hardening:

- `deploy/gcp/systemd/cloud-send-worker.service` now includes:
  - `ExecStartPre=/opt/solar-lead-intelligence/deploy/gcp/restore_gmail_oauth.sh --check-only`
  - `StartLimitIntervalSec=300`
  - `StartLimitBurst=10`
- intent:
  - fail fast when Gmail OAuth files are missing
  - make restart loops more diagnosable

VM environment template additions:

- `CLOUD_WORKER_ALERT_WEBHOOK`
- `SOLAR_SECRET_SOURCE_DIR`
- `SOLAR_GMAIL_CLIENT_SECRET_NAME`
- `SOLAR_GMAIL_TOKEN_SECRET_NAME`
- `REPO_BRANCH`

Canonical runbooks added:

- `GITHUB_VM_UPDATE_RUNBOOK.md`
- `GMAIL_SECRET_AND_RECOVERY_RUNBOOK.md`

These document the intended V2 operator flow:

- GitHub becomes the code source of truth
- VM updates happen via `deploy/gcp/update_vm.sh`
- Gmail runtime secrets are restored via `deploy/gcp/restore_gmail_oauth.sh`
- worker recovery happens via `deploy/gcp/recover_cloud_worker.sh`

### V2 observability + worker predictability pass (2026-03-23)

The next V2 slice focused on:

1. cloud observability
2. clearer deploy/send/status visibility
3. stronger worker predictability after restart

Cloud worker observability additions:

- `src/workflow_9_5_streamlit_control_panel/ui_state.py` now exposes
  `load_cloud_worker_health()`
- health snapshot sources:
  - `data/cloud_send_worker_state.json`
  - `data/cloud_worker_alerts.jsonl`
  - `data/deploy_release.json`
- current dashboard visibility now includes:
  - worker health (`healthy` / `stalled` / `offline` / `unknown`)
  - active campaign id
  - alerts in the last 24 hours
  - deployed commit / branch
  - last poll / last success / last error
  - last alert metadata

Cloud handoff visibility additions:

- KPI dashboard now also includes a `Current Cloud Handoff Detail` expander
  showing:
  - local run status
  - cloud deploy status
  - cloud send status
  - cloud send due time
  - market
  - latest send/deploy error when present

Worker predictability / idempotency additions:

- `scripts/cloud_send_worker.py` now records more heartbeat fields:
  - `last_idle_reason`
  - `last_manifest_count`
  - `last_wait_campaign_id`
  - `last_wait_due_at`
  - `last_completed_campaign_id`
  - `last_failed_campaign_id`
  - `last_processed_manifest_uri`
- before normal candidate preparation, the worker now reconciles each manifest
  against the run's persisted `cloud_send_status.json`
- if a run is already `completed` in run state, the worker reconciles the
  manifest into `processed` rather than re-sending
- if a run is already `failed` in run state, the worker skips it and keeps it
  in manual-recovery mode

Intent:

- a worker restart should rely less on in-memory sets
- repeated sends after restart should be less likely
- operators should be able to see worker health, release version, and recent
  alert context directly from the control panel

### V2 upload-path optimization pass (2026-03-23)

Cloud deploy / sync-back previously relied on many per-file `gcloud storage cp`
calls, which increased CLI overhead and made feedback less explicit.

Upload-path changes:

- `scripts/deploy_run_to_gcloud.py`
  - deploy upload now uses a single recursive directory copy instead of one
    `cp` per file
  - upload stats are computed locally before transfer:
    - file count
    - total bytes
    - elapsed seconds
  - these are persisted into `cloud_deploy_status.json` as:
    - `cloud_deploy_upload_mode`
    - `cloud_deploy_file_count`
    - `cloud_deploy_bytes`
    - `cloud_deploy_elapsed_seconds`
- `scripts/cloud_send_worker.py`
  - cloud sync-back also uses recursive directory upload
  - sync-back stats are persisted into `cloud_send_status.json` as:
    - `cloud_send_upload_mode`
    - `cloud_send_uploaded_file_count`
    - `cloud_send_uploaded_bytes`
    - `cloud_send_upload_elapsed_seconds`

UI visibility:

- `src/workflow_9_5_streamlit_control_panel/ui_state.py` now merges these upload
  stats into the current run state
- `src/workflow_9_5_streamlit_control_panel/ui_views.py` shows them inside
  `Current Cloud Handoff Detail` as:
  - deploy upload stats
  - cloud send sync-back stats

Intent:

- fewer GCS CLI invocations
- faster / smoother batch handoff
- clearer operator feedback when a deploy succeeded but felt "stuck"
- better debugging when upload is slow because the UI now shows file count,
  bytes, and elapsed seconds

Batch deploy improvement:

- `scripts/deploy_run_to_gcloud.py` now supports:
  - repeated `--campaign` arguments
  - `--all-ready` to auto-discover eligible completed runs
  - `--limit N` to deploy only the newest N discovered runs
  - `--force` to redeploy runs even if cloud deploy status is already pending /
    started / completed
- default batch behavior skips runs whose `cloud_deploy_status` is already:
  - `pending`
  - `started`
  - `completed`
- intent:
  - smoother multi-run handoff after a batch of local campaigns finishes
  - less operator friction than deploying one campaign at a time
  - safer retries because already-deployed runs are skipped unless explicitly forced

Control-panel batch deploy entry:

- `src/workflow_9_5_streamlit_control_panel/ui_state.py`
  - added `load_ready_cloud_deploys()`
  - returns completed non-dry-run runs with `final_send_queue.csv` whose
    cloud deploy state is not already `pending` / `started` / `completed`
- `src/workflow_9_5_streamlit_control_panel/ui_actions.py`
  - added `trigger_cloud_batch_deploy()`
- `src/workflow_9_5_streamlit_control_panel/ui_views.py`
  - KPI dashboard now includes a `Ready To Deploy` section
  - shows a list of deploy-ready runs
  - provides:
    - `Deploy Top 5`
    - `Deploy All Ready`
    - `Deploy Selected`
  - deploy-ready rows are now selectable via a checkbox column in the control panel
- `src/workflow_9_5_streamlit_control_panel/ui_actions.py`
  - `trigger_cloud_batch_deploy()` now also accepts explicit `campaign_ids`
    so UI-triggered deploy can target selected runs instead of only top-N/all

Intent:

- remove the need to manually type batch deploy commands in normal operator flow
- make cloud handoff visible and actionable from the same dashboard
- keep a lightweight operator flow while still allowing precise per-run selection

Manifest lifecycle hardening:

- new storage prefix supported:
  - `GCS_FAILED_PREFIX` (default: `failed`)
- `scripts/cloud_send_worker.py` now moves failed manifests out of the live
  `manifests/` prefix and into `failed/`
- this applies to:
  - prepare-stage failures
  - send-stage failures
  - restart-time reconciliation of runs already marked `failed`
- `cloud_send_status.json` now persists:
  - `cloud_send_failed_manifest_uri`

Intent:

- keep the active manifest queue cleaner
- reduce repeated scanning of manifests that already require manual recovery
- make failure recovery more explicit because each failed manifest now has a
  durable failed-location URI

### V2 deploy + Gmail recovery closure (2026-03-23)

Real VM validation now completed against the GitHub-hosted
`solar-lead-intelligence-v2` checkout.

Deployment/version-management validation:

- verified `bash deploy/gcp/update_vm.sh` from the VM clone
- verified `data/deploy_release.json` writes branch / commit / requested ref
- verified `bash deploy/gcp/release_status.sh`
- verified `bash deploy/gcp/rollback_vm.sh <commit>`
- verified return from pinned detached-HEAD deploy back to `main`
- verified `cloud-send-worker.service` remains healthy after update and rollback

Path-hardening changes made during validation:

- `deploy/gcp/update_vm.sh`
- `deploy/gcp/bootstrap_vm.sh`
- `deploy/gcp/restore_gmail_oauth.sh`
- `deploy/gcp/recover_cloud_worker.sh`
- `deploy/gcp/release_status.sh`
- `deploy/gcp/rollback_vm.sh`
- `deploy/gcp/systemd/cloud-send-worker.service`

These now resolve the current repo location instead of assuming the legacy
`/opt/solar-lead-intelligence` path.

Gmail secret-management hardening:

- `.env.gcp.example` now recommends setting a fixed absolute
  `SOLAR_SECRET_SOURCE_DIR` outside the repo
- new helper:
  - `deploy/gcp/stage_gmail_oauth.sh`
- new helper:
  - `deploy/gcp/publish_gmail_oauth_to_secret_manager.sh`
- purpose:
  - copy working `config/gmail_client_secret.json` and `config/gmail_token.json`
    into the fixed restore-source directory
  - give VM rebuilds and token refreshes one predictable restore location
  - reduce future operator reliance on manual re-upload
  - provide a scripted path to publish the current working OAuth pair into
    Google Secret Manager

Recommended operator sequence after Gmail OAuth is confirmed working:

1. set `SOLAR_SECRET_SOURCE_DIR` in `.env` to a stable absolute VM path
2. run `bash deploy/gcp/stage_gmail_oauth.sh`
3. future recoveries use:
   - `bash deploy/gcp/restore_gmail_oauth.sh --force`
   - `bash deploy/gcp/recover_cloud_worker.sh --skip-update`

Systemd cleanup:

- `deploy/gcp/systemd/cloud-send-worker.service`
  - moved `StartLimitIntervalSec` / `StartLimitBurst` into `[Unit]`
  - removes the VM warning:
    - `Unknown key 'StartLimitIntervalSec' in section [Service], ignoring.`

### V2 VM config drift incident: GCS bucket mismatch (2026-03-23)

Issue observed after the new GitHub-based VM checkout was brought online:

- `cloud-send-worker.service` was healthy
- Gmail OAuth validation passed
- real manifests existed in GCS under:
  - `gs://emailoutbound/manifests/...`
- but worker state still showed:
  - `last_manifest_count = 0`
  - `last_idle_reason = no_manifests`

Root cause:

- the new VM `.env` had been created from `deploy/gcp/.env.gcp.example`
- `GCS_BUCKET` was still the placeholder value:
  - `your-gcs-bucket-name`
- the worker therefore polled:
  - `gs://your-gcs-bucket-name/manifests`
  instead of the live bucket:
  - `gs://emailoutbound/manifests`

Impact:

- previously uploaded Brazil campaigns were not lost
- no re-queue or re-generation was needed
- the worker simply could not see the live manifest queue until the bucket was corrected

Fix applied on the VM:

- updated `.env`:
  - `GCS_BUCKET=emailoutbound`
- restarted via:
  - `bash deploy/gcp/update_vm.sh`
- verified:
  - `_list_manifest_uris()` returned the expected manifest list
  - worker state moved from `0` manifests to the real manifest count
  - worker began syncing queued campaigns from GCS

Operator rule added:

- after bootstrapping any new VM or new GitHub checkout, compare the new `.env`
  against the previously working environment before trusting worker state
- minimum required checks:
  - `GCS_BUCKET`
  - `GCS_RUNS_PREFIX`
  - `GCS_MANIFESTS_PREFIX`
  - `EMAIL_SEND_MODE`
  - `CLOUD_SEND_ENABLED`
  - Gmail secret source configuration:
    - `SOLAR_SECRET_SOURCE_DIR`
    - or `SOLAR_GMAIL_CLIENT_SECRET_NAME` / `SOLAR_GMAIL_TOKEN_SECRET_NAME`

Key lesson:

- if worker state says `no_manifests` but direct `gcloud storage ls` against the
  real bucket shows manifests, treat it as environment-config drift first,
  not as lost campaigns.

### V2 recovery drill finding: Secret Manager read permission gap (2026-03-23)

Recovery drill scenario:

- intentionally removed:
  - `config/gmail_client_secret.json`
  - `config/gmail_token.json`
- then ran:
  - `bash deploy/gcp/restore_gmail_oauth.sh --force`

Observed result:

- VM service account could not read Secret Manager values
- GCP returned:
  - `secretmanager.versions.access` permission denied
- this exposed a real recovery gap:
  - Secret Manager publish was configured
  - but VM runtime identity still lacked read access

Code hardening applied:

- `deploy/gcp/restore_gmail_oauth.sh`
  - Secret Manager restore now writes into temporary files first
  - only promotes them into `config/` if both files are non-empty
  - permissions or empty-secret failures now raise explicit errors
  - avoids previous false-positive behavior where empty target files could be
    left behind after a failed read

Operational requirement added:

- VM runtime identity must have Secret Manager read access before Secret Manager
  is considered a valid recovery source
- required permission:
  - `secretmanager.versions.access`
- practical fix:
  - grant Secret Manager Secret Accessor to the VM service account
    `503580949036-compute@developer.gserviceaccount.com`

Recovery status after drill:

- runtime OAuth files were restored from backup
- `bash deploy/gcp/recover_cloud_worker.sh --skip-update` returned the worker to
  healthy state
- restart recovery path is good
- Secret Manager recovery path is code-safe now, but still needs IAM access to
  be fully validated in production

### V2 recovery drill closure: Secret Manager restore succeeded (2026-03-23)

Follow-up validation completed after granting the VM runtime access to read the
published Gmail OAuth secrets.

Successful drill steps:

1. removed runtime files:
   - `config/gmail_client_secret.json`
   - `config/gmail_token.json`
2. ran:
   - `bash deploy/gcp/restore_gmail_oauth.sh --force`
3. confirmed restored files were non-empty and had `600` permissions
4. ran:
   - `bash deploy/gcp/recover_cloud_worker.sh --skip-update`
5. confirmed:
   - `cloud-send-worker.service` returned to `active (running)`
   - worker resumed scanning live manifests including queued Brazil campaigns

Outcome:

- fixed restore directory path is validated
- Secret Manager restore path is validated
- worker restart recovery path is validated
- V2 items now effectively complete:
  - `1. deployment/version management`
  - `3. Gmail secret management`
  - `7. recovery ability`

Current operator note:

- the live GCS bucket for this environment is:
  - `emailoutbound`
- do not leave `GCS_BUCKET=your-gcs-bucket-name` in a new VM `.env`
- if worker reports `no_manifests`, first compare `.env` bucket/prefix values
  against the previously working environment

### V2 observability hardening: worker config visibility (2026-03-23)

Cloud worker state now exposes more direct operator signals so dashboard users
can distinguish "truly idle" from "misconfigured" and "actively scanning".

Worker state additions in `data/cloud_send_worker_state.json`:

- `last_poll_result`
- `last_candidate_count`
- `last_manifest_sample`
- `last_sync_campaign_id`
- `last_reconciled_campaign_id`
- `worker_config_ok`
- `worker_config_issue`
- `worker_bucket`
- `worker_manifests_prefix`

Behavior change:

- `scripts/cloud_send_worker.py` now validates obvious bucket misconfiguration
  before polling
- placeholder or empty `GCS_BUCKET` now surfaces as:
  - `last_idle_reason = config_error`
  - `last_poll_result = config_error`
  - structured alert:
    - `worker_config_invalid`
- this prevents the previous failure mode where the worker looked "healthy" but
  silently polled the wrong manifest queue

Dashboard visibility:

- `load_cloud_worker_health()` now returns the new worker config / poll fields
- KPI dashboard `Cloud Worker Health` now shows:
  - worker bucket
  - manifest prefix
  - manifest count
  - actionable candidate count
  - last synced campaign
  - last reconciled campaign
  - manifest sample
- health is now promoted to `misconfigured` when worker config issues are present

### V2 worker stability hardening: selection predictability (2026-03-23)

The cloud worker now persists more of its selection state so restart behavior
and wait-state diagnosis are easier to reason about.

New persistent fields:

- `last_candidate_campaign_ids`
- `last_selected_campaign_id`
- `last_selected_due_at`

Behavior improvement:

- after building the actionable candidate list, the worker now re-checks current
  UTC time immediately before deciding whether the selected campaign is still in
  `waiting_window`
- this reduces a subtle stale-time risk where long manifest scanning could make
  an already-due campaign still appear to be waiting

Operator benefit:

- dashboard can now show:
  - which campaign the worker actually selected this cycle
  - when that selected campaign is due
  - a sample of actionable candidate campaign ids
- this makes "why is the worker idle?" and "which run will go next?" much more
  obvious during long queue windows

### V2 worker stability hardening: inflight manifest claiming (2026-03-23)
- Root cause of repeated manifest scanning: manifests remained in `gs://.../manifests/` during `waiting_window`, so every poll re-downloaded and re-evaluated the same pending campaigns.
- Worker now claims the selected earliest campaign by moving its manifest from `manifests/` to `inflight/` before waiting or sending.
- While any inflight manifest exists, the worker prioritizes inflight processing instead of rescanning the visible queue on every poll.
- New worker state fields track queue convergence: `last_inflight_count`, `last_inflight_sample`, `claimed_campaign_id`, and `claimed_manifest_uri`.

### V2 worker stability validation: inflight + failure routing confirmed on VM (2026-03-23)

Real VM behavior now confirms the intended queue lifecycle:

- `sao-paulo_20260322_140220_648a` was claimed into:
  - `gs://emailoutbound/inflight/sao-paulo_20260322_140220_648a.json`
- `sao-bernardo-do-campo_20260322_164133_5ad8` failed after entering inflight and was routed to:
  - `gs://emailoutbound/failed/sao-bernardo-do-campo_20260322_164133_5ad8-1774269084.json`
- completed runs are now collecting under:
  - `gs://emailoutbound/processed/...`

This validates that V2 no longer relies on repeated full rescans of `manifests/`
for selected campaigns, and failed sends no longer need to masquerade as
completed queue work.

### V2 Gmail send correctness hardening: zero-success batches must fail (2026-03-23)

Root cause:

- `scripts/auto_send_runs.py` treated Workflow 7 as successful whenever
  `run_send()` returned without raising, even if the batch had:
  - `sent = 0`
  - `dry_run = 0`
  - `failed > 0`

Fix:

- `_run_campaign_send()` now raises when a batch produces zero successful
  deliveries but one or more failures.
- This ensures the cloud worker sends those campaigns to the `failed/` queue
  path instead of `processed/`.

### V2 Gmail runtime dependency gap fixed on VM (2026-03-23)

Root cause:

- Gmail API runtime packages were missing from `requirements.txt`, so VM sends
  could fail with:
  - `No module named 'google'`

Fix:

- Added required Gmail API packages to `requirements.txt`:
  - `google-auth`
  - `google-auth-oauthlib`
  - `google-api-python-client`
- VM verification completed successfully:
  - `python -c "import google.oauth2.credentials, google_auth_oauthlib.flow, googleapiclient.discovery; print('google deps ok')"`
  - output: `google deps ok`

### V2 result consistency closure: deploy status backfill validated (2026-03-23)

The remaining state-consistency gap was that some historical runs still had:

- `cloud_deploy_status = started`

even though the cloud worker had already taken over and later produced
`synced`, `sending`, `completed`, or `failed` send states.

Fix:

- `scripts/cloud_send_worker.py` now backfills local `cloud_deploy_status.json`
  to `completed` whenever a run is already under cloud-worker management.
- Backfill runs every worker loop, so older runs no longer depend on being
  selected again before their deploy state is corrected.

VM validation:

- `sao-bernardo-do-campo_20260322_164133_5ad8`
- `santo-andre_20260322_170155_2c77`
- `sorocaba_20260322_175256_9118`

all now show:

- `cloud_deploy_status = completed`
- `cloud_deploy_reconciled_at = ...`

This closes V2 item `4. cloud send result and local state consistency`.

### V2 standard operator workflow snapshot (2026-03-23)

Current working release process:

1. make and validate changes locally
2. mirror release-ready files into `_github_export`
3. commit and push from `_github_export`
4. on the VM, run:
   - `bash deploy/gcp/update_vm.sh`
5. verify:
   - `bash deploy/gcp/release_status.sh`
   - `systemctl status cloud-send-worker`
   - `cat data/cloud_send_worker_state.json`
6. if recovery is needed:
   - `bash deploy/gcp/recover_cloud_worker.sh`
7. if rollback is needed:
   - `bash deploy/gcp/rollback_vm.sh <git-tag-or-commit>`

Live queue meaning:

- `manifests/` = queued
- `inflight/` = claimed / waiting / sending
- `processed/` = completed
- `failed/` = failed

### V2 observability closure: direct email alert validated (2026-03-23)

The cloud worker now supports direct operator alerts by email without relying on
Feishu, WeCom, or third-party webhook relays.

Validated configuration on the live VM:

- `CLOUD_WORKER_ALERT_EMAIL_TO=yangzuwei@gmail.com`
- `CLOUD_WORKER_ALERT_EMAIL_MODE=gmail_api`
- `CLOUD_WORKER_ALERT_EMAIL_FROM=wayne@try-omnisol.com`
- `CLOUD_WORKER_ALERT_SUBJECT_PREFIX=[CloudWorker]`

Validation flow:

1. pushed the worker email-alert code path to GitHub
2. updated the VM with:
   - `bash deploy/gcp/update_vm.sh`
3. ran:
   - `python scripts/cloud_send_worker.py --test-alert`
4. confirmed delivery of:
   - `[CloudWorker] INFO test_alert`
   from `wayne@try-omnisol.com` to `yangzuwei@gmail.com`

Operational note:

- if alert test says delivery attempted but no email arrives, first compare VM
  `.env` values rather than local `.env`
- the alert sender must resolve from one of:
  - `CLOUD_WORKER_ALERT_EMAIL_FROM`
  - `SMTP_FROM_EMAIL`
  - `SMTP_USERNAME`
- `data/cloud_worker_alerts.jsonl` must remain writable by the VM user running
  the worker

### V2 stable baseline snapshot (2026-03-23)

Current completion status:

- `1. deployment/version management` = complete
- `2. observability` = complete
- `3. Gmail secret management` = complete
- `4. cloud send result/local consistency` = complete
- `5. worker long-run stability` = functionally complete, continue observing
- `6. GitHub/local/cloud operating standardization` = complete
- `7. recovery ability` = complete
- `8. stable-version closeout` = complete

Remaining watch items are now operational, not architectural:

- continue observing long-running queue convergence for `inflight/`
- optionally add `inflight` timeout reclaim in a future hardening pass
- keep worker alert email credentials and target address current

### V2 post-closeout gap found: reply/bounce intake was not live-wired (2026-03-23)

Operational review after V2 closeout found that the live VM was producing:

- `data/crm/send_logs.csv`

but not:

- `data/crm/reply_logs.csv`
- `data/crm/engagement_logs.csv`

Meaning:

- Workflow 7 send was live
- but Workflow 7.8 reply intelligence was not being executed on the VM
- therefore Gmail bounce messages visible in the inbox were not yet being
  converted into suppression state automatically

Hardening applied:

- added `scripts/run_reply_intelligence.py`
  - runs Workflow 7.8 reply intake
  - then runs Workflow 7.5 engagement aggregation
  - writes `data/reply_intelligence_status.json`
- added systemd units:
  - `reply-intelligence.service`
  - `reply-intelligence.timer`
- `deploy/gcp/update_vm.sh` now renders and installs all unit files under
  `deploy/gcp/systemd/` and enables `reply-intelligence.timer`

Result:

- bounce / reply suppression is now designed to run independently from the main
  cloud-send worker loop
- this avoids coupling inbound-reply ingestion to send timing

### Reply / bounce intake live validation completed (2026-03-23)

The previously missing live wiring for Workflow 7.8 / 7.5 has now been fully
validated on the VM.

Operational fixes applied during validation:

- `REPLY_INTELLIGENCE_OUR_EMAIL=wayne@try-omnisol.com` added to VM `.env`
- Gmail OAuth token re-authorized with both scopes:
  - `https://www.googleapis.com/auth/gmail.send`
  - `https://www.googleapis.com/auth/gmail.readonly`
- reply-intelligence write-permission issues fixed by restoring VM user
  ownership on `data/`
- `scripts/authorize_gmail.py` upgraded to support a manual two-step headless
  OAuth flow:
  - `--manual-start`
  - `--manual-finish --code ...`

Live validation result:

- `cloud-send-worker.service` recovered and returned to `active (running)`
- `reply-intelligence.timer` remained enabled and waiting normally
- manual run of `scripts/run_reply_intelligence.py` completed successfully
- `data/crm/reply_logs.csv` was created
- `data/crm/engagement_logs.csv` was created
- `data/reply_intelligence_status.json` reported:
  - `fetched = 15`
  - `matched = 3`
  - `unmatched = 12`
  - `reply_type bounce = 8`
  - `suppressed = 8`
  - `paused = 7`

Meaning:

- bounce / reply intake is no longer only "supported in code"
- it is now live on the VM
- Gmail bounce messages can now flow into reply intelligence and suppression
  state automatically

Current operational interpretation for bounce handling:

- send-time API success and later mailbox bounce remain separate events
- Workflow 7 sends the email
- Workflow 7.8 ingests later bounce/reply messages from Gmail
- matched bounces become suppression state
- subsequent automation can stop re-sending to those addresses

### Cloud Send Market Metadata Hydration (2026-03-24)

Two distinct failure modes were found behind queue-run cloud sends falling back
to `cloud_send_market=""` and `cloud_send_timezone="UTC"`:

1. Worker-side fallback gap:
   - some `final_send_queue.csv` rows carry no `city` / `country`
   - some queue-run folders also lack `data/runs/<campaign_id>/campaign_run_state.json`
   - `send_guard._resolve_location()` therefore used to exhaust its old fallback
     chain and silently default to `UTC`

2. Deploy-side metadata loss:
   - `scripts/deploy_run_to_gcloud.py` originally built manifests only from the
     run folder's `campaign_run_state.json`
   - older queue-run folders without that file therefore uploaded:
     - manifests with blank `city` / `region` / `country`
     - run folders with no run-scoped campaign metadata for worker fallback

Current hardening:

- `src/workflow_7_email_sending/send_guard.py` now resolves market context in
  this order:
  1. row-level `city` / `country`
  2. row-level `source_location`
  3. run-scoped `data/runs/<campaign_id>/campaign_run_state.json`
  4. global `data/campaign_run_state.json`
  5. `data/campaign_queue.json` match by `campaign_id`

- `scripts/deploy_run_to_gcloud.py` now:
  - reads `data/campaign_queue.json` by `campaign_id` when the run folder has
    no usable run-scoped state
  - hydrates a minimal `data/runs/<campaign_id>/campaign_run_state.json`
    before cloud upload
  - builds the cloud manifest from the hydrated config, not only from the
    pre-existing run-state file

Operational effect:

- queue-generated cloud deploys remain self-contained after upload to GCS
- worker-side market resolution no longer depends on the VM also having a local
  `campaign_queue.json` copy for older queue campaigns
- re-deployed queue runs can recover the correct country/city timezone instead
  of silently inheriting `UTC`

Related control-panel hardening:

- `src/workflow_9_5_streamlit_control_panel/ui_state.py` `_load_run_config()`
  now also falls back to `data/campaign_queue.json` by `campaign_id` when a run
  folder has no `campaign_run_state.json`.
- This closes a UI recovery bug where **Ready To Deploy** could misclassify old
  queue-generated `dry_run` campaigns as cloud-send eligible simply because the
  run folder lacked stored config.
- `load_ready_cloud_deploys()` now also falls back to the matching queue job's
  `status` when a run folder lacks `campaign_run_state.json`.
- This closes a second visibility bug where older queue-generated `gmail_api`
  runs could be genuinely completed and cloud-deployable, but still be hidden
  from **Ready To Deploy** only because the run folder lacked stored run-state.
- `load_ready_cloud_deploys()` now also cross-checks cloud state before showing
  `stale_handoff_redeploy` rows:
  - if local run state still says `queued` but the GCS run copy already reports
    `cloud_send_status=completed`, the row is hidden
  - if the worker mirror already lists the campaign in `completed_campaigns`,
    the row is hidden only when no newer live GCS `cloud_send_status.json`
    exists for that campaign
  - if the worker mirror currently lists the campaign as `active`, `claimed`,
    `selected`, or `waiting_window`, the row is hidden
  - if the worker mirror lists the campaign in `failed_campaigns`, the row is
    treated as a recovery candidate instead of a generic stale queued run
- This prevents already-processed cloud campaigns from reappearing in
  **Ready To Deploy** just because local run-scoped cloud-send JSON was stale.
- `load_delivery_ops_snapshot()` now follows the same cloud-first reconciliation
  rule for KPI dashboard counters:
  - local run folders still contribute deploy/send summaries
  - but stale local `cloud_send_status=queued` is overridden by:
    1. GCS `runs/<campaign_id>/cloud_send_status.json`
    2. worker mirror `completed_campaigns` / `failed_campaigns`
  - worker `completed_campaigns` only wins when there is no newer live GCS
    `cloud_send_status.json` for that campaign
- This prevents `Queued in Cloud` and related counters from showing inflated
  values after dry-run cleanup or after cloud worker has already processed older
  manifests while local run JSON remains stale.
- KPI Dashboard `Send Ops Snapshot` now explicitly captions that cloud queue
  counters are reconciled against live GCS / worker state, so operators can
  distinguish them from purely local run-folder summaries.
- `scripts/cloud_send_worker.py` now clears stale `last_wait_campaign_id` /
  `last_wait_due_at` whenever the queue is empty or no actionable manifests are
  present, so worker state mirrors no longer keep showing an old waiting-window
  campaign after invalid manifests have been removed.
- `src/workflow_9_5_streamlit_control_panel/ui_views.py` now applies a short
  auto-refresh pause after `Ready To Deploy` table selection changes:
  - selecting rows in `st.data_editor` stores the current selection signature in
    `st.session_state`
  - queue-panel fragment auto-refresh is suspended for a few seconds after the
  selection changes
  - purpose: reduce the “screen goes pale / temporarily unclickable” feeling
    while operators are ticking deploy rows during frequent Streamlit reruns
- Queue jobs table time display in `ui_views.py` now formats `started` /
  `finished` timestamps into the operator machine's local timezone instead of
  showing raw UTC queue-store strings.
- `Ready To Deploy` now explicitly warns that some `gmail_api` runs may already
  have local `cloud_deploy_status.json` because Workflow 9 can auto-trigger
  cloud handoff on completion:
  - trigger conditions live in `campaign_runner._should_auto_deploy()`
  - `send_mode` must be live (`gmail_api` / non-`dry_run`)
  - run must finish at `campaign_status`
  - `final_send_queue.csv` must exist
  - consequence: an operator can truthfully remember clicking only a small
    number of manual Deploy actions while local run folders still show many
    `cloud_deploy_status=completed` records from `auto_on_complete`
- Per-run cloud handoff control added:
  - `CampaignConfig` now carries `auto_cloud_deploy`
  - Streamlit Campaign Configuration shows `Auto Upload To Cloud After Completion`
  - `dry_run` forces this off in the UI
  - Single Run and queued Multiple Run jobs now persist the chosen value
  - queue duplicate detection now treats `(send_mode, run_until, auto_cloud_deploy)`
    as part of the operational uniqueness key
  - Workflow 9 auto handoff no longer depends only on the global env flag:
    per-run `auto_cloud_deploy=False` keeps a live run local until the operator
    manually uses **Ready To Deploy**
- Queue panel visibility upgrade:
  - the jobs table now shows `cloud_handoff`
  - values are:
    - `auto`    — queue job explicitly set to auto cloud handoff
    - `manual`  — queue job explicitly requires manual deploy
    - `legacy`  — older job created before the per-run handoff flag existed
    - `disabled` — `dry_run`, so cloud handoff is off
- KPI dashboard now includes **Cloud Deploy History / Reconciliation**:
  - data source merges:
    - local run-folder `cloud_deploy_status.json`
    - local / GCS `cloud_send_status.json`
    - queue metadata (`campaign_queue.json`)
    - worker mirror `cloud_send_worker_state.json`
  - each row separates:
    - `Cloud Handoff` (auto/manual/legacy)
    - `Deploy Status`
    - `Cloud Send`
    - `Reconciliation`
    - `Note`
    - `Emails In Final Queue` (campaign-level email count, distinct from the
      KPI card `Queued in Cloud`, which is a live cloud campaign count)
  - reconciliation labels currently include:
    - `manual_deploy`
    - `auto_on_complete`
    - `failed_recovery`
    - `currently_waiting`
    - `historical_inconsistent`
  - cloud-send wording in the UI is now intentionally human-readable:
    - `queued in cloud manifest backlog`
    - `claimed by cloud worker, waiting for send window`
    - `sending`
    - `completed`
  - the reconciliation section also shows this lifecycle explicitly so operators
    can understand that `claimed / waiting` is a later state than `queued`
  - purpose: stop operators from inferring cloud handoff history indirectly from
    stale local files or memory of which Deploy button was clicked
- Multi-run queue panel operator-clarity pass:
  - `ui_views._describe_queue_runner_phase()` now converts raw queue/scheduler
    state into plain-language phases such as:
    - `Queue paused`
    - `Runner alive, waiting to claim next job`
    - `Job claimed, pipeline starting`
    - `Running workflow steps`
    - `Runner idle`
  - the queue panel now shows an explicit lifecycle caption:
    - `Start Runner -> queued in scheduler -> claimed job / waiting for first workflow step -> running workflow step -> completed`
  - when a queue job has been claimed but `campaign_runner_logs.csv` has not yet
    written the first step, the progress area no longer looks empty; it shows a
    small placeholder progress bar plus explanatory text that this short gap is
    normal
  - when the runner process is alive and pending jobs exist but no job is yet
    marked `running`, the panel now says the runner is alive and waiting to
    claim the next pending job instead of looking like nothing happened after
    the operator clicked **Start Runner**
- Multi-run queue add compatibility fix:
  - `ui_views._add_jobs_to_queue()` now inspects the imported
    `queue_store.add_job()` signature before passing `auto_cloud_deploy`
  - purpose: avoid `TypeError: add_job() got an unexpected keyword argument
    'auto_cloud_deploy'` when Streamlit is running against an older in-memory
    queue-store implementation or mixed code state during iterative local
    development
  - if the queue-store function exposes `auto_cloud_deploy`, the value is
    passed normally; otherwise the UI falls back gracefully and still creates
    the queued jobs
  - additionally, when the imported `add_job()` does not expose
    `auto_cloud_deploy`, the UI now immediately calls `update_job()` to persist
    the operator's chosen auto/manual cloud handoff setting onto the queued job
  - motivation: without this second step, operators could truthfully tick
    **Auto Upload To Cloud After Completion** and still end up with queue jobs
    missing the field entirely, causing completed gmail_api runs to show
    `cloud_deploy_status=not_enabled`
- Multi-run Windows queue-write hardening:
  - `queue_store._save_raw()` now retries atomic replacement of
    `data/campaign_queue.json` for a short window before failing
  - motivation: on Windows, Streamlit and the detached queue runner can briefly
    contend for the queue file; `Path(tmp).replace(campaign_queue.json)` may
    raise `PermissionError: [WinError 5]`
  - without the retry, clicking **Start Runner** could appear to do nothing
    because the scheduler process started, hit the queue-file race while moving
    the first job to `running`, then exited immediately
- Queue -> Ready To Deploy sync fix:
  - `render_queue_panel()` already used a Streamlit fragment for live queue
    refresh, but `Ready To Deploy` lived outside that fragment in the normal
    dashboard render path
  - consequence: queue counts could update to `completed` while the deploy table
    still showed stale rows until the operator manually refreshed the whole page
  - `_render_queue_panel_content()` now snapshots the queue summary
    (`running/pending/completed/failed/total` plus current/next job ids) and
    triggers a full `st.rerun()` only when that snapshot changes
  - effect: when a multi-run job completes, the rest of the dashboard catches
    up automatically and newly completed campaigns appear in **Ready To Deploy**
    without requiring a manual page refresh
- Paused-queue resume UX hardening:
  - when the queue pause flag existed, operators still had to reason about two
    independent controls: whether the detached runner PID was alive and whether
    the queue itself was paused
  - the queue panel primary action now adapts to paused state:
    - paused + runner alive     -> `Resume Queue`
    - paused + runner stopped   -> `Resume Queue & Start Runner`
    - not paused + runner alive -> `Stop Runner`
    - not paused + runner dead  -> `Start Runner`
  - effect: after pausing and refreshing the page, operators can continue with
    one obvious primary action instead of getting stuck between Start and Resume
- Cloud-send suppress-window investigation (Brazil):
  - Investigated why `sao-paulo_20260324_063452_3b0e` reached the cloud send
    window but still ended with `sent=0, deferred=2`
  - VM-side `data/crm/send_logs.csv` showed both records were deferred with
    `deferred_same_company_domain_in_suppress_window`
  - Root cause: `workflow_7_email_sending.send_guard._root_domain()` used the
    last two labels only, so unrelated Brazilian domains like
    `incasolar.com.br` and `solargroup.com.br` both collapsed to `com.br`
  - Fixed `_root_domain()` to preserve the company label for known multi-part
    public suffixes such as `com.br`, `co.uk`, and `com.au`
  - Added regression test `tests/test_send_guard_domains.py` so distinct
    `.com.br` companies no longer suppress each other as if they were the same
    domain
- 2026-03-24: Added a first-pass Brazil language pack. Default Brazil runs now use Portuguese solar search keywords instead of the global English defaults; the active crawler prioritizes Brazil contact-page paths like `/contato`, `/fale-conosco`, and `/orcamento`; and Brazil guessed/generic mailbox handling now recognizes and prioritizes prefixes such as `contato`, `comercial`, `vendas`, `atendimento`, `orcamento`, and `suporte`. This touched `src/market_localization.py`, `campaign_config.py`, `keyword_generator.py`, `workflow_3_web_crawler/website_crawler.py`, `workflow_5_5_lead_enrichment/enricher.py`, `workflow_5_9_email_verification/email_verifier.py`, `workflow_6_email_generation/email_merge.py`, plus new regression tests in `tests/test_brazil_market_localization.py`.
- 2026-03-24: Ran a lightweight Brazil validation campaign locally for `Salvador, Bahia, Brazil` with `run_until=crawl` and small limits (`company_limit=5`, `crawl_limit=5`, `enrich_limit=5`) to verify the new localization before release.
  - campaign_id: `salvador_20260324_230957_f7a7`
  - `search_tasks.json` confirmed the generated queries are now Portuguese (`energia solar`, `empresa de energia solar`, `instalador solar`, `energia fotovoltaica`, etc.)
  - `company_pages.json` confirmed crawled pages were strongly local-language (`lang="pt-BR"`, titles like `Instalação de Energia Solar em Salvador`)
  - this validation was intentionally dry-run and stopped at `crawl`, so it did not enter enrichment, email generation, or send
- 2026-03-24: Extended the Brazil localization pack into Workflow 6 email generation so Brazil now defaults to Portuguese outreach without any UI change.
  - `src/workflow_6_email_generation/email_templates.py` now generates Portuguese rule-based subjects, greetings, opening lines, CTAs, and sign-off for `country=Brazil`
  - `src/workflow_6_email_generation/email_generator.py` now localizes greetings and injects a country-aware output-language instruction into the OpenRouter prompt, so AI-generated Brazil drafts are requested in `Brazilian Portuguese`
  - added regression coverage in `tests/test_brazil_market_localization.py` for:
    - Brazil default email language = `pt-BR`
    - Brazil rule-based drafts use Portuguese subject/body/sign-off
    - Brazil prompt localization requests `Brazilian Portuguese`
  - scope intentionally kept country-driven: UI remains unchanged; selecting Brazil is the trigger for Portuguese defaults
  - status note: this Workflow 6 email-language layer is implemented in the local workspace, but has not yet been pushed to GitHub or deployed to the VM in this pass
  - local verification note: no runnable local Python interpreter / virtualenv was available in the current shell, so this pass was verified by code inspection rather than executing pytest locally
- 2026-03-24: Pushed and deployed the Brazil email-generation localization to GitHub / VM.
  - GitHub commits:
    - `a1f01d2` — `Localize Brazil email generation to Portuguese`
    - `35af9bb` — `Recognize Brazil generic mailbox prefixes`
  - VM updated via `bash deploy/gcp/update_vm.sh`, and `cloud-send-worker` was restarted successfully
  - VM regression result:
    - `./.venv/bin/pytest tests/test_brazil_market_localization.py -q`
    - result: `7 passed`
  - VM sample generation result confirmed the deployed Brazil output is Portuguese:
    - subject: `Pergunta rápida sobre suas instalações`
    - body opened with `Olá equipe da Marsol Energia Solar,`
    - prompt localization resolved to `Brazilian Portuguese`
  - note: a full Workflow 9 Brazil dry-run on the VM was not usable for this verification because `GOOGLE_MAPS_API_KEY` was not available in that shell context, so the final runtime check was done directly against Workflow 6 on the VM
- 2026-03-25: Resolved the Workflow 9 auto-cloud-handoff configuration conflict.
  - decision: keep `CLOUD_SEND_ENABLED` as the global capability gate; do **not** let a per-run `auto_cloud_deploy=true` bypass it
  - `campaign_runner._should_auto_deploy()` continues to require `CLOUD_SEND_ENABLED=true`
  - `ui_views.py` now disables **Auto Upload To Cloud After Completion** whenever:
    - `send_mode == dry_run`, or
    - `CLOUD_SEND_ENABLED` is off in the local environment
  - `ui_config.build_campaign_config()` now also forces `auto_cloud_deploy=False` when `CLOUD_SEND_ENABLED` is off, so stale widget state cannot leak an impossible `auto` setting into completed runs
  - result: operators no longer see the contradictory state where the UI says `auto` but the run later lands as `cloud_deploy_status=not_enabled`
- 2026-03-25: Added low-risk Streamlit control-panel speedups for queue/deploy interactions.
  - `ui_state._read_json_from_gcs()` now uses a short in-memory TTL cache (10s) so repeated reruns do not shell out to `gcloud storage cat` on every button click
  - `ui_state._read_run_json_from_gcs()` now also uses a short TTL cache for per-run cloud status reads
  - `ui_state._count_csv()` now uses fast line counting first, which avoids fully parsing CSV files just to show queue counts in `Ready To Deploy` / reconciliation views
  - intent: reduce the long white-screen / frozen feeling when clicking `Start Runner`, selecting deploy rows, and re-rendering cloud reconciliation sections
- 2026-03-25: KPI Dashboard is now hidden by default in the Streamlit control panel.
  - `app.py` no longer renders `render_kpi_dashboard()` on every page load
  - operators must explicitly enable **Show KPI Dashboard** to render KPI metrics
  - intent: cut down unnecessary file reads and cloud-status reconciliation work during queue/deploy operations, since KPI was not critical for short-term operations
- 2026-03-25: Local development `.env` now explicitly sets `CLOUD_SEND_ENABLED=true`.
  - purpose: when `send_mode=gmail_api` and an operator selects **Auto Upload To Cloud After Completion**, the local Streamlit control panel can actually enable the auto-handoff checkbox instead of hard-disabling it behind the global gate
  - `dry_run` remains protected: dry-run runs still cannot auto-upload because both the UI and Workflow 9 runtime continue to block cloud handoff for dry-run campaigns
- 2026-03-25: Low-frequency lower-page panels are now hidden by default behind **Show Advanced Panels**.
  - moved off the default render path:
    - `Manual Review Queue`
    - `Runner Logs`
    - `High Priority Leads`
    - `Company Lifecycle Detail`
    - `Manual Action: Send followup_1`
    - `Campaign Status Table`
    - `Pipeline / Enhanced File Status`
  - intent: keep the main control surface focused on configuration, queue, current state, and deploy actions, while reducing rerender cost during normal operations
- 2026-03-25: Queue progress feedback was simplified from a live progress bar to lightweight text status.
  - `ui_views.py` no longer renders `st.progress(...)` for the active queue job on every refresh
  - the queue panel now shows a compact text-only workflow-step label such as `Workflow step: 3/12 scrape`
  - intent: keep operators informed about the current pipeline stage while reducing rerender cost and the slow/white-flash feeling during queue refreshes
- 2026-03-25: Implemented and deployed cloud-worker send-capacity scheduling so inbox throttling is visible and enforceable at the worker layer instead of being implicit inside a single Workflow 7 batch.
  - new cloud settings were added in `config/settings.py`:
    - `CLOUD_SEND_INBOX_DAILY_LIMIT`
    - `CLOUD_SEND_INBOX_HOURLY_LIMIT`
    - `CLOUD_SEND_SKIP_WEEKENDS`
    - `CLOUD_SEND_CAP_TIMEZONE`
  - `send_pipeline.run(...)` now accepts optional `daily_limit_override` / `hourly_limit_override` values and records:
    - `processed`
    - `remaining_unprocessed`
    - `stopped_daily_limit`
    - `stopped_hourly_limit`
  - `scripts/auto_send_runs._run_campaign_send(...)` now returns `(send_summary, status_summary)` and forwards the per-run cap overrides down into Workflow 7
  - `scripts/cloud_send_worker.py` now:
    - calculates an inbox-capacity snapshot from global CRM `send_logs.csv`
    - tracks sent-today / remaining-today / sent-last-hour / remaining-this-hour in worker state
    - tracks live waiting email counts across inflight + manifest backlog
    - keeps weekend sending disabled through the worker-level scheduler when `CLOUD_SEND_SKIP_WEEKENDS=true`
    - prepares to leave inflight campaigns in place when a send batch hits the daily/hourly cap instead of immediately treating the campaign as fully completed
  - `ui_state.load_cloud_worker_health()` now exposes the new worker-capacity fields to the UI
  - `render_campaign_state_view()` now shows a lightweight always-visible `Cloud Send Capacity` block with:
    - inbox sent today
    - remaining today
    - sent last hour
    - remaining this hour
    - live cloud emails waiting / backlog / inflight
    - next capacity slot / carryover email count
  - deployed status:
    - GitHub / VM commit: `29c0e21` (`Add cloud send capacity scheduling and worker capacity UI`)
    - VM updated via `bash deploy/gcp/update_vm.sh`
    - `cloud-send-worker` restarted successfully and now exposes the new capacity fields in `data/cloud_send_worker_state.json`
    - live worker snapshot after deployment confirmed:
      - `inbox_daily_cap = 50`
      - `inbox_hourly_cap = 20`
      - `weekend_sending_enabled = false`
      - live waiting/backlog email counters are now populated (`last_live_email_count`, `last_manifest_email_count`, `last_inflight_email_count`, `last_carryover_email_count`)
  - local validation note: the current Windows shell still did not expose a usable Python entry point for local compile/test execution, so final validation for this pass was completed on the VM through deployment + live worker-state inspection
- 2026-03-25: Fixed Workflow 9 auto-deploy reliability so auto-upload no longer depends on a silent detached subprocess.
  - root cause we confirmed: `campaign_runner._trigger_cloud_deploy()` was launching `deploy_run_to_gcloud.py` via detached `subprocess.Popen(..., stdout=DEVNULL, stderr=DEVNULL)`, so startup failures left campaigns stuck at `cloud_deploy_status = pending`
  - fix:
    - `campaign_runner._trigger_cloud_deploy()` now calls `deploy_run(...)` synchronously inside Workflow 9
    - auto-upload now either finishes immediately into cloud handoff, or fails visibly and lands in `cloud_deploy_status = failed`
  - supporting consistency note:
    - `queue_runner._job_to_config(...)` carries `auto_cloud_deploy`
    - campaign-state config serialization keeps `auto_cloud_deploy` visible in run state
- 2026-03-25: Clarified and fixed the cloud-worker send-window behavior so deploy time and send time are separated correctly.
  - operator expectation confirmed: runs should be deployable to cloud at **any** time; cloud worker should be responsible for waiting until the correct market-local send window before entering Workflow 7
  - root issue:
    - rows were still reaching Workflow 7 outside the Brazil market window
    - Workflow 7 then logged them as `deferred` with `Outside target-market send window ...`
    - this mixed up *deploy accepted* with *send eligible now*
  - fix in `scripts/cloud_send_worker.py`:
    - added a pre-send partition step over each run's `final_send_queue.csv`
    - rows that are eligible **right now** are separated from rows still waiting for the next local send window
    - if no rows are currently eligible, the worker keeps the campaign in `waiting_window` and does **not** enter Workflow 7
    - if some rows are eligible and some are not:
      - only the eligible subset is sent now
      - the waiting subset stays in `final_send_queue.csv` for a later cloud retry
    - market-window waits and inbox-capacity waits are now merged by choosing the later due time when both apply
  - intended result:
    - deploy anytime
    - cloud queues anytime
    - send only when market-local window is open
    - rows should no longer be deferred **just because** they were processed outside the target-market send window
- 2026-03-25: Fixed a UI counting bug that made `Queue` / `Emails In Final Queue` look much larger than reality.
  - root cause: `ui_state._count_csv()` was counting raw file lines
  - `final_send_queue.csv` and similar files contain multi-line email bodies, so one CSV record could span many text lines
  - result: queue counts like `49`, `72`, `108`, `117` were inflated and did not match actual sendable record counts
  - fix: `ui_state._count_csv()` now counts parsed CSV records via `csv.DictReader`
  - effect: Ready To Deploy / reconciliation queue counts should now align with actual email record counts and with `send_batch_summary.total`
- 2026-03-25: Increased default daily email caps from `50` to `100`.
  - updated `DAILY_EMAIL_LIMIT` default to `100`
  - updated `CLOUD_SEND_INBOX_DAILY_LIMIT` fallback to `100`
  - updated `.env.example` to document the new `100/day` default
- 2026-03-25: Hardened cloud manifest loading against UTF-8 BOM.
  - root cause: at least one manifest uploaded to GCS contained a Windows-style UTF-8 BOM
  - symptom: worker alert `manifest_load_failed` with `Unexpected UTF-8 BOM (decode using utf-8-sig)`
  - fix: `cloud_send_worker._download_manifest()` now reads manifests with `encoding=\"utf-8-sig\"`
