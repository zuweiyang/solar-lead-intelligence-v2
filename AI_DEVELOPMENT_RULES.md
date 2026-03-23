# AI_DEVELOPMENT_RULES.md
# Rules for Claude Code when developing in this repository.
# Read this file alongside PROJECT_CONTEXT.md before writing any code.

---

## CODING STYLE

### General
- Python 3.10+. Use built-in type hints (`list[dict]`, `str | None`, not `List`, `Optional`).
- Each module has one responsibility. Do not mix scraping, parsing, and persistence in one file.
- Functions should do one thing. If a function needs a long docstring to explain what it does, split it.
- No classes unless state genuinely needs to persist across calls. Prefer plain functions + dicts.
- Keep files short. If a file exceeds ~150 lines, consider splitting it.

### Naming
- Files and functions: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- No abbreviations except established ones (`url`, `id`, `csv`, `html`, `ai`).

### Imports
- Standard library first, then third-party, then local — one blank line between each group.
- Always import from `config.settings` for file paths and environment values. Never hardcode paths.

### Error handling
- Catch exceptions at the boundary of external calls (HTTP, file I/O, API).
- Log the error with context (`[Workflow N] Error doing X for Y: {exc}`) and continue.
- Never swallow exceptions silently with a bare `except: pass`.
- Do not add try/except around internal logic that cannot fail.

### Comments
- Only comment non-obvious logic. Do not comment what the code already says.
- Use `# TODO:` for unimplemented stubs. Include what needs to be done and why.

### Data
- All intermediate data files live in `data/`. Never write outside this directory.
- Use `config.settings` path constants (`RAW_LEADS_FILE`, `COMPANY_TEXT_FILE`, etc.) — never construct paths inline.
- JSON files use `indent=2` and `ensure_ascii=False`.
- CSV files always include a header row. Use `csv.DictWriter` with explicit `fieldnames`.

---

## TESTING RULES

### Structure
- Every workflow has a corresponding smoke test script in `scripts/test_workflowN_*.py`.
- Test scripts are standalone: they import from `src/` and `config/`, run end-to-end, and print a clear pass/fail summary.
- Test scripts exit with code `1` on failure (`sys.exit(1)`), `0` on success.

### What every test script must do
1. Run the workflow function(s) being tested.
2. Print a count of records produced.
3. Validate required fields are present and non-empty.
4. Print the first 3–5 result records for manual inspection.
5. Print a final summary line: `Workflow N smoke test completed successfully.`

### Limits during testing
- Workflow 2 (scraper): use a single search task (`data/search_tasks.json` with one entry).
- Workflow 3 (crawler): cap at `limit=50` leads.
- Workflow 4 (AI analysis): cap at 5 companies to control token costs.
- Never run the full keyword × location matrix during a smoke test.

### What tests must NOT do
- Do not modify production data files permanently during a test (use the test limit).
- Do not send real emails during testing.
- Do not assert on exact content — websites change. Assert on structure (fields exist, types correct).

### Running tests
```bash
py scripts/test_workflow2_scraper.py
py scripts/test_data_cleaner.py
py scripts/test_workflow3_crawler.py
```

---

## API USAGE LIMITS

### Google Places API

| Call type | Trigger | Unit cost (approx) |
|---|---|---|
| Text Search | 1 per search task | $0.032 |
| Place Details | Only when `website` or `phone` missing | $0.017 |

**Rules:**
- Never call Place Details unconditionally. Check Text Search result first.
- 1 search task = up to 3 pages × 20 results = max 60 companies.
- Full pipeline (216 tasks) estimated cost: ~$25/run. Confirm before scaling.
- During smoke tests: use exactly **1 search task**.

### AI APIs (OpenAI / Anthropic)

| Workflow | Usage | Cost control |
|---|---|---|
| Workflow 4 — Company Analysis | 1 call per company | Cap at 5 companies during tests |
| Workflow 6 — Email Generation | 1 call per qualified lead | Only run on grade A/B leads |

**Rules:**
- Always truncate company text to ≤ 5000 characters before sending to AI (done in `content_extractor.py`).
- Use the cheapest model that produces acceptable output. Do not default to the most powerful model.
- Log token usage when available. Add usage tracking before scaling to full pipeline.
- Never call AI APIs in a loop without a rate-limit delay (`time.sleep(0.5)` minimum).

### Email Sending (SMTP)

| Limit | Value |
|---|---|
| Daily send limit | 50 emails |
| Send window | 08:00 – 17:00 local time |
| Delay between sends | 30 seconds |

**Rules:**
- Never exceed the daily limit. Check `email_logs.csv` before each batch.
- Do not send outside the configured send window.
- Test email sending with a dummy address before running on real leads.

### General API rules
- All API keys live in `.env`. Never hardcode a key anywhere in source code.
- If an API call fails, log the error and continue — do not retry in a tight loop.
- Add `time.sleep()` between calls that hit rate limits (Google Maps: 0.1 s between Detail calls).

---

## END OF FILE
