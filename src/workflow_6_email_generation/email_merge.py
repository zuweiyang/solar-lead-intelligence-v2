# Workflow 6: Email Generation — Lead Merger
# Merges enriched_leads.csv with research_signals.json by place_id → website → company_name.

import csv
import json

import tldextract

from config.settings import (
    CAMPAIGN_RUN_STATE_FILE,
    SEARCH_TASKS_FILE,
    ENRICHED_LEADS_FILE,
    VERIFIED_ENRICHED_LEADS_FILE,
    RESEARCH_SIGNALS_FILE,
    COMPANY_OPENINGS_FILE,
    SCORED_CONTACTS_FILE,
)


def _normalize_url(url: str) -> str:
    ext = tldextract.extract(url or "")
    return f"{ext.domain}.{ext.suffix}".lower() if ext.domain else ""


def _company_key(record: dict) -> str:
    """Build a stable company key across lead/contact files."""
    place_id = (record.get("place_id") or "").strip()
    if place_id:
        return f"pid:{place_id}"
    website = _normalize_url(record.get("website", ""))
    if website:
        return f"web:{website}"
    name = (record.get("company_name") or "").strip().lower()
    return f"name:{name}" if name else ""


def _as_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() == "true"


_GENERIC_LOCALPARTS: tuple[str, ...] = (
    "info", "sales", "contato", "comercial", "office",
    "atendimento", "support", "contact", "hello", "ola",
)


def _is_generic_email(email: str) -> bool:
    email = (email or "").strip().lower()
    if "@" not in email:
        return False
    local = email.split("@", 1)[0]
    return local in _GENERIC_LOCALPARTS


def _contact_rank_value(row: dict) -> tuple[int, int]:
    """Sort lower priority rank first, then higher fit score first."""
    try:
        rank = int(row.get("contact_priority_rank") or row.get("contact_rank") or 999)
    except (TypeError, ValueError):
        rank = 999
    try:
        fit = int(float(row.get("contact_fit_score") or 0))
    except (TypeError, ValueError):
        fit = 0
    return rank, -fit


def _is_sendable_contact(row: dict) -> bool:
    """
    Decide whether a contact is usable for routing.

    Priority:
    - explicit email_sendable=true
    - verification outcome is not a hard block
    """
    email = (row.get("kp_email") or "").strip()
    if not email or "@" not in email:
        return False

    if _as_bool(row.get("email_sendable", "")):
        return True

    eligibility = (row.get("send_eligibility") or "").strip().lower()
    return eligibility in {"allow", "allow_limited", "generic_pool_only"}


def _is_trusted_contact(row: dict) -> bool:
    return (row.get("contact_trust") or "").strip().lower() == "trusted"


def _has_relevant_title(title: str) -> bool:
    lower = (title or "").strip().lower()
    if not lower:
        return False
    strong_fragments = (
        "owner", "founder", "partner", "ceo", "chief", "director",
        "manager", "head", "procurement", "purchasing", "commercial",
        "operations", "project", "engineering",
    )
    return any(fragment in lower for fragment in strong_fragments)


# ---------------------------------------------------------------------------
# Junk / platform email detection
# ---------------------------------------------------------------------------

# Domains that are never real business contact emails — only website-builder
# artifact addresses, filler placeholders, or CMS-level auto-addresses.
_JUNK_EMAIL_DOMAINS: frozenset[str] = frozenset({
    "wix.com",
    "godaddy.com",
    "squarespace.com",
    "weebly.com",
    "jimdo.com",
    "yola.com",
    "webnode.com",
    "site123.com",
    "strikingly.com",
})

# Localpart patterns that indicate a filler/placeholder address.
# Checked as prefix or exact match against the part before @.
_JUNK_LOCALPART_EXACT: frozenset[str] = frozenset({
    "filler",
    "placeholder",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
    "mailer-daemon",
    "postmaster",
    "abuse",
    "webmaster",
})

_JUNK_LOCALPART_PREFIX: tuple[str, ...] = (
    "wixofday",   # Wix contact-form forwarding artifacts
    "filler",
)


def is_junk_email(email: str) -> tuple[bool, str]:
    """
    Return (is_junk, reason).

    Detects website-builder placeholders, CMS filler addresses, and
    no-reply / postmaster / abuse accounts that are never real contacts.

    Returns (False, "") for valid-looking business emails.
    """
    if not email or "@" not in email:
        return False, ""

    local, _, domain = email.lower().strip().partition("@")
    domain = domain.strip()
    local  = local.strip()

    if domain in _JUNK_EMAIL_DOMAINS:
        return True, f"junk_domain:{domain}"

    if local in _JUNK_LOCALPART_EXACT:
        return True, f"junk_localpart:{local}"

    for prefix in _JUNK_LOCALPART_PREFIX:
        if local.startswith(prefix):
            return True, f"junk_localpart_prefix:{prefix}"

    return False, ""


def _parse_services(raw: str) -> list[str]:
    """CSV stores services_detected as semicolon-separated string."""
    return [s.strip() for s in raw.split(";") if s.strip()] if raw else []


def _campaign_location_defaults() -> dict[str, str]:
    """Return fallback location fields from campaign_run_state.json."""
    if SEARCH_TASKS_FILE.exists():
        try:
            with open(SEARCH_TASKS_FILE, encoding="utf-8") as f:
                tasks = json.load(f)
            if isinstance(tasks, list) and tasks:
                location = (tasks[0].get("location") or "").strip()
                parts = [part.strip() for part in location.split(",") if part.strip()]
                city = parts[0] if parts else ""
                country = parts[-1] if len(parts) >= 2 else ""
                region = parts[1] if len(parts) >= 3 else ""
                return {
                    "city": city,
                    "region": region,
                    "country": country,
                    "source_location": location,
                }
        except Exception:
            pass

    if not CAMPAIGN_RUN_STATE_FILE.exists():
        return {"city": "", "region": "", "country": "", "source_location": ""}
    try:
        with open(CAMPAIGN_RUN_STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return {"city": "", "region": "", "country": "", "source_location": ""}

    cfg = state.get("config", {}) if isinstance(state, dict) else {}
    city = (cfg.get("base_city") or cfg.get("city") or "").strip()
    region = (cfg.get("region") or "").strip()
    country = (cfg.get("country") or "").strip()
    parts = [part for part in (city, region, country) if part]
    return {
        "city": city,
        "region": region,
        "country": country,
        "source_location": ", ".join(parts),
    }


def load_enriched_leads(limit: int = 0) -> list[dict]:
    # Prefer verified_enriched_leads.csv when Workflow 5.9 has run for this campaign
    source_path = (
        VERIFIED_ENRICHED_LEADS_FILE
        if VERIFIED_ENRICHED_LEADS_FILE.exists()
        else ENRICHED_LEADS_FILE
    )
    with open(source_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[:limit] if limit else rows


def load_research_signals() -> list[dict]:
    if not RESEARCH_SIGNALS_FILE.exists():
        print("[Workflow 6] research_signals.json not found — signals will be empty.")
        return []
    with open(RESEARCH_SIGNALS_FILE, encoding="utf-8") as f:
        return json.load(f)


def load_company_openings() -> dict[str, dict]:
    """
    Load company_openings.json written by Workflow 6.2.
    Returns {normalized_company_name → {"opening_line": ..., "best_signal": ..., "signal_facts": ...}}.
    signal_facts is a structured whitelist used by the generator/rewriter to prevent hallucination.
    """
    if not COMPANY_OPENINGS_FILE.exists():
        return {}
    with open(COMPANY_OPENINGS_FILE, encoding="utf-8") as f:
        records = json.load(f)
    return {
        (r.get("company_name") or "").strip().lower(): {
            "opening_line": r.get("opening_line", ""),
            "best_signal":  r.get("best_signal",  ""),
            "signal_facts": r.get("signal_facts",  {}),
        }
        for r in records
        if r.get("opening_line")
    }


def load_scored_contacts() -> dict[str, list[dict]]:
    """
    Load scored_contacts.csv and group contacts per company.

    Used to make the send-target decision at company level:
    named first, generic only as fallback.
    """
    if not SCORED_CONTACTS_FILE.exists():
        return {}

    with open(SCORED_CONTACTS_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        key = _company_key(row)
        if not key:
            continue
        grouped.setdefault(key, []).append(row)

    for key, contacts in grouped.items():
        grouped[key] = sorted(contacts, key=_contact_rank_value)

    return grouped


def _fallback_contact_from_lead(lead: dict) -> dict | None:
    """Fallback when scored_contacts.csv is unavailable for this run."""
    email = (lead.get("kp_email") or "").strip()
    if not email or "@" not in email:
        return None

    is_generic = _as_bool(lead.get("is_generic_mailbox", "")) or _is_generic_email(email)
    if is_generic:
        return {
            "kp_name":              "",
            "kp_title":             "",
            "kp_email":             email,
            "enrichment_source":    lead.get("enrichment_source", ""),
            "email_confidence_tier": lead.get("email_confidence_tier", ""),
            "send_eligibility":     lead.get("send_eligibility", ""),
            "send_pool":            lead.get("send_pool", ""),
            "contact_trust":        lead.get("contact_trust", ""),
            "send_target_type":     "generic",
            "contact_source":       "fallback",
            "named_contact_available": "false",
            "generic_contact_available": "true",
            "contact_quality":      "generic_only",
            "generic_only":         "true",
        }

    if not (lead.get("kp_name") or "").strip():
        return None

    quality = (
        "strong_named"
        if _is_trusted_contact(lead) and _has_relevant_title(lead.get("kp_title", ""))
        else "weak_named"
    )
    return {
        "kp_name":              lead.get("kp_name", ""),
        "kp_title":             lead.get("kp_title", ""),
        "kp_email":             email,
        "enrichment_source":    lead.get("enrichment_source", ""),
        "email_confidence_tier": lead.get("email_confidence_tier", ""),
        "send_eligibility":     lead.get("send_eligibility", ""),
        "send_pool":            lead.get("send_pool", ""),
        "contact_trust":        lead.get("contact_trust", ""),
        "send_target_type":     "named",
        "contact_source":       "fallback",
        "named_contact_available": "true",
        "generic_contact_available": "false",
        "contact_quality":      quality,
        "generic_only":         "false",
    }


def _route_contact(lead: dict, contacts_by_company: dict[str, list[dict]]) -> dict | None:
    """
    Select exactly one first-touch send target per company.

    Rules:
    - named contact wins when available
    - generic mailbox is used only when no named contact is usable
    - no usable contact => company is not queued for email generation
    """
    key = _company_key(lead)
    contacts = contacts_by_company.get(key, []) if key else []

    named_contacts = [
        row for row in contacts
        if (row.get("kp_name") or "").strip()
        and (row.get("kp_email") or "").strip()
        and not (_as_bool(row.get("is_generic_mailbox", "")) or _is_generic_email(row.get("kp_email", "")))
        and _is_sendable_contact(row)
    ]
    generic_contacts = [
        row for row in contacts
        if (row.get("kp_email") or "").strip()
        and (_as_bool(row.get("is_generic_mailbox", "")) or _is_generic_email(row.get("kp_email", "")))
        and _is_sendable_contact(row)
    ]

    named_available = bool(named_contacts)
    generic_available = bool(generic_contacts)

    if named_contacts:
        trusted_named = [row for row in named_contacts if _is_trusted_contact(row)]
        chosen = trusted_named[0] if trusted_named else named_contacts[0]
        quality = (
            "strong_named"
            if _is_trusted_contact(chosen) and _has_relevant_title(chosen.get("kp_title", ""))
            else "weak_named"
        )
        return {
            "kp_name":                  chosen.get("kp_name", ""),
            "kp_title":                 chosen.get("kp_title", ""),
            "kp_email":                 chosen.get("kp_email", ""),
            "enrichment_source":        chosen.get("enrichment_source", ""),
            "email_confidence_tier":    chosen.get("email_confidence_tier", ""),
            "send_eligibility":         chosen.get("send_eligibility", ""),
            "send_pool":                chosen.get("send_pool", ""),
            "contact_trust":            chosen.get("contact_trust", ""),
            "send_target_type":         "named",
            "contact_source":           "kp",
            "named_contact_available":  "true",
            "generic_contact_available": "true" if generic_available else "false",
            "contact_quality":          quality,
            "generic_only":             "false",
        }

    if generic_contacts:
        chosen = generic_contacts[0]
        return {
            "kp_name":                  "",
            "kp_title":                 chosen.get("kp_title", ""),
            "kp_email":                 chosen.get("kp_email", ""),
            "enrichment_source":        chosen.get("enrichment_source", ""),
            "email_confidence_tier":    chosen.get("email_confidence_tier", ""),
            "send_eligibility":         chosen.get("send_eligibility", ""),
            "send_pool":                chosen.get("send_pool", ""),
            "contact_trust":            chosen.get("contact_trust", ""),
            "send_target_type":         "generic",
            "contact_source":           "generic",
            "named_contact_available":  "false",
            "generic_contact_available": "true",
            "contact_quality":          "generic_only",
            "generic_only":             "true",
        }

    return _fallback_contact_from_lead(lead)


def _send_tier(target_tier: str, enrichment_source: str, kp_email: str) -> str:
    """
    Compute an operational send tier from classification strength and contact quality.

    A  — high-confidence type + named contact (apollo/hunter): send first
    B1 — high-confidence type + site contact, OR secondary type + named contact
    B2 — high-confidence type + guessed, secondary type + site, OR weak type + named
    C  — weak contact on secondary/weak type, OR no valid email: hold for review
    """
    has_email = bool(kp_email and "@" in kp_email)
    if not has_email:
        return "C"

    named   = enrichment_source in {"apollo", "hunter"}
    site    = enrichment_source in {"website"}

    if target_tier == "A":
        if named: return "A"
        if site:  return "B1"
        return "B2"    # guessed email, high-confidence type

    if target_tier == "B":
        if named: return "B1"
        if site:  return "B2"
        return "C"     # guessed email on secondary type

    # target_tier "C" or unknown
    if named: return "B2"
    return "C"


def _derive_email_angle(company_type: str, send_tier: str = "") -> str:
    """
    Derive a specific email angle from company_type, with tier override.

    Tier C always produces cautious_outreach — prevents over-personalizing leads
    with weak classification confidence or contact signals.

    Angle values consumed by email_generator.py's system prompt:
      project_delivery   — EPC / contractor focus: project execution, materials, procurement
      installation       — Installer / panel installer: volume, mounting systems
      storage_integration — BESS / battery storage: hybrid projects, storage deployment
      distributor_supply — Component distributor: supply chain, product range, channel
      project_pipeline   — Developer / farm developer: project pipeline, EPC partnerships
      general_solar      — Broad solar energy company: installation or project work
      cautious_outreach  — Tier C: softer, less specific, no forced personalization
    """
    if send_tier == "C":
        return "cautious_outreach"
    ct = (company_type or "").lower()
    if "epc" in ct or "contractor" in ct:
        return "project_delivery"
    if "battery" in ct or "bess" in ct or "storage" in ct:
        return "storage_integration"
    if "distributor" in ct:
        return "distributor_supply"
    if "developer" in ct or "farm" in ct:
        return "project_pipeline"
    if "installer" in ct or "panel" in ct:
        return "installation"
    return "general_solar"


def merge_leads(limit: int = 0) -> list[dict]:
    """
    Merge enriched leads with research signals.
    Primary key: place_id. Fallbacks: normalized website, company_name.
    Returns list of merged records ready for email generation.
    """
    leads    = load_enriched_leads(limit=limit)
    signals  = load_research_signals()
    openings = load_company_openings()   # Workflow 6.2 output
    contacts_by_company = load_scored_contacts()
    location_defaults = _campaign_location_defaults()

    # Build lookup maps for signals
    by_place_id: dict[str, dict] = {}
    by_website:  dict[str, dict] = {}
    by_name:     dict[str, dict] = {}
    for sig in signals:
        if sig.get("place_id"):
            by_place_id[sig["place_id"]] = sig
        url = _normalize_url(sig.get("website", ""))
        if url:
            by_website[url] = sig
        name_key = (sig.get("company_name") or "").lower()
        if name_key:
            by_name[name_key] = sig

    skipped_mismatch = 0
    skipped_no_target = 0
    named_targets = 0
    generic_targets = 0
    merged: list[dict] = []
    seen_place_ids: set[str] = set()

    for lead in leads:
        # Skip leads where contact domain didn't match company website (Apollo false match)
        if lead.get("skip_reason") == "contact_domain_mismatch":
            skipped_mismatch += 1
            print(
                f"[Workflow 6] Skipping {lead.get('company_name', '?')} — "
                f"contact_domain_mismatch ({lead.get('kp_email', '')})"
            )
            continue

        # Skip E0 contacts — invalid / undeliverable addresses must not enter send flow
        if lead.get("email_confidence_tier") == "E0":
            skipped_mismatch += 1  # reuse counter; logged distinctly
            print(
                f"[Workflow 6] Skipping {lead.get('company_name', '?')} — "
                f"email_confidence_tier=E0 ({lead.get('kp_email', '')})"
            )
            continue

        # Skip junk / platform / template emails — website-builder artifacts that
        # are never real decision-maker addresses (e.g. wixofday@wix.com, filler@godaddy.com).
        kp_email_raw = lead.get("kp_email", "")
        junk, junk_reason = is_junk_email(kp_email_raw)
        if junk:
            skipped_mismatch += 1
            print(
                f"[Workflow 6] Skipping {lead.get('company_name', '?')} — "
                f"junk_email ({junk_reason}): {kp_email_raw}"
            )
            continue

        place_id = lead.get("place_id", "")

        # Dedup by place_id
        if place_id and place_id in seen_place_ids:
            continue
        if place_id:
            seen_place_ids.add(place_id)

        routed_contact = _route_contact(lead, contacts_by_company)
        if not routed_contact:
            skipped_no_target += 1
            print(
                f"[Workflow 6] Skipping {lead.get('company_name', '?')} — "
                "no usable named or generic contact"
            )
            continue

        # Find matching signal record
        sig = (
            by_place_id.get(place_id)
            or by_website.get(_normalize_url(lead.get("website", "")))
            or by_name.get((lead.get("company_name") or "").lower())
            or {}
        )

        company_type  = lead.get("company_type", "")
        target_tier   = lead.get("target_tier", "")
        enrich_source = routed_contact.get("enrichment_source", lead.get("enrichment_source", ""))
        kp_email      = routed_contact.get("kp_email", lead.get("kp_email", ""))

        send_tier = _send_tier(target_tier, enrich_source, kp_email)

        # Prefer signal-derived angle; fall back to type+tier-derived angle
        sig_angle   = sig.get("email_angle", "")
        email_angle = sig_angle if sig_angle and sig_angle != "General solar outreach" \
                      else _derive_email_angle(company_type, send_tier)

        # opening_line + best_signal: from Workflow 6.2 output
        company_key  = (lead.get("company_name") or "").strip().lower()
        opening_data = openings.get(company_key, {})
        opening_line = opening_data.get("opening_line", "")
        best_signal  = opening_data.get("best_signal",  "")
        signal_facts = opening_data.get("signal_facts",  {})

        merged.append({
            "company_name":         lead.get("company_name", ""),
            "website":              lead.get("website", ""),
            "place_id":             place_id,
            "city":                 lead.get("city", "") or location_defaults["city"],
            "region":               lead.get("region", "") or location_defaults["region"],
            "country":              lead.get("country", "") or location_defaults["country"],
            "source_location":      lead.get("source_location", "") or location_defaults["source_location"],
            "company_type":         company_type,
            "market_focus":         lead.get("market_focus", ""),
            "services_detected":    _parse_services(lead.get("services_detected", "")),
            "confidence_score":     float(lead.get("confidence_score") or 0),
            "lead_score":           int(lead.get("lead_score") or 0),
            "target_tier":          target_tier,
            "send_tier":            send_tier,
            "kp_name":              routed_contact.get("kp_name", ""),
            "kp_title":             routed_contact.get("kp_title", ""),
            "kp_email":             routed_contact.get("kp_email", ""),
            "enrichment_source":    routed_contact.get("enrichment_source", enrich_source),
            "contact_name":         routed_contact.get("kp_name", ""),
            "contact_title":        routed_contact.get("kp_title", ""),
            "contact_email":        routed_contact.get("kp_email", ""),
            "send_target_type":     routed_contact.get("send_target_type", ""),
            "contact_source":       routed_contact.get("contact_source", ""),
            "named_contact_available": routed_contact.get("named_contact_available", "false"),
            "generic_contact_available": routed_contact.get("generic_contact_available", "false"),
            "contact_quality":      routed_contact.get("contact_quality", "none"),
            "generic_only":         routed_contact.get("generic_only", "false"),
            "recent_signals":       sig.get("recent_signals", []),
            "research_summary":     sig.get("research_summary", ""),
            "email_angle":          email_angle,
            "opening_line":         opening_line,
            "best_signal":          best_signal,
            "signal_facts":         signal_facts,
            # Workflow 5.9 verification fields (empty string when verification not run)
            "email_confidence_tier": routed_contact.get("email_confidence_tier", lead.get("email_confidence_tier", "")),
            "send_eligibility":      routed_contact.get("send_eligibility", lead.get("send_eligibility", "")),
            "send_pool":             routed_contact.get("send_pool", lead.get("send_pool", "")),
        })
        if routed_contact.get("send_target_type") == "named":
            named_targets += 1
        elif routed_contact.get("send_target_type") == "generic":
            generic_targets += 1

    with_signals  = sum(1 for r in merged if r["recent_signals"])
    with_openings = sum(1 for r in merged if r["opening_line"])
    skip_note = f", {skipped_mismatch} skipped (domain_mismatch/E0/junk_email)" if skipped_mismatch else ""
    route_note = (
        f", targets: named={named_targets}, generic={generic_targets}, "
        f"skipped_no_target={skipped_no_target}"
    )
    print(
        f"[Workflow 6] Merged {len(merged)} leads "
        f"({with_signals} with signals, {with_openings} with personalized opening"
        f"{skip_note}{route_note})"
    )
    return merged
