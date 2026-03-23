"""
AI response JSON extraction utilities — shared across email generation, repair,
follow-up generation, and company analysis.

Problems addressed:
  - Empty AI response (Expecting value: line 1 column 1 (char 0))
  - Extra text after the JSON object (Extra data: line N column 1 (char N))
  - Bare newlines/tabs inside JSON string values
  - Markdown code fences wrapping the JSON
  - Partial / truncated responses
  - Missing required keys in an otherwise valid JSON object

Failure classification
----------------------
Every failure is tagged with a `failure_class` string so callers can
distinguish the root cause in counters and logs:

  "empty_response"       — AI returned an empty or whitespace-only string
  "malformed_json"       — Could not parse JSON after all repair attempts
  "extra_text"           — JSON was buried in prose (recovered via extraction)
  "missing_keys"         — Parsed OK but required fields absent
  "truncated"            — Opening bracket found but never closed
  "timeout"              — Caller should tag requests.exceptions.Timeout / similar
"""
from __future__ import annotations

import json


# ---------------------------------------------------------------------------
# Public exception — carries a failure_class for caller categorisation
# ---------------------------------------------------------------------------

class AIParseError(Exception):
    """
    Raised when an AI response cannot be converted to a usable dict.

    Attributes
    ----------
    failure_class : str
        One of: empty_response | malformed_json | extra_text |
                missing_keys | truncated
    raw_preview   : str
        First 120 chars of the raw response (safe for logging).
    """
    def __init__(self, message: str, failure_class: str, raw: str = "") -> None:
        super().__init__(message)
        self.failure_class = failure_class
        self.raw_preview   = (raw or "")[:120]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def fix_json_control_chars(raw: str) -> str:
    """
    Replace bare newlines / tabs inside JSON string values with their escape
    sequences.  Characters outside string values are passed through unchanged.

    This is needed because AI models sometimes emit real newlines inside a JSON
    string rather than the escaped form \\n.
    """
    out: list[str] = []
    in_str = False
    i = 0
    while i < len(raw):
        c = raw[i]
        if c == "\\" and in_str:
            # Consume the escape sequence whole so we don't mis-detect \"
            out.append(c)
            i += 1
            if i < len(raw):
                out.append(raw[i])
        elif c == '"':
            in_str = not in_str
            out.append(c)
        elif in_str and c == "\n":
            out.append("\\n")
        elif in_str and c == "\r":
            out.append("\\r")
        elif in_str and c == "\t":
            out.append("\\t")
        else:
            out.append(c)
        i += 1
    return "".join(out)


def _strip_markdown_fences(raw: str) -> str:
    """Remove ``` or ```json fences wrapping the response."""
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return raw.strip()


def _extract_json_object(raw: str) -> tuple[str, bool]:
    """
    Extract the first complete JSON object or array from `raw` using bracket
    counting.  Returns (substring, was_truncated) where was_truncated=True
    means the opening bracket was found but the matching close bracket was not.

    Falls back to returning (raw, False) when no bracket is found at all.
    """
    start = -1
    open_char  = ""
    close_char = ""
    for idx, ch in enumerate(raw):
        if ch in ("{", "["):
            start = idx
            open_char  = ch
            close_char = "}" if ch == "{" else "]"
            break

    if start == -1:
        return raw, False  # no JSON-like structure found

    depth = 0
    in_str = False
    i = start
    while i < len(raw):
        c = raw[i]
        if c == "\\" and in_str:
            i += 2  # skip escaped char
            continue
        if c == '"':
            in_str = not in_str
        elif not in_str:
            if c == open_char:
                depth += 1
            elif c == close_char:
                depth -= 1
                if depth == 0:
                    return raw[start : i + 1], False
        i += 1

    # Bracket never closed — truncated response
    return raw[start:], True


# ---------------------------------------------------------------------------
# Public — robust parser (backward-compatible, raises json.JSONDecodeError)
# ---------------------------------------------------------------------------

def parse_ai_json(raw: str, context: str = "") -> dict:
    """
    Robustly parse a JSON object from an AI response string.

    Steps:
      1. Strip whitespace and markdown fences.
      2. Try direct json.loads().
      3. On failure, fix bare control chars inside strings and retry.
      4. On failure, extract the first JSON object/array via bracket counting
         (handles "Extra data" errors where the model emits text after the JSON).
      5. Raise json.JSONDecodeError with an informative message if all attempts fail.

    Args:
        raw:     The raw string returned by the AI model.
        context: Optional label (e.g. company name) for error messages.

    Raises:
        json.JSONDecodeError — with a descriptive message that includes the
        failure class prefix so callers can classify errors in logs.
    """
    label = f" for {context}" if context else ""

    if not raw or not raw.strip():
        raise json.JSONDecodeError(
            f"[empty_response] Empty AI response{label}",
            raw or "", 0,
        )

    cleaned = _strip_markdown_fences(raw)

    # Attempt 1: direct parse
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Attempt 2: fix control chars then parse
    fixed = fix_json_control_chars(cleaned)
    try:
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass

    # Attempt 3: extract first JSON object (handles "Extra data" / trailing text)
    extracted, truncated = _extract_json_object(fixed)
    if truncated:
        raise json.JSONDecodeError(
            f"[truncated] AI response appears truncated (unclosed bracket){label}",
            fixed, 0,
        )
    try:
        return json.loads(extracted)
    except json.JSONDecodeError as exc:
        raise json.JSONDecodeError(
            f"[malformed_json] AI response JSON parse failed{label} after all attempts: {exc.msg}",
            exc.doc, exc.pos,
        ) from exc


# ---------------------------------------------------------------------------
# Public — failure classifier helper for callers
# ---------------------------------------------------------------------------

def classify_parse_failure(exc: Exception) -> str:
    """
    Extract the failure_class from a json.JSONDecodeError raised by
    parse_ai_json(), or classify common request exceptions.

    Returns one of:
      "empty_response" | "malformed_json" | "extra_text" |
      "truncated"      | "timeout"        | "http_error" |
      "unknown"
    """
    msg = str(exc).lower()
    if "[empty_response]" in msg or "empty ai response" in msg:
        return "empty_response"
    if "[truncated]" in msg:
        return "truncated"
    if "[malformed_json]" in msg:
        return "malformed_json"
    # requests exceptions
    try:
        import requests
        if isinstance(exc, requests.exceptions.Timeout):
            return "timeout"
        if isinstance(exc, requests.exceptions.HTTPError):
            return "http_error"
    except ImportError:
        pass
    return "unknown"


# ---------------------------------------------------------------------------
# Public — required-key validation (post-parse quality gate)
# ---------------------------------------------------------------------------

def validate_required_keys(parsed: dict, required: list[str], context: str = "") -> None:
    """
    Raise AIParseError('missing_keys') if any key in `required` is absent or
    has a falsy value in `parsed`.

    Args:
        parsed:   The dict returned by parse_ai_json().
        required: List of key names that must be present and non-empty.
        context:  Optional label for error messages.

    Raises:
        AIParseError with failure_class="missing_keys"
    """
    missing = [k for k in required if not parsed.get(k)]
    if missing:
        label = f" for {context}" if context else ""
        raise AIParseError(
            f"[missing_keys] AI response missing required fields{label}: {missing}",
            failure_class="missing_keys",
        )
