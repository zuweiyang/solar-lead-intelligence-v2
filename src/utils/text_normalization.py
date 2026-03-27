from __future__ import annotations

import unicodedata
from typing import Any

_MOJIBAKE_MARKERS: tuple[str, ...] = (
    "Ã",
    "Â",
    "â",
    "Ð",
    "Ñ",
    "谩",
    "贸",
    "茫",
    "锟",
    "�",
)


def _mojibake_score(value: str) -> int:
    return sum(value.count(marker) for marker in _MOJIBAKE_MARKERS)


def normalize_text(value: str) -> str:
    """
    Normalize text to NFC and repair common UTF-8 mojibake when it is safe.

    The repair step is conservative: it only keeps a transcoded candidate when
    the candidate contains fewer mojibake markers than the original string.
    """
    if not isinstance(value, str):
        return value

    text = unicodedata.normalize("NFC", value).replace("\u200b", "").replace("\ufeff", "")
    best = text
    best_score = _mojibake_score(best)

    for source_encoding in ("latin-1", "cp1252"):
        try:
            candidate = text.encode(source_encoding).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        candidate = unicodedata.normalize("NFC", candidate)
        candidate_score = _mojibake_score(candidate)
        if candidate_score < best_score:
            best = candidate
            best_score = candidate_score

    return best


def normalize_value(value: Any) -> Any:
    """Recursively normalize strings inside common JSON/CSV payload types."""
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_value(item) for key, item in value.items()}
    return value
