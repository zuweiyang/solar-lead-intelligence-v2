"""
Local safety check before turning this project into a GitHub repository.

This script does not inspect git history. It focuses on the current working
directory and flags files that should not be uploaded.

Usage:
    D:\Python\python.exe scripts/check_github_readiness.py
"""
from __future__ import annotations

import fnmatch
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

BLOCKED_PATTERNS = [
    ".env",
    ".env.*",
    "config/gmail_client_secret.json",
    "config/gmail_token.json",
    "data/**",
    "node_modules/**",
    ".claude/**",
    ".tldextract-cache/**",
    "*.log",
]

EXPECTED_SAFE_TEMPLATES = [
    ".env.example",
    "deploy/gcp/.env.gcp.example",
    ".gitignore",
]


def _norm(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _matches(rel_path: str, pattern: str) -> bool:
    if pattern.endswith("/**"):
        prefix = pattern[:-3]
        return rel_path == prefix.rstrip("/") or rel_path.startswith(prefix.rstrip("/") + "/")
    return fnmatch.fnmatch(rel_path, pattern)


def _scan_blocked() -> list[str]:
    hits: list[str] = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        rel_path = _norm(path)
        if rel_path in EXPECTED_SAFE_TEMPLATES:
            continue
        if any(_matches(rel_path, pattern) for pattern in BLOCKED_PATTERNS):
            hits.append(rel_path)
    return sorted(hits)


def _check_templates() -> list[str]:
    missing: list[str] = []
    for rel_path in EXPECTED_SAFE_TEMPLATES:
        if not (ROOT / rel_path).exists():
            missing.append(rel_path)
    return missing


def main() -> int:
    blocked_hits = _scan_blocked()
    missing_templates = _check_templates()

    print("GitHub readiness check")
    print(f"Project root: {ROOT}")
    print()

    if blocked_hits:
        print("Sensitive or local-only files currently present in the working tree:")
        for rel_path in blocked_hits:
            print(f"  - {rel_path}")
    else:
        print("No blocked local files detected.")

    print()
    if missing_templates:
        print("Missing safe-share templates:")
        for rel_path in missing_templates:
            print(f"  - {rel_path}")
    else:
        print("Required safe-share templates are present.")

    print()
    if blocked_hits:
        print("Result: NOT GitHub-ready yet.")
        print("Recommendation: keep the repo private and do not upload blocked files.")
        return 1

    print("Result: Working tree looks GitHub-ready for a private repo upload.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
