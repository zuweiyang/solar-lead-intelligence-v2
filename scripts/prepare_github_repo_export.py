"""
Create a clean GitHub-safe export directory from the current local workspace.

The export keeps source code, docs, templates, and deployment assets while
excluding runtime data, secrets, caches, and other local-only files.

Usage:
    D:\Python\python.exe scripts\prepare_github_repo_export.py
    D:\Python\python.exe scripts\prepare_github_repo_export.py --output D:\exports\solar-lead-intelligence-github
    D:\Python\python.exe scripts\prepare_github_repo_export.py --overwrite --zip
"""
from __future__ import annotations

import argparse
import fnmatch
import shutil
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "_github_export"

EXCLUDE_PATTERNS = [
    ".env",
    ".env.*",
    "!.env.example",
    "!deploy/gcp/.env.gcp.example",
    ".git",
    ".git/**",
    ".pytest_cache/**",
    ".claude/**",
    ".tldextract-cache/**",
    "_github*",
    "_github*/**",
    "__pycache__/**",
    "*.pyc",
    "*.pyo",
    "*.pyd",
    ".venv/**",
    "venv/**",
    "env/**",
    "node_modules/**",
    "config/gmail_client_secret.json",
    "config/gmail_token.json",
    "data/**",
    "logs/**",
    "*.log",
]

INCLUDE_EMPTY_FILES = [
    "data/.gitkeep",
]


def _norm(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _matches(rel_path: str, pattern: str) -> bool:
    if pattern.endswith("/**"):
        prefix = pattern[:-3].rstrip("/")
        return rel_path == prefix or rel_path.startswith(prefix + "/")
    return fnmatch.fnmatch(rel_path, pattern)


def _is_excluded(rel_path: str) -> bool:
    excluded = False
    for pattern in EXCLUDE_PATTERNS:
        negate = pattern.startswith("!")
        clean = pattern[1:] if negate else pattern
        if _matches(rel_path, clean):
            excluded = not negate
    return excluded


def _copy_project(output_dir: Path) -> tuple[int, int]:
    copied = 0
    skipped = 0
    for path in ROOT.rglob("*"):
        if output_dir == path or output_dir in path.parents:
            continue
        rel_path = _norm(path)
        if rel_path.startswith(".git/"):
            continue
        if _is_excluded(rel_path):
            skipped += 1
            continue
        target = output_dir / rel_path
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        copied += 1
    return copied, skipped


def _ensure_placeholders(output_dir: Path) -> None:
    for rel_path in INCLUDE_EMPTY_FILES:
        target = output_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.touch(exist_ok=True)


def prepare_export(output_dir: Path, overwrite: bool = False) -> Path:
    if output_dir.exists():
        if not overwrite:
            raise FileExistsError(f"Output directory already exists: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    copied, skipped = _copy_project(output_dir)
    _ensure_placeholders(output_dir)
    print(f"Prepared GitHub-safe export at: {output_dir}")
    print(f"Copied files: {copied}")
    print(f"Skipped paths: {skipped}")
    return output_dir


def create_zip_bundle(output_dir: Path, archive_base: Path | None = None) -> Path:
    base = archive_base or output_dir
    zip_path = Path(shutil.make_archive(str(base), "zip", root_dir=output_dir))
    print(f"Created zip bundle at: {zip_path}")
    return zip_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare a GitHub-safe export directory")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Export directory path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete the output directory first if it already exists",
    )
    parser.add_argument(
        "--zip",
        action="store_true",
        help="Also create a .zip archive from the prepared export directory",
    )
    parser.add_argument(
        "--archive-base",
        default="",
        help="Optional base path for the zip archive (without .zip extension)",
    )
    args = parser.parse_args()
    output_dir = prepare_export(Path(args.output).resolve(), overwrite=args.overwrite)
    if args.zip:
        archive_base = Path(args.archive_base).resolve() if args.archive_base else (
            output_dir.parent / f"{output_dir.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        create_zip_bundle(output_dir, archive_base=archive_base)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
