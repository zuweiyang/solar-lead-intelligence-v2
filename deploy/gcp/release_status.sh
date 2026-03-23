#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$DEFAULT_PROJECT_ROOT}"
RELEASE_FILE="$PROJECT_ROOT/data/deploy_release.json"

if [[ ! -f "$RELEASE_FILE" ]]; then
  echo "[release-status] No deploy_release.json found at $RELEASE_FILE"
  exit 1
fi

cat "$RELEASE_FILE"
