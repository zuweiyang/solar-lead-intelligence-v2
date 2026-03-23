#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/solar-lead-intelligence}"
RELEASE_FILE="$PROJECT_ROOT/data/deploy_release.json"

if [[ ! -f "$RELEASE_FILE" ]]; then
  echo "[release-status] No deploy_release.json found at $RELEASE_FILE"
  exit 1
fi

cat "$RELEASE_FILE"
