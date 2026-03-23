#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/solar-lead-intelligence}"
TARGET_REF="${1:-}"

if [[ -z "$TARGET_REF" ]]; then
  echo "Usage: bash deploy/gcp/rollback_vm.sh <git-tag-or-commit>"
  exit 1
fi

cd "$PROJECT_ROOT"

echo "[rollback-vm] Rolling VM checkout back to: $TARGET_REF"
bash "$PROJECT_ROOT/deploy/gcp/update_vm.sh" --ref "$TARGET_REF"
echo "[rollback-vm] Rollback complete"
