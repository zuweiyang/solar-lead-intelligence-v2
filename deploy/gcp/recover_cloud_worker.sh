#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$DEFAULT_PROJECT_ROOT}"
RUN_UPDATE="true"
RESTART_WORKER="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-update)
      RUN_UPDATE="false"
      shift
      ;;
    --skip-restart)
      RESTART_WORKER="false"
      shift
      ;;
    *)
      echo "[recover-cloud-worker] Unknown argument: $1"
      exit 1
      ;;
  esac
done

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  SUDO="sudo"
fi

cd "$PROJECT_ROOT"

if [[ "$RUN_UPDATE" == "true" ]]; then
  echo "[recover-cloud-worker] Updating VM checkout"
  bash "$PROJECT_ROOT/deploy/gcp/update_vm.sh"
fi

echo "[recover-cloud-worker] Restoring / validating Gmail OAuth files"
bash "$PROJECT_ROOT/deploy/gcp/restore_gmail_oauth.sh"

if [[ "$RESTART_WORKER" == "true" ]]; then
  echo "[recover-cloud-worker] Restarting worker service"
  $SUDO systemctl daemon-reload
  $SUDO systemctl restart cloud-send-worker
fi

echo "[recover-cloud-worker] Service status"
$SUDO systemctl --no-pager --full status cloud-send-worker || true

echo "[recover-cloud-worker] Recent worker logs"
$SUDO journalctl -u cloud-send-worker -n 40 --no-pager || true

alerts_file="$PROJECT_ROOT/data/cloud_worker_alerts.jsonl"
if [[ -f "$alerts_file" ]]; then
  echo "[recover-cloud-worker] Recent structured alerts"
  tail -n 20 "$alerts_file" || true
fi

echo "[recover-cloud-worker] Recovery flow complete"
