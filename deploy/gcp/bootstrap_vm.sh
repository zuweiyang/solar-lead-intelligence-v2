#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/solar-lead-intelligence}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-$PROJECT_ROOT/.venv}"
REPO_URL="${REPO_URL:-}"
REPO_BRANCH="${REPO_BRANCH:-main}"
INSTALL_GCLOUD="${INSTALL_GCLOUD:-true}"

echo "[bootstrap] Updating apt packages"
sudo apt-get update
sudo apt-get install -y git python3 python3-venv python3-pip

if [[ "$INSTALL_GCLOUD" == "true" ]] && ! command -v gcloud >/dev/null 2>&1; then
  echo "[bootstrap] Installing Google Cloud CLI"
  sudo apt-get install -y apt-transport-https ca-certificates gnupg curl
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list >/dev/null
  sudo apt-get update
  sudo apt-get install -y google-cloud-cli
fi

if [[ ! -d "$PROJECT_ROOT" ]]; then
  if [[ -z "$REPO_URL" ]]; then
    echo "[bootstrap] REPO_URL is required when $PROJECT_ROOT does not exist"
    exit 1
  fi
  echo "[bootstrap] Cloning repo into $PROJECT_ROOT"
  sudo mkdir -p "$(dirname "$PROJECT_ROOT")"
  sudo chown -R "$USER":"$USER" "$(dirname "$PROJECT_ROOT")"
  git clone --branch "$REPO_BRANCH" "$REPO_URL" "$PROJECT_ROOT"
fi

cd "$PROJECT_ROOT"

echo "[bootstrap] Creating virtual environment"
$PYTHON_BIN -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

echo "[bootstrap] Installing Python dependencies"
pip install --upgrade pip
pip install -r requirements.txt

echo "[bootstrap] Creating runtime directories"
mkdir -p "$PROJECT_ROOT/data"
mkdir -p "$PROJECT_ROOT/data/runs"
mkdir -p "$PROJECT_ROOT/data/crm"

echo "[bootstrap] Done"
echo "[bootstrap] Next steps:"
echo "  1. copy deploy/gcp/.env.gcp.example to $PROJECT_ROOT/.env and fill values (optionally set CLOUD_WORKER_ALERT_WEBHOOK / Secret Manager names)"
echo "  2. place Gmail OAuth files in config/ or set SOLAR_SECRET_SOURCE_DIR / SOLAR_GMAIL_* secret names"
echo "  3. gcloud auth login or attach a VM service account with Storage/Secret Manager access"
echo "  4. install deploy/gcp/systemd/cloud-send-worker.service"
echo "  5. use deploy/gcp/update_vm.sh for repeatable updates and deploy/gcp/recover_cloud_worker.sh for recovery"
