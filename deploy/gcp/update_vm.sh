#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/solar-lead-intelligence}"
REMOTE_NAME="${REMOTE_NAME:-origin}"
REPO_BRANCH="${REPO_BRANCH:-main}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-$PROJECT_ROOT/.venv}"
INSTALL_REQUIREMENTS="${INSTALL_REQUIREMENTS:-true}"
RESTART_WORKER="${RESTART_WORKER:-true}"
TARGET_REF="${TARGET_REF:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ref)
      TARGET_REF="${2:-}"
      shift 2
      ;;
    --skip-install)
      INSTALL_REQUIREMENTS="false"
      shift
      ;;
    --skip-restart)
      RESTART_WORKER="false"
      shift
      ;;
    *)
      echo "[update-vm] Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [[ ! -d "$PROJECT_ROOT/.git" ]]; then
  echo "[update-vm] Not a git checkout: $PROJECT_ROOT"
  exit 1
fi

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  SUDO="sudo"
fi

cd "$PROJECT_ROOT"

echo "[update-vm] Fetching latest code from $REMOTE_NAME"
git fetch --tags --prune "$REMOTE_NAME"

if [[ -n "$TARGET_REF" ]]; then
  echo "[update-vm] Checking out explicit ref: $TARGET_REF"
  git checkout "$TARGET_REF"
else
  if git show-ref --verify --quiet "refs/heads/$REPO_BRANCH"; then
    git checkout "$REPO_BRANCH"
  else
    git checkout -b "$REPO_BRANCH" "$REMOTE_NAME/$REPO_BRANCH"
  fi
  git pull --ff-only "$REMOTE_NAME" "$REPO_BRANCH"
fi

if [[ ! -d "$VENV_DIR" ]]; then
  echo "[update-vm] Creating virtual environment at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

if [[ "$INSTALL_REQUIREMENTS" == "true" ]]; then
  echo "[update-vm] Installing Python dependencies"
  pip install --upgrade pip
  pip install -r requirements.txt
fi

mkdir -p "$PROJECT_ROOT/data"

branch_name="$(git rev-parse --abbrev-ref HEAD)"
commit_sha="$(git rev-parse HEAD)"
short_sha="$(git rev-parse --short HEAD)"
updated_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
resolved_ref="${TARGET_REF:-$branch_name}"
release_mode="branch_update"
if [[ -n "$TARGET_REF" ]]; then
  release_mode="pinned_ref"
fi
dirty_state="clean"
if [[ -n "$(git status --porcelain)" ]]; then
  dirty_state="dirty"
fi

release_file="$PROJECT_ROOT/data/deploy_release.json"
cat > "$release_file" <<EOF
{
  "updated_at_utc": "$updated_at",
  "git_remote": "$REMOTE_NAME",
  "git_branch": "$branch_name",
  "git_ref_requested": "$resolved_ref",
  "git_commit": "$commit_sha",
  "git_commit_short": "$short_sha",
  "deploy_mode": "$release_mode",
  "git_worktree_state": "$dirty_state",
  "project_root": "$PROJECT_ROOT"
}
EOF

echo "[update-vm] Wrote release metadata to $release_file"

if [[ -f "$PROJECT_ROOT/deploy/gcp/systemd/cloud-send-worker.service" ]]; then
  echo "[update-vm] Refreshing systemd unit"
  $SUDO cp "$PROJECT_ROOT/deploy/gcp/systemd/cloud-send-worker.service" /etc/systemd/system/cloud-send-worker.service
  $SUDO systemctl daemon-reload
fi

if [[ "$RESTART_WORKER" == "true" ]] && $SUDO systemctl list-unit-files cloud-send-worker.service >/dev/null 2>&1; then
  echo "[update-vm] Restarting cloud-send-worker"
  $SUDO systemctl restart cloud-send-worker
  $SUDO systemctl --no-pager --full status cloud-send-worker || true
fi

echo "[update-vm] Done"
echo "[update-vm] Release: $short_sha ($branch_name) at $updated_at"
