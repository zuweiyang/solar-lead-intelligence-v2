#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$DEFAULT_PROJECT_ROOT}"
CONFIG_DIR="${CONFIG_DIR:-$PROJECT_ROOT/config}"
CLIENT_TARGET="${CLIENT_TARGET:-$CONFIG_DIR/gmail_client_secret.json}"
TOKEN_TARGET="${TOKEN_TARGET:-$CONFIG_DIR/gmail_token.json}"
SOLAR_SECRET_SOURCE_DIR="${SOLAR_SECRET_SOURCE_DIR:-}"
SOLAR_GMAIL_CLIENT_SECRET_NAME="${SOLAR_GMAIL_CLIENT_SECRET_NAME:-}"
SOLAR_GMAIL_TOKEN_SECRET_NAME="${SOLAR_GMAIL_TOKEN_SECRET_NAME:-}"

CHECK_ONLY="false"
QUIET="false"
FORCE="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only)
      CHECK_ONLY="true"
      shift
      ;;
    --quiet)
      QUIET="true"
      shift
      ;;
    --force)
      FORCE="true"
      shift
      ;;
    *)
      echo "[restore-gmail-oauth] Unknown argument: $1"
      exit 1
      ;;
  esac
done

log() {
  if [[ "$QUIET" != "true" ]]; then
    echo "$@"
  fi
}

fail() {
  echo "[restore-gmail-oauth] $*" >&2
  exit 1
}

ensure_file_ok() {
  local path="$1"
  [[ -f "$path" ]] || return 1
  [[ -s "$path" ]] || return 1
  return 0
}

copy_from_source_dir() {
  [[ -n "$SOLAR_SECRET_SOURCE_DIR" ]] || return 1
  local client_src="$SOLAR_SECRET_SOURCE_DIR/gmail_client_secret.json"
  local token_src="$SOLAR_SECRET_SOURCE_DIR/gmail_token.json"
  ensure_file_ok "$client_src" || return 1
  ensure_file_ok "$token_src" || return 1

  mkdir -p "$CONFIG_DIR"
  cp "$client_src" "$CLIENT_TARGET"
  cp "$token_src" "$TOKEN_TARGET"
  chmod 600 "$CLIENT_TARGET" "$TOKEN_TARGET"
  log "[restore-gmail-oauth] Restored Gmail OAuth files from $SOLAR_SECRET_SOURCE_DIR"
}

copy_from_secret_manager() {
  [[ -n "$SOLAR_GMAIL_CLIENT_SECRET_NAME" ]] || return 1
  [[ -n "$SOLAR_GMAIL_TOKEN_SECRET_NAME" ]] || return 1
  command -v gcloud >/dev/null 2>&1 || fail "gcloud is required for Secret Manager restore"

  mkdir -p "$CONFIG_DIR"
  gcloud secrets versions access latest --secret="$SOLAR_GMAIL_CLIENT_SECRET_NAME" > "$CLIENT_TARGET"
  gcloud secrets versions access latest --secret="$SOLAR_GMAIL_TOKEN_SECRET_NAME" > "$TOKEN_TARGET"
  chmod 600 "$CLIENT_TARGET" "$TOKEN_TARGET"
  log "[restore-gmail-oauth] Restored Gmail OAuth files from Secret Manager"
}

if [[ "$CHECK_ONLY" != "true" ]]; then
  if [[ "$FORCE" == "true" ]] || ! ensure_file_ok "$CLIENT_TARGET" || ! ensure_file_ok "$TOKEN_TARGET"; then
    if ! copy_from_source_dir; then
      copy_from_secret_manager || true
    fi
  fi
fi

ensure_file_ok "$CLIENT_TARGET" || fail "Missing Gmail client secret at $CLIENT_TARGET"
ensure_file_ok "$TOKEN_TARGET" || fail "Missing Gmail token at $TOKEN_TARGET"

log "[restore-gmail-oauth] Gmail OAuth files are present"
