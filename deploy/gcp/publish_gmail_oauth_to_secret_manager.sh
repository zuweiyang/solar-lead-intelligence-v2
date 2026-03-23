#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$DEFAULT_PROJECT_ROOT}"
CONFIG_DIR="${CONFIG_DIR:-$PROJECT_ROOT/config}"
CLIENT_SOURCE="${CLIENT_SOURCE:-$CONFIG_DIR/gmail_client_secret.json}"
TOKEN_SOURCE="${TOKEN_SOURCE:-$CONFIG_DIR/gmail_token.json}"
CLIENT_SECRET_NAME="${CLIENT_SECRET_NAME:-${SOLAR_GMAIL_CLIENT_SECRET_NAME:-}}"
TOKEN_SECRET_NAME="${TOKEN_SECRET_NAME:-${SOLAR_GMAIL_TOKEN_SECRET_NAME:-}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --client-secret-name)
      CLIENT_SECRET_NAME="${2:-}"
      shift 2
      ;;
    --token-secret-name)
      TOKEN_SECRET_NAME="${2:-}"
      shift 2
      ;;
    --client-source)
      CLIENT_SOURCE="${2:-}"
      shift 2
      ;;
    --token-source)
      TOKEN_SOURCE="${2:-}"
      shift 2
      ;;
    *)
      echo "[publish-gmail-oauth] Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [[ -z "$CLIENT_SECRET_NAME" || -z "$TOKEN_SECRET_NAME" ]]; then
  echo "[publish-gmail-oauth] Secret names are required. Set SOLAR_GMAIL_CLIENT_SECRET_NAME and SOLAR_GMAIL_TOKEN_SECRET_NAME or pass flags." >&2
  exit 1
fi

if [[ ! -f "$CLIENT_SOURCE" || ! -s "$CLIENT_SOURCE" ]]; then
  echo "[publish-gmail-oauth] Missing Gmail client secret at $CLIENT_SOURCE" >&2
  exit 1
fi

if [[ ! -f "$TOKEN_SOURCE" || ! -s "$TOKEN_SOURCE" ]]; then
  echo "[publish-gmail-oauth] Missing Gmail token at $TOKEN_SOURCE" >&2
  exit 1
fi

command -v gcloud >/dev/null 2>&1 || {
  echo "[publish-gmail-oauth] gcloud is required" >&2
  exit 1
}

ensure_secret() {
  local name="$1"
  if ! gcloud secrets describe "$name" >/dev/null 2>&1; then
    gcloud secrets create "$name" --replication-policy="automatic"
  fi
}

ensure_secret "$CLIENT_SECRET_NAME"
ensure_secret "$TOKEN_SECRET_NAME"

gcloud secrets versions add "$CLIENT_SECRET_NAME" --data-file="$CLIENT_SOURCE" >/dev/null
gcloud secrets versions add "$TOKEN_SECRET_NAME" --data-file="$TOKEN_SOURCE" >/dev/null

echo "[publish-gmail-oauth] Uploaded Gmail OAuth files to Secret Manager:"
echo "  client secret: $CLIENT_SECRET_NAME"
echo "  token secret:  $TOKEN_SECRET_NAME"
