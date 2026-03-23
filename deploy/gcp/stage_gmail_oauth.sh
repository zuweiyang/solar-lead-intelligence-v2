#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$DEFAULT_PROJECT_ROOT}"
CONFIG_DIR="${CONFIG_DIR:-$PROJECT_ROOT/config}"
CLIENT_SOURCE="${CLIENT_SOURCE:-$CONFIG_DIR/gmail_client_secret.json}"
TOKEN_SOURCE="${TOKEN_SOURCE:-$CONFIG_DIR/gmail_token.json}"
TARGET_DIR="${TARGET_DIR:-${SOLAR_SECRET_SOURCE_DIR:-}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-dir)
      TARGET_DIR="${2:-}"
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
      echo "[stage-gmail-oauth] Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [[ -z "$TARGET_DIR" ]]; then
  echo "[stage-gmail-oauth] TARGET_DIR is required. Set SOLAR_SECRET_SOURCE_DIR or pass --target-dir." >&2
  exit 1
fi

if [[ ! -f "$CLIENT_SOURCE" || ! -s "$CLIENT_SOURCE" ]]; then
  echo "[stage-gmail-oauth] Missing Gmail client secret at $CLIENT_SOURCE" >&2
  exit 1
fi

if [[ ! -f "$TOKEN_SOURCE" || ! -s "$TOKEN_SOURCE" ]]; then
  echo "[stage-gmail-oauth] Missing Gmail token at $TOKEN_SOURCE" >&2
  exit 1
fi

mkdir -p "$TARGET_DIR"
cp "$CLIENT_SOURCE" "$TARGET_DIR/gmail_client_secret.json"
cp "$TOKEN_SOURCE" "$TARGET_DIR/gmail_token.json"
chmod 600 "$TARGET_DIR/gmail_client_secret.json" "$TARGET_DIR/gmail_token.json"

echo "[stage-gmail-oauth] Staged Gmail OAuth files to $TARGET_DIR"
