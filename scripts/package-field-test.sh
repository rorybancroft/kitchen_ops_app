#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$(cd "$ROOT_DIR/.." && pwd)"
OUT_FILE="$OUT_DIR/kitchen_ops_field_test.zip"

cd "$OUT_DIR"
rm -f "$OUT_FILE"
zip -r "$OUT_FILE" kitchen_ops_app \
  -x "*/.venv/*" "*/__pycache__/*" "*.DS_Store" "*/uploads/*"

echo "Created: $OUT_FILE"
