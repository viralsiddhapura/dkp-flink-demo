#!/usr/bin/env bash
#
# Verifies that schemas/business_date_v2.avsc is BACKWARD compatible with
# schemas/business_date_v1.avsc against your Schema Registry.
#
# Usage:
#   SR_URL=https://psrc-xxxxx.region.aws.confluent.cloud \
#   SR_KEY=... SR_SECRET=... \
#   SUBJECT=business_date-value \
#   ./01_compat_check.sh
#
# This script does NOT register the schema; it only checks compatibility.
set -euo pipefail

: "${SR_URL:?set SR_URL}"
: "${SR_KEY:?set SR_KEY}"
: "${SR_SECRET:?set SR_SECRET}"
: "${SUBJECT:?set SUBJECT (e.g. business_date-value)}"

V2_FILE="$(dirname "$0")/schemas/business_date_v2.avsc"
[[ -f "$V2_FILE" ]] || { echo "missing $V2_FILE" >&2; exit 2; }

# Confluent Schema Registry expects the schema as a JSON-encoded string
# inside a wrapper. jq -Rs '.' reads the file and emits it as a JSON string.
PAYLOAD=$(jq -nc --arg s "$(cat "$V2_FILE")" '{schema: $s, schemaType: "AVRO"}')

curl -sS -u "$SR_KEY:$SR_SECRET" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -X POST \
  --data "$PAYLOAD" \
  "$SR_URL/compatibility/subjects/$SUBJECT/versions/latest" \
  | tee /tmp/sr_compat.json

if jq -e '.is_compatible == true' /tmp/sr_compat.json >/dev/null; then
  echo "OK: schema is BACKWARD compatible with current latest of $SUBJECT"
else
  echo "FAIL: schema is NOT compatible — see response above" >&2
  exit 1
fi
