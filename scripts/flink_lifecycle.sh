#!/usr/bin/env bash
#
# flink_lifecycle.sh <action> [statement-name]
#
# Performs lifecycle operations on a Confluent Cloud Flink statement via the
# Flink Statement REST API (no CLI). HTTP Basic auth with the Flink-region
# API key.
#
# Actions:
#   stop       — gracefully stop a running statement (state retained for resume)
#   resume     — resume a stopped statement from its retained state
#   delete     — permanently delete a statement (state lost; not reversible)
#   describe   — print the current spec/status YAML-ish view
#   list       — list all statements in the env+pool
#
# Required env (set by the workflow from GitHub Actions secrets):
#   ORG_ID, ENV_ID, COMPUTE_POOL_ID, CLOUD, REGION
#   CONFLUENT_FLINK_API_KEY,  CONFLUENT_FLINK_API_SECRET   (Flink-region-level)
#
# Usage from the workflow:
#   ./scripts/flink_lifecycle.sh stop inventory_by_bucket
#   ./scripts/flink_lifecycle.sh resume inventory_by_bucket
#   ./scripts/flink_lifecycle.sh list
#
set -euo pipefail

ACTION="${1:?usage: flink_lifecycle.sh <stop|resume|delete|describe|list> [statement-name]}"
STATEMENT="${2:-}"

if [[ "${ACTION}" != "list" && -z "${STATEMENT}" ]]; then
  echo "ERROR: statement name is required for action '${ACTION}'" >&2
  exit 2
fi

FLINK_HOST="https://flink.${REGION}.${CLOUD}.confluent.cloud"
FLINK_BASE="${FLINK_HOST}/sql/v1/organizations/${ORG_ID}/environments/${ENV_ID}/statements"

api_call() {
  local method="$1" path="$2" body="${3:-}"
  local resp; resp="$(mktemp)"
  local http
  if [[ -n "${body}" ]]; then
    http="$(curl -sS -u "${CONFLUENT_FLINK_API_KEY}:${CONFLUENT_FLINK_API_SECRET}" \
      -X "${method}" "${FLINK_BASE}${path}" \
      -H "Content-Type: application/json" -d "${body}" \
      -o "${resp}" -w "%{http_code}")"
  else
    http="$(curl -sS -u "${CONFLUENT_FLINK_API_KEY}:${CONFLUENT_FLINK_API_SECRET}" \
      -X "${method}" "${FLINK_BASE}${path}" \
      -o "${resp}" -w "%{http_code}")"
  fi
  echo "${http}|${resp}"
}

case "${ACTION}" in
  stop)
    echo "==> Stopping statement: ${STATEMENT} (state retained)"
    RESULT="$(api_call PATCH "/${STATEMENT}" '{"spec":{"stopped":true}}')"
    ;;
  resume)
    echo "==> Resuming statement: ${STATEMENT}"
    RESULT="$(api_call PATCH "/${STATEMENT}" '{"spec":{"stopped":false}}')"
    ;;
  delete)
    echo "==> Deleting statement: ${STATEMENT} (NOT reversible)"
    RESULT="$(api_call DELETE "/${STATEMENT}")"
    ;;
  describe)
    echo "==> Describing statement: ${STATEMENT}"
    RESULT="$(api_call GET "/${STATEMENT}")"
    ;;
  list)
    echo "==> Listing statements in env=${ENV_ID} pool=${COMPUTE_POOL_ID}"
    RESULT="$(api_call GET "?spec.compute_pool_id=${COMPUTE_POOL_ID}")"
    ;;
  *)
    echo "ERROR: unknown action '${ACTION}' (expected: stop|resume|delete|describe|list)" >&2
    exit 2
    ;;
esac

HTTP="${RESULT%%|*}"
RESP="${RESULT##*|}"

if [[ "${HTTP}" -ge 300 ]]; then
  echo "ERROR: ${ACTION} failed (HTTP ${HTTP}):" >&2
  cat "${RESP}" >&2
  exit 1
fi

# Pretty-print successful response (if any)
if [[ -s "${RESP}" ]]; then
  jq . "${RESP}"
fi

echo "==> Done."
