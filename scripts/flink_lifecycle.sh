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
  # PATCH against the Flink Statement API takes RFC 6902 JSON Patch arrays,
  # so we send the matching content type for that method specifically.
  local content_type="application/json"
  [[ "${method}" == "PATCH" ]] && content_type="application/json-patch+json"
  if [[ -n "${body}" ]]; then
    http="$(curl -sS -u "${CONFLUENT_FLINK_API_KEY}:${CONFLUENT_FLINK_API_SECRET}" \
      -X "${method}" "${FLINK_BASE}${path}" \
      -H "Content-Type: ${content_type}" -d "${body}" \
      -o "${resp}" -w "%{http_code}")"
  else
    http="$(curl -sS -u "${CONFLUENT_FLINK_API_KEY}:${CONFLUENT_FLINK_API_SECRET}" \
      -X "${method}" "${FLINK_BASE}${path}" \
      -o "${resp}" -w "%{http_code}")"
  fi
  echo "${http}|${resp}"
}

# Normalize the statement name to match Confluent Cloud Flink naming rules
# (lowercase alphanumeric + hyphens). Allows users to type either underscore
# or hyphen form; the actual deployed statement name uses hyphens.
if [[ -n "${STATEMENT}" ]]; then
  STATEMENT="$(echo "${STATEMENT}" | tr '_A-Z' '-a-z')"
fi

case "${ACTION}" in
  stop)
    echo "==> Stopping statement: ${STATEMENT} (state retained)"
    # The API only supports `replace` op on /spec/stopped, not `add`.
    # The current value of /spec/stopped must differ from the requested one
    # — i.e., you can only stop a running statement, not stop a stopped one.
    RESULT="$(api_call PATCH "/${STATEMENT}" '[{"op":"replace","path":"/spec/stopped","value":true}]')"
    ;;
  resume)
    echo "==> Resuming statement: ${STATEMENT}"
    # Likewise: resume only makes sense if the statement is currently stopped.
    # If you get HTTP 422 ("Invalid patch data"), describe the statement —
    # spec.stopped is probably already false (statement is already running).
    RESULT="$(api_call PATCH "/${STATEMENT}" '[{"op":"replace","path":"/spec/stopped","value":false}]')"
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
