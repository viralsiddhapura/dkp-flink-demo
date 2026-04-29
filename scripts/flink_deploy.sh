#!/usr/bin/env bash
#
# flink_deploy.sh <path-to-sql-file>
#
# Deploys (or updates) a Flink SQL statement on Confluent Cloud using the
# stop -> submit -> verify pattern.
#
# Calls the Confluent Cloud Flink REST API directly (no CLI). The Flink-region
# API key authenticates statement operations via HTTP Basic; the cloud-resource
# key is used once to look up the principal that owns the Flink key.
#
# Required env (set by the workflow from GitHub Actions secrets):
#   ORG_ID, ENV_ID, COMPUTE_POOL_ID, CLOUD, REGION, CATALOG, DATABASE
#   CONFLUENT_CLOUD_API_KEY  / CONFLUENT_CLOUD_API_SECRET   (resource-level)
#   CONFLUENT_FLINK_API_KEY  / CONFLUENT_FLINK_API_SECRET   (Flink-region-level)
#
# CATALOG  = the Confluent Cloud environment display name (e.g., "default")
# DATABASE = the Kafka cluster display name within that environment (e.g., "cluster_0")
# These are passed as Flink SQL session properties so unqualified table names
# in the SQL resolve correctly.
#
# Notes on safety:
#   - We never `delete` a stateful statement before its replacement is RUNNING.
#     We `stop` it first (state retained), then submit the new versioned
#     statement, then delete the old name once the new one is healthy.
#   - If the new statement enters FAILED, we delete it and re-resume the prior
#     statement so the pipeline self-heals.
set -euo pipefail

SQL_FILE="${1:?usage: flink_deploy.sh <sql-file>}"
[[ -f "$SQL_FILE" ]] || { echo "missing file: $SQL_FILE" >&2; exit 2; }

# Confluent Cloud Flink statement name rules: lowercase alphanumeric + hyphens
# only, must start with alphanumeric, max 100 chars. We derive from the SQL
# filename, translating underscores to hyphens and lowercasing.
STATEMENT_NAME="$(basename "$SQL_FILE" .sql | tr '_A-Z' '-a-z')"
DEPLOY_TIMEOUT="${DEPLOY_TIMEOUT:-300}"

# REST endpoints
FLINK_HOST="https://flink.${REGION}.${CLOUD}.confluent.cloud"
FLINK_BASE="${FLINK_HOST}/sql/v1/organizations/${ORG_ID}/environments/${ENV_ID}/statements"
IAM_BASE="https://api.confluent.cloud/iam/v2"

# Resolve the principal that owns the Flink API key. The Confluent Cloud Flink
# Statement API requires a `principal` in the spec — typically u-XXXX (user) or
# sa-XXXX (service account). We auto-discover it from the Flink API key's
# owner, looked up via the cloud-resource API key.
echo "==> Resolving principal for Flink API key"
PRINCIPAL_RESP="$(mktemp)"
PRINCIPAL_HTTP="$(curl -sS -u "${CONFLUENT_CLOUD_API_KEY}:${CONFLUENT_CLOUD_API_SECRET}" \
  "${IAM_BASE}/api-keys/${CONFLUENT_FLINK_API_KEY}" \
  -o "${PRINCIPAL_RESP}" -w "%{http_code}")"
if [[ "${PRINCIPAL_HTTP}" -ge 300 ]]; then
  echo "ERROR: principal lookup failed (HTTP ${PRINCIPAL_HTTP}):" >&2
  cat "${PRINCIPAL_RESP}" >&2
  exit 1
fi
PRINCIPAL="$(jq -r '.spec.owner.id' "${PRINCIPAL_RESP}")"
[[ -n "${PRINCIPAL}" && "${PRINCIPAL}" != "null" ]] \
  || { echo "ERROR: empty principal in lookup response" >&2; cat "${PRINCIPAL_RESP}" >&2; exit 1; }

echo "==> Deploying statement: ${STATEMENT_NAME}"
echo "    file:      ${SQL_FILE}"
echo "    region:    ${REGION} (${CLOUD})"
echo "    pool:      ${COMPUTE_POOL_ID}"
echo "    principal: ${PRINCIPAL}"

# ---------- API helpers (HTTP Basic auth with Flink-region key) ----------
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

# ---------- Step 1: discover current state ----------
RESULT="$(api_call GET "/${STATEMENT_NAME}")"
HTTP="${RESULT%%|*}"
RESP="${RESULT##*|}"

if [[ "${HTTP}" == "200" ]]; then
  CURRENT="EXISTS"
  CURRENT_PHASE="$(jq -r '.status.phase' "${RESP}")"
  CURRENT_STOPPED="$(jq -r '.spec.stopped // false' "${RESP}")"
  echo "    current:   phase=${CURRENT_PHASE} stopped=${CURRENT_STOPPED}"
elif [[ "${HTTP}" == "404" ]]; then
  CURRENT="ABSENT"
  CURRENT_PHASE=""
  CURRENT_STOPPED=""
  echo "    current:   ABSENT"
else
  echo "ERROR: unexpected HTTP ${HTTP} on initial describe:" >&2
  cat "${RESP}" >&2
  exit 1
fi

# ---------- Step 2: if existing is running, stop it (state retained) ----------
if [[ "${CURRENT}" == "EXISTS" && "${CURRENT_STOPPED}" != "true" ]]; then
  echo "==> Stopping existing statement (state retained)"
  RESULT="$(api_call PATCH "/${STATEMENT_NAME}" '[{"op":"add","path":"/spec/stopped","value":true}]')"
  HTTP="${RESULT%%|*}"
  RESP="${RESULT##*|}"
  if [[ "${HTTP}" -ge 300 ]]; then
    echo "ERROR: stop failed (HTTP ${HTTP}):" >&2; cat "${RESP}" >&2; exit 1
  fi
fi

# ---------- Step 3: submit new versioned statement ----------
GIT_SHA="$(git rev-parse --short=8 HEAD)"
NEW_NAME="${STATEMENT_NAME}-${GIT_SHA}"

# Build the request body with jq to safely escape the SQL contents.
# `properties.sql.current-catalog` / `sql.current-database` set the SQL session
# context so unqualified table references (e.g., `inventory.avro.topic`)
# resolve to the right Confluent Cloud env + Kafka cluster.
BODY="$(jq -n \
  --arg name      "${NEW_NAME}" \
  --arg statement "$(cat "${SQL_FILE}")" \
  --arg pool      "${COMPUTE_POOL_ID}" \
  --arg principal "${PRINCIPAL}" \
  --arg catalog   "${CATALOG}" \
  --arg database  "${DATABASE}" \
  '{
    name: $name,
    spec: {
      statement: $statement,
      properties: {
        "sql.current-catalog":  $catalog,
        "sql.current-database": $database
      },
      compute_pool_id: $pool,
      principal: $principal,
      stopped: false
    }
  }')"

echo "==> Submitting new statement: ${NEW_NAME}"
RESULT="$(api_call POST "" "${BODY}")"
HTTP="${RESULT%%|*}"
RESP="${RESULT##*|}"
if [[ "${HTTP}" -ge 300 ]]; then
  echo "ERROR: create failed (HTTP ${HTTP}):" >&2; cat "${RESP}" >&2; exit 1
fi

# ---------- Step 4: wait for RUNNING (or COMPLETED for DDL) ----------
echo "==> Waiting up to ${DEPLOY_TIMEOUT}s for ${NEW_NAME} to reach RUNNING"
deadline=$(( $(date +%s) + DEPLOY_TIMEOUT ))
while :; do
  RESULT="$(api_call GET "/${NEW_NAME}")"
  HTTP="${RESULT%%|*}"
  RESP="${RESULT##*|}"
  if [[ "${HTTP}" -ge 300 ]]; then
    echo "ERROR: describe failed (HTTP ${HTTP}):" >&2; cat "${RESP}" >&2; exit 1
  fi
  PHASE="$(jq -r '.status.phase' "${RESP}")"
  echo "    phase: ${PHASE}"
  case "${PHASE}" in
    RUNNING|COMPLETED) break ;;
    FAILED|FAILING|DEGRADED)
      echo "ERROR: statement entered ${PHASE}. Rolling back." >&2
      jq -r '.status.detail // "(no detail)"' "${RESP}" >&2
      api_call DELETE "/${NEW_NAME}" >/dev/null || true
      if [[ "${CURRENT}" == "EXISTS" && "${CURRENT_STOPPED}" != "true" ]]; then
        echo "==> Resuming prior statement ${STATEMENT_NAME}"
        api_call PATCH "/${STATEMENT_NAME}" '[{"op":"add","path":"/spec/stopped","value":false}]' >/dev/null || true
      fi
      exit 1
      ;;
  esac
  if [[ $(date +%s) -ge ${deadline} ]]; then
    echo "ERROR: timeout waiting for RUNNING. Last response:" >&2
    cat "${RESP}" >&2
    exit 1
  fi
  sleep 5
done

# ---------- Step 5: delete the prior statement only after new one is healthy ----------
if [[ "${CURRENT}" == "EXISTS" ]]; then
  echo "==> Removing prior statement: ${STATEMENT_NAME}"
  api_call DELETE "/${STATEMENT_NAME}" >/dev/null || true
fi

echo "==> ${NEW_NAME} ${PHASE}. Done."
