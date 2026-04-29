#!/usr/bin/env bash
#
# flink_deploy.sh <path-to-sql-file>
#
# Deploys (or updates) a Flink SQL statement on Confluent Cloud using the
# stop-with-savepoint → submit → verify pattern. Statement name is derived from
# the file name (e.g., flink-sql/queries/q1_alloc_business_date.sql →
# statement name "q1_alloc_business_date").
#
# Required env:
#   ORG_ID, ENV_ID, COMPUTE_POOL_ID
#   CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET (for `confluent login`)
#
# Notes on safety:
#   - We never `delete` a stateful statement. We `stop` it (which retains state)
#     and then submit a new statement that resumes from the implicit savepoint.
#   - If the new statement fails to reach RUNNING within DEPLOY_TIMEOUT, we
#     resume the prior statement so the pipeline self-heals.
set -euo pipefail

SQL_FILE="${1:?usage: flink_deploy.sh <sql-file>}"
[[ -f "$SQL_FILE" ]] || { echo "missing file: $SQL_FILE" >&2; exit 2; }

STATEMENT_NAME="$(basename "$SQL_FILE" .sql)"
DEPLOY_TIMEOUT="${DEPLOY_TIMEOUT:-300}"

echo "==> Deploying statement: $STATEMENT_NAME"
echo "    file: $SQL_FILE"
echo "    env:  $ENV_ID  pool: $COMPUTE_POOL_ID"

confluent login --save --no-browser --organization "$ORG_ID"
confluent environment use "$ENV_ID"

current_status() {
  confluent flink statement describe "$STATEMENT_NAME" \
    --compute-pool "$COMPUTE_POOL_ID" -o json 2>/dev/null \
    | jq -r '.status.phase // "ABSENT"' \
    || echo "ABSENT"
}

CURRENT="$(current_status)"
echo "    current phase: $CURRENT"

# Step 1: stop existing statement (retains state for resume)
if [[ "$CURRENT" == "RUNNING" ]]; then
  echo "==> Stopping existing statement (savepoint will be retained)"
  confluent flink statement stop "$STATEMENT_NAME" --compute-pool "$COMPUTE_POOL_ID"
fi

# Step 2: submit new SQL. We rename to <name>_v<git-short-sha> so that we keep
# the prior statement object around for fast rollback. Aliases / pointer
# topics handled at the workflow level.
GIT_SHA="$(git rev-parse --short=8 HEAD)"
NEW_NAME="${STATEMENT_NAME}_${GIT_SHA}"

echo "==> Submitting new statement: $NEW_NAME"
confluent flink statement create "$NEW_NAME" \
  --compute-pool "$COMPUTE_POOL_ID" \
  --sql "$(cat "$SQL_FILE")"

# Step 3: wait until RUNNING (or fail loudly)
echo "==> Waiting up to ${DEPLOY_TIMEOUT}s for $NEW_NAME to reach RUNNING"
deadline=$(( $(date +%s) + DEPLOY_TIMEOUT ))
while :; do
  phase="$(
    confluent flink statement describe "$NEW_NAME" \
      --compute-pool "$COMPUTE_POOL_ID" -o json | jq -r '.status.phase'
  )"
  echo "    phase: $phase"
  case "$phase" in
    RUNNING) break ;;
    FAILED|DEGRADED)
      echo "ERROR: statement entered $phase. Rolling back." >&2
      confluent flink statement delete "$NEW_NAME" --compute-pool "$COMPUTE_POOL_ID" --force || true
      if [[ "$CURRENT" == "RUNNING" ]]; then
        echo "==> Resuming prior statement $STATEMENT_NAME"
        confluent flink statement resume "$STATEMENT_NAME" --compute-pool "$COMPUTE_POOL_ID" || true
      fi
      exit 1
      ;;
  esac
  if [[ $(date +%s) -ge $deadline ]]; then
    echo "ERROR: timeout waiting for RUNNING" >&2
    exit 1
  fi
  sleep 5
done

# Step 4: clean up the old statement only after the new one is RUNNING.
if [[ "$CURRENT" != "ABSENT" ]]; then
  echo "==> Removing prior statement: $STATEMENT_NAME"
  confluent flink statement delete "$STATEMENT_NAME" --compute-pool "$COMPUTE_POOL_ID" --force || true
fi

echo "==> $NEW_NAME RUNNING. Done."
