#!/usr/bin/env bash
#
# flink_lifecycle.sh <action> [statement-name]
#
# Performs lifecycle operations on a Confluent Cloud Flink statement.
#
# Actions:
#   stop       — gracefully stop a running statement (state retained for resume)
#   resume     — resume a stopped statement from its retained state
#   delete     — permanently delete a statement (state lost; not reversible)
#   describe   — print the current phase and metadata for a statement
#   list       — list all statements in the compute pool (statement name not required)
#
# Required env (set by the workflow from GitHub Actions secrets):
#   ORG_ID, ENV_ID, COMPUTE_POOL_ID
#   CONFLUENT_CLOUD_API_KEY, CONFLUENT_CLOUD_API_SECRET    (resource-level)
#   CONFLUENT_FLINK_API_KEY,  CONFLUENT_FLINK_API_SECRET   (Flink-region-level)
#
# Auth model:
#   No `confluent login` is performed. The Confluent CLI authenticates Flink
#   statement commands from the CONFLUENT_FLINK_API_KEY / CONFLUENT_FLINK_API_SECRET
#   env vars (Flink-region-scoped). The cloud-level API key is set for any
#   non-Flink CLI command that may run alongside.
#
# Usage from the workflow:
#   ./scripts/flink_lifecycle.sh stop orders_by_item
#   ./scripts/flink_lifecycle.sh resume orders_by_item
#   ./scripts/flink_lifecycle.sh list
#
set -euo pipefail

ACTION="${1:?usage: flink_lifecycle.sh <stop|resume|delete|describe|list> [statement-name]}"
STATEMENT="${2:-}"

if [[ "$ACTION" != "list" && -z "$STATEMENT" ]]; then
  echo "ERROR: statement name is required for action '$ACTION'" >&2
  exit 2
fi

# Common flags applied to every `confluent flink statement *` invocation.
# --environment selects the Confluent Cloud env without needing `confluent environment use`,
# which would require an active login session.
FLINK_FLAGS=(
  --environment   "$ENV_ID"
  --compute-pool  "$COMPUTE_POOL_ID"
)

case "$ACTION" in
  stop)
    echo "==> Stopping statement: $STATEMENT (state retained)"
    confluent flink statement stop "$STATEMENT" "${FLINK_FLAGS[@]}"
    ;;
  resume)
    echo "==> Resuming statement: $STATEMENT"
    confluent flink statement resume "$STATEMENT" "${FLINK_FLAGS[@]}"
    ;;
  delete)
    echo "==> Deleting statement: $STATEMENT (NOT reversible)"
    confluent flink statement delete "$STATEMENT" "${FLINK_FLAGS[@]}" --force
    ;;
  describe)
    echo "==> Describing statement: $STATEMENT"
    confluent flink statement describe "$STATEMENT" "${FLINK_FLAGS[@]}" -o yaml
    ;;
  list)
    echo "==> Listing statements in compute pool $COMPUTE_POOL_ID"
    confluent flink statement list "${FLINK_FLAGS[@]}"
    ;;
  *)
    echo "ERROR: unknown action '$ACTION' (expected: stop|resume|delete|describe|list)" >&2
    exit 2
    ;;
esac

echo "==> Done."
