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
#   CONFLUENT_CLOUD_API_KEY, CONFLUENT_CLOUD_API_SECRET
#   CONFLUENT_FLINK_API_KEY,  CONFLUENT_FLINK_API_SECRET
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

echo "==> Logging in to Confluent Cloud (org $ORG_ID)"
confluent login --save --no-browser --organization-id "$ORG_ID"
confluent environment use "$ENV_ID"

case "$ACTION" in
  stop)
    echo "==> Stopping statement: $STATEMENT (state retained)"
    confluent flink statement stop "$STATEMENT" --compute-pool "$COMPUTE_POOL_ID"
    ;;
  resume)
    echo "==> Resuming statement: $STATEMENT"
    confluent flink statement resume "$STATEMENT" --compute-pool "$COMPUTE_POOL_ID"
    ;;
  delete)
    echo "==> Deleting statement: $STATEMENT (NOT reversible)"
    confluent flink statement delete "$STATEMENT" --compute-pool "$COMPUTE_POOL_ID" --force
    ;;
  describe)
    echo "==> Describing statement: $STATEMENT"
    confluent flink statement describe "$STATEMENT" --compute-pool "$COMPUTE_POOL_ID" -o yaml
    ;;
  list)
    echo "==> Listing statements in compute pool $COMPUTE_POOL_ID"
    confluent flink statement list --compute-pool "$COMPUTE_POOL_ID"
    ;;
  *)
    echo "ERROR: unknown action '$ACTION' (expected: stop|resume|delete|describe|list)" >&2
    exit 2
    ;;
esac

echo "==> Done."
