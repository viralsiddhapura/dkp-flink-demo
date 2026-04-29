#!/usr/bin/env bash
#
# flink_explain.sh <path-to-sql-file>
#
# PR-time validator: runs `EXPLAIN` of the given SQL against the dev compute
# pool. Fails on syntax errors or planner errors. Does NOT submit the statement.
set -euo pipefail

SQL_FILE="${1:?usage: flink_explain.sh <sql-file>}"
[[ -f "$SQL_FILE" ]] || { echo "missing file: $SQL_FILE" >&2; exit 2; }

# Wrap user SQL in EXPLAIN. If the file contains multiple statements separated
# by semicolons, EXPLAIN is applied to the last (the actual INSERT or SELECT);
# preceding DDL is still parsed and validated.
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
sed -e 's/^[[:space:]]*INSERT INTO/EXPLAIN INSERT INTO/' "$SQL_FILE" > "$TMP"

confluent flink statement create \
  "explain_$(basename "$SQL_FILE" .sql)_$$" \
  --compute-pool "$COMPUTE_POOL_ID" \
  --sql "$(cat "$TMP")" \
  --wait \
  -o json | jq '.status'
