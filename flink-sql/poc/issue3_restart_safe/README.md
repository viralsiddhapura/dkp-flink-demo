# POC — Issue 3: Restart-Safe Behavior

**Client problem statement (verbatim):**
> "Intraday restarts of Flink queries should not impact positions or downstream state. The system should ensure stability even with continuous query execution."

**What this POC proves:** A Flink statement configured with `sink.delivery-guarantee = 'exactly-once'`, deployed via the project's `stop-with-savepoint -> submit -> verify` pipeline, can be stopped intraday and resumed without:

- double-emitting any record to downstream consumers (Kafka transactions abort in-flight work),
- losing any record (replay from last successful checkpoint),
- corrupting the sink topic's per-PK state (compacted upsert sink + idempotent observable state).

This is the **mechanical demonstration** of Issue 3's recommended fix in `ANALYSIS.md`. Combined with the operational pieces (RBAC blocking UI deploys, `flink-deploy.yml` enforcing stop-with-savepoint), it closes the restart-safety gap end-to-end.

---

## Why all four mechanisms are needed (defense in depth)

| Layer | What it protects against | Where it's configured |
|---|---|---|
| 1. Stop-with-savepoint | State loss on stop | `scripts/flink_deploy.sh` |
| 2. `sink.delivery-guarantee = 'exactly-once'` | Double-emission on resume | `02_sink_exactly_once.sql` |
| 3. Upsert+compacted sink (idempotent observable state) | Even if 1 or 2 fail, observable state stays correct | `02_sink_exactly_once.sql` |
| 4. RBAC: only CI/CD has `FlinkAdmin` | Engineer-induced bypass of layers 1-3 | Terraform IAM modules |

If any layer is removed, the others continue to protect — but layer 1 is load-bearing for the recovery semantics, and layer 2 is required for non-idempotent sinks. The POC covers layers 1-3; layer 4 is enforced outside the POC.

---

## Files

| File | Purpose |
|---|---|
| `01_source_table.sql` | Upsert source over the SOD topic (same shape as Issue 2). |
| `02_sink_exactly_once.sql` | Compacted upsert sink with `sink.delivery-guarantee = 'exactly-once'` and a unique `transactional-id-prefix`. |
| `03_query.sql` | Passthrough `INSERT` — minimal query that still exercises offset state and transactional commit. |
| `04_restart_test.sql` | "Fingerprint" SELECTs to compare sink state before vs. after restart. |

## Test procedure

```bash
# 0) Set context.
confluent environment use $CONFLUENT_ENV_ID_DEV
confluent flink compute-pool use $CONFLUENT_FLINK_COMPUTE_POOL_ID_DEV

# 1) Deploy the POC.
confluent flink statement create poc3_src   --sql "$(cat 01_source_table.sql)"
confluent flink statement create poc3_sink  --sql "$(cat 02_sink_exactly_once.sql)"
confluent flink statement create poc3_query --sql "$(cat 03_query.sql)"

# 2) Let it run. Wait until the source has produced enough records that you
#    can see committed rows in the sink (read_committed consumer required).
kcat -b $BOOTSTRAP -X 'isolation.level=read_committed' -t sod_positions_eo -C -q -e | wc -l

# 3) Capture the pre-restart fingerprint.
confluent flink statement create poc3_check_pre --sql "$(cat 04_restart_test.sql)"
# Save the (total_pks, total_quantity_sum, latest_committed_load_ts) values.

# 4) Stop the query with savepoint preservation. The deploy script does this
#    automatically on a normal deploy; for this manual test:
confluent flink statement stop poc3_query --compute-pool $POOL

# 5) Confirm the sink stops advancing while the statement is stopped.
#    latest_committed_load_ts should not change.

# 6) Resume.
confluent flink statement resume poc3_query --compute-pool $POOL

# 7) Capture the post-restart fingerprint after a brief warmup.
confluent flink statement create poc3_check_post --sql "$(cat 04_restart_test.sql)"

# 8) Compare. The PASS criteria are:
#    - total_pks did not regress
#    - total_quantity_sum is consistent (no double-counted records)
#    - latest_committed_load_ts advances or stays equal — never goes backward
#    - Spot-checked PKs have the same Quantity pre and post restart
```

## Edge cases to test additionally

- **Cold restart (no savepoint).** Delete the statement entirely (`statement delete`) and re-create it. Behavior depends on `scan.startup.mode`: `earliest-offset` will replay the entire topic and rebuild state — this exposes whether the *query* is idempotent against full replay (Issue 4 territory).
- **Long stop window.** Stop for >30 minutes, then resume. If the query has event-time semantics, watermark state may have drifted; bounded out-of-orderness must be sized accordingly.
- **Schema change during stop.** Modify a value-side column type and resume. Confluent Cloud Flink's in-place upgrade tolerates some changes; `flink-validate.yml` should run an `EXPLAIN` plan diff before any deploy that follows a savepoint.
- **Two statements with the same `transactional-id-prefix`.** Should fence each other. The deploy script's `_<git-sha>` naming prevents this in the normal case.

## What this POC does NOT prove

- That every existing production query is restart-safe — they need to be retrofitted with the same DDL pattern. This POC is the template.
- That **all** downstream consumers of the sink topic use `read_committed`. Audit consumer configs separately (open question for the next sync).
- Idempotence under transaction-level replay — see `../issue4_idempotence/`.
