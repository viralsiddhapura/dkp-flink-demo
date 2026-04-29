# POC — Issue 4: Idempotence (Transaction & SOD level)

**Client problem statement (verbatim):**
> "The system should support idempotent processing — recomputing or reapplying data for a given business date should always result in the same final position/state, regardless of how many times it is run."

**The umbrella view:** Idempotence has two dimensions, and the four issues map to them:

| Dimension | Meaning | Where it's covered |
|---|---|---|
| **SOD-level idempotence** | Re-running SOD for a date converges to the same final position. | `../issue1_dedup/` (dedup approach) and `../issue2_composite_pk/` (upsert source approach). |
| **Transaction-level idempotence** | Re-emitting the same allocation transaction does not double-count. | **This POC.** Dedup by `TransactionId` at the entry of Q1. |

Issues 1, 2, 3 are *necessary but not sufficient* for full idempotence — they don't address the transaction replay path. This POC closes that gap.

---

## What this POC proves

A `ROW_NUMBER() OVER (PARTITION BY TransactionId)` dedup at the entry of Q1, materialized to an upsert+compacted sink, gives transaction-level idempotence. Replaying the same set of `TransactionId`s upstream (broker retry, savepoint resume, manual replay) does not change the downstream position state.

The exactly-once sink in `03_position_sink.sql` (lifted from `../issue3_restart_safe/`) ensures that the position aggregation also doesn't double-emit on Flink restart — combining transaction-level idempotence with restart safety in one demo.

---

## Files

| File | Purpose |
|---|---|
| `01_allocation_source.sql` | Raw allocation event topic. Each event has a `TransactionId` field that must be unique per logical event. |
| `02_dedup_by_txn.sql` | Dedup-by-TransactionId sink + the `INSERT` that populates it. Creates a clean idempotent allocation stream. |
| `03_position_sink.sql` | Position aggregation (`SUM(Quantity)` per position PK) reading from the dedup'd stream. Sink is upsert+compacted with `sink.delivery-guarantee = 'exactly-once'`. |
| `04_chaos_test.sql` | Fingerprint queries to compare pre-replay vs post-replay state. |

## Test procedure (chaos replay)

```bash
# 1) Deploy the POC.
confluent flink statement create poc4_src    --sql "$(cat 01_allocation_source.sql)"
confluent flink statement create poc4_dedup  --sql "$(cat 02_dedup_by_txn.sql)"
confluent flink statement create poc4_pos    --sql "$(cat 03_position_sink.sql)"

# 2) Let the producer push N original allocation events. Wait until the
#    position sink has caught up.

# 3) Capture pre-replay fingerprint.
confluent flink statement create poc4_check_pre --sql "$(cat 04_chaos_test.sql)"
# Save (distinct_transactions, dedup_total_qty, the position rows).

# 4) Replay the same N events upstream — same TransactionIds, same payloads.
#    Easiest: capture the original events with kcat and re-publish them.
kcat -b $BOOTSTRAP -t allocation_events_raw -C -e -o beginning -q > replay.bin
kcat -b $BOOTSTRAP -t allocation_events_raw -P -l replay.bin

# 5) Wait for Flink to catch up. Capture post-replay fingerprint.
confluent flink statement create poc4_check_post --sql "$(cat 04_chaos_test.sql)"

# 6) PASS criteria:
#    - distinct_transactions: unchanged
#    - dedup_total_qty: unchanged
#    - Each row in positions_from_allocations: unchanged TotalQuantity
#    - raw_total_qty has roughly doubled (the replay was admitted upstream)
#      while the dedup'd stream did not — that gap is the bug surface this
#      POC eliminates.
```

## Edge cases this POC covers

- **Broker retries** — same TransactionId, same payload, multiple times. The dedup operator drops all but the first.
- **Savepoint resume after partial commit** — the upstream may re-emit transactions already processed; same dedup logic handles them.
- **Out-of-order arrival of duplicates** — `ORDER BY emitted_ts ASC` keeps the original (earliest) record; later duplicates are discarded.

## Edge cases that need additional handling

- **Corrections / reversals** — a real business correction (a later event that amends an earlier one) MUST carry a different `TransactionId`. Confirm with the upstream producer team that corrections never reuse a TransactionId. If they do, the dedup logic here would silently swallow the correction.
- **TransactionId = NULL** — schema must enforce `NOT NULL` on `TransactionId`. A null id breaks the dedup partition.
- **Backfills for historical business dates** — the dedup state grows with cardinality. For a long-running POC, add a state TTL to the dedup query to bound memory.

## What this POC does NOT prove

- **Idempotence under additive aggregation when the source is not dedup'd.** That's the bug surface; this POC's whole point is that adding the dedup pass eliminates it. Removing `02_dedup_by_txn.sql` and reading directly from `allocation_events_raw` in `03_position_sink.sql` would reproduce the bug.
- **End-to-end idempotence including SOD restate.** That requires combining this POC with Issue 1 or Issue 2's fix on the SOD side.
- **State-machine modeling** (positions as `f(SOD, set-of-transactions)` instead of `f(SOD, sum-of-transactions)`). That is the longer-term direction in `ANALYSIS.md`'s Issue 4 Option 3, and is out of scope here.
