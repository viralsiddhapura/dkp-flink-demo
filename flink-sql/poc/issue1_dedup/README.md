# POC — Issue 1: Duplicate SOD Runs (multiplication symptom)

**Client problem statement (verbatim):**
> "When the Start of Day (SOD) process is executed multiple times (e.g., three times), all SOD data shares the same set of keys. However, the SOD positions in the `prod.position.global.compact.avro` Position topic appear to be aggregated, resulting in values being multiplied (e.g., 3x the original position)."

**What this POC proves:** A Flink-side `ROW_NUMBER()` dedup pass collapses N runs of SOD to one row per logical key, regardless of how the producer behaves. The downstream join no longer multiplies.

**Why this is the stopgap, not the long-term fix:** The cleaner fix is to model the SOD source as `changelog.mode='upsert'` so Flink natively treats every SOD record as an upsert by key (no dedup query needed). That's demonstrated in `../issue2_composite_pk/`. This POC exists for the case where the source DDL cannot be changed quickly (e.g., shared statements, downstream dependencies), or as a defensive layer at uncontrolled boundaries.

---

## What you'll see

1. Replay the SOD producer 3 times against the same business date.
2. `sod_positions_raw` — row count grows to roughly 3x distinct PKs (the bug surface).
3. `sod_positions_dedup` — row count stays equal to the distinct PK count.
4. Any downstream query reading from `sod_positions_dedup` produces the correct, non-multiplied counts.

## Files

| File | Purpose |
|---|---|
| `01_source_table.sql` | Registers the upstream SOD topic as an **append-mode** Flink table. This is the configuration that exhibits the bug. |
| `02_sink_table.sql` | Creates the compacted, upsert-keyed sink topic with the production 6-field PK: `(BusinessDate, FundId, PmuId, DealId, SecurityId, Direction)`. |
| `03_dedup_query.sql` | Long-running `INSERT` with `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY load_ts DESC) = 1`. |
| `04_assert_query.sql` | One-shot SELECTs that demonstrate raw-rows vs deduped-rows divergence after replays. |

## Run order in Confluent Cloud Flink workspace

```bash
# Adjust topic name / column types in 01_source_table.sql to match your datagen
# schema before running, then:
confluent flink statement create poc1_src    --sql "$(cat 01_source_table.sql)"
confluent flink statement create poc1_sink   --sql "$(cat 02_sink_table.sql)"
confluent flink statement create poc1_dedup  --sql "$(cat 03_dedup_query.sql)"

# Replay your datagen 3 times for the same business date, then:
confluent flink statement create poc1_check  --sql "$(cat 04_assert_query.sql)"
```

## Edge cases this POC covers

- **Same PK across runs with different quantities** (legitimate restate) — latest wins. Correct.
- **Same PK identical across runs** — single row in dedup output. Idempotent.
- **New PK appearing only in run 2** — appended on first sight.
- **PK present in run 1, absent in run 2** — **lingers** in the dedup sink. This is a known limitation: dedup-only cannot express "removed positions." The fix requires producer-side tombstones (out of scope for this POC).

## What this POC does NOT prove

- Producer-side tombstones for removed positions (Issue 1 long-term Option 4 in `ANALYSIS.md`).
- `changelog.mode='upsert'` source semantics — see `../issue2_composite_pk/` for that.
- Restart safety under intraday Flink restarts — see `../issue3_restart_safe/`.
- Transaction-level idempotence on the allocation side — see `../issue4_idempotence/`.

## Production schema reference

The production sink DDL (from screenshot of `uat.sod.position.global.new.compact.avro`) declares:

```sql
PRIMARY KEY (BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) NOT ENFORCED
DISTRIBUTED BY HASH(BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
)
```

The sink in this POC mirrors that shape exactly (with a representative subset of value columns to keep the demo readable).
