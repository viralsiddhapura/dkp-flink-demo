# POC — Issue 2: Kafka Key Enforcement (the `NOT ENFORCED` reality)

**Client problem statement (verbatim):**
> "We need to ensure that Kafka topic keys are properly enforced in the underlying Flink tables. While objects are being published as keys and Flink derives primary keys from constraints, [those PK] constraints are not currently enforced."

**The reframing this POC delivers:**
> Flink SQL has **no `ENFORCED` option for PRIMARY KEY**. The constraint clause is `PRIMARY KEY (...) NOT ENFORCED` — that is the only legal form. This is a Flink SQL **language limitation**, not a DKP configuration issue. Flink does not validate uniqueness at insert time the way a relational database would.
>
> Effective end-to-end "enforcement" comes from three mechanisms used together:
> 1. `changelog.mode='upsert'` on every keyed source.
> 2. `changelog.mode='upsert'` + `kafka.cleanup-policy='compact'` on every keyed sink.
> 3. Code review discipline — flagging any DDL on a keyed compacted topic that uses `'append'` mode.

**What this POC proves:** A passthrough pipeline (upsert source → upsert sink) emits exactly one logical row per PK end-to-end, even when the upstream SOD producer replays the same business date multiple times. No `ROW_NUMBER`, no aggregation, no dedup query — Flink does it for you because the changelog mode is correct.

---

## Why this is the cleaner fix vs. issue1_dedup

| Dimension | issue1_dedup (`ROW_NUMBER`) | issue2_composite_pk (upsert source) |
|---|---|---|
| Source DDL `changelog.mode` | `'append'` (the buggy state) | `'upsert'` (the fix) |
| Extra Flink statement | Yes (long-running dedup `INSERT`) | No |
| Extra intermediate topic | Yes | No |
| State growth | Grows with cardinality | None (Flink doesn't materialize) |
| Late-arrival handling | `load_ts DESC` keeps latest | Native upsert (latest wins by Kafka offset) |
| Tombstone support | None | Yes (null value = delete) |
| When to use | Source DDL is owned elsewhere and can't be changed quickly | You own the table DDL |

If you can change the source DDL, this is the fix. If not, fall back to `issue1_dedup`.

---

## Files

| File | Purpose |
|---|---|
| `01_source_table.sql` | Registers the upstream SOD topic as a Flink table with `changelog.mode='upsert'`. The PK is declared `NOT ENFORCED` because that is the only Flink option — the comment explains why this is fine. |
| `02_sink_table.sql` | Compacted, upsert-keyed passthrough sink. Same PK and `DISTRIBUTED BY HASH(...) INTO 3 BUCKETS` as the production target topic. |
| `03_restream_query.sql` | Plain `INSERT INTO sink SELECT * FROM source` — no dedup logic. Flink handles it because both ends are upsert. |
| `04_consumer_test.sql` | One-shot SELECTs that confirm one row per PK end-to-end and zero duplicate-PK rows in the sink. |

## Run order in Confluent Cloud Flink workspace

```bash
# Adjust topic name / column types in 01_source_table.sql to match your
# upstream SOD schema before running, then:
confluent flink statement create poc2_src     --sql "$(cat 01_source_table.sql)"
confluent flink statement create poc2_sink    --sql "$(cat 02_sink_table.sql)"
confluent flink statement create poc2_through --sql "$(cat 03_restream_query.sql)"

# Replay your SOD producer 3 times for the same business date, then:
confluent flink statement create poc2_check   --sql "$(cat 04_consumer_test.sql)"
```

## Edge cases this POC covers

- **N replays of the same SOD batch upstream** — sink converges to one row per PK. The `NOT ENFORCED` constraint behaves as if it were enforced, by virtue of the upsert mode.
- **Same PK with different quantities** across replays — latest value wins, by Kafka offset order.
- **Tombstones (null value for an existing key)** — passthrough propagates them; downstream consumers see the row disappear after compaction.

## What this POC does NOT prove

- Producer-side correctness — if the upstream producer emits *deltas* rather than full snapshots per key, upsert semantics multiply problems rather than fix them. Confirm with the SOD producer team that each emitted record represents the intended *latest value* per key.
- Restart safety — see `../issue3_restart_safe/`.
- Transaction-level idempotence — see `../issue4_idempotence/`.

## Code-review checklist (organizational fix that complements this POC)

When reviewing any new Flink SQL DDL on a compacted, keyed topic, check:

- [ ] `changelog.mode = 'upsert'` (not the default `'append'`)?
- [ ] `PRIMARY KEY (...) NOT ENFORCED` is declared and matches the topic's Avro key fields?
- [ ] `DISTRIBUTED BY HASH(pk_columns)` matches the PK?
- [ ] `key.format = 'avro-registry'` (or other structured key format — never `'raw'` for keyed topics)?
- [ ] If the table is a sink: `kafka.cleanup-policy = 'compact'`?

Any "no" is a latent multiplication bug.
