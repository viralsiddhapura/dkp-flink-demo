# Appendix POC — `business_date` schema migration (`timestamp_micro` -> logical `date`)

> **Status: out of scope for the client's authoritative 4 issues.**
>
> This POC was scaffolded based on a concern carried over from prior project memory. **It was not raised in the client's April 2026 issue write-up.** It is preserved here for reference because the underlying concern is a real Avro/Confluent Cloud Flink anti-pattern — but it is not on the critical path.
>
> Before investing in this migration, **verify**: does the production Avro schema actually use `timestamp_micro` for `BusinessDate`? The screenshot of `uat.sod.position.global.new.compact.avro` shows `BusinessDate DATE NOT NULL` (logical date) on the value side — so the value-side concern may already be handled. Check the upstream `transaction_allocation` and `business_date` topic schemas before reopening this work item.

---

## Original goal (preserved as written)

Demonstrate a backward-compatible schema evolution that introduces `business_date_v2` as a logical `date` field alongside the existing `timestamp_micro`-typed `business_date`, without breaking current consumers.

## Original approach

1. Register an updated Avro schema that adds the new `business_date_v2` field with a default value (this is BACKWARD-compatible per Schema Registry rules).
2. Producers begin dual-populating both fields.
3. New Flink queries read `COALESCE(business_date_v2, CAST(business_date AS DATE))` so they work whether the producer has been updated or not.
4. After all producers are updated, queries can switch to reading `business_date_v2` exclusively and the old field is dropped in a future schema version.

## Files (preserved as-is)

- `schemas/business_date_v1.avsc` — current schema (for reference).
- `schemas/business_date_v2.avsc` — updated schema with the new logical-date field.
- `01_compat_check.sh` — script to verify Schema Registry accepts v2 as BACKWARD-compatible against v1.
- `02_consumer_table.sql` — Flink table that reads either schema version transparently.
- `03_dual_write_check.sql` — verifies that records produced under v2 are correctly read with both fields, and records produced under v1 fall back to the cast.

## What this POC would prove (if the underlying concern is confirmed)

- Adding a logical-date field to an Avro schema with a sensible default is BACKWARD compatible.
- A single Flink query can transparently consume both old and new producer outputs.
- The TZ drift from `timestamp_micro -> UTC` is eliminated for any consumer that reads the new field.

## Edge cases this POC covers

- Records with only `business_date` populated (legacy producer): `COALESCE` falls back to the cast.
- Records with both fields populated (post-migration): consumer reads `business_date_v2` directly.
- Records with only `business_date_v2` populated (future state, after v3 drops the old field): same query still works.

## What this POC does NOT prove

- Migration of the **key** schema (a separate, harder problem — see `ANALYSIS.md` appendix). If `business_date` is in the message key, you cannot deprecate it in place; you need a v2 topic with a new key schema.

## Cross-reference

See `ANALYSIS.md` -> "Appendix — Additional finding (not in client's 4 issues)" for the analytical write-up of when this work might be reopened.
