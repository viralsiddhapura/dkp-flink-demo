-- Verification queries for Issue 2.
--
-- The point of these queries is to demonstrate that:
--   1. Re-running SOD upstream multiple times does NOT multiply rows when
--      the source is read with changelog.mode='upsert'.
--   2. The PRIMARY KEY ... NOT ENFORCED declaration is honored in practice
--      because Flink processes the stream as a changelog (one row per key).
--   3. The passthrough sink eventually contains exactly one record per PK
--      after compaction — the standard production shape.

-- (a) Distinct PKs visible at the source. Should be stable across SOD replays.
SELECT
  COUNT(DISTINCT (BusinessDate, FundId, PmuId, DealId, SecurityId, Direction))   AS distinct_pks
FROM sod_positions_keyed;

-- (b) Total rows visible to a downstream consumer of the upsert source.
--     With changelog.mode='upsert', this returns one row per PK at any point
--     in time — even after N upstream replays.
SELECT COUNT(*) AS rows_visible_to_upsert_consumer FROM sod_positions_keyed;

-- (c) Rows in the passthrough sink. Should equal distinct_pks.
SELECT COUNT(*) AS passthrough_rows FROM sod_positions_passthrough;

-- (d) Sanity: any PK with multiple rows in the passthrough sink? (Must be 0.)
SELECT
  BusinessDate, FundId, PmuId, DealId, SecurityId, Direction,
  COUNT(*) AS cnt
FROM sod_positions_passthrough
GROUP BY BusinessDate, FundId, PmuId, DealId, SecurityId, Direction
HAVING COUNT(*) > 1;
