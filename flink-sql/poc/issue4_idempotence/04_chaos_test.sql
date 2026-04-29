-- Verification queries for the idempotence chaos test.
--
-- The test re-emits a known set of allocation events (same TransactionIds,
-- same payloads) and asserts that:
--   1. allocation_events_dedup row count is unchanged.
--   2. positions_from_allocations TotalQuantity per PK is unchanged.
--
-- See README.md for the full test procedure (replay producer or kcat
-- pipeline that re-publishes a captured set of events).

-- (a) Cardinality of unique transactions seen so far. Stable across replays.
SELECT COUNT(*) AS distinct_transactions FROM allocation_events_dedup;

-- (b) Position fingerprint. The set of (PK, TotalQuantity) tuples is the
--     observable system state. Must be byte-identical pre- and post-replay.
SELECT
  BusinessDate, FundId, PmuId, DealId, SecurityId, Direction,
  TotalQuantity
FROM positions_from_allocations
ORDER BY BusinessDate, FundId, PmuId, DealId, SecurityId, Direction;

-- (c) Sanity: any TransactionId appearing more than once in the dedup sink?
--     (Must return 0 rows; would indicate the dedup operator itself failed.)
SELECT TransactionId, COUNT(*) AS cnt
FROM allocation_events_dedup
GROUP BY TransactionId
HAVING COUNT(*) > 1;

-- (d) Total allocated quantity, raw vs dedup'd. Pre-replay these can be
--     equal or raw can be slightly higher (broker retries already in flight).
--     Post-replay, raw will jump by N x replay-set; dedup'd must NOT change.
SELECT
  (SELECT CAST(SUM(Quantity) AS DECIMAL(38, 14)) FROM allocation_events_raw)   AS raw_total_qty,
  (SELECT CAST(SUM(Quantity) AS DECIMAL(38, 14)) FROM allocation_events_dedup) AS dedup_total_qty;
