-- Verification queries for the restart-safety test.
--
-- Run BEFORE the restart, then run AGAIN AFTER the restart, and compare.
-- Restart procedure is documented in this directory's README.md.

-- (a) Total rows in the exactly-once sink, by PK, with quantity sum.
--     This is the "fingerprint" of the sink state. It must be byte-identical
--     before and after a stop-with-savepoint -> resume cycle.
SELECT
  COUNT(*)                                                                      AS total_pks,
  SUM(CASE WHEN Quantity IS NULL THEN 0 ELSE 1 END)                             AS pks_with_quantity,
  CAST(SUM(COALESCE(Quantity, 0)) AS DECIMAL(38, 14))                           AS total_quantity_sum
FROM sod_positions_eo;

-- (b) Latest load_ts seen at the sink. Should advance monotonically across
--     restarts (no regression -> no replay of already-committed records).
SELECT MAX(load_ts) AS latest_committed_load_ts FROM sod_positions_eo;

-- (c) Spot-check a small set of PKs (substitute real values from your
--     environment). The same PK should have the same Quantity / MktValLocal
--     pre-restart and post-restart.
SELECT BusinessDate, FundId, PmuId, DealId, SecurityId, Direction, Quantity, MktValLocal, load_ts
FROM sod_positions_eo
WHERE BusinessDate = DATE '2026-04-29'
ORDER BY FundId, PmuId, DealId, SecurityId, Direction
LIMIT 20;
