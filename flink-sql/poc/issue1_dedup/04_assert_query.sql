-- Verification queries. Run as one-shot SELECTs in the Flink workspace.

-- (a) Raw count vs distinct-PK count.
--     After 1 SOD run: roughly equal.
--     After 3 SOD runs: raw_rows ~= 3 x distinct_pks (the bug surface).
SELECT
  COUNT(*)                                                                       AS raw_rows,
  COUNT(DISTINCT (BusinessDate, FundId, PmuId, DealId, SecurityId, Direction))   AS distinct_pks
FROM sod_positions_raw;

-- (b) Deduped count: should equal distinct_pks above, regardless of how many
--     times SOD was re-run upstream.
SELECT COUNT(*) AS deduped_rows FROM sod_positions_dedup;

-- (c) Sanity: any PK with multiple rows in the dedup sink? (Must return 0 rows.)
SELECT
  BusinessDate, FundId, PmuId, DealId, SecurityId, Direction,
  COUNT(*) AS cnt
FROM sod_positions_dedup
GROUP BY BusinessDate, FundId, PmuId, DealId, SecurityId, Direction
HAVING COUNT(*) > 1;
