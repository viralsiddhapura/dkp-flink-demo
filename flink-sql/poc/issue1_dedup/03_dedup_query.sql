-- Streaming dedup: keep the latest row per
--   (BusinessDate, FundId, PmuId, DealId, SecurityId, Direction)
-- ordered by load_ts DESC. Late arrivals with older load_ts are dropped,
-- which is correct in normal SOD operation (the producer always emits the
-- intended latest value).
--
-- Run as a long-running INSERT on Confluent Cloud Flink.

INSERT INTO sod_positions_dedup
SELECT
  BusinessDate,
  FundId,
  PmuId,
  DealId,
  SecurityId,
  Direction,
  Quantity,
  MktValLocal,
  load_ts
FROM (
  SELECT
    BusinessDate,
    FundId,
    PmuId,
    DealId,
    SecurityId,
    Direction,
    Quantity,
    MktValLocal,
    load_ts,
    ROW_NUMBER() OVER (
      PARTITION BY BusinessDate, FundId, PmuId, DealId, SecurityId, Direction
      ORDER BY load_ts DESC
    ) AS rn
  FROM sod_positions_raw
)
WHERE rn = 1;
