-- A position sink that aggregates the dedup'd allocation stream.
--
-- The aggregation is an additive SUM(Quantity) per position PK. This is the
-- exact pattern that multiplies under transaction replay if the source is
-- not dedup'd. Reading from allocation_events_dedup (instead of the raw
-- topic) makes the aggregation idempotent against TransactionId replays.

CREATE TABLE IF NOT EXISTS positions_from_allocations (
  BusinessDate  DATE NOT NULL,
  FundId        INT NOT NULL,
  PmuId         INT NOT NULL,
  DealId        INT NOT NULL,
  SecurityId    BIGINT NOT NULL,
  Direction     VARCHAR(64) NOT NULL,
  TotalQuantity DECIMAL(29, 14),
  PRIMARY KEY (BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) NOT ENFORCED
)
DISTRIBUTED BY HASH(BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) INTO 3 BUCKETS
WITH (
  'changelog.mode'              = 'upsert',
  'kafka.cleanup-policy'        = 'compact',
  'kafka.retention.time'        = '0',
  'key.format'                  = 'avro-registry',
  'value.format'                = 'avro-registry',
  'sink.delivery-guarantee'     = 'exactly-once',
  'sink.transactional-id-prefix' = 'dkp-poc4-positions-'
);

INSERT INTO positions_from_allocations
SELECT
  BusinessDate,
  FundId,
  PmuId,
  DealId,
  SecurityId,
  Direction,
  CAST(SUM(Quantity) AS DECIMAL(29, 14)) AS TotalQuantity
FROM allocation_events_dedup
GROUP BY
  BusinessDate, FundId, PmuId, DealId, SecurityId, Direction;
