-- Transaction-id dedup at the entry of Q1.
--
-- This sink is the cleansed allocation stream that downstream queries should
-- read instead of the raw topic. Every TransactionId appears at most once,
-- regardless of broker retries or upstream re-emission.
--
-- PK is TransactionId (one logical event per id). Sink is upsert+compacted
-- so that a re-emission of the same TransactionId is a no-op upsert at the
-- Kafka level even if it sneaks past the dedup operator.

CREATE TABLE IF NOT EXISTS allocation_events_dedup (
  TransactionId  STRING NOT NULL,
  BusinessDate   DATE NOT NULL,
  FundId         INT NOT NULL,
  PmuId          INT NOT NULL,
  DealId         INT NOT NULL,
  SecurityId     BIGINT NOT NULL,
  Direction      VARCHAR(64) NOT NULL,
  Quantity       DECIMAL(29, 14),
  emitted_ts     TIMESTAMP_LTZ(3),
  PRIMARY KEY (TransactionId) NOT ENFORCED
)
DISTRIBUTED BY HASH(TransactionId) INTO 6 BUCKETS
WITH (
  'changelog.mode'       = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'kafka.retention.time' = '0',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry'
);

-- The dedup query: keep the FIRST observation of each TransactionId.
-- We pick FIRST (oldest emitted_ts) on the assumption that the original
-- event is the source of truth and any later duplicate is a retry. If your
-- producer's semantics are different (e.g., later observations are
-- corrections that share the same id), invert the ORDER BY.

INSERT INTO allocation_events_dedup
SELECT
  TransactionId,
  BusinessDate,
  FundId,
  PmuId,
  DealId,
  SecurityId,
  Direction,
  Quantity,
  emitted_ts
FROM (
  SELECT
    TransactionId,
    BusinessDate,
    FundId,
    PmuId,
    DealId,
    SecurityId,
    Direction,
    Quantity,
    emitted_ts,
    ROW_NUMBER() OVER (
      PARTITION BY TransactionId
      ORDER BY emitted_ts ASC
    ) AS rn
  FROM allocation_events_raw
)
WHERE rn = 1;
