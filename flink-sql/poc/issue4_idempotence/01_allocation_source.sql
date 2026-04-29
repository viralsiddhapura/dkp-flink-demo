-- Issue 4 source: transaction allocation events. The producer (or broker
-- retry) may emit the same TransactionId more than once. Without dedup at
-- the entry of Q1, every replay double-counts in any downstream
-- aggregation that sums allocation quantity.
--
-- The TransactionId is the natural idempotency key for an allocation event.
-- It must be unique per *logical* transaction; corrections (a real business
-- event that reverses or amends a prior allocation) must carry a different
-- TransactionId.

CREATE TABLE IF NOT EXISTS allocation_events_raw (
  TransactionId  STRING NOT NULL,
  BusinessDate   DATE NOT NULL,
  FundId         INT NOT NULL,
  PmuId          INT NOT NULL,
  DealId         INT NOT NULL,
  SecurityId     BIGINT NOT NULL,
  Direction      VARCHAR(64) NOT NULL,
  Quantity       DECIMAL(29, 14),
  emitted_ts     TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR emitted_ts AS emitted_ts - INTERVAL '5' SECOND
) WITH (
  'changelog.mode'    = 'append',
  'value.format'      = 'avro-registry',
  'key.format'        = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);
