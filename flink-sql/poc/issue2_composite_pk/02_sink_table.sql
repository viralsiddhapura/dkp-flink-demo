-- Issue 2 sink: passthrough copy of the upstream upsert source. Same shape
-- as production sod_positions sink — proves that an upsert-mode source
-- followed by an upsert-mode sink yields one row per PK end-to-end, with
-- no Flink-side dedup query in the middle.

CREATE TABLE IF NOT EXISTS sod_positions_passthrough (
  BusinessDate  DATE NOT NULL,
  FundId        INT NOT NULL,
  PmuId         INT NOT NULL,
  DealId        INT NOT NULL,
  SecurityId    BIGINT NOT NULL,
  Direction     VARCHAR(64) NOT NULL,
  Quantity      DECIMAL(29, 14),
  MktValLocal   DECIMAL(29, 14),
  load_ts       TIMESTAMP_LTZ(3),
  PRIMARY KEY (BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) NOT ENFORCED
)
DISTRIBUTED BY HASH(BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) INTO 3 BUCKETS
WITH (
  'changelog.mode'       = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'kafka.retention.time' = '0',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry'
);
