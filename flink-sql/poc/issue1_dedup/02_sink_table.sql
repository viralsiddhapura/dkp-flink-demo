-- Issue 1 sink: compacted, upsert-keyed SOD positions. Shape mirrors the
-- production DDL on prod.position.global.compact.avro:
--   PRIMARY KEY (BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) NOT ENFORCED
--   DISTRIBUTED BY HASH(...) INTO 3 BUCKETS
--   changelog.mode='upsert'
--   kafka.cleanup-policy='compact'
--
-- Downstream consumers should read this table with changelog.mode='upsert'
-- (see Issue 2 POC). Reading it as 'append' reintroduces the multiplication
-- problem this POC was created to fix.

CREATE TABLE IF NOT EXISTS sod_positions_dedup (
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
