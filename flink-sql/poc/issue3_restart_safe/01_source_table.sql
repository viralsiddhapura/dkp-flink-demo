-- Issue 3 source: any keyed compacted topic that downstream restart-safety
-- must be tested against. We re-use the upsert SOD source from Issue 2.
--
-- The relevant detail for restart safety is NOT in this DDL — it is in the
-- sink (02_sink_exactly_once.sql) and in the deploy procedure
-- (../../../scripts/flink_deploy.sh, which uses stop-with-savepoint).

CREATE TABLE IF NOT EXISTS sod_positions_keyed (
  BusinessDate  DATE NOT NULL,
  FundId        INT NOT NULL,
  PmuId         INT NOT NULL,
  DealId        INT NOT NULL,
  SecurityId    BIGINT NOT NULL,
  Direction     VARCHAR(64) NOT NULL,
  Quantity      DECIMAL(29, 14),
  MktValLocal   DECIMAL(29, 14),
  load_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  PRIMARY KEY (BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) NOT ENFORCED
) WITH (
  'changelog.mode'    = 'upsert',
  'value.format'      = 'avro-registry',
  'key.format'        = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);
