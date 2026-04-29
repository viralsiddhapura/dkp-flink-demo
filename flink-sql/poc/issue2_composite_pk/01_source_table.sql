-- Issue 2 source: SOD positions topic, read with changelog.mode='upsert'.
--
-- This is the SAME upstream topic that issue1_dedup reads as 'append'. The
-- only difference is the changelog.mode. With 'upsert', Flink treats each
-- record as a row-by-PK upsert; with 'append', Flink treats each record as
-- a new row and downstream aggregations or joins multiply.
--
-- The PK declaration is NOT ENFORCED — that is the only option Flink SQL
-- offers; it is a language-level limitation, not a configuration choice.
-- "Enforcement" is achieved by the upsert-source semantics declared here:
-- Flink will deliver one logical row per PK to downstream operators,
-- regardless of how many physical records exist on the upstream topic.

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
