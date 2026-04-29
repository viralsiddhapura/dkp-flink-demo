-- Issue 1 source: SOD positions topic, read as APPEND-ONLY.
--
-- This is the configuration that exhibits the multiplication symptom: N runs
-- of SOD produce N rows per logical key in the table, and any downstream
-- aggregation or join over this source multiplies as a result.
--
-- The upstream Kafka topic is compacted and Avro-keyed in production
-- (matching prod.position.global.compact.avro). We are deliberately reading
-- it as 'append' here to reproduce the bug surface and prove that a
-- Flink-side dedup pass corrects it without changing the producer.
--
-- The companion POC (issue2_composite_pk) reads the SAME topic with
-- changelog.mode='upsert' and shows that to be the cleaner fix.

CREATE TABLE IF NOT EXISTS sod_positions_raw (
  BusinessDate  DATE NOT NULL,
  FundId        INT NOT NULL,
  PmuId         INT NOT NULL,
  DealId        INT NOT NULL,
  SecurityId    BIGINT NOT NULL,
  Direction     VARCHAR(64) NOT NULL,
  Quantity      DECIMAL(29, 14),
  MktValLocal   DECIMAL(29, 14),
  load_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR load_ts AS load_ts - INTERVAL '5' SECOND
) WITH (
  'changelog.mode'    = 'append',
  'value.format'      = 'avro-registry',
  'key.format'        = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);
