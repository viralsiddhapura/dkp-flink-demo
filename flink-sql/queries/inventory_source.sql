-- Demo source: the existing datagen `inventory.avro.topic`.
--
-- Standard Confluent "inventory" datagen quickstart schema (Avro):
--   id        INT  (monotonically increasing record id)
--   productid INT  (echoes the same monotonic counter — datagen quirk)
--   quantity  INT  (echoes the same monotonic counter)
--
-- The topic name has dots, so we backtick it. In Confluent Cloud Flink,
-- every Kafka topic in the same environment is auto-mapped to a Flink
-- table with the same name; this DDL is `CREATE TABLE IF NOT EXISTS` so
-- it is a no-op against the auto-mapped table when shapes match. Its
-- value is documenting the schema in source-of-truth SQL and pinning the
-- watermark / scan strategy.

CREATE TABLE IF NOT EXISTS `inventory.avro.topic` (
  id         INT,
  productid  INT,
  quantity   INT,
  ts         TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'changelog.mode'    = 'append',
  'value.format'      = 'avro-registry',
  'key.format'        = 'raw',
  'scan.startup.mode' = 'latest-offset'
);
