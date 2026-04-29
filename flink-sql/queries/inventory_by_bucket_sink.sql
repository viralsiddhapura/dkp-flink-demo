-- Demo sink: continuously-updated inventory rollup, bucketed by
-- productid mod 10. Ten upsert-keyed rows that grow in real time as the
-- datagen connector keeps producing — easy to watch in the Confluent UI.
--
-- Compacted + upsert means downstream consumers always see one row per
-- bucket (the latest aggregate). Same shape as the production
-- live-positions sink — useful as a teaching moment in the demo.

CREATE TABLE IF NOT EXISTS inventory_by_bucket (
  bucket_id          INT NOT NULL,
  records_in_bucket  BIGINT,
  total_quantity     BIGINT,
  last_updated_at    TIMESTAMP_LTZ(3),
  PRIMARY KEY (bucket_id) NOT ENFORCED
)
DISTRIBUTED BY HASH(bucket_id) INTO 3 BUCKETS
WITH (
  'changelog.mode'              = 'upsert',
  'kafka.cleanup-policy'        = 'compact',
  'kafka.retention.time'        = '0',
  'key.format'                  = 'avro-registry',
  'value.format'                = 'avro-registry',
  'sink.delivery-guarantee'     = 'exactly-once',
  'sink.transactional-id-prefix' = 'dkp-demo-inventory-by-bucket-'
);
