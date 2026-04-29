-- Demo streaming aggregation. This is the long-running statement the
-- CI/CD pipeline manages — deploy, stop, resume, and delete actions all
-- target this statement.
--
-- Bucketing by productid % 10 produces ten upsert rows (bucket_id 0..9).
-- The datagen connector emits monotonically increasing values, so each
-- bucket's count and total_quantity grow continuously — easy to see in
-- the Confluent UI when you point it at the inventory_by_bucket topic.

INSERT INTO inventory_by_bucket
SELECT
  productid % 10                 AS bucket_id,
  COUNT(*)                       AS records_in_bucket,
  CAST(SUM(quantity) AS BIGINT)  AS total_quantity,
  MAX(ts)                        AS last_updated_at
FROM `inventory.avro.topic`
GROUP BY productid % 10;
