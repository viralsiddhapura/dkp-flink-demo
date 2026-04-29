-- Consumer table that handles both v1 and v2 producer schemas via COALESCE.
-- Once all producers emit v2, switch to reading business_date_v2 directly.

CREATE TABLE IF NOT EXISTS business_date (
  business_date     TIMESTAMP_LTZ(3),    -- legacy timestamp_micro (nullable post-migration)
  business_date_v2  DATE,                -- new logical-date field (nullable until producers update)
  trading_session   STRING
) WITH (
  'changelog.mode' = 'append',
  'value.format'   = 'avro-registry',
  'key.format'     = 'raw'
);

-- A view that gives downstream queries a single, always-correct calendar date.
CREATE VIEW IF NOT EXISTS business_date_normalized AS
SELECT
  COALESCE(
    business_date_v2,
    CAST(business_date AS DATE)          -- fallback while producers migrate
  ) AS bd,
  trading_session
FROM business_date;
