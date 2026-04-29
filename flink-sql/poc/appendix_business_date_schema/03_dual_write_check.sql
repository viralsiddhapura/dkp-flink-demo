-- Sanity checks during migration:
--
-- (a) How many records have ONLY the legacy field populated? (legacy producers)
SELECT COUNT(*) AS legacy_only
FROM business_date
WHERE business_date_v2 IS NULL AND business_date IS NOT NULL;

-- (b) How many records have BOTH fields populated? (migrating producers)
SELECT COUNT(*) AS dual_populated
FROM business_date
WHERE business_date_v2 IS NOT NULL AND business_date IS NOT NULL;

-- (c) How many records have ONLY the new field? (post-migration; v3 will reach this state)
SELECT COUNT(*) AS new_only
FROM business_date
WHERE business_date_v2 IS NOT NULL AND business_date IS NULL;

-- (d) Detect TZ drift on the legacy field: any pair of records where the legacy
--     timestamp casts to a different date than the v2 field?
SELECT COUNT(*) AS tz_drift_count
FROM business_date
WHERE business_date_v2 IS NOT NULL
  AND business_date IS NOT NULL
  AND CAST(business_date AS DATE) <> business_date_v2;
