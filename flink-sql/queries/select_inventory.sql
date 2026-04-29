-- Lifecycle smoke test.
--
-- Purpose: deploy ONE Flink statement to validate that the CI/CD pipeline
-- can drive its full lifecycle (deploy / stop / resume / delete / describe / list)
-- without depending on any sink topic, schema registration, or compaction setup.
--
-- A streaming SELECT stays in phase RUNNING indefinitely (no LIMIT), so it
-- behaves exactly like a long-running INSERT for lifecycle purposes — but
-- requires nothing other than the auto-mapped source table to work.
--
-- Source: `inventory.avro.topic`, resolved via Confluent Cloud Flink's
-- auto-mapping (sql.current-catalog / sql.current-database are passed by
-- the deploy script from your env secrets).

SELECT productid, quantity
FROM `inventory.avro.topic`;
