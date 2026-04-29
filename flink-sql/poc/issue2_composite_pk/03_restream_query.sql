-- Passthrough INSERT from the upsert-mode source to the upsert-mode sink.
-- No ROW_NUMBER, no aggregation, no dedup query — Flink handles upsert
-- semantics natively because both source and sink declare changelog.mode='upsert'.
--
-- Compare against ../issue1_dedup/03_dedup_query.sql, which reads the same
-- topic as 'append' and needs ROW_NUMBER to achieve the same end state.
-- The two POCs are different fixes for the same underlying issue.

INSERT INTO sod_positions_passthrough
SELECT
  BusinessDate,
  FundId,
  PmuId,
  DealId,
  SecurityId,
  Direction,
  Quantity,
  MktValLocal,
  load_ts
FROM sod_positions_keyed;
