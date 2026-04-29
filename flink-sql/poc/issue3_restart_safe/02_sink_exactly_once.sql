-- Issue 3 sink: same compacted upsert topic as Issue 2, but with the
-- exactly-once delivery guarantee turned on. This is what makes intraday
-- restarts safe end-to-end:
--
--   * The Kafka producer inside Flink writes records inside transactions.
--   * Each checkpoint commits the transaction. Until commit, downstream
--     read_committed consumers do not see the records.
--   * On restart, in-flight (uncommitted) records are aborted by Kafka,
--     and Flink replays from the last successful checkpoint.
--
-- IMPORTANT: every downstream consumer of this topic must use
-- isolation.level=read_committed. A single read_uncommitted consumer
-- defeats the guarantee.
--
-- The transactional-id-prefix MUST be unique per statement deployment.
-- The deploy pipeline (scripts/flink_deploy.sh) renames statements to
-- <name>_<git-short-sha>, which gives a natural unique prefix.

CREATE TABLE IF NOT EXISTS sod_positions_eo (
  BusinessDate  DATE NOT NULL,
  FundId        INT NOT NULL,
  PmuId         INT NOT NULL,
  DealId        INT NOT NULL,
  SecurityId    BIGINT NOT NULL,
  Direction     VARCHAR(64) NOT NULL,
  Quantity      DECIMAL(29, 14),
  MktValLocal   DECIMAL(29, 14),
  load_ts       TIMESTAMP_LTZ(3),
  PRIMARY KEY (BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) NOT ENFORCED
)
DISTRIBUTED BY HASH(BusinessDate, FundId, PmuId, DealId, SecurityId, Direction) INTO 3 BUCKETS
WITH (
  'changelog.mode'              = 'upsert',
  'kafka.cleanup-policy'        = 'compact',
  'kafka.retention.time'        = '0',
  'key.format'                  = 'avro-registry',
  'value.format'                = 'avro-registry',
  'sink.delivery-guarantee'     = 'exactly-once',
  'sink.transactional-id-prefix' = 'dkp-poc3-restart-safe-'
);
