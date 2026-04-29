-- The query under test. A simple passthrough is enough to exercise the
-- restart-safety machinery — the relevant operators are the source's
-- offset state and the sink's transactional commit cycle. More complex
-- queries (joins, dedups) layer additional state on top, but the
-- restart-safety contract is the same.

INSERT INTO sod_positions_eo
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
