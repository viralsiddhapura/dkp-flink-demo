# DKP Live-Positions Pipeline — Issue Analysis & Trade-offs

**Audience:** DKP review meeting follow-up.
**Scope:** 4 production issues (now sourced from the authoritative client write-up) + 1 decision (CI/CD over UI) + 1 appendix finding.
**Pipeline recap:**
- 4 Avro Kafka topics: `sod_positions`, `business_date`, `transaction_allocation`, `live_positions`.
- SOD loaded daily 6–7 PM ET. Allocation is real-time (~3s).
- Q1: `transaction_allocation ⨝ business_date` → `intermediate_positions`.
- Q2: `intermediate_positions ⨝ sod_positions` → `live_positions`.

> **Composite primary key (from production DDL, screenshot of `uat.sod.position.global.new.compact.avro`):**
> `(BusinessDate, FundId, PmuId, DealId, SecurityId, Direction)` — 6 fields.
> Sink is declared `changelog.mode = 'upsert'`, `kafka.cleanup-policy = 'compact'`, `key.format = 'avro-registry'`, `DISTRIBUTED BY HASH(...) INTO 3 BUCKETS`, and `PRIMARY KEY (...) NOT ENFORCED`.

---

## Issue 1 — Duplicate SOD Runs (positions multiplied N×)

**Symptom (from client doc):** When the Start-of-Day (SOD) process runs N times for the same business date, the position **values** in `prod.position.global.compact.avro` come out as N× the correct value (3 runs → 3× the position). All N runs emit the same set of keys.

**Root cause (likely):** The compacted sink topic and its DDL look correct (`upsert` + Avro key + composite PK + compaction). So the multiplication is not happening *at the sink* — it is happening **inside the Flink query that produces positions**. Two probable mechanisms, either or both:

1. **Aggregation is additive.** The query producing live positions performs `SUM(quantity)` (or equivalent) over the SOD source. Since the SOD source is read as **append-only**, three SOD runs for the same key produce three rows in the source, and `SUM` adds them. The fix is to either (a) pre-dedup SOD to one row per PK before aggregation, or (b) read SOD as an `upsert-kafka` source so Flink processes only the latest version of each key.
2. **Join multiplies rows.** Q2 (`intermediate_positions ⨝ sod_positions`) is a regular streaming join. If `sod_positions` is append-only and contains N copies of every key, every allocation row joins against N SOD rows → N output rows. Same fix: read SOD as upsert (or pre-dedup).

The `NOT ENFORCED` PK constraint is **not preventing** Flink from rejecting the duplicates — Flink SQL can't enforce PK constraints at all (see Issue 2). Enforcement has to come from query semantics, not the constraint declaration.

### Options

| # | Approach | Where the fix lives | Trade-off |
|---|---|---|---|
| 1 | **Read SOD as `upsert-kafka` source** with PK = `(BusinessDate, FundId, PmuId, DealId, SecurityId, Direction)`. The source topic is already compacted + Avro-keyed; only the Flink table DDL needs to change from `changelog.mode = 'append'` to `'upsert'`. | Flink DDL only | **Cleanest fix.** Flink treats each SOD record as an upsert by key. N re-runs collapse to N upserts of the same key → final state = latest. No SQL rewrite of Q2. Requires that the producer is in fact emitting the latest value (not deltas) — verify with SOD owner. |
| 2 | **Flink dedup query** (`ROW_NUMBER() OVER (PARTITION BY pk ORDER BY load_ts DESC) = 1`) materialized to an upsert sink, with Q2 reading from that sink instead of the raw SOD topic. | Flink only | **Stopgap.** No producer or DDL change. Adds an intermediate compacted topic + a long-running dedup statement. State grows with cardinality unless TTL'd. Deduplication watermark/event-time correctness matters; pick `kafka_timestamp` or a producer-injected `load_ts` as `ORDER BY`. |
| 3 | **Replace `SUM` with `LAST_VALUE` (or row-level upsert)** in the position-producing query. | Flink query SQL | Works *only* if the duplication is purely additive aggregation, not join-multiplication. Doesn't fix the join case. Use only if profiling confirms the aggregation is the multiplier. |
| 4 | **Producer-side idempotence** — SOD writer emits a load-id and replaces prior load atomically (e.g., publishes tombstones for absent keys, then new values). | Upstream system | Most semantically correct; typically slowest to ship. Fixes "removed positions" case (which Options 1–3 don't fully). |

### Recommendation
- **Short term (this sprint):** Option 1. The sink is already shaped correctly; making the SOD **source** an upsert-kafka table is a one-line DDL change that exercises the rest of the pipeline without rewriting Q2.
- **If Option 1 reveals the producer doesn't actually emit "latest value per key" semantics** (e.g., it emits deltas or partial loads): fall back to Option 2 (dedup query) and start a parallel conversation with the SOD producer team for Option 4.
- **Validate first:** before changing anything, run a one-shot `SELECT COUNT(*), COUNT(DISTINCT pk) FROM sod_positions` on the raw topic after a triple-run. If raw rows = 3× distinct PKs, it's a duplication problem (Options 1/2 fix it). If raw rows = distinct PKs but values are still 3×, the producer itself is summing — Option 4 only.

### Edge cases to validate
- Same PK in two distinct SOD runs the same day with **different quantities** (the legitimate restate case): latest wins → correct under Options 1/2.
- A PK present in run 1 but **absent** in run 2 (a removed position): Options 1–3 leave the row lingering until next-day SOD. Option 4 (producer tombstones) is the only proper fix.
- Run 1 partially completes, run 2 completes: only Option 4 handles this cleanly.
- Direction field as part of PK: a position flipping `LONG → SHORT` for the same security creates **two** PKs, not one updated PK. Confirm with the trading team this is intended (it almost certainly is, but worth a written check).

---

## Issue 2 — Kafka Key Enforcement in Flink (`NOT ENFORCED`)

**Symptom (from client doc):** "Kafka topic keys are properly enforced in the underlying Flink tables. While objects are being published as keys and Flink derives primary keys from constraints, [those PK] constraints are not currently enforced."

**Root cause:** This is a **Flink SQL language limitation, not a DKP configuration bug.** Flink SQL only supports `PRIMARY KEY ... NOT ENFORCED`. There is no `ENFORCED` option. Flink will not reject a duplicate-PK row at insert time the way a relational database would. The `PRIMARY KEY` declaration tells Flink's planner *how to interpret* the stream (for upsert sinks, joins, dedup) — it does not enforce uniqueness on the data.

So the question "how do we make Flink enforce the PK?" has no direct answer. The right reframe is: **how do we make duplicate-keyed records not cause downstream harm?** Three mechanisms, used in combination, give you effective end-to-end enforcement:

1. **Upsert sink semantics.** `changelog.mode = 'upsert'` + `kafka.cleanup-policy = 'compact'` + Avro structured key means the sink topic *eventually* contains one record per key (latest wins), regardless of how many duplicates the query emits. The current production DDL already does this correctly.
2. **Upsert source semantics.** When a downstream Flink query reads a compacted, keyed topic, it should declare the table with `changelog.mode = 'upsert'` so Flink processes the stream as a changelog (one row per key) rather than append (N rows per key). Failing to do this causes the multiplication seen in Issue 1.
3. **Dedup at boundaries.** When a topic enters the pipeline that may contain duplicates (e.g., SOD source after multiple runs), insert a `ROW_NUMBER()`-based dedup before joins or aggregations, materialized to an upsert-keyed intermediate topic.

### Options

| # | Approach | Trade-off |
|---|---|---|
| 1 | **Audit every Flink table DDL in Q1/Q2:** every compacted, keyed topic that's read downstream must be declared with `changelog.mode = 'upsert'` (not the default `'append'`). | **Highest leverage.** Often a one-line change per DDL. Closes the most common multiplication path. Requires reading every existing statement and confirming source mode. |
| 2 | **Add explicit dedup statements at every boundary** where a topic could contain duplicate-keyed records, even if compaction will eventually clean it. | More defensive. Costs an extra statement and an intermediate topic per boundary. Worth it where the source is genuinely append-only and not under your control. |
| 3 | **Document the `NOT ENFORCED` reality and make it a code-review checklist item** so engineers don't write queries that assume Flink will reject dupes. | Cheap, organizational fix. Should accompany Options 1/2 anyway — without it, the same class of bug recurs. |
| 4 | **Switch to a sink that does enforce PK uniqueness** (e.g., a JDBC sink to Postgres with a real PK constraint). | Doesn't help here — the sink is Kafka, and downstream consumers are Kafka consumers. Mentioned only to be explicit that "enforcement" via storage isn't an option for this pipeline. |

### Recommendation
- **Option 1 + Option 3 together.** Audit existing DDL for any compacted/keyed topic read as `'append'` and switch to `'upsert'` (this *is* the fix for most of Issue 1's multiplication). Bake a "did you choose the right `changelog.mode`?" item into the SQL PR template.
- **Option 2 only at uncontrolled boundaries** — i.e., the SOD source if Issue 1 Option 4 (producer tombstones) doesn't ship.
- **Reframe the conversation with the team:** "Flink can't enforce PKs the way an RDBMS does — it's a known limitation. We enforce by *modeling* the stream as a changelog end-to-end. Here's how to spot the wrong mode in a code review."

### Edge cases
- **Buckets vs. keys:** the production DDL uses `DISTRIBUTED BY HASH(pk_columns) INTO 3 BUCKETS`. This controls Kafka partition assignment, not uniqueness. A Flink upsert sink writes one record per key per bucket; compaction within each partition then collapses to latest. Misalignment between the hash columns and the PK would split a key across buckets — confirm they always match.
- **`NOT NULL` on PK columns:** the DDL marks PK columns `NOT NULL`. Confirm the Avro key schema in Schema Registry also marks every key field as non-nullable, otherwise a producer can sneak a null in and Flink behavior on upsert is undefined.
- **Tombstones:** Avro structured key + `null` value = tombstone for compacted topics. Confirm whether the SOD producer can emit them; without tombstones, "removed position" is impossible to express through this pipeline. (Same point as Issue 1 edge case.)

---

## Issue 3 — Restart-Safe Behavior (intraday restarts must not corrupt state)

**Symptom (from client doc):** "Intraday restarts of Flink queries should not impact positions or downstream state. The system should ensure stability even with continuous query execution."

**Root cause:** Three things must align for a Flink restart to be safe:

1. **Source offsets** committed transactionally with state checkpoints (Flink default — Kafka source offsets live in checkpoint state, not in `__consumer_offsets`).
2. **Sink delivery guarantee** = `exactly-once` (Kafka transactional producer + downstream consumers in `read_committed`).
3. **Stateful operators** (joins, dedup, aggregates) restored from a savepoint, not restarted cold.

In Confluent Cloud Flink, statements have a managed lifecycle (`STOPPED` retains state; resume restores it). The risk is a manual UI "stop" or a deploy that doesn't go through stop-with-savepoint — that path can drop state or replay from `earliest-offset` (note: the production DDL has `'scan.startup.mode' = 'earliest-offset'`, which on a cold restart means *full topic replay* — almost certainly not what is wanted at intraday restart).

### Options

| # | Approach | Trade-off |
|---|---|---|
| 1 | **Stop-with-savepoint + resume-from-savepoint** as the only deploy/restart path, enforced by CI/CD (`flink-deploy.yml`). | **Correct fix.** Requires no SQL change. Forces the deploy pipeline to be the only way state ever stops, gating restart on a clean savepoint. UI stops must be disabled by RBAC. |
| 2 | **Exactly-once sink** (`'sink.delivery-guarantee' = 'exactly-once'`, transactional ID prefix per statement) and downstream consumers in `read_committed`. | **Required complement to #1.** Adds 2PC commit latency (default 1 min, tunable to ~10–30s). Every downstream reader must be `read_committed` end-to-end; one stale `read_uncommitted` consumer breaks the guarantee. |
| 3 | **Idempotent observable state** via upsert-keyed compacted sinks (already in production DDL — preserves correctness even under at-least-once delivery). | **Pragmatic resilience layer.** Even if #1 and #2 are not perfect, observable state stays correct because re-emission of the same key is a no-op upsert. The current production DDL already gives you this — don't lose it in any future refactor. |
| 4 | **RBAC: only the CI/CD service account holds `FlinkAdmin`** on the prod compute pool; engineers get `FlinkDeveloper` (read-only on prod statements). | Operational, not technical, but essential — closes the human-error path. |

### Recommendation
**Defense-in-depth: do all four.**
1. CI/CD enforces stop-with-savepoint on every deploy/restart (`scripts/flink_deploy.sh` already does this).
2. Add `'sink.delivery-guarantee' = 'exactly-once'` to all sink DDLs, with a per-statement `transactional-id-prefix`. Tune commit interval to balance latency vs. throughput (~10–30s typical).
3. Keep the upsert + compacted sink shape that production already has.
4. RBAC: only CI/CD service account can mutate prod statements.

### Edge cases
- **Schema evolution mid-restart:** a savepoint encodes operator state shape. SQL changes that alter join state shape (e.g., adding a join column) may not be restorable. Confluent Cloud Flink's "in-place upgrade" tolerates some changes, not all — `flink-validate.yml` should run an `EXPLAIN` plan diff before deploy.
- **Read-committed everywhere:** if `live_positions` has any external consumer in `read_uncommitted`, an aborted transaction's records leak. Audit downstream consumer configs (open question for the next sync).
- **Watermark on resume:** if Q1/Q2 use event-time joins, the savepoint also restores watermark state. A long stop window could cause the resumed job to skip late data older than the held watermark. Size bounded out-of-orderness ≥ max expected stop duration plus source lag.
- **Tx ID collisions on parallel deploys:** two deploys with the same `transactional-id-prefix` will fence each other. Pipeline must serialize deploys per statement or use a unique prefix per deploy run.
- **`scan.startup.mode = earliest-offset`:** the production DDL has this. On a *cold* restart (state lost), the statement will replay the entire topic — which is what triggers the multiplication symptom in Issue 1 if dedup isn't in place. Stop-with-savepoint avoids cold restart in the normal case; the `earliest-offset` setting is the disaster-recovery path.

---

## Issue 4 — Idempotence (Transaction & SOD Level)

**Symptom (from client doc):** "The system should support idempotent processing — recomputing or reapplying data for a given business date should always result in the same final position/state, regardless of how many times it is run."

**Relationship to the other issues:** Issue 4 is the **umbrella property**. Issue 1 is one observed violation of it (re-running SOD doesn't converge), and Issue 3 is another (intraday restart shouldn't drift). Fixing 1, 2, 3 is *necessary but not sufficient* for Issue 4 — Issue 4 also requires **transaction-level** idempotency, which is a distinct dimension.

**Two dimensions of idempotence that must hold:**

| Dimension | Meaning | What enforces it |
|---|---|---|
| **SOD-level** | Re-running SOD for a business date converges to the same final position regardless of N runs. | Issue 1's fix (upsert source / dedup) + Issue 2's fix (correct `changelog.mode` end-to-end). |
| **Transaction-level** | Re-applying the same allocation transaction (replay, retry, recovery) doesn't double-count. | Each transaction must have a stable unique ID, and downstream effects must key on (or dedup by) that ID. |

The transaction-level dimension is the gap the other three issues don't cover. Today, if an allocation event is re-emitted (broker retry, consumer offset reset, savepoint restore), Q1 will join it again against business_date and emit a fresh row downstream. Whether that row *changes* downstream state depends on what `live_positions` is keyed on: if the downstream PK includes the transaction ID (so re-emission is an upsert no-op), it's idempotent; if the PK is `(BusinessDate, FundId, …, Direction)` and the query does a `SUM(allocation.qty)`, it's not.

### Options

| # | Approach | Trade-off |
|---|---|---|
| 1 | **All sinks are upsert-keyed** with PKs that make re-emission a no-op (current state for the position sink — verify same for any intermediate topic). | Already mostly in place. Audit every intermediate topic; any append-mode internal topic is an idempotency hole. |
| 2 | **Transaction-id dedup** at the entry of Q1: `ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY emitted_ts DESC) = 1`. Drops broker-retry duplicates before they reach the join. | Adds a stateful operator scaled to the rate of unique transactions × dedup window. Critical if the allocation producer is at-least-once. |
| 3 | **Replace additive aggregations with state-machine logic** in Q1/Q2 — e.g., the position is a function of (SOD + set of applied transactions) rather than `SOD + SUM(transactions)`. Re-applying a transaction that's already in the set is a no-op. | More complex query. Often the cleanest semantics: explicit state (set of transactions applied) instead of implicit accumulator. |
| 4 | **Transactional source guarantees** — confirm the allocation source is producing with exactly-once semantics, then read it as `read_committed` in Flink so retries don't reach the query at all. | Requires upstream producer is transactional. Doesn't help if retries happen *before* the broker. |

### Recommendation
- **Option 1 + Option 2 as baseline.** Audit all intermediate topics; add transaction-id dedup at Q1's entry. This combined with Issues 1, 2, 3 gives idempotence under nearly all replay scenarios.
- **Option 3 as the long-term direction.** Modeling positions as `f(SOD, set-of-transactions)` rather than `f(SOD, sum-of-transactions)` is the most principled fix and decouples idempotence from delivery semantics. Worth scoping but not a sprint item.
- **Verify with a chaos test:** stop a Flink statement, replay the last 30 minutes of allocation events, restart, and assert `live_positions` ends up byte-identical (per PK) to a clean run. This is the simplest end-to-end idempotence assertion.

### Edge cases
- **Compensating transactions / corrections:** an allocation correction that flips a prior `+100` to `-100` (a real business event) must NOT be deduped as "duplicate." Dedup keys on `transaction_id`; corrections must carry a different `transaction_id`. Confirm with the upstream system.
- **SOD applied mid-transactions:** if SOD lands while allocations are flowing, the order of application matters for derivation. Document which topic's event-time leads in Q2.
- **Cross-day reprocessing:** a backfill for `BusinessDate = T-7` must not affect today's positions. Since `BusinessDate` is part of the PK, this is implicitly handled — confirm by running a `T-7` reprocess in dev and asserting today's `live_positions` is byte-stable.

---

## Decision: CI/CD over UI for Flink Job Control

**Yes, fully feasible.** The Confluent Terraform provider supports Flink resources, and the Confluent CLI/REST API supports statement lifecycle.

### Architecture

| Layer | Tool | Why |
|---|---|---|
| **Compute pool, service accounts, RBAC, API keys, Kafka topics** | Terraform + Terragrunt | Declarative, drift-detectable, env-promotable. Slow-changing. |
| **Flink statement deploy/stop/resume** | Confluent CLI in GitHub Actions | Imperative lifecycle (stop-with-savepoint, then submit, then resume) cannot be expressed in Terraform's create-then-immutable model without losing state. |
| **SQL source of truth** | `flink-sql/` directory in repo | PR-reviewed, blame-trackable, env-templatable. |

### Why not Terraform's `confluent_flink_statement` resource for everything?
Its update behavior is **destroy + create**. For a stateful streaming job that destroys all state. Use it only for stateless DDL (table definitions). Use the CLI/REST API for stateful query lifecycle.

### Pipeline shape (workflows in `.github/workflows/`):

1. `terraform-plan.yml` (PR) — `terraform plan` on infra changes; comments diff on PR.
2. `terraform-apply.yml` (push to main) — applies infra (compute pools, service accounts, topics).
3. `flink-validate.yml` (PR on `flink-sql/**`) — validates SQL via `EXPLAIN` against staging compute pool. Fails on syntax or plan errors.
4. `flink-deploy.yml` (push to main on `flink-sql/**`) — for each changed statement:
   1. Diff old vs. new SQL → if unchanged, skip.
   2. `confluent flink statement stop <name>` (creates implicit savepoint).
   3. `confluent flink statement create` with new SQL.
   4. Verify `RUNNING` status; if failed, attempt resume from savepoint; alert on persistent failure.

### What you (DKP / Psyncopate) need to provide for the implementation
1. Confluent Cloud **API key + secret** with `OrganizationAdmin` (or scoped equivalent) for Terraform bootstrap.
2. Confluent Cloud **environment ID** (`env-xxxx`) for dev/staging/prod (or one env with multiple Kafka clusters).
3. Confluent Cloud **Kafka cluster ID** (`lkc-xxxx`) and **REST endpoint** for each env.
4. Confluent Cloud **Schema Registry endpoint + API key** for each env.
5. Cloud + region selections (e.g., `AWS / us-east-1`).
6. GitHub repo with permissions to add **OIDC + secrets** (Confluent API keys stored as GitHub Actions secrets).
7. Decision on Terraform state backend (S3 + DynamoDB lock recommended).
8. List of existing topic names and existing schema subjects (to import into Terraform without re-creation).
9. The current SQL of Q1 and Q2 (so we don't have to reverse-engineer them — even sanitized versions are fine).

---

## Cross-cutting summary table

| Issue | Priority | Quick fix | Long-term fix | Blocked by |
|---|---|---|---|---|
| 1. Duplicate SOD runs | P0 | Switch SOD source DDL to `changelog.mode = 'upsert'`; or Flink `ROW_NUMBER()` dedup as stopgap | Producer emits tombstones for removed positions | Producer team for tombstones |
| 2. PK `NOT ENFORCED` | P0 | Audit every DDL for correct `changelog.mode`; document Flink SQL limitation | Code-review checklist + dedup at uncontrolled boundaries | — |
| 3. Restart-safe | P0 | CI/CD enforces stop-with-savepoint; RBAC blocks UI stops | Add `sink.delivery-guarantee = exactly-once` end-to-end with `read_committed` consumers | Downstream consumer audit |
| 4. Idempotence | P0 | Audit intermediate topics for upsert mode; add transaction-id dedup at Q1 entry | Replace additive aggregations with state-machine logic (set-of-transactions) | Issues 1–3 first |
| Decision: CI/CD | — | — | Terraform + GH Actions (this repo) | None |

---

## Appendix — Additional finding (not in client's 4 issues)

### `business_date` as `timestamp_micro` vs. logical `date`

**Status:** Not raised by the client in the authoritative issue list. Surfaced during a prior review and worth flagging for a future cycle.

**Concern:** If `business_date` is typed as `timestamp_micro` (microseconds since epoch) in any topic schema, midnight-ET timestamps cross the calendar boundary in UTC and produce wrong `business_date` values for downstream consumers. A calendar date is not an instant; Avro has a `logical type: date` (int days since epoch, no TZ) that is the correct type.

**If confirmed in DKP schemas, the fix path is:**
1. Add `business_date_v2` (Avro `logical date`) to all four topic schemas (BACKWARD-compatible).
2. Producers dual-populate; consumers (Flink queries) read via `COALESCE(business_date_v2, CAST(business_date AS DATE))`.
3. Once all consumers are migrated, drop the old field in a future schema version.
4. **If `business_date` is in any topic's *key***: cannot deprecate in place — needs a v2 topic with a new key schema. Plan as Option 2 cutover.

This is **not on the critical path for the four authoritative issues**, but the production DDL screenshot does show `BusinessDate DATE NOT NULL` (logical date already), so the value side appears fine. Confirm the source topics for Q1 (`transaction_allocation`, `business_date` topic) use the same logical-date type before closing this out.

---

## POC mapping (current state of `flink-sql/poc/`)

The POC subdirectories were scaffolded against the **older issue numbering** and need realignment:

| POC dir (current) | Originally targeted | Now maps to | Status |
|---|---|---|---|
| `issue1_dedup/` | SOD dedup with 4-field PK | **Issue 1 (Duplicate SOD Runs)** | **Needs PK update to 6 fields** (`BusinessDate, FundId, PmuId, DealId, SecurityId, Direction`) |
| `issue2_composite_pk/` | Restream string-keyed → Avro-keyed | **Issue 2 (NOT ENFORCED PK)** | **Needs reframing** — source is already Avro-keyed in production; the real demonstration is "upsert source semantics + dedup as enforcement" |
| `issue3_business_date_schema/` | timestamp_micro → date migration | **Appendix only — not in client's 4 issues** | **Move out of `poc/` or rename** to `appendix_business_date_schema/` to avoid confusion |
| *(missing)* | — | **Issue 3 (Restart-Safe)** | **No POC yet** — needs a savepoint/resume demo + exactly-once sink config |
| *(missing)* | — | **Issue 4 (Idempotence)** | **No POC yet** — needs transaction-id dedup demo + chaos-replay assertion |
