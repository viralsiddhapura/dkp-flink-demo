# DKP Flink Work — Live Positions Pipeline Fixes & CI/CD

This repo contains:

1. **`ANALYSIS.md`** — written-up trade-off analysis for the 4 production issues raised in the DKP review meeting (2026-04-28). Read this first.
2. **`terraform/` + `terragrunt/`** — IaC for Confluent Cloud Flink compute pool, service accounts, RBAC, API keys, and Kafka topics across `dev`/`staging`/`prod`.
3. **`.github/workflows/`** — GitHub Actions pipelines for `terraform plan/apply` and `flink validate/deploy`. Implements the "no UI deploys" decision.
4. **`scripts/`** — `flink_deploy.sh` (stop-with-savepoint → submit → verify with rollback) and `flink_explain.sh` (PR-time SQL validation).
5. **`flink-sql/queries/`** — production-shaped Flink SQL statements managed by the deploy pipeline. Includes a small **demo aggregation** (`inventory_by_bucket.sql`) over the existing datagen `inventory.avro.topic` you can use to convince stakeholders the CI/CD lifecycle works end-to-end. **See [`flink-sql/queries/README.md`](flink-sql/queries/README.md) for the 5-minute demo runbook.**
6. **`flink-sql/poc/`** — runnable POCs aligned with the client's 4 authoritative issues. One POC directory per issue, plus an appendix directory for an out-of-scope finding.

---

## Repo layout

```
.
├── ANALYSIS.md
├── README.md                              <- you are here
├── terraform/
│   ├── envs/_template/                    <- one Terraform root, parametrized per env
│   └── modules/
│       ├── flink-compute-pool/
│       └── kafka-topics/
├── terragrunt/
│   ├── terragrunt.hcl                     <- root: backend + provider
│   ├── dev/, staging/, prod/              <- per-env inputs from secrets
├── .github/workflows/
│   ├── terraform-plan.yml                 <- PR
│   ├── terraform-apply.yml                <- main / manual
│   ├── flink-validate.yml                 <- PR
│   ├── flink-deploy.yml                   <- main / manual: stop-with-savepoint -> submit
│   └── flink-control.yml                  <- manual: stop / resume / delete / describe / list
├── scripts/
│   ├── flink_deploy.sh
│   ├── flink_lifecycle.sh                 <- backs flink-control.yml
│   └── flink_explain.sh
└── flink-sql/
    ├── queries/                              <- Production-shaped statements managed by the deploy pipeline
    │   ├── inventory_source.sql              <- Demo: pin schema of existing inventory.topic
    │   ├── inventory_by_bucket_sink.sql      <- Demo: compacted upsert sink (10 buckets)
    │   ├── inventory_by_bucket.sql           <- Demo: streaming aggregation (the long-running statement)
    │   └── README.md                         <- 5-minute demo runbook for the CI/CD lifecycle
    └── poc/
        ├── issue1_dedup/                     <- Issue 1: Duplicate SOD Runs (dedup approach)
        ├── issue2_composite_pk/              <- Issue 2: PK NOT ENFORCED (upsert source approach)
        ├── issue3_restart_safe/              <- Issue 3: Restart-Safe Behavior
        ├── issue4_idempotence/               <- Issue 4: Transaction-level idempotence
        └── appendix_business_date_schema/    <- Out of scope: business_date schema concern
```

---

## What I need from you to make this real

Mark these off as you provide them; they slot into GitHub Actions secrets and Terragrunt env files.

### Confluent Cloud — bootstrap (one-time)
- [ ] `CONFLUENT_CLOUD_API_KEY` / `CONFLUENT_CLOUD_API_SECRET` — Cloud API key with `OrganizationAdmin` (used only by Terraform; we'll scope down to a CI service account afterwards).
- [ ] `CONFLUENT_ORG_ID` — your Confluent Cloud organization id.

### Per environment (`DEV`, `STAGING`, `PROD`)
- [ ] `CONFLUENT_ENV_ID_<ENV>` — environment id (`env-xxxxx`).
- [ ] `CONFLUENT_ENV_CRN_<ENV>` — environment CRN (visible in environment URL or `confluent environment describe`).
- [ ] `CONFLUENT_FLINK_REGION_ID_<ENV>` — Flink region id (Confluent CLI: `confluent flink region list`).
- [ ] `CONFLUENT_KAFKA_CLUSTER_ID_<ENV>` — `lkc-xxxxx`.
- [ ] `CONFLUENT_KAFKA_REST_ENDPOINT_<ENV>` — Kafka REST endpoint URL.
- [ ] `CONFLUENT_KAFKA_ADMIN_API_KEY_<ENV>` / `..._SECRET_<ENV>` — Kafka cluster admin key (for topic creation).
- [ ] **(After first apply)** `CONFLUENT_FLINK_API_KEY_<ENV>` / `..._SECRET_<ENV>` — outputs from Terraform (`runner_api_key_id` / `runner_api_key_secret`); copy into GitHub secrets.
- [ ] **(After first apply)** `CONFLUENT_FLINK_COMPUTE_POOL_ID_<ENV>` — output from Terraform.

### Schema Registry (per env)
- [ ] `SR_URL_<ENV>` — Schema Registry URL.
- [ ] `SR_API_KEY_<ENV>` / `SR_API_SECRET_<ENV>`.

### Terraform state backend
- [ ] `TG_S3_BUCKET` — S3 bucket for state.
- [ ] `TG_S3_REGION` — region.
- [ ] `TG_DDB_LOCK_TABLE` — DynamoDB table for state locking.
- [ ] `AWS_TF_STATE_ROLE_ARN` — IAM role assumable from GitHub OIDC for state-bucket access.

### POCs (client's 4 authoritative issues)
- [ ] **Datagen / source topic name** for SOD positions — POC SQL uses logical names like `sod_positions_raw` and `sod_positions_keyed`; map them to the actual Kafka topic names in your environment (e.g., `uat.sod.position.global.new.compact.avro`). Adjust the `CREATE TABLE` names accordingly. (For the lifecycle demo in `flink-sql/queries/`, the topic is `inventory.avro.topic` from the inventory datagen quickstart — already wired in.)
- [ ] **Datagen Avro schema** for SOD positions — POCs are now aligned with the production 6-field PK `(BusinessDate, FundId, PmuId, DealId, SecurityId, Direction)` and a representative subset of value columns (`Quantity`, `MktValLocal`, `load_ts`). If your datagen schema differs, adjust column types/names in `01_source_table.sql` for each POC.
- [ ] **Allocation topic schema** (Issue 4) — the dedup POC assumes each allocation event carries a `TransactionId STRING NOT NULL`. Confirm your allocation producer emits a stable per-event id; if not, that's the first conversation to have with the allocation producer team.
- [ ] If you'd like me to also include a `confluent_kafka_connector` Terraform resource for the datagen connectors themselves (so the whole POC env stands up from `terragrunt apply`), share the connector configs.

### Optional but recommended
- [ ] Sanitized copies of current Q1 and Q2 SQL — so we can write the production deploy targets in `flink-sql/queries/`, not just the POCs.
- [ ] A list of downstream consumers of `live_positions` and their `isolation.level` config (matters for Issue 4 exactly-once end-to-end).

---

## How the CI/CD answers DKP's "Decision Point 1"

> "Use GitHub Actions pipeline to version-control and deploy Flink SQL queries to production rather than manual UI deployment."

**Yes, fully feasible. Here's how the pieces snap together:**

| Pipeline | Trigger | What it does |
|---|---|---|
| `terraform-plan.yml` | PR touching `terraform/` or `terragrunt/` | Runs `terragrunt plan` for each env; comments diff on the PR. |
| `terraform-apply.yml` | Push to `main` (or manual dispatch) | Runs `terragrunt apply` for the chosen env — creates/updates compute pool, service accounts, RBAC, API keys, topics. |
| `flink-validate.yml` | PR touching `flink-sql/**` | Runs `EXPLAIN` of each changed SQL against the **dev** compute pool. Fails fast on syntax/planner errors. **No deploy.** |
| `flink-deploy.yml` | Push to `main` touching `flink-sql/**` (or manual dispatch with target env) | For each changed statement: stops the running statement (state retained), submits the new SQL with a versioned name (`<name>_<git-sha>`), waits for `RUNNING`, deletes the old. Auto-rolls-back on failure. |
| `flink-control.yml` | Manual dispatch only | Operator actions on already-deployed statements: **stop / resume / delete / describe / list**. Replaces clicking buttons in the Confluent UI. See the demo runbook in `flink-sql/queries/README.md`. |

**Why the deploy script works the way it does (key insight):** Terraform's `confluent_flink_statement` resource handles updates by destroy-then-create — fatal for a stateful streaming job. We use Terraform for everything *around* the statements (pool, accounts, RBAC, topics) and the Confluent CLI for the statement lifecycle itself. This is why you'll see infra resources in `terraform/` and SQL in `flink-sql/` deployed by `scripts/flink_deploy.sh`.

**Manual UI deploys are blocked by RBAC:** since only the CI/CD service account holds `FlinkAdmin` on the prod compute pool, engineers literally cannot deploy via the Confluent UI — they get an authorization error. Engineers retain `FlinkDeveloper` (read-only) for inspection.

---

## How to test the POCs in your account

Quickstart, assuming Terraform/Terragrunt have run for `dev`:

```bash
# 1) Set context to your dev env
confluent environment use $CONFLUENT_ENV_ID_DEV
confluent flink compute-pool use $CONFLUENT_FLINK_COMPUTE_POOL_ID_DEV

# 2) Issue 1 — Duplicate SOD Runs (dedup approach)
confluent flink statement create poc1_src   --sql "$(cat flink-sql/poc/issue1_dedup/01_source_table.sql)"
confluent flink statement create poc1_sink  --sql "$(cat flink-sql/poc/issue1_dedup/02_sink_table.sql)"
confluent flink statement create poc1_dedup --sql "$(cat flink-sql/poc/issue1_dedup/03_dedup_query.sql)"
# Replay datagen 3x for the same business date, then:
confluent flink statement create poc1_check --sql "$(cat flink-sql/poc/issue1_dedup/04_assert_query.sql)"

# 3) Issue 2 — PK NOT ENFORCED (upsert source approach; cleaner than POC1's dedup)
confluent flink statement create poc2_src     --sql "$(cat flink-sql/poc/issue2_composite_pk/01_source_table.sql)"
confluent flink statement create poc2_sink    --sql "$(cat flink-sql/poc/issue2_composite_pk/02_sink_table.sql)"
confluent flink statement create poc2_through --sql "$(cat flink-sql/poc/issue2_composite_pk/03_restream_query.sql)"
confluent flink statement create poc2_check   --sql "$(cat flink-sql/poc/issue2_composite_pk/04_consumer_test.sql)"

# 4) Issue 3 — Restart-Safe (exactly-once sink + savepoint resume)
confluent flink statement create poc3_src    --sql "$(cat flink-sql/poc/issue3_restart_safe/01_source_table.sql)"
confluent flink statement create poc3_sink   --sql "$(cat flink-sql/poc/issue3_restart_safe/02_sink_exactly_once.sql)"
confluent flink statement create poc3_query  --sql "$(cat flink-sql/poc/issue3_restart_safe/03_query.sql)"
# See flink-sql/poc/issue3_restart_safe/README.md for the stop/resume procedure.

# 5) Issue 4 — Transaction-level idempotence (TransactionId dedup)
confluent flink statement create poc4_src    --sql "$(cat flink-sql/poc/issue4_idempotence/01_allocation_source.sql)"
confluent flink statement create poc4_dedup  --sql "$(cat flink-sql/poc/issue4_idempotence/02_dedup_by_txn.sql)"
confluent flink statement create poc4_pos    --sql "$(cat flink-sql/poc/issue4_idempotence/03_position_sink.sql)"
# See flink-sql/poc/issue4_idempotence/README.md for the chaos-replay procedure.
```

Or once the deploy pipeline is wired up, the same is achieved by pushing the SQL files to `main` and letting `flink-deploy.yml` run (against `flink-sql/queries/**`, not `flink-sql/poc/**` — POCs are run-by-hand demos).

---

## Open questions for our next sync

These came out of the ANALYSIS write-up against the client's authoritative 4-issue list; flagging them now so DKP can come prepared:

1. **SOD producer semantics** (Issues 1, 2) — does the SOD producer emit the *intended latest value* per key, or deltas? Upsert source semantics only fix the multiplication if the producer is snapshot-style. Also: can the producer emit tombstones for removed positions?
2. **Allocation producer semantics** (Issue 4) — does every allocation event carry a stable, unique `TransactionId`? If retries reuse the same id, dedup-by-id works; if not, we need to derive one (hash of payload + emit time?). Confirm corrections always carry a NEW id, never reuse.
3. **Downstream consumer isolation level** (Issue 3) — are any consumers of `live_positions` (or other production topics) running `read_uncommitted`? Even one breaks end-to-end exactly-once.
4. **Existing Q1/Q2 source DDL** (Issue 2 audit) — for every Flink table that reads a compacted, keyed topic, what is the current `changelog.mode`? Any `'append'` reads on keyed topics are latent multiplication bugs.
5. **Production statement names + git mapping** — for the eventual migration off UI deploys, we need the current statement names so the deploy pipeline can target them with `stop`/`resume` instead of fresh creates.
