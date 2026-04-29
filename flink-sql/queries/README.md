# Flink Queries — Demo Runbook (CI/CD-Driven Lifecycle)

This directory contains a small end-to-end demo to convince stakeholders that **GitHub Actions can fully replace the Confluent Cloud UI** for managing Flink SQL statements: deploy, stop, resume, delete, inspect — all from the Actions tab. No buttons in the Confluent UI required.

The demo reads from your existing **`inventory.avro.topic`** (datagen "inventory" quickstart) and writes a continuously-updated rollup to a new compacted upsert topic. You watch values grow, stop the statement, watch them freeze, resume, watch them advance again — five minutes of "this works."

---

## Prerequisites

- A Confluent Cloud environment with:
  - A Kafka cluster.
  - A Flink compute pool (any size; even the smallest works for the demo).
  - A datagen source connector producing into a topic named **`inventory.avro.topic`** (the standard "inventory" quickstart, Avro). Schema: `{id INT, productid INT, quantity INT}`.
- This repo pushed to GitHub, with secrets configured (next section).

> The demo expects Avro on the value side. The datagen connector should be configured with `output.data.format = AVRO` and a Schema Registry connection, producing the standard "inventory" quickstart schema.

## GitHub Actions secrets — exactly what to set, and where

**All credentials are pulled from GitHub Actions secrets, never hardcoded in the workflow.** Set them under **Repo Settings → Environments → `dev`** (and repeat for `staging` / `prod` when ready).

| Secret name | Value | Purpose |
|---|---|---|
| `CONFLUENT_ORG_ID` | Your Confluent org id (e.g., `b0b1c2d3-...`) | `confluent login` target |
| `CONFLUENT_CLOUD_API_KEY` | Cloud API key id | Auth for org/env-level CLI ops |
| `CONFLUENT_CLOUD_API_SECRET` | Cloud API key secret | (same) |
| `CONFLUENT_FLINK_API_KEY` | Flink-pool-scoped API key id | Auth for `flink statement *` |
| `CONFLUENT_FLINK_API_SECRET` | Flink-pool-scoped API key secret | (same) |
| `CONFLUENT_ENV_ID_DEV` | Confluent environment id (`env-xxxxx`) | Per-env target |
| `CONFLUENT_FLINK_COMPUTE_POOL_ID_DEV` | Flink compute pool id (`lfcp-xxxxx`) | Per-env target |

> **Why GitHub Environments and not just repo-level secrets?** Environments give per-env secret scoping plus a manual approval gate — so a `prod` deploy can require human approval even though the workflow itself is identical to `dev`. This is how we enforce "no accidental prod pushes" without making the engineer think about it.

> **Tip:** the workflows reference these secrets dynamically using `secrets[format('CONFLUENT_ENV_ID_{0}', inputs.env)]`. Naming convention is fixed — keep the `_DEV` / `_STAGING` / `_PROD` suffix exact (case-insensitive in GitHub).

---

## Files in this directory

| File | What it does | Lifecycle |
|---|---|---|
| `inventory_source.sql` | Pins the schema of the existing `inventory.avro.topic` (3 INT fields + Kafka record timestamp). `CREATE TABLE IF NOT EXISTS` — idempotent, completes immediately. | DDL, one-shot |
| `inventory_by_bucket_sink.sql` | Creates the demo output topic — compacted upsert sink keyed by `bucket_id`, with `sink.delivery-guarantee = exactly-once`. | DDL, one-shot |
| `inventory_by_bucket.sql` | The actual streaming aggregation: `INSERT INTO inventory_by_bucket SELECT productid % 10, COUNT(*), SUM(quantity), MAX(ts) FROM inventory.topic GROUP BY productid % 10`. Ten upsert rows that grow continuously. | **Long-running. This is the statement you'll deploy/stop/resume/delete.** |

---

## 5-minute demo runbook

### Step 1 — Push the code

The `flink-deploy.yml` workflow watches `flink-sql/**` on push to `main`. Pushing this directory to `main` deploys all three statements automatically.

```bash
git add flink-sql/queries/
git commit -m "demo: inventory_by_bucket streaming aggregation"
git push origin main
```

Open the **Actions** tab in GitHub. You should see `flink-deploy` running. Each statement takes ~30 seconds to reach `RUNNING`.

> If you don't want to push to main yet, use the manual trigger instead: **Actions → flink-deploy → Run workflow**, set `env=dev`, set `statement=inventory_by_bucket` (the streaming query). You can run the DDL files (`inventory_source`, `inventory_by_bucket_sink`) the same way.

### Step 2 — Watch it work

In Confluent Cloud:

- **Topics → `inventory_by_bucket`** — you'll see exactly **10 records** (one per bucket, `bucket_id` 0 through 9), with `records_in_bucket` and `total_quantity` advancing as the datagen connector keeps producing into `inventory.avro.topic`. Each bucket is upsert-keyed: a single row per bucket, value updated continuously.
- **Flink → Statements** — the `inventory_by_bucket_<git-sha>` statement is in `RUNNING` phase.

### Step 3 — Stop it from GitHub Actions

**Actions → flink-control → Run workflow:**
- `action`: `stop`
- `env`: `dev`
- `statement`: `inventory_by_bucket_<git-sha>` (copy from the previous step)

The statement transitions to `STOPPED`. The `inventory_by_bucket` topic stops advancing — but the existing 10 rows stay (this is the "state retained" property).

### Step 4 — Resume it

**Actions → flink-control → Run workflow:**
- `action`: `resume`
- `env`: `dev`
- `statement`: `inventory_by_bucket_<git-sha>`

Statement returns to `RUNNING`. Values resume advancing from where they paused — no replay, no double-count. **This is the restart-safety property** the production pipeline needs (Issue 3 in `ANALYSIS.md`).

### Step 5 — Inspect / list / delete

**Actions → flink-control → Run workflow** with one of:
- `action`: `describe` — prints YAML metadata (compute pool, phase, SQL, lineage).
- `action`: `list` — lists all statements in the compute pool. Leave `statement` blank.
- `action`: `delete` — permanently removes the statement. State is lost; not reversible.

That's the full operator surface. Anything an engineer would do in the Confluent UI is now a **GitHub Actions Run Workflow** click.

---

## What this demo proves to the client

1. **Source of truth is git.** Every Flink SQL statement is a file in `flink-sql/queries/`. PRs review changes, blame tracks history.
2. **Push-to-deploy works.** No human clicks anything in Confluent Cloud to ship a query change.
3. **Lifecycle is fully scriptable.** Stop / resume / delete / describe / list — all via workflow_dispatch. The Confluent UI becomes a viewing surface, not an action surface.
4. **State is preserved across restarts.** Stop → resume keeps the aggregated counts; this is the same machinery that makes the production pipeline restart-safe.
5. **Secrets stay in secrets.** Nothing is checked into the repo; per-env GitHub Environments enforce scoping.
6. **RBAC is the next step.** Once the demo lands, restrict the Confluent CLI's `FlinkAdmin` role to the CI/CD service account only — engineers keep `FlinkDeveloper` (read-only). At that point the UI literally cannot deploy, even if someone tries.

---

## Cleanup after the demo

```text
Actions → flink-control → Run workflow
  action: delete   env: dev   statement: inventory_by_bucket_<git-sha>
Actions → flink-control → Run workflow
  action: delete   env: dev   statement: inventory_by_bucket_sink_<git-sha>
Actions → flink-control → Run workflow
  action: delete   env: dev   statement: inventory_source_<git-sha>
```

The `inventory_by_bucket` Kafka topic itself remains until you delete it (via Confluent UI, Terraform, or `confluent kafka topic delete`).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `flink-deploy` job fails at `confluent login` | Cloud API key wrong or scoped to wrong org | Verify `CONFLUENT_ORG_ID` and the cloud API key both belong to the same org |
| Statement stuck in `PENDING` for >2 min | Compute pool out of CFU | Resize the pool or reduce parallelism |
| `inventory_by_bucket` topic not appearing | DDL didn't run; deploy only picked up the streaming statement | Manually trigger deploy on `inventory_by_bucket_sink` |
| `confluent flink statement *` returns 401 | Flink API key not set or expired | Re-issue the Flink-scoped API key under the compute pool, update `CONFLUENT_FLINK_API_KEY` / `..._SECRET` secrets |
| Statement fails with "schema mismatch" on the source | Datagen output format does not match `'value.format'` in the DDL | Verify the connector's `output.data.format = AVRO` and that the Schema Registry subject `inventory.avro.topic-value` resolves; redeploy if the connector was reconfigured after the topic was created |
| Statement fails with "table not found: inventory.avro.topic" | Topic auto-mapping uses different name (e.g., topic naming with dots gets escaped differently) | Run `SHOW TABLES;` in the Flink workspace and adjust the source DDL / `FROM` clause |
| Statement renamed with `_<git-sha>` suffix doesn't match what you typed | Deploy script appends a short git sha for safe rollback. Use the actual name from `flink-control list` | Run `action: list` first, copy the exact name |

---

## Reference: schema of `inventory.avro.topic` (from the screenshot)

```text
Total messages: 1,136,553   Retention: 1 week   Partitions: 6+

Sample value: { "id": 2339278, "quantity": 2339278, "productid": 2339278 }
```

The standard Confluent "inventory" datagen quickstart emits monotonically increasing integers in all three fields. That's a known datagen quirk — every record's `id`, `productid`, and `quantity` are the same counter. The bucketing trick (`productid % 10`) gives us 10 distinct keys to aggregate over so the demo has visible structure.
