# NR — the Databricks alternate universe (basics refresher)

**What this doc is:** a mental rehearsal. In a parallel timeline, New Relic's Product Rating platform is on **Databricks + `dbt-databricks`** instead of Snowflake + open Spark/Iceberg. Every real win you actually shipped, translated into how you would have done it on Databricks.

**Why it matters:** interviewers ask "how would you do X on Databricks?" — this doc has the exact answer for every X you can point to on your resume. You've already done the reasoning; this doc just re-labels the pieces.

**Interview honesty:** this is thought-exercise material. In the actual room, say "we did this on open Spark; on Databricks I'd do it this way" — not "we did this on Databricks."

---

## Since you last used Databricks (~2020) — what's new that matters here

You already know the fundamentals cold: Spark, Delta Lake basics, notebooks, job clusters, dbt, Snowflake, cloud storage. The 2021+ era mostly added a **governance layer, declarative pipeline framework, IaC deployment story, and a set of Delta-side performance features that quietly rewrite the "how do I make MERGE fast" playbook**. None of it invalidates what you know — it's all layered on top.

The glossary you should skim before reading the four bullets below:

- **Unity Catalog (UC)** — Databricks' unified governance layer (GA 2022). Three-level namespace `catalog.schema.table` (vs Hive Metastore's two-level `schema.table`). Handles table ACLs, row-level security, column masks, lineage, and cross-workspace sharing centrally. This is the replacement for Hive Metastore that you probably remember.
- **Delta Live Tables (DLT)** — declarative pipeline framework (2022). You define target tables + data-quality expectations; DLT figures out the DAG, incrementalization, retries, and quarantine. Think "dbt-shaped Spark" but native to Databricks.
- **Databricks Asset Bundles (DABs)** — YAML-based IaC (2023) to deploy jobs, DLT pipelines, and notebooks across dev/staging/prod. Replaces the older `dbx` CLI. This is how modern Databricks projects ship code — bundle YAML in git, `databricks bundle deploy -t prod`.
- **SQL Warehouse (Classic / Pro / Serverless)** — persistent JDBC compute purpose-built for SQL, dbt, and BI. **Classic** = customer cloud; **Pro** = Classic + Photon + Predictive I/O; **Serverless** = Databricks-managed, starts in seconds, per-second billing. Not the same thing as a job cluster — SQL Warehouses are for interactive/dbt/BI, job clusters are for pipelines.
- **Photon** — vectorized C++ query engine (GA 2022). ~2× DBU rate but often 3–8× faster wall-clock, so typically **net cheaper** on SQL-heavy workloads. Toggle at the cluster / warehouse level.
- **Liquid Clustering** — 2024 successor to partitioning + Z-Order for **new** Delta tables. You declare clustering keys; Databricks continuously reorganizes files. Keys can be changed later without table rewrite (the big win over `PARTITIONED BY`).
- **Deletion Vectors (DVs)** — 2023 feature that soft-deletes rows via a sidecar bitmap instead of rewriting Parquet files. Turns MERGE / UPDATE / DELETE from "rewrite whole files" into "flip bits" — order-of-magnitude speedups on MERGE-heavy tables.
- **Predictive Optimization (PO)** — 2024 Databricks-managed auto-`OPTIMIZE` and auto-`VACUUM`. Databricks decides when to compact and clean up files based on your read/write patterns. Replaces hand-rolled maintenance cron jobs.
- **Delta UniForm** — 2024 feature. Write once as Delta, expose the same table as Iceberg (and Hudi) metadata for external engines like Snowflake, Trino, BigQuery. Solves the "we're on Delta but partners need Iceberg" problem.
- **Auto Loader** (`cloudFiles`) — incremental file ingestion from cloud storage. Two modes: **directory listing** (poll bucket) and **file notification** (SNS/SQS on AWS, Event Grid on Azure, Pub/Sub on GCP) — the latter is O(1) discovery, no rescans.
- **`dbt-databricks` adapter** — the Databricks-maintained dbt adapter (2023+). Preferred over the older community `dbt-spark`. Understands UC, Liquid Clustering, Deletion Vectors, materialized views, and SQL Warehouse targets natively.
- **AI/BI Dashboards** — 2024 successor to Databricks SQL dashboards. Serverless, LLM-assisted authoring, cross-filter support. Still SQL-backed; the "AI" part is optional.

Now the four bullets, with brief inline primers on first mention.

---

## The alternate-universe stack (memorize this)

| Layer | Real NR (today) | Alternate NR (Databricks) |
|---|---|---|
| Warehouse (source) | Snowflake | Snowflake |
| Lakehouse (target) | Iceberg on S3 | **Delta Lake on ADLS/S3** (or **Iceberg via UniForm**) |
| Compute — SQL / dbt | Spark Thrift Server | **Databricks SQL Warehouse (Serverless Pro)** with **Photon** |
| Compute — streaming | Spark Structured Streaming (on internal Spark cluster) | **Databricks job clusters** or **DLT compute (serverless)** |
| Catalog | Project Nessie | **Unity Catalog** (three-level `catalog.schema.table`) |
| Orchestration | Airflow 3 | **Databricks Workflows** (+ Airflow only for cross-system) |
| Ingestion | Kafka + custom | **Auto Loader** (cloudFiles) + **Kafka Structured Streaming** |
| Ops for tables | Manual OPTIMIZE / cleanup jobs | **Predictive Optimization** + **Liquid Clustering** + **Deletion Vectors** |
| Governance | Nessie + custom ACLs | **Unity Catalog** — RLS, column masks, lineage, ABAC |
| dbt adapter | `dbt-spark` | **`dbt-databricks`** |
| Cross-warehouse SQL bridge | `dbt-polyglot` | **Same `dbt-polyglot`** — Spark is first-class, so it runs on Databricks Spark unchanged |
| CI/CD | Jenkins + GitHub + dbt Cloud triggers | **GitHub Actions + Databricks Asset Bundles (DABs)** |
| Model-parity validation | Custom framework | **DLT expectations** + Databricks Workflows validation task + system tables |

---

## Bullet 1 — Open Lakehouse Migration

**What you actually did:** Led the 440-model dbt pipeline migration from Snowflake to Iceberg + Spark + Nessie + Airflow 3, with automated model-parity validation and a mismatch-spike dashboard for zero-downtime cutover.

### How you'd do it on Databricks

**Storage & format.** Target = **Delta Lake tables** in **Unity Catalog** (*UC = Databricks' central governance layer; uses a three-level `catalog.schema.table` namespace, replaces the two-level Hive Metastore you remember*). If cross-engine reads are required (Snowflake/Trino/BigQuery need to read the same tables during co-existence), enable **Delta UniForm** (*write once as Delta, expose the same table as Iceberg metadata to external engines — no dual-write, no duplicated storage*). Otherwise pure Delta.

**Catalog & migration namespacing.** Two catalogs — `prod` (current Snowflake mirror) and `prod_lakehouse` (new Delta target). Same schema/table names. All 440 models rewritten to write to `prod_lakehouse` first. Downstream consumers read from a **view layer** — flip views to point at the new catalog when validation clears.

**Compute for dbt.** A dedicated **Databricks SQL Warehouse** (*persistent JDBC compute purpose-built for SQL / dbt / BI — not the same as a job cluster*) on the **Serverless Pro** tier (*Serverless = Databricks-managed, starts in seconds; Pro = includes Photon and Predictive I/O*) as the `dbt-databricks` target. **Photon** on (*vectorized C++ query engine — ~2× DBU rate but typically 3–8× faster wall-clock, net cheaper on SQL workloads*). Autoscaling 1–8 clusters. Two warehouses in practice — `dbt_prod_wh` and `dbt_ci_wh` (the latter smaller, always warm, for `dbt build --empty` gates).

**Transformation.** Swap the `dbt-spark` adapter for **`dbt-databricks`** (*Databricks-maintained dbt adapter, preferred since 2023 — understands UC, Liquid Clustering, Deletion Vectors, materialized views, and SQL Warehouse targets natively; `dbt-spark` is community-maintained and lags on Databricks features*). `dbt-polyglot` still handles the source-side Snowflake dialect gaps — Spark is its first-class target regardless of whether that Spark is your internal cluster or a Databricks SQL Warehouse. Zero adapter-specific changes to the SQL.

**Materialization strategy per model class.**
- Heavy fact tables → `incremental` with `+incremental_strategy: merge` and **`+liquid_clustered_by: ['event_date', 'account_id']`** (*Liquid Clustering = 2024 replacement for partitioning + Z-Order on new tables; declare clustering keys, Databricks continuously reorganizes files, and — critically — keys can change later without a table rewrite*).
- Dimension tables → `table` with **Deletion Vectors** enabled (*soft-delete rows via a sidecar bitmap instead of rewriting Parquet files — makes UPDATE / DELETE / MERGE much cheaper*) for cheap in-place updates.
- Hot mart layer → `materialized_view` (refresh on write to upstream).
- Cheap staging views → `view`.

**Migration safety net — the parity framework.**
- A `dbt build --empty` step in CI validates every model against the SQL Warehouse without moving any data. This is the drop-in Databricks equivalent of your custom validation gate.
- Runtime parity check: a scheduled **Databricks Workflow** with three tasks — (1) run the model on Snowflake, (2) run the same model on Databricks, (3) row-count + column-checksum diff + sampled row-level diff. Results land in a `prod_lakehouse.audit.parity_results` Delta table.
- Mismatch dashboard: **AI/BI Dashboard** (*2024 successor to Databricks SQL dashboards — serverless, SQL-backed, optional LLM-assisted authoring*) over `parity_results` — per-model equivalence rate over time, red on breach, alert to Slack via job webhook.

**Cutover.** Blue/green at the view layer. Views in a `serving` schema originally point to Snowflake; per-model, flip them to `prod_lakehouse.<model>` after parity is green for N consecutive runs. Zero downtime for consumers.

**Resume-bullet form of this alternate:**

> **Snowflake → Databricks Lakehouse Migration:** Led migration of 440+ dbt models from Snowflake to a Delta Lake on Unity Catalog, using **`dbt-databricks`** on a Serverless Pro SQL Warehouse with Photon. Adopted **Liquid Clustering** and **Deletion Vectors** for MERGE-heavy incremental models. Built automated model-parity validation via a scheduled Workflow + `dbt build --empty` CI gate, with an AI/BI dashboard tracking per-model equivalence — enabling **zero-downtime blue/green cutover** at the view layer.

**Interview 30-sec version:**

> "440 dbt models moving off Snowflake. On Databricks I'd land them as Delta in Unity Catalog on a Serverless Pro SQL Warehouse via `dbt-databricks`. Liquid Clustering on the hot incremental fact tables, Deletion Vectors for MERGE speed, Photon for the ~3-5× wall-clock speedup that pays for its DBU premium. Migration safety: a Databricks Workflow does dual-run parity checks — row counts, column checksums, sampled row-level diff — landing results in an audit Delta table with an AI/BI dashboard on top. Cutover is blue/green at the view layer, model by model."

---

## Bullet 2 — Snowflake Cost & Performance ($45.2K savings, 50% faster)

**What you actually did:** Refactored dbt models on Snowflake — $45.2K annual credit savings, 50% faster queries, 100% data parity via automated validation.

### How you'd do it on Databricks

**The savings mechanism translates directly.** On Databricks the analogous levers are:
- **Photon** — 2× DBU rate but typically 3–8× faster on SQL-heavy workloads → net cheaper AND faster. This is the biggest single win most Snowflake → Databricks migrations see.
- **Liquid Clustering** on the right keys — replaces `PARTITIONED BY` + `ZORDER BY`. Doesn't require rewrites when clustering keys change. `dbt-databricks` supports it directly (`+liquid_clustered_by: [...]`).
- **Deletion Vectors** — for MERGE-heavy tables, MERGE becomes ~10× faster because rows are soft-deleted rather than files rewritten.
- **Predictive Optimization** (*2024 Databricks-managed auto-`OPTIMIZE` and auto-`VACUUM` — Databricks decides compaction/cleanup timing based on read/write patterns; replaces the maintenance cron jobs you'd hand-roll*) — replaces your manual compaction cron jobs, and Databricks tunes when to run.
- **Warehouse right-sizing** — SQL Warehouses come in T-shirt sizes. Smaller warehouse × higher concurrency is often cheaper than one big warehouse.
- **Serverless SQL Warehouse** — for spiky BI workloads, pay per-second. No idle cost.

**Validation pattern is identical** to what you built on Snowflake — a scheduled Databricks job that runs the old & new model, diffs, and posts to a dashboard.

**Resume-bullet form:**

> **Databricks Cost & Performance:** Refactored critical dbt models on Databricks — **$45K+ in annual DBU savings**, **50% faster** queries via **Photon + Liquid Clustering + Deletion Vectors**, and 100% data parity via automated dual-run validation frameworks. Adopted **Predictive Optimization** to auto-tune `OPTIMIZE` / `VACUUM` cadence per table.

**Interview 30-sec version:**

> "Same $45K savings story on Databricks reads as: turned on Photon (2× DBU rate but 3-8× faster net-cheaper), moved incremental fact tables to Liquid Clustering, enabled Deletion Vectors on the MERGE-heavy ones, right-sized the SQL Warehouse T-shirt, and let Predictive Optimization handle OPTIMIZE and VACUUM cadence instead of running my own crons. Validation is the same dual-run parity check landing to an audit Delta table."

---

## Bullet 3 — CI/CD for Data (85% deployment lead time cut)

**What you actually did:** Jenkins + GitHub CI/CD pipeline automating linting, BDD / unit tests, dbt Cloud triggers, quality gates, and Slack alerts — 85% deployment lead time cut, eliminated manual PR overhead.

### How you'd do it on Databricks

**Replace Jenkins with GitHub Actions.** Everything Jenkins did is a `steps:` in a `.github/workflows/ci.yml`. Simpler, no self-hosted runner.

**Replace dbt Cloud triggers with `dbt-databricks` running in-place on Databricks Asset Bundles (DABs)** (*DABs = 2023 YAML-based IaC that bundles your jobs, DLT pipelines, and notebooks and deploys them across environments; replaces the older `dbx` CLI. Think Terraform-shaped, but scoped to Databricks resources*). You deploy a dbt project as a DAB job:

```yaml
resources:
  jobs:
    dbt_prod:
      name: dbt Prod Run
      tasks:
        - task_key: dbt_build
          dbt_task:
            project_directory: ./dbt
            commands:
              - "dbt deps"
              - "dbt build --target prod"
            warehouse_id: ${var.prod_wh_id}
```

**CI flow, PR-scoped:**

1. **On PR open:** GitHub Actions checks out code, runs `sqlfluff lint`, `dbt parse`, `dbt compile`.
2. **`dbt build --empty` gate** against a CI SQL Warehouse — validates every model's SQL executes on Databricks without moving data. Same trust semantic as your current CI.
3. **BDD / unit tests** — `pytest` + `dbt-databricks` in Python-based unit tests, `dbt-expectations` for data quality.
4. **Slim CI** — dbt's `state:modified+` selector runs only changed models + downstream, keeping CI fast even at 440-model scale.
5. **PR preview environment** — GH Action deploys to a `pr_<num>` schema in Unity Catalog. Reviewers can query the PR's output. Cleaned up on merge/close.
6. **On merge to `main`:** Slim deploy via `databricks bundle deploy -t prod`, triggers the prod Workflow.
7. **Slack alerts** — Databricks Workflow webhook + GitHub Actions Slack action for failures.

**Result:** same 85% lead-time cut, now with proper preview environments per PR (something that was harder on Snowflake).

**Resume-bullet form:**

> **CI/CD for Data on Databricks:** Architected a GitHub Actions + **Databricks Asset Bundles** pipeline for `dbt-databricks` — sqlfluff linting, `dbt build --empty` validation gates on a CI SQL Warehouse, dbt-expectations quality tests, PR preview schemas in Unity Catalog, and Slack alerts on failure. **85% cut in deployment lead time** with per-PR data previews.

**Interview 30-sec version:**

> "GitHub Actions is the CI runner. Databricks Asset Bundles is the deploy target — YAML defines the dbt job that runs `dbt build`. Every PR gets a temporary `pr_<num>` schema in Unity Catalog, so reviewers can actually query the changed model output before merging. Quality gates: sqlfluff, `dbt build --empty` on a small CI SQL Warehouse, dbt-expectations. Slack on failure via Databricks Workflow webhook. Same 85% lead-time win, better preview story."

---

## Bullet 4 — Zero-Downtime Migration & Monetization Modeling

**What you actually did:** Migrated a 10 GB/hour billing pipeline from Airflow 1.0 → dbt Cloud + Snowflake with 100% data parity and zero customer-visible downtime. Engineered a rating engine combining usage meters and complex billing rules — accurate monetization of 15+ new product SKUs.

### How you'd do it on Databricks

**Ingestion.** 10 GB/hour of usage events lands in cloud storage. Use **Auto Loader** (*`cloudFiles` source — incremental file ingest from cloud storage with two modes: directory listing (poll bucket) and file notification (SNS/SQS / Event Grid / Pub/Sub); the latter is O(1) discovery, no rescans*) in a Structured Streaming job:

```python
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/chk/usage/schema")
   .option("cloudFiles.inferColumnTypes", "true")
   .load("s3://usage-events/")
   .writeStream
   .format("delta")
   .option("checkpointLocation", "/chk/usage/bronze")
   .trigger(processingTime="1 minute")
   .toTable("prod.bronze.usage_events"))
```

Auto Loader with **file notification mode** (SNS/SQS on AWS, Event Grid on Azure, Pub/Sub on GCP) is O(1) discovery — you never rescan the bucket.

**Transformation.** Silver → Gold via **DLT** (*Delta Live Tables — declarative pipeline framework introduced 2022; you define target tables + expectations (data-quality rules), and DLT infers the DAG, incrementalization, retries, and quarantine of bad rows. Think "dbt-shaped but Spark-native and built into Databricks"*) — declarative, built-in expectations, incremental by default:

```python
import dlt

@dlt.table(comment="Cleaned usage events")
@dlt.expect_or_drop("valid_ts", "event_ts IS NOT NULL")
@dlt.expect_or_fail("valid_meter", "usage_meter IN (SELECT meter FROM prod.reference.meters)")
def silver_usage():
    return dlt.read_stream("prod.bronze.usage_events")

@dlt.table(comment="Rated usage — the monetization engine")
def gold_rating():
    usage = dlt.read("silver_usage")
    modifiers = spark.table("prod.reference.usage_modifiers")
    entitlements = spark.table("prod.silver.entitlements")

    return apply_rating_logic(usage, modifiers, entitlements)
```

The rating engine — which was your consumption-modifier refactor at NR — is the `apply_rating_logic` function. On Databricks it lives as pure PySpark using `arrays_overlap` for the entitlement match (the exact pattern you already wrote for the OSS-Spark version — completely portable).

**Zero-downtime cutover.** Airflow 1.0 → Databricks Workflows via a shadow-write pattern:
1. Bring up the DLT pipeline in a parallel Unity Catalog schema (`prod_v2`).
2. Downstream consumers read from `prod.serving.*` views. These views point to old Airflow pipeline output initially.
3. Dual-run for 2 weeks — both pipelines write to their own schemas.
4. Parity check: scheduled Workflow diffs `prod.rating.*` vs `prod_v2.rating.*` — row counts, revenue totals, per-account rollups.
5. Flip the `serving` views to `prod_v2` after N parity-green days. Airflow 1.0 pipeline decommissioned.

**Monetization for 15+ new SKUs.** Same seed-driven pattern you already use — a `usage_modifiers` seed + rating logic that reads from it. Adding a new SKU = one seed row + optional entitlement row. No code deploy.

**Resume-bullet form:**

> **Zero-Downtime Billing Migration & Monetization on Databricks:** Migrated a **10 GB/hour billing pipeline** from Airflow 1.0 to a Databricks stack — Auto Loader + Structured Streaming for Bronze ingest, **DLT** for Silver/Gold with declarative expectations, Databricks Workflows for orchestration. Achieved **100% data parity** via a 2-week dual-run + view-swap cutover. Engineered a seed-driven rating engine (dbt macros + PySpark `arrays_overlap` pattern) that monetized **15+ new product SKUs** for the Feb 2025 release cycle.

**Interview 30-sec version:**

> "10 GB/hour billing events. On Databricks that's Auto Loader from S3 in file-notification mode → Bronze Delta. Silver and Gold in DLT with expectations for row-level quality. The rating engine — the actual monetization logic — is PySpark using `arrays_overlap` for entitlement matching, seed-driven so adding a new SKU is a seed row not a code deploy. Cutover is view-swap: `serving` views point at old pipeline, dual-run for two weeks, parity dashboard, flip when green."

---

## Bonus — the dbt-polyglot story, Databricks-flavored

**The pitch reframed:** "I open-sourced `dbt-polyglot` on PyPI so that Snowflake-authored dbt models can execute on **Databricks Spark** unchanged. It's a compile-time transpiler that hooks into dbt's compile phase — parses the model as Snowflake SQL, transpiles to Spark SQL via sqlglot, applies a Spark correctness fix-up layer, and hands the Spark SQL to `dbt-databricks` for execution. It made our 440-model migration feasible without rewriting a single `.sql` file, and the same tool works on any Databricks SQL Warehouse today."

Same code, same behavior, more Databricks-relevant framing.

---

## What this practice buys you

- Fluent, spec-level answers when interviewers ask "how would you build X on Databricks?" — you've already answered each one for a real system.
- Vocabulary alignment — you'll say "Photon" instead of "vectorized engine," "Liquid Clustering" instead of "clustering keys," "Predictive Optimization" instead of "auto-compaction cron."
- Confidence — every claim you make maps back to a real thing you shipped. You aren't making it up; you're just re-labeling it in Databricks language.

---

## The two sentences you should be able to say in your sleep

> "I've done every architectural pattern this Databricks job requires — 440-model dbt migration, MERGE-heavy incremental modeling, streaming ingest with parity validation, sub-minute latency on rating pipelines, cost tuning that shaved 45K/year off compute. On Databricks the primitives are Delta + Unity Catalog + Photon + Liquid Clustering + DLT + Workflows instead of Iceberg + Nessie + Spark Thrift + Airflow, but the reasoning is identical and I have Databricks production hands-on from Infosys/Walmart plus my `dbt-polyglot` OSS work explicitly targeting Databricks Spark."

Rehearse that. It's your master answer.
