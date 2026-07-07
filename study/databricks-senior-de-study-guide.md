# Databricks — Senior Data Engineer Interview Study Guide

Comprehensive prep for a Senior DE role where Databricks is the primary platform. Organized by **must-know → likely to be asked → nice-to-have**.

**Recommended prep time:** 8–12 hours of focused study over 3–5 days.

---

## 1. The Platform Mental Model (must know cold)

Databricks positions itself as **the Lakehouse Platform** — one unified system for data engineering, data warehousing, ML, and BI on top of open table formats.

**Three architectural pillars:**

1. **Compute** — Spark-based (with Photon), running on cloud VMs. Multiple flavors: All-purpose clusters, Job clusters, SQL Warehouses, Serverless.
2. **Storage** — Cloud object storage (S3 / ADLS / GCS). Tables written as **Delta Lake** (default) or **Apache Iceberg** (via UniForm).
3. **Governance** — **Unity Catalog** provides catalog / schema / table / volume namespace, ACLs, lineage, and cross-workspace sharing.

**Multi-cloud:** Runs on AWS, Azure, GCP with near-parity. Some cloud-specific bindings (IAM on AWS, service principals on Azure).

---

## 2. Unity Catalog (UC) — the #1 governance topic

**Three-level namespace:** `catalog.schema.table`. Old two-level `schema.table` is legacy (Hive Metastore).

- **Metastore** — top-level container per region, one per Databricks account.
- **Catalog** — logical grouping (e.g. `prod`, `dev`, per-team catalogs).
- **Schema** (aka database) — inside a catalog.
- **Table** / **View** / **Materialized View** / **Volume** — leaf objects.
- **Volume** — first-class object for unstructured/semi-structured files (replaces DBFS mounts for governed access).

**Key UC features you should be able to explain:**

- **Managed vs External tables.** Managed = UC owns storage lifecycle; External = you own storage path, UC owns metadata + ACL.
- **Data lineage** — automatic, column-level, across notebooks / jobs / DLT.
- **Access control** — GRANT / REVOKE, row-level filters, column masks, ABAC (attribute-based).
- **Cross-workspace sharing** via metastore attach.
- **Delta Sharing** — open protocol for cross-org sharing (data recipients don't need Databricks).
- **UC volumes** — replace `dbfs:/mnt/...` and `dbfs:/FileStore/...` (both deprecated for new workloads).
- **System tables** — `system.access.audit`, `system.billing.usage`, `system.query.history` — inspect billing, audits, and query cost.

**Interview probe examples:**
- "How would you migrate from Hive Metastore to Unity Catalog?" → UCX tool, principal mappings, table-by-table upgrade with `SYNC` command.
- "How do you implement row-level security?" → `ROW FILTER` function + `SET ROW FILTER` on the table, or dynamic views.

---

## 3. Delta Lake — must be strong here

**What it is:** Open-source ACID transaction layer on Parquet. Stored as `_delta_log/00000000.json`, `00000001.json`, … in the table directory.

**Must-know operations:**

| Operation | What it does | When to use |
|---|---|---|
| `OPTIMIZE table` | Compacts small files into larger (~1 GB by default) | After incremental writes |
| `OPTIMIZE table ZORDER BY (col)` | Multi-dim clustering | Legacy; prefer Liquid Clustering |
| `VACUUM table RETAIN N HOURS` | Deletes files no longer referenced | Storage cost hygiene |
| `MERGE INTO` | Upsert / delete / SCD-2 | CDC ingestion |
| `DELETE / UPDATE` | Row-level operations | ACID mutations |
| `TIME TRAVEL` — `VERSION AS OF n` / `TIMESTAMP AS OF t` | Read historical snapshot | Audits, incident recovery, ML reproducibility |
| `CHANGE DATA FEED` (CDF) | Read row-level changes between versions | Downstream incremental consumption |
| `RESTORE table VERSION AS OF n` | Rollback a table | Recover from bad write |

**Advanced concepts to be fluent on:**

- **Liquid Clustering** — replaces partitioning + Z-Order. Doesn't require rewriting when clustering keys change. `CLUSTER BY (col1, col2)`. Databricks-only (not OSS Delta).
- **Deletion Vectors** — soft-delete rows without rewriting Parquet files. Massive MERGE speedup. Enable with `TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')`.
- **Delta UniForm** — write once as Delta, read as Iceberg. Enables cross-engine reads (Snowflake, BigQuery, Trino can read a Delta table as Iceberg).
- **Column Mapping** — rename/drop columns without file rewrite. Requires `delta.columnMapping.mode='name'`.
- **Predictive Optimization** — Databricks-managed auto-OPTIMIZE and auto-VACUUM. Cost model is per-table.
- **Schema evolution** — `mergeSchema = true` on write appends new columns; `overwriteSchema = true` replaces schema.
- **Constraints** — `NOT NULL`, `CHECK (col > 0)`. Enforced on write.
- **Generated columns** — `col1 GENERATED ALWAYS AS (expression)`. Useful for partition columns without user maintenance.

**Delta transaction log** is worth understanding at protocol level:
- Every write = new JSON commit file in `_delta_log/`
- Every 10 commits = checkpoint parquet
- Multi-version concurrency control (MVCC) via optimistic concurrency + isolation levels (`WriteSerializable` default)

**Interview probes:**
- "Why is MERGE slow, and how do you speed it up?" → Deletion Vectors, right partitioning / Liquid Clustering on merge keys, low-shuffle merge.
- "Delta vs Iceberg?" → Formats: both open, both ACID. Delta is Databricks-native (via UniForm now Iceberg-readable). Iceberg has stronger multi-engine support. Databricks acquired Tabular (Iceberg company) in 2024 → Iceberg support is first-class in Databricks now.

---

## 4. Compute Types — critical to explain when to use what

| Compute | Use case | Pricing model | Startup time |
|---|---|---|---|
| **All-purpose cluster** | Interactive notebooks, dev/debug | DBUs while running (any state) | 3–5 min |
| **Job cluster** | Scheduled workflows (jobs), one job per cluster | DBUs while running, terminates on job finish | 3–5 min |
| **SQL Warehouse — Classic** | BI/SQL workloads, always-warm queries | Per-cluster-hour DBUs | 3–5 min |
| **SQL Warehouse — Pro** | Above + Photon, Predictive I/O | Photon-priced DBUs | 3–5 min |
| **SQL Warehouse — Serverless** | Same as Pro, but Databricks-managed compute (no VM startup on user account) | Per-second serverless DBUs, higher rate | 5–15 seconds |
| **Serverless Jobs / DLT Compute** | Serverless for pipelines, no cluster management | Per-second | Instant-ish |

**When to pick which (this WILL be asked):**
- Scheduled ETL → **Job cluster** (or Serverless if latency-sensitive)
- Ad-hoc analysis → **All-purpose**
- BI dashboards / concurrent SQL → **SQL Warehouse (Serverless preferred)**
- DLT pipelines → **DLT compute** (Serverless recommended)

**Cluster configuration knobs to know:**
- Databricks Runtime (DBR) version — LTS vs standard
- Photon on/off — 2× DBU rate but often 3-8× faster
- Worker type / driver type — match workload (memory-optimized for shuffle-heavy, compute-optimized for CPU-heavy)
- Autoscaling min/max
- Instance pools (pre-warmed VMs across clusters)
- Cluster policies (admin-enforced constraints)
- Spot / preemptible instances for cost savings
- Init scripts (deprecated for cluster-scoped; use cluster libraries or DBR init)

---

## 5. Photon — vectorized query engine

**One-liner:** A drop-in Spark query engine written in C++ (SIMD, vectorized) that runs the same Spark SQL but much faster.

- **2× the DBU rate but usually 3-8× faster on typical workloads** → net cheaper AND faster.
- Supports **SQL, DataFrame API**; some functions still fall back to non-Photon Spark for unsupported ops.
- Enabled per-cluster: `Databricks Runtime Photon Compute Node`.
- Native code, not JVM. No Python UDF acceleration (UDFs run in Python worker, not Photon).

**Interview probe:** "When would you NOT use Photon?" → When your workload is entirely Python UDF-heavy (Photon can't accelerate custom Python), or for very small workloads where DBU rate matters more than latency.

---

## 6. Auto Loader & Ingestion — must be strong

**Auto Loader** = `cloudFiles` format in Structured Streaming, incrementally ingests new files from cloud storage.

```python
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/checkpoints/schema")
   .option("cloudFiles.inferColumnTypes", "true")
   .load("s3://bucket/raw/")
   .writeStream
   .option("checkpointLocation", "/checkpoints/checkpoint")
   .table("bronze.events"))
```

**Key features:**
- **File notification vs directory listing** modes — notification uses cloud events (SNS/EventBridge on AWS, Event Grid on Azure, Pub/Sub on GCP) for O(1) discovery; directory listing scans the folder each trigger.
- **Schema inference & evolution** — infers types from JSON/CSV, evolves as new fields appear.
- **Rescued data column** — captures malformed rows in `_rescued_data`.
- **Backfill mode** — `cloudFiles.backfillInterval` re-scans in case notifications lag.

**vs COPY INTO:** COPY INTO is idempotent, batch, SQL-only. Auto Loader is streaming, more scalable, supports arbitrary triggers.

**Interview probe:** "Ingest 10K files/hour into Bronze — Auto Loader or COPY INTO?" → Auto Loader with file notification mode.

---

## 7. Delta Live Tables (DLT) — declarative pipelines

**What it is:** Declarative framework where you define target tables + expectations; DLT figures out DAG, retries, backfill, and monitoring.

```python
import dlt
from pyspark.sql.functions import *

@dlt.table(comment="Cleaned events")
@dlt.expect_or_drop("valid_ts", "event_ts IS NOT NULL")
def silver_events():
    return dlt.read_stream("bronze_events").filter("action != 'debug'")
```

**Key concepts:**
- **Streaming tables** — incremental, one row-per-microbatch semantics.
- **Materialized views** — recomputed on refresh, best for aggregations.
- **Expectations** — data quality rules (`expect_or_drop`, `expect_or_fail`, `expect`).
- **Live dependencies** — `dlt.read` / `dlt.read_stream` create the DAG.
- **Enhanced autoscaling** — scales workers based on backlog.
- **Change Data Capture** — `apply_changes` for SCD-1/SCD-2 from CDC feeds.
- **Development vs Production** mode — dev retries fast, prod is robust.

**Interview probe:** "Why DLT over regular jobs?" → Declarative DAG, built-in quality gates via expectations, automatic backfill, unified batch + streaming, less orchestration code.

**Caveat:** DLT lock-in. Migrating out is painful because the framework generates the DAG.

---

## 8. Databricks Workflows (Jobs) — orchestration

- Multi-task workflows with dependencies (DAG).
- Task types: Notebook, JAR, Python script, dbt task, SQL task, DLT pipeline, another job (nested), Spark submit.
- Triggers: schedule (cron), file arrival, continuous, manual.
- Parameters, task values (mini-XComs), conditional tasks (`if/else`).
- Retry policies, timeouts, notifications (email / Slack / webhook).
- Serverless jobs compute now supported.

**vs Airflow:** Workflows is Databricks-native, tightly integrated with Unity Catalog and lineage. Airflow is more portable and has richer ecosystem. Many teams use **Airflow for cross-system orchestration, Workflows for Databricks-internal**.

**Databricks LakeFlow** (announced 2024) — Databricks' answer to Airflow, integrated ingestion + transformation + orchestration. Watch as this evolves.

---

## 9. dbt on Databricks — specifically

**Adapter: `dbt-databricks`** (recommended, maintained by Databricks). Older `dbt-spark` still works but lacks UC integration.

Key config:

```yaml
# profiles.yml
databricks:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace-host>
      http_path: /sql/1.0/warehouses/<id>  # SQL Warehouse
      token: {{ env_var('DATABRICKS_TOKEN') }}
      catalog: prod
      schema: analytics
```

**Materializations available:**
- `view`, `table`, `incremental` (standard)
- `materialized_view` (Databricks-native, backed by Delta Live-style refresh)
- `streaming_table` (via `+incremental_strategy: append` + streaming source)

**Incremental strategies:**
- `merge` — default, uses MERGE INTO
- `append` — pure insert
- `insert_overwrite` — partition overwrite
- `replace_where` — dynamic partition overwrite via WHERE predicate

**Best practices:**
- Use **SQL Warehouse** as compute target (faster startup than all-purpose cluster).
- Use **liquid clustering** on incremental models: `config(liquid_clustered_by=['event_date', 'account_id'])`.
- Enable **Deletion Vectors** for MERGE-heavy tables.
- Use **dbt-databricks Unity Catalog integration** — three-level references (`ref('catalog.schema.model')` implicit).
- **Photon** for the SQL Warehouse — dbt runs sensitive to per-query latency benefit hugely.

**Interview probe:** "You have 1000 dbt models on Snowflake, migrating to Databricks — what breaks?" → SQL dialect differences (this is exactly what `dbt-polyglot` solves), Snowflake-only functions (LATERAL FLATTEN, VARIANT, IFF, `::`, QUALIFY), null ordering, incremental strategy differences.

---

## 10. Structured Streaming — must know for senior

**Triggers:**
- `.trigger(processingTime='1 minute')` — micro-batch on interval
- `.trigger(availableNow=True)` — process all available, then stop (great for backfill)
- `.trigger(continuous='1 second')` — experimental sub-second
- `.trigger(once=True)` — deprecated, use `availableNow`

**Output modes:**
- `append` — new rows only (works with almost all queries)
- `complete` — full result table (aggregations only)
- `update` — updated rows only (aggregations)

**State management:**
- Watermarks — `withWatermark('event_ts', '10 minutes')` — drops late data + bounds state
- Stateful ops: `groupBy` aggregations, joins, `mapGroupsWithState`, `flatMapGroupsWithState`
- Checkpoint location — must be durable, unique per query

**Kafka source/sink is standard.**

**Idempotency:** Use `foreachBatch` with `MERGE` for exactly-once against Delta. Structured Streaming to Delta gives exactly-once natively via `_spark_metadata`.

**Interview probe:** "Backfill a streaming pipeline?" → `trigger(availableNow=True)` on the source to process historical files, then switch to `processingTime` for live.

---

## 11. Databricks Asset Bundles (DABs) — IaC

Replaces `dbx`. YAML-defined bundles ship notebooks, jobs, DLT, and configuration.

```yaml
# databricks.yml
bundle:
  name: my-project

resources:
  jobs:
    my-etl:
      name: My ETL Job
      tasks:
        - task_key: process
          notebook_task:
            notebook_path: ./src/etl.py

targets:
  dev:
    workspace:
      host: https://dev.cloud.databricks.com
  prod:
    workspace:
      host: https://prod.cloud.databricks.com
```

Commands: `databricks bundle validate`, `databricks bundle deploy -t dev`, `databricks bundle run -t prod my-etl`.

**Alongside DABs:** Terraform provider for infra (workspaces, metastores, secrets), Databricks CLI v2, REST API.

**Interview probe:** "How do you version-control notebooks and jobs?" → Databricks Repos (Git integration) + DABs for job/DLT/pipeline definitions + Terraform for workspace-level infra.

---

## 12. Cost Model — DBUs

**DBU = Databricks Unit** — normalized compute unit. Rate varies by:
- Cluster type (all-purpose > job > SQL Warehouse Serverless > SQL Warehouse Classic per DBU/hour)
- Photon on/off (~2× rate with Photon)
- Runtime type (ML DBR = higher rate)
- Compute type (serverless has a per-second rate that includes infra + Databricks fee)

**Plus underlying cloud VM cost** (EC2 / VM / GCE) — separate line item on your cloud bill.

**Cost-optimization moves:**
- Job clusters over all-purpose for scheduled work
- Serverless for spiky BI workloads (SQL Warehouse Serverless)
- Photon for query-heavy workloads (usually cheaper at wall-clock cost)
- Spot instances for fault-tolerant workloads
- Cluster policies to prevent oversizing
- Predictive Optimization (auto-OPTIMIZE and auto-VACUUM to reduce query time)
- Autoscaling min/max tuned to actual load
- Delta Log checkpointing to keep log reads fast
- Small file compaction to keep OPTIMIZE cheap

**Interview probe:** "Your Snowflake bill was $X. Estimate Databricks equivalent." → It depends on workload profile. Analytical/BI is roughly parity; heavy ETL/ML is usually cheaper on Databricks with Photon.

---

## 13. What's New in 2024–2025 (recruiter loves this stuff)

- **Databricks acquired Tabular (June 2024)** — Iceberg company. Iceberg is now first-class alongside Delta.
- **Delta UniForm** — write Delta, read as Iceberg. Cross-engine compatibility.
- **Liquid Clustering** GA — replaces Z-Order + partitioning for new tables.
- **Predictive Optimization** — auto-OPTIMIZE and auto-VACUUM.
- **Deletion Vectors** GA — MERGE performance.
- **Materialized Views + Streaming Tables** as first-class citizens in SQL (not only DLT).
- **Databricks Assistant** — AI code helper in notebooks.
- **Genie** — text-to-SQL for business users.
- **LakeFlow** (announced 2024) — Databricks' Airflow-like orchestrator + Connect ingestion.
- **AI/BI Dashboards** — new BI product (successor to Databricks SQL dashboards).
- **Foundation Model APIs** — Databricks-hosted LLM serving.
- **Vector Search** — vector DB integrated with UC tables.
- **Databricks Marketplace** — data + AI models marketplace.
- **Serverless job compute** GA.
- **Runtime 15.x and 16.x** — Spark 4.0.x under the hood in DBR 16+.

---

## 14. Databricks vs Snowflake — you WILL be asked

| Dimension | Databricks | Snowflake |
|---|---|---|
| Origin | Compute-first (Spark) | Warehouse-first (SQL) |
| Storage | Open (Delta / Iceberg on your cloud) | Proprietary format on Snowflake-managed storage (Iceberg tables now available too) |
| Compute model | You pick VM types, tune cluster | Fully abstracted virtual warehouses |
| Concurrency scaling | Autoscaling clusters + multi-cluster SQL warehouses | Multi-cluster warehouses (auto), separate compute per workload |
| ML/AI | First-class (Mosaic AI, MLflow, Model Serving) | Snowpark ML growing, Cortex for LLMs |
| Streaming | Native (Structured Streaming, Auto Loader, DLT) | Streams + Tasks, less mature |
| dbt support | dbt-databricks | dbt-snowflake |
| Cost predictability | Harder — depends on cluster config | Easier — per-second credits |
| Open-format story | Strong (Delta, Iceberg via UniForm/UniForm-out) | Weaker historically, Iceberg externally now |
| Best fit | Heavy ETL, ML, mixed batch+stream, complex Python | BI, SQL-heavy analytics, warehouse workloads |

**Honest answer for interview:** "Both are strong. Snowflake wins on SQL-first BI simplicity and cost predictability. Databricks wins when you have heavy ETL, streaming, ML, or want to keep data in open formats. Convergence is real — Snowflake now supports Iceberg tables, Databricks doubled down on Iceberg via Tabular acquisition."

---

## 15. Interview-Ready Deep-Dive Topics (pick 3 to master)

Pick three of these and be able to **draw the architecture, quote the tradeoffs, and speak to a real project**:

1. **CDC ingestion end-to-end** — Kafka → Auto Loader → Bronze → Silver (with `apply_changes`) → Gold. When to use Debezium vs Fivetran.
2. **Slowly Changing Dimensions Type 2** — MERGE INTO pattern, effective_from/effective_to, DLT `apply_changes`.
3. **Cost optimization on Databricks** — Photon, Serverless, Predictive Optimization, cluster policies, storage layout.
4. **Migrating from Hive Metastore to Unity Catalog** — UCX tool, principal mapping, table upgrade, sunset plan.
5. **Real-time analytics pipeline** — Kafka → Structured Streaming → Delta → Materialized View → BI. Latency budget breakdown.
6. **ML pipeline** — Feature Store (now Feature Engineering in UC) → training → MLflow → Model Serving → monitoring.
7. **Governance model** — UC catalogs per environment, per-team schemas, service principals for jobs, RLS for PII.

---

## 16. Practical Prep — do these 3 things in the next 3 days

1. **Sign up for Databricks Community Edition** (free) → spin up a cluster, load a CSV, write a Delta table, run OPTIMIZE, do time travel. Actual button-clicks.
2. **Read the [Databricks Well-Architected Framework](https://www.databricks.com/discover/well-architected)** — one afternoon, becomes your architecture vocabulary.
3. **Watch 2–3 recent Databricks Data + AI Summit talks** on YouTube — DAIS 2024 highlights (UniForm, Predictive Optimization, LakeFlow). Recruiters love current references.

---

## 17. Quick-hit revision — 60 seconds of buzzwords

If someone asks "what do you know about Databricks?" for a quick screen — this is your 60-second dump:

> "Databricks is a Lakehouse platform on Spark + Photon over Delta Lake and Iceberg on cloud object storage. Governance is Unity Catalog with three-level namespacing, lineage, and Delta Sharing. Compute is a mix of all-purpose clusters, job clusters, and SQL Warehouses — with Serverless increasingly the default. Data engineering leans on Auto Loader for ingestion, DLT for declarative pipelines, and dbt-databricks for transformations. Optimizations that matter: Liquid Clustering, Deletion Vectors, Photon, Predictive Optimization. Big 2024–25 moves: Tabular acquisition, UniForm, LakeFlow, Genie, Mosaic AI, Databricks Assistant. Cost is DBUs plus underlying cloud VMs; serverless is more predictable."

That paragraph, said confidently and correctly, tells any interviewer you're current.

---

## Cheat sheet reference

- Docs: <https://docs.databricks.com>
- Well-Architected: <https://www.databricks.com/discover/well-architected>
- Delta protocol: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- Unity Catalog docs: <https://docs.databricks.com/en/data-governance/unity-catalog/index.html>
- dbt-databricks: <https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup>
- Databricks Runtime release notes: <https://docs.databricks.com/en/release-notes/runtime/index.html>
