# Databricks — Senior Data Engineer Study Guide (Back-to-Databricks Edition)

Comprehensive prep for a Senior DE role where Databricks is the primary platform. **Audience:** You know Spark deeply, know SQL, know Python, know cloud storage, know ETL patterns — but you last touched Databricks around 2020. Nearly everything below marked "NEW SINCE ~2022" is a feature that did not exist (or was not usable in production) when you last logged in.

**Recommended prep time:** 10–14 hours of focused study over 4–6 days.

---

## 0. What Changed Since 2020 (read this first)

If you froze your Databricks knowledge in 2020 and thawed it in 2026, these are the changes you must internalize. Everything else in this doc is expansion of the below.

| Area | 2020 (what you remember) | 2026 (what's true now) |
|---|---|---|
| **Governance** | Hive Metastore per workspace, table ACLs bolted on | **Unity Catalog** — account-level metastore, cross-workspace, lineage, column masks, row filters |
| **Namespace** | Two-level: `database.table` | **Three-level: `catalog.schema.table`** |
| **File access** | `dbutils.fs.mount()` + `dbfs:/mnt/...` | **UC External Locations + Volumes** — `/Volumes/catalog/schema/vol/` |
| **Table layout** | Partitioning + `ZORDER BY` | **Liquid Clustering** (`CLUSTER BY`) — no rewrites when keys change |
| **MERGE performance** | Rewrite whole files on any change | **Deletion Vectors** — soft-delete without file rewrite |
| **Table maintenance** | You cron `OPTIMIZE` and `VACUUM` yourself | **Predictive Optimization** — Databricks does it for you |
| **Pipelines** | Handwritten notebooks in Jobs | **Delta Live Tables (DLT)** — declarative pipelines with quality expectations |
| **Ingestion** | `spark.readStream` + custom file listing | **Auto Loader (`cloudFiles`)** — schema evolution, notification mode, rescue column |
| **IaC / deploy** | Notebooks pushed via `dbx` CLI, manually wired jobs | **Databricks Asset Bundles (DABs)** — YAML for jobs/DLT/notebooks |
| **BI compute** | Interactive cluster running SQL, slow startup | **SQL Warehouses** — Classic, Pro, Serverless; Serverless starts in ~10 sec |
| **Query engine** | JVM Spark | **Photon** — C++ vectorized engine, 2× DBU rate but 3–8× faster |
| **Open formats** | Delta (fairly new) | **Delta + Iceberg first-class**; Databricks acquired Tabular in 2024; **UniForm** writes once as Delta, reads as Iceberg |
| **Streaming** | Structured Streaming | Structured Streaming + DLT streaming tables; **`trigger(availableNow)`** replaces `trigger(once)` |
| **CDC** | Handwritten `MERGE INTO` | **Change Data Feed (CDF)** on Delta + DLT `apply_changes` |
| **AI** | MLflow open source | **Mosaic AI** — Model Serving, Vector Search, Feature Engineering, Foundation Model APIs, **Databricks Assistant**, **Genie** (text-to-SQL) |
| **Orchestration** | Databricks Jobs (single task) or Airflow | **Workflows** (multi-task DAG) + **LakeFlow** (new, Airflow-competitor, integrated ingest/transform) |
| **BI product** | Third-party BI on top of Spark SQL | **AI/BI Dashboards** (native, successor to Databricks SQL dashboards) |
| **Runtime** | DBR 7.x (Spark 3.0) | **DBR 15.x / 16.x** — Spark 4.0 under the hood in 16+ |

If someone asks in interview "what's changed since you last used Databricks," you point at this table. Being explicit about the gap is more credible than pretending it doesn't exist.

---

## 1. The Platform Mental Model (must know cold)

**What this is:** Databricks positions itself as **the Lakehouse Platform** — one unified system for data engineering, data warehousing, ML, and BI on top of open table formats stored in your own cloud object storage.

The "Lakehouse" pitch: get warehouse-grade semantics (ACID, versioning, governance, BI performance) on top of data-lake economics (open formats, cheap object storage, decoupled compute).

**Three architectural pillars:**

1. **Compute** — Spark-based (with Photon), running on cloud VMs. Multiple flavors: All-purpose clusters, Job clusters, SQL Warehouses (Classic/Pro/Serverless), Serverless Jobs, DLT compute.
2. **Storage** — Cloud object storage (S3 / ADLS Gen2 / GCS). Tables written as **Delta Lake** (default) or **Apache Iceberg** (via UniForm, or natively since 2024).
3. **Governance** — **Unity Catalog** provides catalog / schema / table / volume namespace, ACLs, lineage, and cross-workspace sharing.

**NEW SINCE ~2022:** In 2020 you had a Hive Metastore per workspace and roughly one storage account per workspace. In 2026 the metastore is account-wide, storage credentials are UC-managed, and one metastore serves many workspaces in a region.

**Multi-cloud:** Runs on AWS, Azure, GCP with near-parity. Cloud-specific bindings: IAM roles / instance profiles on AWS, managed identities / service principals on Azure, service accounts on GCP.

---

## 2. Unity Catalog (UC) — the #1 governance topic

**What this is (NEW SINCE 2022 — GA):** Unity Catalog is the single account-wide governance layer over data, ML models, notebooks, dashboards, and files. It replaces the per-workspace Hive Metastore and the ad-hoc table ACL system you may have used in 2020. If an interviewer asks one question about "modern Databricks," it's about UC.

**Three-level namespace:** `catalog.schema.table`. The old two-level `schema.table` is legacy (Hive Metastore) and workspaces created after ~2023 are UC-first.

- **Metastore** — top-level container per region, one per Databricks account. Multiple workspaces can share it.
- **Catalog** — logical grouping (e.g. `prod`, `dev`, `sandbox_teamA`).
- **Schema** (aka database) — inside a catalog. Same concept you knew before.
- **Table** / **View** / **Materialized View** / **Volume** / **Function** / **Model** — leaf objects, all first-class securable objects in UC.
- **Volume** — first-class object for **unstructured / semi-structured files** (CSVs, PDFs, images, model artifacts). Replaces DBFS mounts for governed access.

**Old vs New — path examples:**

| Purpose | Old (2020) | New (2026) |
|---|---|---|
| Read a mounted bucket | `dbutils.fs.ls("dbfs:/mnt/raw/")` | `dbutils.fs.ls("/Volumes/prod/raw/landing/")` |
| Register a table | `CREATE TABLE db.table USING DELTA LOCATION '...'` | `CREATE TABLE prod.silver.orders USING DELTA` (managed) or `LOCATION 'abfss://...'` (external) with an **External Location** UC object |
| Cross-workspace read | Copy the mount config, hope it works | Both workspaces attach to same UC metastore, just `SELECT` |

**Key UC features you should be able to explain:**

- **Managed vs External tables.** Managed = UC owns storage lifecycle (drops the data on `DROP TABLE`); External = you own the storage path, UC owns metadata + ACL only. New guidance: **default to managed** unless you have a specific reason for external.
- **External Locations + Storage Credentials.** The UC objects that replace mounts. A Storage Credential holds the IAM role / managed identity; an External Location binds it to a storage URI (`abfss://.../` or `s3://bucket/prefix/`). Grants control who can read/write.
- **Volumes.** Two types: **managed volumes** (UC-owned path) and **external volumes** (bring your own path). Access via `/Volumes/catalog/schema/volume/` in the workspace, `SELECT` in SQL, and cloud-native SDKs.
- **Data lineage.** Automatic, column-level, across notebooks / jobs / DLT / dashboards. Shows up in the UI and via `system.access` tables.
- **Access control.** `GRANT SELECT ON TABLE prod.silver.orders TO group_analysts`. Row-level filters, column masks, and **ABAC** (attribute-based access control) using tags.
- **Row filters and column masks.** Row filter = a SQL function that returns a boolean per row; attach with `ALTER TABLE ... SET ROW FILTER`. Column mask = SQL function per column returning the (possibly masked) value; attach with `ALTER TABLE ... ALTER COLUMN ... SET MASK`.
- **Cross-workspace sharing** via metastore attach — same metastore = same catalog visibility (with grants).
- **Delta Sharing** — open protocol for cross-org sharing. Data recipients don't need a Databricks account; open-source Delta Sharing clients exist for Python, Spark, Pandas, Power BI.
- **System tables** (NEW): `system.access.audit`, `system.billing.usage`, `system.query.history`, `system.information_schema.*`, `system.compute.*`. Use these to inspect billing, audits, and query cost with plain SQL.
- **Service Principals** — first-class UC principals for automation. Jobs should run as SPs, not humans.

**Old way vs new way — Hive Metastore migration:**

- Old: Hive Metastore per workspace, table ACLs bolted on (or none).
- New: One UC metastore per region, tables migrated. Use the **UCX** tool (open-source Databricks Labs project) — it inventories your HMS, maps principals, and upgrades tables. The `SYNC` SQL command copies HMS tables into UC.

**Interview probe examples:**
- "How would you migrate from Hive Metastore to Unity Catalog?" → **UCX** tool, principal mappings, table-by-table upgrade with `SYNC TABLE prod.silver.orders FROM hive_metastore.silver.orders`, drop the HMS registration after cutover.
- "How do you implement row-level security?" → Create a SQL function returning boolean, `ALTER TABLE t SET ROW FILTER my_filter ON (region)`. Alternative: dynamic views with `IS_ACCOUNT_GROUP_MEMBER('region_us')`.
- "How do you protect PII columns?" → Column mask function that returns `***` unless the user is in an approved group. Attach via `ALTER TABLE ... ALTER COLUMN ssn SET MASK mask_ssn`.

---

## 3. Delta Lake — must be strong here

**What this is:** Open-source ACID transaction layer on top of Parquet files. A "Delta table" is just a folder of Parquet files plus a `_delta_log/` directory containing JSON transaction records. The transaction log makes Parquet look like a real database with ACID guarantees, versioning, schema evolution, and MERGE.

You almost certainly know Delta from 2020 — but the following bullet features are all newer.

**Must-know operations (mix of old and new):**

| Operation | What it does | When to use |
|---|---|---|
| `OPTIMIZE table` | Compacts small files into larger (~1 GB target) | After incremental writes |
| `OPTIMIZE table ZORDER BY (col)` | Multi-dim clustering | Legacy; **prefer Liquid Clustering** |
| `VACUUM table RETAIN N HOURS` | Deletes files no longer referenced (default retention 7 days) | Storage cost hygiene |
| `MERGE INTO` | Upsert / delete / SCD-2 | CDC ingestion |
| `DELETE / UPDATE` | Row-level operations | ACID mutations |
| `TIME TRAVEL` — `VERSION AS OF n` / `TIMESTAMP AS OF t` | Read historical snapshot | Audits, incident recovery, ML reproducibility |
| `CHANGE DATA FEED` (CDF) — NEW | Read row-level changes between versions | Downstream incremental consumption without a manual watermark |
| `RESTORE table VERSION AS OF n` | Rollback a table to an earlier version | Recover from bad write |
| `CLONE` (SHALLOW / DEEP) | Copy metadata (shallow) or metadata + data (deep) | Dev/prod snapshots, backups |

**Advanced concepts (mostly NEW SINCE ~2022) to be fluent on:**

- **Liquid Clustering (NEW, 2024 GA)** — replaces partitioning + Z-Order for new tables. `CLUSTER BY (col1, col2)` at create-time or `ALTER TABLE ... CLUSTER BY (col)` after. Doesn't require rewriting when clustering keys change. Databricks-only (not fully OSS Delta yet). **Old way:** you'd `PARTITION BY (event_date)` and then `OPTIMIZE ... ZORDER BY (customer_id)` and cross your fingers about partition skew. **New way:** `CLUSTER BY (event_date, customer_id)` and Databricks handles data layout.
- **Deletion Vectors (NEW, 2023)** — soft-delete rows without rewriting Parquet files. Massive MERGE and DELETE speedup. Enable with `TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')` or via cluster/table default. Reads apply the deletion vector at query time; a later `OPTIMIZE` physically removes them.
- **Delta UniForm (NEW, 2024 GA)** — write once as Delta, read as Iceberg. Enables cross-engine reads (Snowflake, BigQuery, Trino, Athena can read a Delta table as if it were Iceberg). Set `TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')`.
- **Iceberg first-class (NEW, 2024)** — after acquiring Tabular (Iceberg's founding company) in June 2024, Databricks supports Iceberg natively for read AND write in UC. You can create an Iceberg-native table in UC; Databricks manages metadata and manifest files.
- **Column Mapping (NEW)** — rename/drop columns without file rewrite. Requires `delta.columnMapping.mode='name'`. Necessary for column masks.
- **Predictive Optimization (NEW, 2024 GA)** — Databricks-managed auto-`OPTIMIZE` and auto-`VACUUM`. Cost model is per-table; Databricks decides when it's worth it. Turn on per catalog: `ALTER CATALOG prod ENABLE PREDICTIVE OPTIMIZATION`. **Old way:** you'd run a maintenance notebook every night. **New way:** flip a switch.
- **Change Data Feed (CDF, NEW)** — enable with `TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`. Then query `SELECT * FROM table_changes('t', startVersion, endVersion)` to get inserted/updated/deleted rows. Powers CDC-style downstream consumption without you tracking watermarks.
- **Schema evolution** — `mergeSchema = true` on write appends new columns; `overwriteSchema = true` replaces schema. Same as 2020, but interacts with column mapping now.
- **Constraints** — `NOT NULL`, `CHECK (col > 0)`. Enforced on write. Same as 2020.
- **Generated columns** — `col1 GENERATED ALWAYS AS (expression)`. Useful for partition/cluster columns without user maintenance.
- **Identity columns** — `id BIGINT GENERATED ALWAYS AS IDENTITY` — auto-incrementing surrogate keys.

**Delta transaction log — protocol level:**
- Every write = new JSON commit file in `_delta_log/` (e.g. `00000000000000000001.json`).
- Every 10 commits = checkpoint parquet (`00000000000000000010.checkpoint.parquet`) so readers don't have to replay from zero.
- Multi-version concurrency control (MVCC) via optimistic concurrency + isolation levels (`WriteSerializable` is the default; `Serializable` also supported).
- Conflict detection is at file granularity — two writes touching disjoint files commit fine.

**Interview probes:**
- "Why is MERGE slow, and how do you speed it up?" → Enable **Deletion Vectors**, cluster on merge keys (Liquid), narrow the source with predicates, use `WHEN MATCHED AND ...` conditions to reduce work, ensure the target is not over-partitioned.
- "Delta vs Iceberg?" → Both open, both ACID. Delta is Databricks-native; Iceberg has stronger multi-engine support historically. **UniForm** lets you write Delta and read as Iceberg. **Since June 2024 Tabular acquisition, Iceberg is first-class in Databricks too** — you can pick Iceberg as the physical format.
- "How would you implement CDC into a silver table?" → Bronze has raw with CDF enabled → silver runs a streaming `MERGE` or DLT `apply_changes` reading from `table_changes('bronze', ...)`.

---

## 4. Compute Types — critical to explain when to use what

**What this is:** In 2020 you basically had one thing: "clusters" (interactive or job). In 2026 there are several compute product SKUs, each with different pricing, startup, and workload fit.

| Compute | Use case | Pricing model | Startup time | New since 2020? |
|---|---|---|---|---|
| **All-purpose cluster** | Interactive notebooks, dev/debug | DBUs while running (any state) | 3–5 min | No (existed) |
| **Job cluster** | Scheduled workflows (jobs), one job per cluster | DBUs while running, terminates on job finish | 3–5 min | No (existed) |
| **SQL Warehouse — Classic** | BI/SQL workloads, always-warm queries | Per-cluster-hour DBUs | 3–5 min | Yes (SQL product formalized ~2021) |
| **SQL Warehouse — Pro** | Above + Photon, Predictive I/O | Photon-priced DBUs | 3–5 min | **Yes (NEW)** |
| **SQL Warehouse — Serverless** | Same as Pro, but Databricks-managed compute (no VM startup in your account) | Per-second serverless DBUs, higher rate | **5–15 seconds** | **Yes (NEW, 2022+)** |
| **Serverless Jobs** | Databricks-managed compute for scheduled jobs | Per-second | Instant-ish | **Yes (NEW, 2023+ GA)** |
| **DLT Compute (Serverless)** | Managed compute for DLT pipelines | Per-second | Instant-ish | **Yes (NEW)** |

**When to pick which (this WILL be asked):**
- Scheduled ETL → **Job cluster** (or **Serverless Jobs** if latency-sensitive or spiky)
- Ad-hoc analysis → **All-purpose**
- BI dashboards / concurrent SQL → **SQL Warehouse Serverless** (default choice in 2026)
- DLT pipelines → **DLT compute (Serverless recommended)**
- Long-running ML training → **All-purpose or Job cluster with ML runtime**

**Cluster configuration knobs to know (mostly familiar from 2020, some new):**
- **Databricks Runtime (DBR) version** — LTS vs standard. DBR 15.4 LTS and DBR 16.x are the modern choices; Spark 4.0 landed in DBR 16.
- **Photon on/off** — 2× DBU rate but often 3–8× faster (NEW)
- **Worker type / driver type** — match workload (memory-optimized for shuffle-heavy, compute-optimized for CPU-heavy)
- **Autoscaling min/max**
- **Instance pools** — pre-warmed VMs across clusters (existed but got better)
- **Cluster policies** — admin-enforced constraints (limits on max workers, forbidden instance types, forced tags)
- **Spot / preemptible instances** for cost savings
- **Access mode** — `Single User`, `Shared` (multi-user), `No Isolation Shared` (legacy). Shared mode is the UC-compatible one for multi-user (NEW). If you only remember one thing: **`Shared` access mode was constrained early on** (no Scala, no some DBUtils features) — that gap has largely closed.
- **Init scripts** — deprecated at the workspace / DBFS level; use cluster-scoped or Volumes-hosted init scripts (NEW guidance).

---

## 5. Photon — vectorized query engine

**What this is (NEW SINCE ~2020 preview, GA later):** A drop-in Spark query engine written in C++ (SIMD, vectorized columnar execution) that runs the same Spark SQL / DataFrame API but much faster. You didn't have Photon in 2020 in any real workload; it's now the default for SQL Warehouses.

- **2× the DBU rate but usually 3–8× faster on typical workloads** → net cheaper AND faster on wall-clock.
- Supports **SQL and DataFrame API**; some rare functions fall back to non-Photon Spark for unsupported ops (older versions had more gaps; DBR 15+ is broadly covered).
- Enabled per-cluster (checkbox: "Use Photon acceleration") or automatic on SQL Warehouse Pro/Serverless.
- **Native code, not JVM.** No Python UDF acceleration — UDFs run in Python worker, not Photon. **Arrow-optimized Python UDFs** and **Pandas UDFs** get partial acceleration.

**Interview probe:** "When would you NOT use Photon?" → When your workload is entirely Python UDF-heavy (Photon can't accelerate custom Python), for tiny workloads where DBU rate matters more than latency, or on the Community Edition where it isn't available.

---

## 6. Auto Loader & Ingestion — must be strong

**What this is (NEW SINCE ~2020):** **Auto Loader** is the `cloudFiles` format in Structured Streaming that incrementally ingests new files from cloud object storage. It replaces the "list a folder, filter by last-modified time, hope for the best" pattern from 2020.

```python
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/Volumes/prod/raw/checkpoints/schema")
   .option("cloudFiles.inferColumnTypes", "true")
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
   .load("/Volumes/prod/raw/landing/")
   .writeStream
   .option("checkpointLocation", "/Volumes/prod/raw/checkpoints/checkpoint")
   .trigger(availableNow=True)
   .toTable("prod.bronze.events"))
```

**Key features:**
- **File notification vs directory listing modes** — notification uses cloud events (**SNS/SQS/EventBridge** on AWS, **Event Grid + Queue Storage** on Azure, **Pub/Sub** on GCP) for O(1) discovery; directory listing scans the folder each trigger (works up to millions of files but slower).
- **Schema inference & evolution** — infers types from JSON/CSV/AVRO/Parquet, evolves as new fields appear. Evolution modes: `addNewColumns`, `rescue`, `failOnNewColumns`, `none`.
- **Rescued data column** — captures malformed/unexpected rows in `_rescued_data` JSON column so you don't lose them.
- **Backfill mode** — `cloudFiles.backfillInterval` re-scans in case notifications lag or events are lost.

**vs COPY INTO:** `COPY INTO` is idempotent, batch, SQL-only, tracks loaded files per-target. Auto Loader is streaming, more scalable, supports arbitrary triggers and schema evolution. **Guidance:** COPY INTO for small/simple, Auto Loader for anything real.

**vs 2020's "list a folder" pattern:** Old way was `dbutils.fs.ls(...)` + custom watermark tracking. New way is Auto Loader with a checkpoint — Spark tracks processed files for you.

**Interview probe:** "Ingest 10K files/hour into Bronze — Auto Loader or COPY INTO?" → Auto Loader with file notification mode; run with `trigger(availableNow=True)` in a scheduled job or continuous streaming, depending on latency budget.

---

## 7. Delta Live Tables (DLT) — declarative pipelines

**What this is (NEW SINCE 2022):** A declarative framework where you define target tables + data quality expectations, and DLT figures out the DAG, retries, backfill, and monitoring. Think of it as "dbt-shaped but streaming-native and Databricks-only." **You did not have this in 2020.** Before DLT, you wired jobs by hand.

```python
import dlt
from pyspark.sql.functions import *

@dlt.table(
    comment="Cleaned events",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_ts", "event_ts IS NOT NULL")
@dlt.expect_or_fail("known_action", "action IN ('click', 'view', 'purchase')")
def silver_events():
    return dlt.read_stream("bronze_events").filter("action != 'debug'")
```

**Key concepts:**
- **Streaming tables** — incremental, one row-per-microbatch semantics. Uses Structured Streaming under the hood.
- **Materialized views** — recomputed on refresh, best for aggregations. UC-native, first-class SQL objects.
- **Expectations** — data quality rules. Three severities:
  - `@dlt.expect(name, condition)` — warn, keep the row
  - `@dlt.expect_or_drop(name, condition)` — drop the offending row
  - `@dlt.expect_or_fail(name, condition)` — fail the pipeline
- **Live dependencies** — `dlt.read` / `dlt.read_stream` create the DAG automatically. No hand-wiring.
- **Enhanced autoscaling** — scales workers based on backlog, not just CPU.
- **Change Data Capture** — `dlt.apply_changes` for SCD-1/SCD-2 from CDC feeds. Handles Debezium-style change events natively.
- **Development vs Production mode** — dev retries fast, keeps cluster warm; prod is robust, retries with backoff.
- **DLT Serverless (NEW, GA)** — no cluster management; per-second billing.

**Interview probe:** "Why DLT over regular jobs?" → Declarative DAG, built-in quality gates via expectations, automatic backfill, unified batch + streaming, less orchestration code, native lineage.

**Honest caveat:** DLT lock-in. Migrating out is painful because the framework generates the DAG and manages the compute lifecycle. If your team values portability, you might stick to plain notebook jobs + dbt.

---

## 8. Databricks Workflows (Jobs) — orchestration

**What this is:** Multi-task orchestration native to Databricks. In 2020 "Jobs" meant one task per job. In 2026 Workflows are full multi-task DAGs.

- **Multi-task workflows with dependencies (DAG)** — NEW compared to 2020's single-task jobs.
- Task types: Notebook, JAR, Python script, **dbt task**, SQL task, DLT pipeline, another job (nested), Spark submit, **Python wheel**, **for-each** (NEW).
- Triggers: schedule (cron), **file arrival** (NEW), **table update** (NEW), continuous, manual.
- Parameters, task values (mini-XComs), conditional tasks (`if/else`), **loop tasks** (NEW).
- Retry policies, timeouts, notifications (email / Slack / webhook).
- **Serverless jobs compute** now supported (NEW).
- Jobs run as a **service principal** for automation (best practice).

**vs Airflow:** Workflows is Databricks-native, tightly integrated with Unity Catalog and lineage. Airflow is more portable and has a richer ecosystem. Many teams use **Airflow for cross-system orchestration, Workflows for Databricks-internal**.

**Databricks LakeFlow (NEW, announced 2024)** — Databricks' answer to Airflow, integrated **LakeFlow Connect** (managed ingestion connectors, e.g. Salesforce, Workday, SQL Server CDC), **LakeFlow Pipelines** (successor language to DLT), and **LakeFlow Jobs** (orchestration). Watch as this evolves — mid-2025 many things renamed as "LakeFlow."

---

## 9. dbt on Databricks — specifically

**What this is:** Same dbt you know, running against a Databricks SQL Warehouse or cluster. Adapter maturity has improved a lot since 2020.

**Adapter: `dbt-databricks`** (recommended, maintained by Databricks). Older `dbt-spark` still works but lacks UC integration and modern feature support.

Key config:

```yaml
# profiles.yml
databricks:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace-host>
      http_path: /sql/1.0/warehouses/<id>  # SQL Warehouse endpoint
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      catalog: prod                          # UC three-level namespace
      schema: analytics
      threads: 8
```

**Materializations available:**
- `view`, `table`, `incremental` (standard)
- `materialized_view` — Databricks-native, backed by DLT-style refresh (NEW)
- `streaming_table` — via streaming source + `+incremental_strategy: append` (NEW)

**Incremental strategies:**
- `merge` — default, uses `MERGE INTO`
- `append` — pure insert
- `insert_overwrite` — partition overwrite
- `replace_where` — dynamic partition overwrite via WHERE predicate

**Best practices:**
- Use **SQL Warehouse Serverless** as compute target (fast startup, no cluster idle cost).
- Use **Liquid Clustering** on incremental models: `config(liquid_clustered_by=['event_date', 'account_id'])`.
- Enable **Deletion Vectors** for MERGE-heavy tables.
- Use **dbt-databricks Unity Catalog integration** — three-level references via `catalog` + `schema` config.
- **Photon** is on by default for SQL Warehouse Pro/Serverless — dbt sensitive to per-query latency benefits.

**Interview probe:** "You have 1000 dbt models on Snowflake, migrating to Databricks — what breaks?" → SQL dialect differences (Snowflake-only functions: `LATERAL FLATTEN`, `VARIANT`, `IFF`, `::` cast shorthand, `QUALIFY` behavior), null ordering (Snowflake NULLS LAST default), incremental strategy differences, time zone handling, `PIVOT` syntax variance.

---

## 10. Structured Streaming — must know for senior

**What this is:** Same Structured Streaming API you knew in 2020, but with some new triggers and better Delta integration.

**Triggers:**
- `.trigger(processingTime='1 minute')` — micro-batch on interval
- `.trigger(availableNow=True)` — **NEW** — process all available, then stop (great for backfill and scheduled batch-of-stream)
- `.trigger(continuous='1 second')` — experimental sub-second
- `.trigger(once=True)` — **deprecated, use `availableNow`**

**Output modes:**
- `append` — new rows only (works with almost all queries)
- `complete` — full result table (aggregations only)
- `update` — updated rows only (aggregations)

**State management:**
- **Watermarks** — `withWatermark('event_ts', '10 minutes')` — drops late data + bounds state
- Stateful ops: `groupBy` aggregations, joins, `mapGroupsWithState`, `flatMapGroupsWithState`, **stream-stream joins** (with watermark constraint)
- **Checkpoint location** — must be durable, unique per query. Prefer Volumes over DBFS.
- **RocksDB state store (NEW default in recent DBRs)** — much better for large stateful ops than the old HDFS-backed state store.

**Kafka source/sink is standard.** Confluent Cloud and MSK integrate via SASL/PLAIN or IAM.

**Idempotency:** Use `foreachBatch` with `MERGE` for exactly-once writes to non-Delta sinks. Structured Streaming to Delta gives exactly-once natively via the `_delta_log` transaction log.

**Interview probe:** "Backfill a streaming pipeline?" → `trigger(availableNow=True)` on the source to process historical files, then switch to `processingTime` for live. Same checkpoint location, so exactly-once is preserved.

---

## 11. Databricks Asset Bundles (DABs) — IaC

**What this is (NEW SINCE 2023):** YAML-defined bundles that ship notebooks, jobs, DLT, workflows, and configuration together. **Replaces `dbx`** (the CLI you may have used in 2020–22). If you're going to be responsible for CI/CD, this is the modern answer.

```yaml
# databricks.yml
bundle:
  name: my-project

variables:
  catalog:
    description: UC catalog to deploy to
    default: dev

resources:
  jobs:
    my-etl:
      name: My ETL Job
      tasks:
        - task_key: process
          notebook_task:
            notebook_path: ./src/etl.py
          job_cluster_key: main
      job_clusters:
        - job_cluster_key: main
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: i3.xlarge
            num_workers: 4

targets:
  dev:
    workspace:
      host: https://dev.cloud.databricks.com
    variables:
      catalog: dev
  prod:
    workspace:
      host: https://prod.cloud.databricks.com
    variables:
      catalog: prod
```

Commands:
```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t prod my-etl
```

**Alongside DABs:**
- **Terraform provider** for infra (workspaces, metastores, secrets, external locations, permissions)
- **Databricks CLI v2** (rewrite from the old Python CLI; installed as a single Go binary)
- **REST API** (versioned; v2.1 for Jobs, v2.0 for many others)

**Interview probe:** "How do you version-control notebooks and jobs?" → **Databricks Repos** (Git integration inside the workspace) + **DABs** for job/DLT/pipeline definitions + **Terraform** for workspace-level infra + PR-based deployment via GitHub Actions or similar.

---

## 12. Cost Model — DBUs

**What this is:** DBU = Databricks Unit — a normalized compute unit. Same concept as 2020 but the SKU matrix is larger.

**DBU rate varies by:**
- **Cluster type** — all-purpose > job > SQL Warehouse Serverless > SQL Warehouse Classic per DBU/hour
- **Photon on/off** — ~2× rate with Photon (NEW)
- **Runtime type** — ML DBR = higher rate; Serverless has its own rate that includes infra
- **Compute type** — serverless has a per-second rate that includes the underlying VMs + Databricks fee

**Plus underlying cloud VM cost** (EC2 / VM / GCE) — separate line item on your cloud bill for non-serverless products. For **Serverless SKUs, VM cost is bundled into the DBU rate**.

**Cost-optimization moves:**
- **Job clusters** over all-purpose for scheduled work
- **Serverless** for spiky BI workloads (SQL Warehouse Serverless)
- **Photon** for query-heavy workloads (usually cheaper at wall-clock cost)
- **Spot instances** for fault-tolerant workloads (checkpoint-heavy streaming can survive spot loss)
- **Cluster policies** to prevent oversizing by developers
- **Predictive Optimization** — auto-OPTIMIZE and auto-VACUUM (NEW, saves query time)
- **Autoscaling min/max** tuned to actual load
- **Small file compaction** to keep OPTIMIZE cheap
- **`system.billing.usage`** (NEW) — inspect actual DBU spend by workload, user, job, warehouse — with plain SQL

**Interview probe:** "Your Snowflake bill was $X. Estimate Databricks equivalent." → It depends on workload profile. Analytical/BI is roughly parity; heavy ETL/ML is usually cheaper on Databricks with Photon; **but Databricks cost varies more with cluster config, so a bad config can 2× your bill without warning** — that's why cluster policies + system.billing observability matter.

---

## 13. What's New in 2024–2026 (recruiter loves this stuff)

Explicit list of features that are recent enough to signal you're current. Every one of these is **new since 2020**.

- **Databricks acquired Tabular (June 2024)** — Iceberg's founding company. Iceberg is now first-class alongside Delta in UC.
- **Delta UniForm** (GA) — write Delta, read as Iceberg. Cross-engine compatibility.
- **Liquid Clustering** GA — replaces Z-Order + partitioning for new tables.
- **Predictive Optimization** (GA 2024) — auto-OPTIMIZE and auto-VACUUM.
- **Deletion Vectors** GA — MERGE and DELETE performance.
- **Materialized Views + Streaming Tables** as first-class citizens in SQL (not only DLT).
- **Databricks Assistant** — AI code helper in notebooks (autocomplete, explain, fix).
- **Genie** — text-to-SQL for business users; conversational analytics over UC tables.
- **LakeFlow** (announced 2024) — Databricks' orchestration + managed ingestion (Connect) + declarative pipelines (successor to DLT branding).
- **AI/BI Dashboards** — new BI product (successor to Databricks SQL dashboards), with **AI/BI Genie** for chat-with-your-data.
- **Foundation Model APIs** — Databricks-hosted LLM serving (Meta Llama, Mistral, etc.), pay-per-token or provisioned throughput.
- **Vector Search** — vector DB integrated with UC tables; auto-syncs from a Delta source table.
- **Databricks Marketplace** — data + AI models marketplace via Delta Sharing.
- **Serverless Job compute** GA.
- **Runtime 15.x and 16.x** — Spark 4.0.x under the hood in DBR 16+.
- **UC Volumes** — GA replacement for DBFS mounts.
- **System Tables** — `system.access.audit`, `system.billing.usage`, `system.query.history`, `system.compute.*`.
- **Column masks and row filters** as first-class UC objects.
- **Serverless Notebooks** — attach any notebook to serverless compute.
- **Mosaic AI umbrella** — the ML/AI product family: MLflow, Model Serving, Vector Search, AI Gateway, Feature Engineering, Foundation Model APIs.
- **Model Serving** — real-time model endpoints with autoscaling, deployed from MLflow models.

---

## 14. Databricks vs Snowflake — you WILL be asked

| Dimension | Databricks | Snowflake |
|---|---|---|
| Origin | Compute-first (Spark) | Warehouse-first (SQL) |
| Storage | Open (Delta / Iceberg on your cloud) | Proprietary format on Snowflake-managed storage (Iceberg tables now available too) |
| Compute model | You pick VM types, tune cluster (or Serverless) | Fully abstracted virtual warehouses |
| Concurrency scaling | Autoscaling clusters + multi-cluster SQL warehouses | Multi-cluster warehouses (auto), separate compute per workload |
| ML/AI | First-class (Mosaic AI, MLflow, Model Serving, Vector Search) | Snowpark ML growing, Cortex for LLMs |
| Streaming | Native (Structured Streaming, Auto Loader, DLT) | Streams + Tasks, less mature |
| dbt support | dbt-databricks | dbt-snowflake |
| Cost predictability | Harder — depends on cluster config; Serverless closes gap | Easier — per-second credits |
| Open-format story | Strong (Delta, Iceberg via UniForm and native Iceberg after Tabular) | Weaker historically, Iceberg externally now |
| Governance | Unity Catalog | Snowflake native RBAC + Horizon (2024 governance product) |
| Best fit | Heavy ETL, ML, mixed batch+stream, complex Python | BI, SQL-heavy analytics, warehouse workloads |

**Honest answer for interview:** "Both are strong. Snowflake wins on SQL-first BI simplicity and cost predictability. Databricks wins when you have heavy ETL, streaming, ML, or want data in open formats. Convergence is real — Snowflake now supports Iceberg tables, Databricks doubled down on Iceberg via Tabular acquisition, both have LLM/AI stories now."

---

## 15. Interview-Ready Deep-Dive Topics (pick 3 to master)

Pick three of these and be able to **draw the architecture, quote the tradeoffs, and speak to a real project**:

1. **CDC ingestion end-to-end** — Kafka (or Debezium/Fivetran) → Auto Loader → Bronze → Silver (with DLT `apply_changes` or MERGE) → Gold. When to use Debezium vs Fivetran vs LakeFlow Connect.
2. **Slowly Changing Dimensions Type 2** — MERGE INTO pattern with effective_from/effective_to, or DLT `apply_changes` with `SEQUENCE BY`.
3. **Cost optimization on Databricks** — Photon, Serverless, Predictive Optimization, cluster policies, storage layout, `system.billing.usage` monitoring.
4. **Migrating from Hive Metastore to Unity Catalog** — UCX tool, principal mapping, table upgrade with SYNC, sunset plan, permissions delta.
5. **Real-time analytics pipeline** — Kafka → Structured Streaming → Delta with CDF → Materialized View → AI/BI dashboard. Latency budget breakdown.
6. **ML pipeline** — Feature Engineering in UC → training → MLflow model registry (now UC-hosted) → Model Serving → monitoring with Lakehouse Monitoring.
7. **Governance model** — UC catalogs per environment, per-team schemas, service principals for jobs, row filters and column masks for PII, tag-based ABAC.

---

## 16. Practical Prep — do these 3 things in the next 3 days

1. **Sign up for Databricks Community Edition or a free trial** → spin up a cluster, load a CSV, write a Delta table (three-level UC name), run `OPTIMIZE`, do time travel, enable Deletion Vectors, enable CDF, query `system.query.history`. Actual button-clicks matter — muscle memory has drifted since 2020.
2. **Read the [Databricks Well-Architected Framework](https://www.databricks.com/discover/well-architected)** — one afternoon, becomes your architecture vocabulary. Covers UC design, cost, security, reliability.
3. **Watch 2–3 recent Databricks Data + AI Summit talks** on YouTube — DAIS 2024 and DAIS 2025 highlights (UniForm, Predictive Optimization, LakeFlow, Genie, Mosaic AI). Recruiters love current references.

Bonus if you have an extra day: **read the [Delta Lake protocol spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)** cover to cover. Very few candidates do this; it separates senior from staff.

---

## 17. Quick-hit revision — 60 seconds of buzzwords

If someone asks "what do you know about Databricks?" for a quick screen — this is your 60-second dump:

> "Databricks is a Lakehouse platform on Spark + Photon over Delta Lake and Iceberg on cloud object storage. Governance is Unity Catalog with three-level namespacing — `catalog.schema.table` — plus lineage, row filters, column masks, and Delta Sharing. Compute is a mix of all-purpose clusters, job clusters, and SQL Warehouses, with Serverless increasingly the default. Data engineering leans on Auto Loader for ingestion, DLT for declarative pipelines, and dbt-databricks for transformations. Optimizations that matter: Liquid Clustering, Deletion Vectors, Photon, Predictive Optimization. Big 2024–25 moves: Tabular acquisition (Iceberg first-class), UniForm, LakeFlow, Genie, Mosaic AI, Databricks Assistant. Cost is DBUs plus underlying cloud VMs; Serverless bundles them. IaC is Databricks Asset Bundles plus Terraform, replacing dbx."

That paragraph, said confidently and correctly, tells any interviewer you're current — even if your last real hands-on was five years ago.

---

## Cheat sheet reference

- Docs: <https://docs.databricks.com>
- Well-Architected: <https://www.databricks.com/discover/well-architected>
- Delta protocol: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- Unity Catalog docs: <https://docs.databricks.com/en/data-governance/unity-catalog/index.html>
- Delta Lake docs: <https://docs.delta.io/latest/index.html>
- dbt-databricks: <https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup>
- Databricks Runtime release notes: <https://docs.databricks.com/en/release-notes/runtime/index.html>
- UCX (HMS → UC migration): <https://github.com/databrickslabs/ucx>
- Databricks Asset Bundles: <https://docs.databricks.com/en/dev-tools/bundles/index.html>
- LakeFlow: <https://www.databricks.com/product/data-engineering/lakeflow>
