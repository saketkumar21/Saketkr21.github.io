# Framing my New Relic work for a Databricks interview

**Ground rule:** don't lie. Say "Databricks-equivalent" or "the equivalent architecture on open Spark," not "we used Databricks at New Relic." If they check with a former colleague or Google the stack, "Snowflake → Iceberg / Spark Thrift / Nessie / Airflow" comes out — and you look sharper for framing it accurately.

The strong move is: **"I've architected the same lakehouse migration you'd do on Databricks, but on the fully open-source equivalent stack. Every architectural choice has a direct Databricks mapping. Here's how I'd translate it if we did this on Databricks tomorrow."**

That framing = confident, current, honest, and shows you actually understand the abstractions rather than just clicking Databricks buttons.

---

## 0. Databricks feature primer (what changed since 2020)

You last touched Databricks around 2020. The runtime skeleton (Spark, notebooks, jobs, DBFS, clusters) is the same, but the platform above it has been largely rebuilt. Quick orientation:

- **Governance: Hive Metastore → Unity Catalog (UC).** Old world: workspace-scoped Hive Metastore, two-level `schema.table`. New world: **Unity Catalog** is the account-level governance layer (2022+) with a three-level namespace `catalog.schema.table`, permissions, column/row-level security, cross-workspace metastore, lineage, and Delta Sharing. Every modern Databricks workspace assumes UC; Hive Metastore is legacy.
- **Table format: plain Delta → Delta + Iceberg (via UniForm) + Iceberg-native.** Old world: Delta Lake with `OPTIMIZE`/`ZORDER`. New world: **Delta UniForm** (2024) lets you write once as Delta and expose Iceberg metadata simultaneously for external engines. And after **Databricks acquired Tabular (2024)** — the company behind Apache Iceberg — Iceberg is now first-class alongside Delta.
- **SQL compute: interactive clusters for BI → SQL Warehouses.** Old world: spin up an all-purpose cluster and point a JDBC client at it. New world: **SQL Warehouses** (2022+) are managed, autoscaling SQL endpoints in three flavors: **Classic** (in your cloud account, slowest to start), **Pro** (adds Photon, geospatial, PII, predictive I/O), **Serverless** (Databricks-hosted, sub-10-second startup, best for spiky BI).
- **Execution engine: JVM Spark → Photon-accelerated Spark.** **Photon** (GA 2022) is a C++ vectorized query engine that replaces the Spark SQL execution path underneath. Roughly 2× the DBU rate but 3–8× faster on typical SQL workloads — usually net cheaper on Photon-friendly queries.
- **Pipeline authoring: hand-written notebooks/jobs → Delta Live Tables (DLT).** **DLT** (2022+) is a declarative pipeline framework: you write `@dlt.table` / `@dlt.view` Python or SQL, Databricks builds and manages the DAG, autoscales, handles retries, and enforces **expectations** (declarative data quality — the DLT-native analog of dbt tests). `dlt.apply_changes` handles SCD-1/SCD-2 CDC without writing MERGE.
- **File ingestion: custom Structured Streaming code → Auto Loader.** **Auto Loader** (`cloudFiles` source) is Databricks' incremental file ingestor for object storage. It tracks new files via notifications or directory listing, handles schema inference/evolution, and gives you exactly-once ingestion without writing a bookkeeping table yourself.
- **Layout maintenance: manual `OPTIMIZE`/`ZORDER` → Liquid Clustering + Predictive Optimization.** **Liquid Clustering** (2024) replaces static partition columns and ZORDER — you declare cluster keys, and Databricks reorganizes files incrementally without full rewrites. **Predictive Optimization** (2024) runs `OPTIMIZE` and `VACUUM` automatically on UC-managed tables based on observed access patterns, so you stop running your own compaction cron.
- **DELETE/UPDATE/MERGE performance: full-file rewrites → Deletion Vectors.** **Deletion Vectors** (2023) store deleted-row bitmaps as sidecar files instead of rewriting Parquet files on every DELETE/UPDATE. MERGE-heavy pipelines (like CDC) get large speedups.
- **Deployment: notebooks + Terraform → Databricks Asset Bundles (DABs).** **DABs** (2023+) are YAML-based Infrastructure-as-Code for Databricks — you declare jobs, DLT pipelines, notebooks, and their targets (dev/staging/prod) in `databricks.yml`, then `databricks bundle deploy` from CI. Replaces most Terraform-for-Databricks patterns for job/pipeline shape.
- **dbt: dbt-spark → dbt-databricks.** The **`dbt-databricks`** adapter (Databricks-maintained) is now the recommended way to run dbt on Databricks — better Unity Catalog integration, native Deletion Vector MERGE, uses Databricks Connect. `dbt-spark` still works but is community-maintained and lags on UC/DV features.

Skim once, then read the interview framing below with these terms in your peripheral vision.

---

## 1. The one-sentence positioning

> "At New Relic, I'm the founding data engineer on our Product Rating platform. I'm leading migration of a 440-model dbt pipeline off Snowflake onto an open Spark lakehouse — Iceberg + Spark Thrift + Nessie + Airflow — and I open-sourced the SQL-dialect bridge that made it feasible (`dbt-polyglot` on PyPI). Every decision maps 1:1 to Databricks, and I've delivered Databricks in production previously at Infosys on the Walmart engagement."

Notice the structure:
1. Current role + scope (impressive number: 440 models)
2. Architectural specifics (real OSS lakehouse — sophisticated)
3. Proof point that you shipped it (dbt-polyglot on PyPI — public artifact)
4. Databricks production experience explicitly cited (Infosys/Walmart — this is your actual Databricks-in-prod claim)

**Prepare that paragraph and rehearse it out loud. Say it in 45 seconds.**

---

## 2. Component-by-component mapping — memorize the table

| What I built at New Relic | Databricks equivalent | What I'd say in interview |
|---|---|---|
| Snowflake (source) | Snowflake (source) — same | "We were on Snowflake as the source-of-truth warehouse." |
| Apache Iceberg on S3 (target storage) | Delta Lake (or Iceberg via UniForm) | "Iceberg gave us multi-engine reads. On Databricks I'd use Delta natively with **UniForm** (Delta's 2024 feature that exposes Iceberg-compatible metadata alongside Delta, so external Iceberg readers work against the same table) for interop. And since the Tabular acquisition, Iceberg is now first-class on Databricks — you can create Iceberg-native tables under Unity Catalog." |
| Spark Thrift Server (compute) | Databricks SQL Warehouse or Databricks cluster | "Spark Thrift is the OSS analogue of a **Databricks SQL Warehouse** — a HiveServer2 JDBC endpoint on a persistent Spark cluster. On Databricks that's a **Pro** or **Serverless** SQL Warehouse (Pro = Photon-enabled endpoint in your cloud account; Serverless = Databricks-hosted, sub-10s cold start, ideal for spiky BI)." |
| dbt-spark adapter | dbt-databricks adapter | "We used `dbt-spark` because we're on open Spark. On Databricks I'd swap to **`dbt-databricks`** — the Databricks-maintained adapter, same materializations, better Unity Catalog integration, and it uses Deletion Vectors under the hood for faster MERGE incrementals." |
| Project Nessie catalog | Unity Catalog | "Nessie gave us Git-style branching for data — cheap catalog-only branches per PR. Databricks' analogue is **Unity Catalog** (the account-level governance layer with three-level `catalog.schema.table` namespacing, RLS/CLS, lineage, and Delta Sharing). UC doesn't have Nessie-style branching yet, but gives you cross-workspace metastore, fine-grained access control, and column-level lineage out of the box." |
| Airflow 3 orchestration | Databricks Workflows or LakeFlow (or Airflow, they integrate) | "Airflow 3 for orchestration. On Databricks I'd evaluate **Workflows** (Databricks-native job orchestrator, replaced the old Jobs UI) for the internal DAG and keep Airflow if we needed cross-system orchestration. **LakeFlow** — Databricks' newer end-to-end pipeline product announced in 2024 — is also targeting this space, unifying ingestion, transformation, and orchestration." |
| Manual OPTIMIZE / compaction | Predictive Optimization | "We ran our own compaction schedules on Iceberg. Databricks now offers **Predictive Optimization** (2024) that runs `OPTIMIZE` and `VACUUM` automatically on Unity Catalog managed tables based on observed access patterns — you stop running your own compaction cron." |
| Custom parity-validation framework + mismatch dashboard | DLT expectations + Databricks lineage + system.query.history | "We built a custom framework for model-parity validation and a mismatch-spike dashboard. On Databricks, **Delta Live Tables (DLT)** — the declarative pipeline framework where you write `@dlt.table` decorators and Databricks manages the DAG — supports **expectations** (declarative row-level assertions with quarantine/drop/fail modes). That covers the same ground, and Unity Catalog lineage handles the graph view." |
| `dbt-polyglot` for SQL dialect | The same package — Spark is first-class, works with Databricks Spark too | "`dbt-polyglot` targets Spark generally, so it works on Databricks SQL Warehouse unchanged. That's actually a feature — the transpilation isn't Databricks-specific, which means the same tool ports Snowflake dbt to Databricks." |
| CI/CD (Jenkins + GitHub) for dbt | Databricks Asset Bundles + GitHub Actions | "Jenkins + GitHub for dbt CI. On Databricks I'd use **Databricks Asset Bundles (DABs)** — YAML-based IaC (2023+) where you declare jobs, DLT pipelines, and notebooks in `databricks.yml`, then `databricks bundle deploy` per target from a GitHub Actions workflow." |

---

## 3. The four hooks — talk from these when you get open questions

### Hook 1: "Tell me about a hard migration."

**Answer arc:**
1. **Setup:** "440 dbt models on Snowflake. Business-critical rating pipeline. Target: open lakehouse. Zero downtime."
2. **The dialect chasm:** "Day one, Spark rejected `QUALIFY`. Then `IFF`, `NVL`, `::` casts, `DATEADD`, NULL ordering. Every model breaks differently."
3. **The choice I made:** "Three options — rewrite all models, maintain two versions, or transparent transpilation. I built the transpiler — `dbt-polyglot` on PyPI. Compile-time hook into dbt-core, wraps sqlglot, adds a Spark correctness fix-up layer."
4. **The safety net:** "Model-parity validation. For each model, row count + column checksums + row-level diff on a sampled key set. Mismatch dashboard breaks green → amber → red per model."
5. **Ready to talk Databricks equivalent:** "On Databricks, the same pattern works — dbt-polyglot targets Spark generically, so it runs on a Databricks SQL Warehouse unchanged. I'd swap dbt-spark for `dbt-databricks` and get better MERGE performance via **Deletion Vectors** (Databricks' 2023 feature that stores deleted rows as sidecar bitmaps instead of rewriting full Parquet files — huge win for MERGE-heavy incremental models)."

### Hook 2: "How do you think about cost?"

**Answer arc:**
1. **Concrete win:** "$45K/year Snowflake credit savings on the flagship pipeline alone via dbt refactoring + 50% faster models."
2. **The technique:** "Analyzed query profile → clustering keys wrong → refactored materialization + partition/clustering strategy. Snowflake `QUERY_HISTORY` view is the equivalent of Databricks' `system.query.history` (part of Databricks' system tables under Unity Catalog — queryable telemetry for jobs, queries, warehouses)."
3. **How I'd apply this to Databricks:** "On Databricks the equivalent moves are **Photon** (the C++ vectorized engine — 2× DBU rate, 3–8× faster on typical SQL workloads, so net cheaper), **Predictive Optimization** for auto-`OPTIMIZE` and auto-`VACUUM`, **Liquid Clustering** (2024 — declarative cluster keys that reorganize incrementally, replaces static partitioning + ZORDER and skips full file rewrites), and **Serverless SQL Warehouse** for spiky BI to avoid paying for idle capacity."

### Hook 3: "How do you handle streaming / CDC / real-time?"

**Answer arc:**
1. Current setup — Kafka + Debezium considered but this pipeline is batch (or brief on your streaming exposure).
2. Real hands-on: `lakehouse-lab` curriculum covers Structured Streaming, watermarks, checkpoints, poison-pill handling, backpressure.
3. On Databricks: **Auto Loader** (`cloudFiles` source — Databricks' incremental file ingestor for object storage; handles schema inference/evolution and exactly-once semantics without a manual bookkeeping table) for file ingestion, Structured Streaming to Delta with `foreachBatch` MERGE for exactly-once CDC, **DLT** for declarative streaming pipelines with `apply_changes` handling SCD-1/SCD-2 natively. Trigger modes: `availableNow` for backfill, `processingTime` for live.

### Hook 4: "Tell me about your open-source work."

**Answer arc:**
1. **`dbt-polyglot`** — the SQL transpiler. Explain the parse → fix-up → generate pipeline. Emphasize the trust model: verified or fails loud, never silently wrong.
2. **`lakehouse-lab`** — 58 modules across Spark performance, Iceberg, Kafka + Structured Streaming, Debezium CDC, dbt quality, Airflow. Break → Detect → Fix → Prove methodology.
3. **Why it matters for a Databricks role:** "The curriculum runs all the failure modes you'd hit on Databricks — skew, OOM, spill, MERGE contention, small files, orphaned data files, WAL/slot growth. Different runtime, same pathologies. I know how to spot them because I've reproduced them."

---

## 4. Answers to the traps you'll definitely hit

### Trap: "So you don't actually have production Databricks experience?"

**Truthful, strong answer:**
> "Production Databricks was at Infosys on the Walmart engagement — Spark-Scala and PySpark ETL on Databricks clusters. About 1.5–2 years. My current work at New Relic is on open Spark, but every architectural pattern I'm using — Iceberg storage, dbt transformations, MERGE-heavy incremental loads, structured streaming — is directly transferable, and I've been keeping current via my `dbt-polyglot` project which targets Databricks-Spark specifically, and my `lakehouse-lab` curriculum which reproduces Databricks-relevant failure modes. If you handed me a Databricks workspace tomorrow, my ramp time is measured in days, not months."

Say that confidently. It's true. It's exactly what a hiring manager wants to hear.

### Trap: "Why did New Relic not just use Databricks?"

**Suggested answer** (pick what's honest for your team):
> "Cost economics at our data volume, and a strategic preference for open formats — Iceberg + Nessie + Spark gives us full portability across engines. Databricks would be the natural choice if we needed the managed Photon runtime or MosaicAI capabilities. Both are valid; we optimized for open-format independence and control over compute."

Don't badmouth Databricks. Interviewer works there (metaphorically).

### Trap: "What's the difference between Delta and Iceberg?"

Answer clean and current:
> "Both are open ACID table formats on Parquet with metadata + snapshot semantics. Delta is Databricks-native, has richer Databricks features like **Liquid Clustering** (incremental data-layout reorganization on declared cluster keys, replacing ZORDER + partitioning) and **Deletion Vectors** (bitmap-based soft deletes that skip Parquet rewrites on DELETE/UPDATE/MERGE). Iceberg has stronger multi-engine support historically — Snowflake, Trino, BigQuery, Spark all read it. Databricks acquired **Tabular** — the company founded by Iceberg's original creators — in 2024, so Iceberg is now first-class on Databricks. **Delta UniForm** lets you write once as Delta and expose the same table as Iceberg (writes Iceberg metadata alongside Delta's) for external readers. In practice today, either format is a defensible choice on Databricks; I'd start with Delta for pure Databricks shops, Iceberg if I need multi-engine reads."

### Trap: "Show me code — how would you do CDC ingestion on Databricks?"

Have this pattern memorized:

```python
# Bronze: Auto Loader ingesting Debezium events from Kafka
(spark.readStream
   .format("kafka")
   .option("kafka.bootstrap.servers", "...")
   .option("subscribe", "orders_cdc")
   .load()
   .selectExpr("CAST(value AS STRING) as json_str", "timestamp")
   .writeStream
   .format("delta")
   .option("checkpointLocation", "/chk/bronze_orders")
   .toTable("prod.bronze.orders_cdc"))

# Silver: parse envelope, apply changes to a mirror table
from delta.tables import DeltaTable

def merge_batch(batch_df, batch_id):
    from pyspark.sql.functions import from_json, col
    schema = "op string, before struct<id long,...>, after struct<id long,...>, ts_ms long"
    parsed = batch_df.select(from_json(col("json_str"), schema).alias("d")).select("d.*")

    target = DeltaTable.forName(spark, "prod.silver.orders")
    (target.alias("t")
       .merge(parsed.alias("s"), "t.id = s.after.id")
       .whenMatchedDelete(condition="s.op = 'd'")
       .whenMatchedUpdate(condition="s.op = 'u'", set={"col": "s.after.col", ...})
       .whenNotMatchedInsert(condition="s.op = 'c'", values={"id": "s.after.id", ...})
       .execute())

(spark.readStream.table("prod.bronze.orders_cdc")
  .writeStream
  .foreachBatch(merge_batch)
  .option("checkpointLocation", "/chk/silver_orders")
  .start())
```

Walking through the Databricks-specific pieces:
- **`.toTable("prod.bronze.orders_cdc")`** — the three-part name is a **Unity Catalog** identifier: `catalog.schema.table`. This works because the workspace has UC enabled and `prod` is a UC catalog.
- **`format("delta")`** — the storage engine underneath is Delta Lake; on a modern UC workspace, unqualified managed tables are Delta by default. Swap for `iceberg` if you want Iceberg-native, or enable **UniForm** on the Delta table to publish Iceberg metadata alongside.
- **`DeltaTable.forName(spark, "prod.silver.orders")`** — the Delta transactional API, needed for programmatic MERGE. With **Deletion Vectors** enabled on the target (`TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')`), the `whenMatchedDelete`/`whenMatchedUpdate` branches write bitmap sidecars instead of rewriting Parquet files — big speedup on this MERGE-heavy pattern.
- **`foreachBatch`** — gives you exactly-once MERGE semantics: the checkpoint records `batch_id`, so on retry Delta idempotently applies the same batch.

Or with DLT `apply_changes`:

```python
import dlt

@dlt.view
def orders_changes():
    return spark.readStream.table("prod.bronze.orders_cdc")

dlt.create_streaming_table("silver_orders")
dlt.apply_changes(
    target="silver_orders",
    source="orders_changes",
    keys=["id"],
    sequence_by="ts_ms",
    apply_as_deletes="op = 'd'",
    stored_as_scd_type=1,   # or 2 for history
)
```

The DLT version is what a modern Databricks shop reaches for by default — the framework handles ordering (`sequence_by`), tombstones (`apply_as_deletes`), and SCD Type 1 vs Type 2 without hand-rolled MERGE. `dlt.create_streaming_table` + `apply_changes` is the declarative CDC pattern; you get retries, autoscaling, checkpointing, and expectations for free.

---

## 5. The interview open — practice this cold

You'll be asked "Walk me through your background" or "Tell me about yourself." **60 seconds max.**

> "I'm a Senior Data Engineer with 6+ years across Snowflake, BigQuery, and Databricks. Currently at New Relic, I'm the founding data engineer on the Product Rating platform, leading a Snowflake-to-open-lakehouse migration — Iceberg on S3, Spark, Nessie, Airflow. I open-sourced the SQL-dialect bridge on PyPI called `dbt-polyglot`, and separately maintain a 58-module Data Engineering production-challenges curriculum called `lakehouse-lab`. Before New Relic, I built the Fast Shipping Tags data product at Falabella on BigQuery — 4M+ SKUs, drove a 50% conversion lift. And before that, I worked on Databricks-based Spark-Scala ETL at Infosys for Walmart. I'm here because [company] is doing [specific thing you researched], and I want to bring the migration + tooling experience to a Databricks-native environment."

Rehearse. Say it flat, not sing-song.

---

## 6. Two hours before the interview — checklist

- [ ] Reread this file
- [ ] Reread the Databricks study guide
- [ ] Skim your resume + dbt-polyglot README
- [ ] Look up the company's most-recent Databricks-related blog post or job posting; note one specific thing to reference
- [ ] Have 2–3 questions ready for the interviewer:
  - "How do you use Unity Catalog — one metastore across environments or per-env?"
  - "Are you on Delta, Iceberg via UniForm, or Iceberg-native?"
  - "How do you split responsibility between DLT and Workflows?"
  - "Where does dbt fit — SQL Warehouse target or classic cluster?"
- [ ] Water on your desk, phone silenced, camera framed at eye level, clean background

---

## 7. Post-interview

Send a same-day thank-you note. Reference **one specific thing** the interviewer said (proves you were listening). Keep it 3 sentences.
