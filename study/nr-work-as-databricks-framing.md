# Framing my New Relic work for a Databricks interview

**Ground rule:** don't lie. Say "Databricks-equivalent" or "the equivalent architecture on open Spark," not "we used Databricks at New Relic." If they check with a former colleague or Google the stack, "Snowflake → Iceberg / Spark Thrift / Nessie / Airflow" comes out — and you look sharper for framing it accurately.

The strong move is: **"I've architected the same lakehouse migration you'd do on Databricks, but on the fully open-source equivalent stack. Every architectural choice has a direct Databricks mapping. Here's how I'd translate it if we did this on Databricks tomorrow."**

That framing = confident, current, honest, and shows you actually understand the abstractions rather than just clicking Databricks buttons.

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
| Apache Iceberg on S3 (target storage) | Delta Lake (or Iceberg via UniForm) | "Iceberg gave us multi-engine reads. On Databricks I'd use Delta natively with UniForm to expose Iceberg for interop. Since the Tabular acquisition, Iceberg is also first-class on Databricks." |
| Spark Thrift Server (compute) | Databricks SQL Warehouse or Databricks cluster | "Spark Thrift is the OSS analogue of a Databricks SQL Warehouse — HiveServer2 JDBC endpoint on a persistent Spark cluster. On Databricks that's a Pro or Serverless SQL Warehouse." |
| dbt-spark adapter | dbt-databricks adapter | "We used `dbt-spark` because we're on open Spark. On Databricks I'd swap to `dbt-databricks` — same materializations, better UC integration, and Deletion Vectors for MERGE speed." |
| Project Nessie catalog | Unity Catalog | "Nessie gave us Git-style branching for data — cheap catalog-only branches per PR. Databricks' analogue is Unity Catalog, which doesn't have branching yet but gives you cross-workspace metastore, RLS, lineage, and Delta Sharing." |
| Airflow 3 orchestration | Databricks Workflows or LakeFlow (or Airflow, they integrate) | "Airflow 3 for orchestration. On Databricks I'd evaluate Workflows for the internal DAG and keep Airflow if we needed cross-system orchestration. The new LakeFlow product is also targeting this space." |
| Manual OPTIMIZE / compaction | Predictive Optimization | "We ran our own compaction schedules on Iceberg. Databricks now offers Predictive Optimization that runs OPTIMIZE and VACUUM automatically." |
| Custom parity-validation framework + mismatch dashboard | DLT expectations + Databricks lineage + system.query.history | "We built a custom framework for model-parity validation and a mismatch-spike dashboard. On Databricks, DLT expectations cover the same ground declaratively, and Unity Catalog lineage handles the graph." |
| `dbt-polyglot` for SQL dialect | The same package — Spark is first-class, works with Databricks Spark too | "`dbt-polyglot` targets Spark generally, so it works on Databricks SQL Warehouse unchanged. That's actually a feature — the transpilation isn't Databricks-specific, which means the same tool ports Snowflake dbt to Databricks." |
| CI/CD (Jenkins + GitHub) for dbt | Databricks Asset Bundles + GitHub Actions | "Jenkins + GitHub for dbt CI. On Databricks I'd use Databricks Asset Bundles for job/pipeline definitions with a GitHub Actions workflow to deploy." |

---

## 3. The four hooks — talk from these when you get open questions

### Hook 1: "Tell me about a hard migration."

**Answer arc:**
1. **Setup:** "440 dbt models on Snowflake. Business-critical rating pipeline. Target: open lakehouse. Zero downtime."
2. **The dialect chasm:** "Day one, Spark rejected `QUALIFY`. Then `IFF`, `NVL`, `::` casts, `DATEADD`, NULL ordering. Every model breaks differently."
3. **The choice I made:** "Three options — rewrite all models, maintain two versions, or transparent transpilation. I built the transpiler — `dbt-polyglot` on PyPI. Compile-time hook into dbt-core, wraps sqlglot, adds a Spark correctness fix-up layer."
4. **The safety net:** "Model-parity validation. For each model, row count + column checksums + row-level diff on a sampled key set. Mismatch dashboard breaks green → amber → red per model."
5. **Ready to talk Databricks equivalent:** "On Databricks, the same pattern works — dbt-polyglot targets Spark generically, so it runs on a Databricks SQL Warehouse unchanged. I'd swap dbt-spark for dbt-databricks and get better MERGE performance via Deletion Vectors."

### Hook 2: "How do you think about cost?"

**Answer arc:**
1. **Concrete win:** "$45K/year Snowflake credit savings on the flagship pipeline alone via dbt refactoring + 50% faster models."
2. **The technique:** "Analyzed query profile → clustering keys wrong → refactored materialization + partition/clustering strategy. Snowflake QUERY_HISTORY view is the equivalent of Databricks' system.query.history."
3. **How I'd apply this to Databricks:** "On Databricks the equivalent moves are Photon (2× DBU rate, 3–8× faster on typical workloads → net cheaper), Predictive Optimization for auto-OPTIMIZE and auto-VACUUM, Liquid Clustering to skip file rewrites, and Serverless SQL Warehouse for spiky BI to avoid paying for idle capacity."

### Hook 3: "How do you handle streaming / CDC / real-time?"

**Answer arc:**
1. Current setup — Kafka + Debezium considered but this pipeline is batch (or brief on your streaming exposure).
2. Real hands-on: `lakehouse-lab` curriculum covers Structured Streaming, watermarks, checkpoints, poison-pill handling, backpressure.
3. On Databricks: Auto Loader (`cloudFiles` source) for file ingestion, Structured Streaming to Delta with `foreachBatch` MERGE for exactly-once CDC, DLT for declarative streaming pipelines. Trigger modes: `availableNow` for backfill, `processingTime` for live.

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
> "Both are open ACID table formats on Parquet with metadata + snapshot semantics. Delta is Databricks-native, has richer Databricks features like Liquid Clustering and Deletion Vectors. Iceberg has stronger multi-engine support historically — Snowflake, Trino, BigQuery, Spark all read it. Databricks acquired Tabular in 2024, so Iceberg is now first-class on Databricks. Delta UniForm lets you write once as Delta and expose the same table as Iceberg for external readers. In practice today, either format is a defensible choice on Databricks; I'd start with Delta for pure Databricks shops, Iceberg if I need multi-engine reads."

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
