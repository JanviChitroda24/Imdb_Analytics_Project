# Bronze Layer — Complete Reference Notes
**IMDb Analytics Platform · Medallion Architecture**

---

## 1. What Is the Bronze Layer?

The Bronze layer is the first layer of the Medallion Architecture. Its only job is to land raw data exactly as it arrived — no cleaning, no type casting, no filtering. It is an immutable, permanent record of every row that entered the pipeline.

> **One-line definition:** Bronze = Raw data landed as-is, in Delta format, with audit columns. Nothing more, nothing less.

---

### Why Bronze Exists — Three Core Reasons

**1. Immutability (Source of Truth)**
Once your CSV is loaded into Bronze, it becomes your permanent record of what arrived. If Silver breaks, if Gold breaks, if you make a mistake three months later — you always have Bronze to reprocess from. You never touch the original source file again. Bronze is your insurance policy.

**2. Traceability (Audit Trail)**
Every row in Bronze has audit columns telling you when it arrived, which file it came from, and which process loaded it. Without this, if a bad row surfaces in Gold, you have no way to trace it back. With Bronze audit columns, you can answer: *"This row came from Cleaned_name_basics.csv, loaded by the bronze_ingestion_notebook on March 17, 2026 at 14:32."*

**3. Separation of Concerns**
Bronze = landing zone. Silver = cleaning zone. Gold = business zone. Each layer has exactly one job. If you mix loading and cleaning in the same step, debugging becomes a nightmare — did a row get dropped because the file was corrupt, or because your cleaning logic was wrong? With Bronze as a separate layer, you always know.

---

### Why Not Read CSVs Directly in Silver?

| If you skip Bronze | If you have Bronze |
|---|---|
| No audit trail — you don't know when data arrived | Full audit trail — every row has ingestion_timestamp |
| Reprocessing Silver means re-reading the CSV again | Reprocessing Silver reads from a stable Delta table |
| No transaction safety on the landing | ACID transactions — partial loads fail cleanly |
| No time travel — old data is gone after an update | Time travel — query any previous version |
| Harder to debug: is the issue in the file or the transform? | Easy to debug: Bronze is the clean separation point |

---

## 2. What Is a Delta Table?

A Delta table is a supercharged version of Parquet that adds a transaction log on top. A regular CSV or Parquet file is just a file — it has no memory, no history, no transaction awareness. A Delta table knows everything that has ever happened to it.

---

### Delta vs Parquet vs CSV

| Feature | CSV | Parquet | Delta Table |
|---|---|---|---|
| Schema enforcement | None | None | ✅ Enforced |
| ACID transactions | None | None | ✅ Full ACID |
| Concurrent write safety | ❌ Corrupts | ❌ Corrupts | ✅ Safe |
| Time travel | ❌ No | ❌ No | ✅ Any version |
| Partial write recovery | ❌ No | ❌ No | ✅ Auto-rollback |
| Columnar reads (fast) | ❌ No | ✅ Yes | ✅ Yes |
| Schema evolution | ❌ No | Manual | ✅ Built-in |

---

### What ACID Means Practically

| Letter | Meaning | What It Protects in Bronze |
|---|---|---|
| A — Atomicity | Write fully succeeds or fully fails | If bronze_title_principals crashes at row 50M, none of the rows land — no partial table |
| C — Consistency | Data always valid after transaction | Schema stays correct even if the notebook crashes mid-write |
| I — Isolation | Concurrent reads/writes don't interfere | Silver can query Bronze while you reload it — no dirty reads |
| D — Durability | Committed data survives failures | Once Bronze is written, it persists even if the cluster shuts down |

---

### Time Travel

Because Delta keeps a transaction log of every operation, you can query any previous version of a table.

```python
# Query current version (default)
spark.table("imdb_final_project.bronze_name_basics")

# Query Bronze as it looked on a specific date
spark.read.format("delta") \
    .option("timestampAsOf", "2026-03-16") \
    .table("imdb_final_project.bronze_name_basics")

# Query a specific version number
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table("imdb_final_project.bronze_name_basics")
```

If IMDb releases updated data next month and you reload Bronze, you can still query Bronze Version 0 to see exactly what was there before. This is your audit capability.

---

### What a Delta Table Physically Looks Like

A Delta table is just a folder on DBFS or ADLS. Inside it:

```
imdb_final_project/bronze_name_basics/
  ├── part-00000-xxxx.snappy.parquet   ← Actual data chunks
  ├── part-00001-xxxx.snappy.parquet
  ├── part-00002-xxxx.snappy.parquet
  └── _delta_log/                      ← Transaction log
        ├── 00000000000000000000.json   ← Version 0: initial write
        ├── 00000000000000000001.json   ← Version 1: next write
        └── _last_checkpoint
```

Every time you write to a Delta table, a new JSON entry is added to `_delta_log`. This log records what files were added, what files were removed, the schema, and the timestamp. That is how time travel works — by replaying or rewinding the log.

---

## 3. Bronze Notebook Design Decisions

Every decision in the Bronze notebook was made deliberately.

---

### Decision 1: All Columns Loaded as STRING (`inferSchema=False`)

IMDb delivers all data as TSV/CSV — every value is technically a string. The column `birthYear` looks like a number but arrives as `"1899"` or `"\N"`. If you let Spark infer the schema, it might cast `birthYear` to INT — and then `"\N"` either throws a casting error or silently becomes null, and you lose the original raw value forever.

By keeping everything as string in Bronze, you preserve the exact raw value that arrived. When Silver casts `birthYear` to INT and hits a `"\N"`, you can see that the source value was literally the two-character string backslash-N. That is a documented data quality finding, not a silent null.

| With `inferSchema=True` (wrong) | With `inferSchema=False` (correct) |
|---|---|
| Spark guesses birthYear is INT | birthYear stays as STRING — exact raw value preserved |
| `"\N"` silently becomes null — original value lost | `"\N"` stays as the literal string — visible and auditable |
| If IMDb adds a string to a numeric column, Bronze crashes | Bronze never crashes on unexpected values — Silver handles them |
| Type errors surface at Bronze load time, hardest to debug | Type errors surface at Silver cast time, easiest to handle |

---

### Decision 2: Three Audit Columns on Every Table

Every Bronze table gets exactly three audit columns added after the CSV is read. These do not come from the source — they are added by the pipeline.

| Column | Value | Why It Exists |
|---|---|---|
| `ingestion_timestamp` | `current_timestamp()` | Records exactly when this pipeline run executed. If you reload Bronze in April, you can distinguish March rows from April rows. |
| `source_file` | `"Cleaned_name_basics.csv"` | Records which file the row came from. Lets you trace any Gold row back to its exact origin file. |
| `loaded_by` | `"bronze_ingestion_notebook"` | Records which process loaded it. In production this becomes the ADF pipeline name or the user who triggered the run. |

```python
df = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("Cleaned_name_basics.csv")) \
    .withColumn("loaded_by", lit("bronze_ingestion_notebook"))
```

---

### Decision 3: Overwrite Mode (Not Append)

The Bronze notebook uses `mode("overwrite")`. This means each pipeline run completely replaces the previous Bronze load.

This is correct because the IMDb source files are full snapshots — they contain the complete dataset as of that download, not just the changes. There is no concept of "new rows only" in the source. So Bronze should mirror the source exactly, and a full overwrite achieves that.

> **When would you use append instead?** Append mode makes sense when the source delivers incremental changes — for example, a Kafka stream or a CDC (Change Data Capture) feed that only sends new or changed rows. For batch full-snapshot sources like IMDb, overwrite is correct.

---

### Decision 4: Row Count Validation After Every Write

After writing each Bronze Delta table, the notebook re-reads the table and compares its row count against the source CSV row count.

Delta can silently drop rows in certain edge cases — for example, if schema enforcement rejects rows that don't match an existing schema. Without an explicit count check, you would never know. The validation step makes failure visible immediately rather than letting bad data propagate to Silver and Gold.

```python
source_row_count = df.count()                                         # From CSV
bronze_row_count = spark.table("imdb_final_project.bronze_...").count()  # From Delta

if source_row_count != bronze_row_count:
    print(f"MISMATCH: expected {source_row_count}, got {bronze_row_count}")
```

---

## 4. Expected Bronze Row Counts

These are the actual row counts from the pipeline run. Use these as your validation baseline. Counts are lower than profiling numbers because Alteryx pre-cleaning removed rows with null primary keys before Bronze ingestion.

| Bronze Table | Profiling Count (Raw) | Bronze Count (Post-Alteryx) | Difference | Reason |
|---|---|---|---|---|
| bronze_name_basics | 15,122,182 | 14,909,809 | -212,373 | Rows with null nconst or primaryName='none' removed in Alteryx |
| bronze_title_akas | 55,294,765 | 54,180,283 | -1,114,482 | Rows with null titleId removed |
| bronze_title_basics | 12,323,467 | 12,101,466 | -221,001 | Rows with null tconst removed |
| bronze_title_crew | 12,325,801 | 12,105,185 | -220,616 | Rows with null tconst removed |
| bronze_title_episode | 9,514,619 | 9,329,138 | -185,481 | Rows with null tconst or parentTconst removed |
| bronze_title_principals | 98,073,271 | 96,206,765 | -1,866,506 | Rows with null tconst or nconst removed |
| bronze_title_ratings | 1,641,397 | 1,606,282 | -35,115 | Rows with null or empty tconst removed |

> Every dropped row has a documented reason. This is your data quality story for Bronze — you should be able to state exactly how many rows were dropped and why.

---

## 5. Important Questions — Bronze Layer

---

**Q1: Why do you have a Bronze layer at all? Why not just read the CSVs directly in Silver?**

Bronze is the immutable landing zone. Once data lands in Bronze as a Delta table, it becomes a permanent, ACID-safe record of what arrived. If Silver or Gold breaks, we reprocess from Bronze without re-downloading files. Bronze also adds audit columns — ingestion_timestamp, source_file, loaded_by — which give us full lineage on every row. Without Bronze, there is no clean separation between "what arrived" and "what we did to it," which makes debugging almost impossible.

---

**Q2: Why are all columns string type in Bronze?**

Bronze is a schema-on-read layer. Type casting is Silver's responsibility. If Spark infers schema in Bronze and casts `birthYear` to INT, then the raw value `"\N"` either throws a casting error or silently becomes null — and we lose visibility into what the source actually contained. By keeping everything as string, we preserve the exact raw value. When Silver hits a problem casting a value, we can trace it back to the raw string and document it as a data quality finding.

---

**Q3: Why Delta format instead of Parquet or CSV?**

Delta gives everything Parquet gives — columnar storage, compression, fast reads — plus ACID transactions, time travel, and schema enforcement. The key practical benefits for Bronze: partial writes either fully succeed or fully fail (no silent partial loads), and time travel lets us query any previous version of a Bronze table. Parquet has none of this. CSV has none of this.

---

**Q4: Your Bronze row counts are lower than your profiling counts. Why?**

The profiling was run on the raw IMDb TSV files — 204M rows total. Before Bronze ingestion, the team ran Alteryx pre-cleaning to remove rows with null primary keys, invalid FKs, and known bad values like `primaryName='none'`. So Bronze reflects the post-Alteryx state, not the raw state. Every dropped row is documented with a reason — for example, 212K rows were removed from name_basics because their nconst was null or primaryName was 'none', making them unusable for any downstream join.

---

**Q5: Why overwrite mode instead of append for Bronze?**

The IMDb source files are full snapshots — every download contains the complete dataset as of that date, not just the changes. So Bronze should mirror the source exactly, and a full overwrite achieves that. Append mode would be correct if the source were incremental — for example, a Kafka stream or a CDC feed that only delivers new or changed rows. For batch full-snapshot sources, overwrite is the right choice.

---

**Q6: What are audit columns and why do they matter?**

Audit columns are metadata columns added by the pipeline — not from the source data. Every Bronze table has three: `ingestion_timestamp` (when the load ran), `source_file` (which file the row came from), and `loaded_by` (which notebook or process loaded it). They matter because they give full traceability. If a bad row surfaces in Gold three months later, you can trace it back to the exact source file and pipeline run that produced it. Without audit columns, you are debugging blind.

---

**Q7: What is ACID and why does it matter here?**

ACID stands for Atomicity, Consistency, Isolation, Durability. For Bronze specifically: Atomicity means if the bronze_title_principals load crashes at row 50 million, none of the rows land — the table stays at its previous state, no partial write. Consistency means the schema stays correct even if the notebook crashes. Isolation means Silver can read Bronze while we reload it without seeing a half-written state. Durability means once Bronze is committed, it survives even if the cluster shuts down. Without ACID, a crashed Bronze load could silently produce a partial table that looks complete.

---

**Q8: What is time travel and when would you use it in this project?**

Time travel is Delta's ability to query any previous version of a table, either by version number or by timestamp. In this project, it means if IMDb releases updated data next month and we reload Bronze, we can still query Bronze Version 0 to see exactly what was there before. Practically, you would use it to audit: "Did this row exist in Bronze before the reload?" or "When did this value first appear?" It turns Bronze from a simple landing table into an auditable historical record.

---

## 6. Bronze vs Silver vs Gold — Quick Reference

| | Bronze | Silver | Gold |
|---|---|---|---|
| **Purpose** | Land raw data | Clean and standardize | Business-ready model |
| **Transformations** | None | Type casting, null handling, explode | Star schema, surrogate keys, SCD |
| **Data types** | All strings | Typed (INT, FLOAT, etc.) | Final production types |
| **Row count vs source** | Same as source (post-Alteryx) | May increase (explosion) or decrease (drops) | Subset — only valid modeled data |
| **Schema enforcement** | Minimal | Enforced at cast time | Fully enforced with FKs |
| **Surrogate keys** | None | None | Yes — decimal(10,0) |
| **Audit columns** | ingestion_timestamp, source_file, loaded_by | Same + transformation notes | created_date, modified_date |
| **SCD tracking** | None | None | Type-1, Type-2, Fixed |
| **Format** | Delta | Delta | Delta → loaded to Snowflake |

---

## 7. Delta Tables vs Parquet Files — Clearing the Confusion

---

### The DATABASE Variable — Just a Name, Not a Path

`DATABASE` in the Bronze notebook should only contain the database name, not a file path.

```python
DATABASE = "imdb_final_project"   # ✅ correct — just the name
```

The notebook uses it like this internally:

```python
spark.table(f"{DATABASE}.{bronze_table}")
# becomes → spark.table("imdb_final_project.bronze_name_basics")
```

That is a name reference, not a file path. Databricks knows where to physically store it based on the catalog configuration.

---

### Delta Tables ARE Parquet Files

A Delta table is not a replacement for Parquet. It is Parquet with a transaction log added on top. They are not two different things.

When the notebook runs this line:

```python
df.write.format("delta").saveAsTable("imdb_final_project.bronze_name_basics")
```

Three things get created physically on storage:

```
/Volumes/workspace/imdb_final_project/bronze_name_basics/
  ├── part-00000.snappy.parquet    ← your actual data
  ├── part-00001.snappy.parquet    ← your actual data (split across files)
  ├── part-00002.snappy.parquet    ← your actual data
  └── _delta_log/
        └── 00000000000000000000.json   ← the transaction log
```

- The **data** lives in Parquet files — same as always
- The **transaction log** (`_delta_log`) sits on top of those Parquet files
- Together, Parquet files + `_delta_log` = a **Delta table**

---

### What Is a "Table" Then?

When Databricks says `bronze_name_basics` is a table, it means:

```
The name "bronze_name_basics" in the catalog
    points to
the physical folder on storage
    which contains
Parquet files + _delta_log
```

The table name is just a pointer. The real data is still Parquet files on disk.

---

### So Are We Working With Parquet Files or Tables?

Both. They are the same thing looked at from two angles.

| Angle | What You See | What It Actually Is |
|---|---|---|
| From Databricks catalog | A table called `bronze_name_basics` | A name that points to a storage folder |
| From storage (DBFS/Volumes) | A folder with `.parquet` files + `_delta_log` | The actual physical data |
| From code | `spark.table("imdb_final_project.bronze_name_basics")` | Spark reads the Parquet files via the Delta log |

---

### Why Register It as a Table Instead of Just Leaving It as Parquet Files?

When you register it as a table in the catalog, you get:

- Query it by name instead of a full file path
- See it in the Databricks UI under your schema
- Other notebooks can reference it by name without knowing the storage path
- Governance and permissions apply at the table level

```python
# Without table registration — messy, path-dependent
spark.read.format("delta").load("/Volumes/workspace/imdb_final_project/bronze_name_basics/")

# With table registration — clean, portable
spark.table("imdb_final_project.bronze_name_basics")
```

---

### One-Sentence Summary

> A Delta table is Parquet files on disk + a `_delta_log` folder on top, registered in the Databricks catalog under a name so you can query it cleanly.

Nothing in Bronze changes the data. You are taking your CSV, writing it as Parquet files with a Delta log, and giving it a table name. That is all Bronze does.

## 8. Overwrite, Time Travel & Incremental Updates

---

### Can You Time Travel After Overwrite?

Yes. Overwrite does not delete history in Delta.

When you run `mode("overwrite")`, Delta does not actually delete the old Parquet files immediately. It just writes new Parquet files and adds a new entry to `_delta_log` that says "version 1 replaces version 0". The old files stay on disk until you explicitly run `VACUUM`.

Here is what physically happens:

```
After first run (Version 0):
_delta_log/
  └── 00000000000000000000.json   ← "these parquet files = the table"
part-00000-aaa.snappy.parquet     ← your data
part-00001-aaa.snappy.parquet

After second run with overwrite (Version 1):
_delta_log/
  └── 00000000000000000000.json   ← Version 0 entry — still there
  └── 00000000000000000001.json   ← "these NEW parquet files = the table"
part-00000-aaa.snappy.parquet     ← old data — still physically on disk
part-00001-aaa.snappy.parquet     ← old data — still physically on disk
part-00000-bbb.snappy.parquet     ← new data
part-00001-bbb.snappy.parquet     ← new data
```

So time travel still works after overwrite:

```python
# Read current version (new data)
spark.table("imdb_final_project.bronze_name_basics")

# Read version 0 (original load) — old files still exist on disk
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table("imdb_final_project.bronze_name_basics")
```

---

### What About `overwriteSchema=True`?

Schema overwrite works the same way. The old schema is recorded in the old `_delta_log` entry. The new schema is recorded in the new entry. Time traveling to version 0 brings back both the old data AND the old schema.

```
Version 0 log entry contains:
  - list of parquet files for that version
  - schema at that point in time

Version 1 log entry contains:
  - list of NEW parquet files
  - NEW schema
```

So even if IMDb added a new column tomorrow and you reloaded Bronze with `overwriteSchema=True`, you could still query the old version with the old schema intact.

---

### When Do Old Files Actually Get Deleted?

Only when you explicitly run `VACUUM`:

```sql
-- Deletes all files older than 7 days (default retention)
VACUUM imdb_final_project.bronze_name_basics

-- Deletes all files older than 0 hours — removes ALL history
-- ⚠️ This permanently kills time travel
VACUUM imdb_final_project.bronze_name_basics RETAIN 0 HOURS
```

Until you run VACUUM, every version is queryable. Delta retains 30 days of history by default.

---

### Can We Do Incremental Updates Instead of Full Overwrite?

Technically yes, but for IMDb specifically it does not make sense.

Incremental loading requires the source to tell you what is new. There are two ways sources do this:

**Way 1 — Timestamp column:** Source has an `updated_at` column. You only load rows where `updated_at > last_load_time`. IMDb TSV files have no timestamp column — every row looks identical whether it was added yesterday or 5 years ago.

**Way 2 — CDC (Change Data Capture):** Source sends only changed rows as a stream (Kafka, Kinesis, etc.). IMDb does not do this — they publish full file snapshots. Every download is the complete dataset.

Since IMDb gives you a full snapshot every time, there is no way to identify which rows are "new" without comparing the entire new file against the entire old table — which is more expensive than just overwriting.

---

### When Is Each Approach the Right Choice?

| Source Type | Example | Right Approach |
|---|---|---|
| Full snapshot files | IMDb TSV downloads | Full overwrite ✅ |
| Append-only logs | Server access logs, Kafka events | Append mode |
| CDC stream | Database change feed, Debezium | Merge / upsert |
| API with pagination | REST API with `since` parameter | Incremental append |

---

### What Incremental Bronze Would Look Like (If Source Supported It)

Just for understanding — this is what incremental Bronze would look like if IMDb sent incremental data:

```python
# Append only — adds new rows, never touches existing ones
df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{DATABASE}.{bronze_table}")
```

Or for upsert (update existing rows + insert new ones):

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, f"{DATABASE}.{bronze_table}")

delta_table.alias("existing") \
    .merge(df.alias("new"), "existing.tconst = new.tconst") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

For IMDb, full overwrite is the correct and simpler choice because the source is a full snapshot.

---

### One-Sentence Summaries

**Time travel after overwrite:** Overwrite adds a new version to the Delta log but keeps old Parquet files on disk until VACUUM runs — so every previous version remains queryable.

**Incremental updates:** Not applicable to IMDb because the source publishes full snapshots with no way to identify new rows — full overwrite is the right pattern for this type of source.