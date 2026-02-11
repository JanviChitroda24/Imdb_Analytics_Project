# Databricks Volumes & Snowflake Load — Quick Reference

---

## What is a Volume?

A **Volume** is a folder in the cloud that Databricks can read and write files to — like a shared USB drive that both Databricks and your local machine can access.

---

## Three Ways Databricks Stores Things

### 1. Tables (what we already have)
- Delta tables like `gold_dim_genre`, `gold_dim_title`
- Structured data managed by Spark
- You can query them with `spark.table()` but you **cannot download them as files**

### 2. DBFS (the old way)
- Databricks' internal file system
- Path looks like: `dbfs:/tmp/my_files/`
- Many workspaces (including Free Edition) have **DBFS disabled**
- That's why we got the `DBFS_DISABLED` error

### 3. Volumes (the new way — what we use)
- Unity Catalog's file storage
- Path looks like: `/Volumes/workspace/imdb_final_project/gold_parquet/`
- Behaves like a regular folder — write files in, download files out
- This is the recommended approach going forward

---

## How Volumes Fit Into Our Pipeline

```
Gold Delta Tables (structured, queryable, not downloadable)
        │
        │  spark.table() → df.write.parquet()
        ▼
Volume: gold_parquet (files, downloadable)
        │
        │  Download to your Mac
        ▼
Your Mac: ~/Downloads/gold_export/
        │
        │  SnowSQL PUT command
        ▼
Snowflake Internal Stage (temporary holding area)
        │
        │  COPY INTO
        ▼
Snowflake Tables (final destination)
```

The Volume is the middleman — it converts Delta tables into downloadable Parquet files. Once data is in Snowflake, you don't need the Volume anymore.

---

## Why We Can't Load Directly from Databricks to Snowflake

Databricks Free Edition uses **Serverless Compute**, which only supports DML (write) operations to a limited set of data sources: Delta, Parquet, CSV, JSON, etc. **Snowflake is not on that list for writes.** Reads from Snowflake work fine, but writes are blocked.

On a paid Databricks workspace (with All-Purpose Compute clusters), you can write directly to Snowflake using the Spark-Snowflake connector:

```python
df.write.format("snowflake").options(**sf_options).save()
```

Since we're on Free Edition, we take the alternative path: export as Parquet files → upload to Snowflake stage → COPY INTO.

---

## Volume Commands Reference

```python
# Create a volume
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.imdb_final_project.gold_parquet")

# List volumes in a schema
spark.sql("SHOW VOLUMES IN workspace.imdb_final_project").show()

# Write Parquet files to a volume
df.write.mode("overwrite").parquet("/Volumes/workspace/imdb_final_project/gold_parquet/dim_genre")

# List files in a volume (using Python os module)
import os
files = os.listdir("/Volumes/workspace/imdb_final_project/gold_parquet/dim_genre")

# Drop a volume (deletes all files inside)
spark.sql("DROP VOLUME IF EXISTS workspace.imdb_final_project.gold_parquet")
```

---

## Snowflake Internal Stage — What Is It?

A **Stage** in Snowflake is a temporary holding area for files before they're loaded into tables. Think of it as Snowflake's inbox — you upload files to the stage, then tell Snowflake to copy the data from the stage into the actual tables.

```sql
-- Create a stage
CREATE OR REPLACE STAGE GOLD.IMDB_STAGE FILE_FORMAT = (TYPE = 'PARQUET');

-- Upload files to stage (from Mac terminal via SnowSQL)
-- PUT file://~/Downloads/gold_export/dim_genre/*.parquet @IMDB_STAGE/dim_genre;

-- Load from stage into table
COPY INTO GOLD.DIM_GENRE
FROM (SELECT $1:GenreKey::NUMBER(10,0), $1:GenreName::VARCHAR FROM @IMDB_STAGE/dim_genre/)
FILE_FORMAT = (TYPE = 'PARQUET');

-- List files in stage
LIST @IMDB_STAGE;

-- Clean up stage after load
REMOVE @IMDB_STAGE;
```

---

## Complete Execution Flow

| Step | Where | What |
|------|-------|------|
| 1 | Snowflake UI | Run DDL — create database, schema, 14 empty tables |
| 2 | Databricks (browser) | Run export notebook — writes Parquet to Volume |
| 3 | Your Mac (browser) | Download Parquet files from Databricks Catalog UI |
| 4 | Your Mac (terminal) | SnowSQL PUT — upload Parquet to Snowflake stage |
| 5 | Snowflake UI | Run COPY INTO — load from stage into tables |
| 6 | Snowflake UI | Apply clustering keys (ALTER TABLE CLUSTER BY) |
| 7 | Your Mac (terminal) | Run validation script — verify 57 checks |

---

## Interview Angle

> "In production, I'd use the Spark-Snowflake connector to write directly from Databricks to Snowflake in a single step. However, since I was working with Databricks Free Edition which restricts DML on serverless compute to internal formats only, I exported Gold tables as Parquet files to a Unity Catalog Volume, then loaded them into Snowflake using PUT + COPY INTO. The end result is identical — the same 14 tables with the same row counts — just a different transport mechanism. In a production environment with All-Purpose Compute, the direct connector approach would be the right choice."

---

*IMDb Analytics Platform — Databricks Volumes & Snowflake Load Notes — Janvi Chitroda*