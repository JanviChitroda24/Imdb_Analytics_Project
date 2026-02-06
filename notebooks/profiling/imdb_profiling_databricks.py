# Databricks notebook source
# MAGIC %md
# MAGIC # IMDb Data Profiling — Databricks Notebook
# MAGIC
# MAGIC **Purpose:** Profile all 7 IMDb raw datasets before building the Medallion pipeline.
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Per-dataset profiling stats (row counts, nulls, cardinality, multi-value analysis)
# MAGIC - Profiling summary table for Bronze layer validation
# MAGIC
# MAGIC **Run this BEFORE building Bronze layer.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Upload Files First
# MAGIC
# MAGIC Upload your 7 TSV files to DBFS or a Unity Catalog volume:
# MAGIC ```
# MAGIC /FileStore/imdb_raw/name.basics.tsv
# MAGIC /FileStore/imdb_raw/title.basics.tsv
# MAGIC /FileStore/imdb_raw/title.akas.tsv
# MAGIC /FileStore/imdb_raw/title.crew.tsv
# MAGIC /FileStore/imdb_raw/title.episode.tsv
# MAGIC /FileStore/imdb_raw/title.principals.tsv
# MAGIC /FileStore/imdb_raw/title.ratings.tsv
# MAGIC ```
# MAGIC
# MAGIC Or update the `RAW_PATH` below to match your location.

# COMMAND ----------

# Configuration
RAW_PATH = "/FileStore/imdb_raw"  # UPDATE THIS to your path

# If using Unity Catalog volumes instead:
# RAW_PATH = "/Volumes/your_catalog/your_schema/imdb_raw"

# Dataset definitions
DATASETS = {
    "name_basics":      {"file": "name.basics.tsv",      "pk": "nconst",  "mv_cols": ["primaryProfession", "knownForTitles"]},
    "title_basics":     {"file": "title.basics.tsv",      "pk": "tconst",  "mv_cols": ["genres"]},
    "title_akas":       {"file": "title.akas.tsv",        "pk": None,      "mv_cols": [], "composite_pk": ["titleId", "ordering"]},
    "title_crew":       {"file": "title.crew.tsv",        "pk": "tconst",  "mv_cols": ["directors", "writers"]},
    "title_episode":    {"file": "title.episode.tsv",     "pk": "tconst",  "mv_cols": []},
    "title_principals": {"file": "title.principals.tsv",  "pk": None,      "mv_cols": [], "composite_pk": ["tconst", "ordering"]},
    "title_ratings":    {"file": "title.ratings.tsv",     "pk": "tconst",  "mv_cols": []},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

def load_imdb_tsv(spark, dataset_name, config):
    """Load an IMDb TSV file into a Spark DataFrame."""
    filepath = f"{RAW_PATH}/{config['file']}"
    
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .option("quote", "")          # IMDb TSVs have no quoting
        .option("nullValue", "\\N")   # IMDb uses \N for nulls
        .csv(filepath)
    )
    
    print(f"✅ {dataset_name}: {df.count():,} rows × {len(df.columns)} columns")
    return df


def profile_columns(df, dataset_name):
    """Generate per-column profiling stats."""
    total_rows = df.count()
    results = []
    
    for col_name in df.columns:
        col = F.col(col_name)
        
        stats = df.select(
            F.lit(dataset_name).alias("dataset"),
            F.lit(col_name).alias("column_name"),
            F.lit(total_rows).alias("total_rows"),
            
            # Null analysis
            F.sum(F.when(col.isNull(), 1).otherwise(0)).alias("null_count"),
            F.round(
                (F.sum(F.when(col.isNull(), 1).otherwise(0)) / total_rows) * 100, 2
            ).alias("null_pct"),
            
            # Cardinality
            F.countDistinct(col).alias("unique_count"),
            F.round((F.countDistinct(col) / total_rows) * 100, 2).alias("cardinality_pct"),
            
            # Empty string check
            F.sum(F.when(F.trim(col) == "", 1).otherwise(0)).alias("empty_string_count"),
            
            # "none" literal check
            F.sum(F.when(F.lower(F.trim(col)) == "none", 1).otherwise(0)).alias("none_literal_count"),
        ).collect()[0]
        
        results.append(stats.asDict())
    
    return results


def profile_primary_key(df, config):
    """Validate primary key uniqueness and non-null."""
    pk = config.get("pk")
    cpk = config.get("composite_pk")
    
    if pk:
        null_count = df.filter(F.col(pk).isNull()).count()
        dupe_count = df.groupBy(pk).count().filter(F.col("count") > 1).count()
        return {
            "key_columns": pk,
            "null_count": null_count,
            "duplicate_count": dupe_count,
            "is_valid": null_count == 0 and dupe_count == 0,
        }
    elif cpk:
        null_count = df.filter(
            F.col(cpk[0]).isNull() | F.col(cpk[1]).isNull()
        ).count()
        dupe_count = df.groupBy(cpk).count().filter(F.col("count") > 1).count()
        return {
            "key_columns": cpk,
            "null_count": null_count,
            "duplicate_count": dupe_count,
            "is_valid": null_count == 0 and dupe_count == 0,
        }
    return None


def profile_multi_value(df, mv_cols, separator=","):
    """Analyze multi-value (comma-separated) columns."""
    results = {}
    
    for col_name in mv_cols:
        non_null = df.filter(F.col(col_name).isNotNull())
        
        # Split and analyze
        exploded = non_null.select(
            F.explode(F.split(F.col(col_name), separator)).alias("value")
        ).filter(F.trim(F.col("value")) != "")
        
        value_counts = exploded.groupBy("value").count().orderBy(F.desc("count"))
        
        # Stats
        split_lengths = non_null.select(
            F.size(F.split(F.col(col_name), separator)).alias("num_values")
        )
        
        length_stats = split_lengths.select(
            F.min("num_values").alias("min_values"),
            F.max("num_values").alias("max_values"),
            F.round(F.avg("num_values"), 2).alias("avg_values"),
        ).collect()[0]
        
        distinct_count = exploded.select("value").distinct().count()
        top_10 = value_counts.limit(10).collect()
        
        results[col_name] = {
            "min_values_per_row": length_stats["min_values"],
            "max_values_per_row": length_stats["max_values"],
            "avg_values_per_row": float(length_stats["avg_values"]),
            "total_distinct_values": distinct_count,
            "top_10": [(row["value"], row["count"]) for row in top_10],
        }
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Profiling on All Datasets

# COMMAND ----------

from pyspark.sql import Row

all_column_stats = []
all_row_counts = []
all_pk_results = {}
all_mv_results = {}

for name, config in DATASETS.items():
    print(f"\n{'='*60}")
    print(f"📁 Profiling: {name}")
    print(f"{'='*60}")
    
    # Load
    df = load_imdb_tsv(spark, name, config)
    row_count = df.count()
    all_row_counts.append({"dataset": name, "file": config["file"], "row_count": row_count})
    
    # Column profiling
    print(f"  📊 Profiling columns...")
    col_stats = profile_columns(df, name)
    all_column_stats.extend(col_stats)
    
    # Primary key validation
    print(f"  🔑 Validating primary key...")
    pk_result = profile_primary_key(df, config)
    if pk_result:
        status = "✅ VALID" if pk_result["is_valid"] else "❌ INVALID"
        print(f"     PK {pk_result['key_columns']}: {status} "
              f"(nulls={pk_result['null_count']}, dupes={pk_result['duplicate_count']})")
        all_pk_results[name] = pk_result
    
    # Multi-value analysis
    if config["mv_cols"]:
        print(f"  🔀 Analyzing multi-value columns: {config['mv_cols']}")
        mv_results = profile_multi_value(df, config["mv_cols"])
        all_mv_results[name] = mv_results
        for col, mv in mv_results.items():
            print(f"     {col}: {mv['total_distinct_values']} distinct values, "
                  f"avg {mv['avg_values_per_row']} per row, "
                  f"max {mv['max_values_per_row']} per row")
    
    # Cache the row count for later
    df.unpersist()

print("\n✅ All profiling complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results: Row Count Summary

# COMMAND ----------

# Display row counts as a table
row_count_df = spark.createDataFrame(all_row_counts)
display(row_count_df.orderBy("dataset"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results: Column-Level Profiling

# COMMAND ----------

# Display column stats
col_stats_df = spark.createDataFrame(all_column_stats)
display(
    col_stats_df
    .select("dataset", "column_name", "total_rows", "null_count", "null_pct", 
            "unique_count", "cardinality_pct", "empty_string_count", "none_literal_count")
    .orderBy("dataset", "column_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results: Data Quality Flags
# MAGIC
# MAGIC Columns with empty strings or 'none' literals that need cleaning:

# COMMAND ----------

# Show only columns with data quality issues
display(
    col_stats_df
    .filter((F.col("null_pct") > 0) | (F.col("empty_string_count") > 0) | (F.col("none_literal_count") > 0))
    .select("dataset", "column_name", "null_count", "null_pct", "empty_string_count", "none_literal_count")
    .orderBy(F.desc("null_pct"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results: Multi-Value Column Analysis
# MAGIC
# MAGIC These columns contain comma-separated values that must be **exploded** in the Silver layer.

# COMMAND ----------

# Print multi-value analysis
for dataset, mv_cols in all_mv_results.items():
    for col_name, stats in mv_cols.items():
        print(f"\n📌 {dataset}.{col_name}")
        print(f"   Distinct values: {stats['total_distinct_values']}")
        print(f"   Values per row: min={stats['min_values_per_row']}, "
              f"max={stats['max_values_per_row']}, avg={stats['avg_values_per_row']}")
        print(f"   Top 10 values:")
        for val, count in stats['top_10']:
            print(f"      {val}: {count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results: Primary Key Validation

# COMMAND ----------

for dataset, pk in all_pk_results.items():
    status = "✅" if pk["is_valid"] else "❌"
    print(f"{status} {dataset} | Key: {pk['key_columns']} | "
          f"Nulls: {pk['null_count']:,} | Dupes: {pk['duplicate_count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Row Counts for Bronze Validation
# MAGIC
# MAGIC Save this as a reference table to validate against after Bronze ingestion.

# COMMAND ----------

# Save as Delta table for later validation
(
    row_count_df
    .withColumn("profiling_timestamp", F.current_timestamp())
    .write
    .mode("overwrite")
    .saveAsTable("imdb_final_project.source_row_counts")
)

print("✅ Saved source row counts to: imdb_final_project.source_row_counts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. Review the profiling results above
# MAGIC 2. Update the **mapping document** with cleaning decisions
# MAGIC 3. Proceed to **Bronze Layer** ingestion
# MAGIC 4. After Bronze load, compare `bronze_row_counts` vs `source_row_counts`
