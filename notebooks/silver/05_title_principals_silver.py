# NOTEBOOK 5: silver_title_principals

# imports 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, FloatType

spark = SparkSession.builder.getOrCreate()

NULL_MARKERS = ["none", "None", "NONE", "\\N", ""]
REJECTED_TABLE = "imdb_final_project.silver_rejected_rows"

# ── STEP 1: Read from Bronze ──────────────────────────────────────────────────
df = spark.table("imdb_final_project.bronze_title_episode")
bronze_count = df.count()
print(f"Bronze row count : {bronze_count}")
print("Bronze schema:")
df.printSchema()


# ── STEP 1: Read from Bronze ──────────────────────────────────────────────────
df = spark.table("imdb_final_project.bronze_title_principals")
bronze_count = df.count()
print(f"Bronze row count : {bronze_count}")
print("Bronze schema:")
df.printSchema()

# root
#  |-- tconst: string (nullable = true)
#  |-- ordering: string (nullable = true)
#  |-- nconst: string (nullable = true)
#  |-- category: string (nullable = true)
#  |-- job: string (nullable = true)
#  |-- characters: string (nullable = true)
#  |-- ingestion_timestamp: timestamp (nullable = true)
#  |-- source_file: string (nullable = true)
#  |-- loaded_by: string (nullable = true)

# ── STEP 2: Log rejected rows BEFORE dropping ────────────────────────
rejected_df = df.filter(
    F.col("tconst").isNull() | F.col("nconst").isNull()
)
rejected_count = rejected_df.count()
print(f"Rejected row count (null tconst or nconst): {rejected_count}")

if rejected_count > 0:
    rejected_df \
        .withColumn("pk_value", F.coalesce(F.col("tconst"), F.lit("NULL_TCONST"))) \
        .withColumn("source_table", F.lit("bronze_title_principals")) \
        .withColumn("rejection_reason", F.lit("null_tconst_or_nconst")) \
        .withColumn("rejection_timestamp", F.current_timestamp()) \
        .select("pk_value", "source_table", "rejection_reason", "rejection_timestamp") \
        .write.format("delta").mode("append") \
        .saveAsTable(REJECTED_TABLE)
    print(f"Logged {rejected_count:,} rejected rows → {REJECTED_TABLE}")
else:
    print("No rejected rows — skipping log")

# ── STEP 3: Filter to valid rows ─────────────────────────────────────
df = df.filter(F.col("tconst").isNotNull() & F.col("nconst").isNotNull())

# ── STEP 4: Type casting ─────────────────────────────────────────────
# ordering → BIGINT (your Databricks schema shows bigint for this column)
df = df.withColumn("ordering", F.col("ordering").cast(LongType()))

# ── STEP 5: String standardization ───────────────────────────────────
# category: trim + lower (13 distinct values: actor, director, producer, etc.)
df = df.withColumn("category",
    F.when(
        F.col("category").isNull() | F.trim(F.lower(F.col("category"))).isin(NULL_MARKERS),
        F.lit("Unknown")
    ).otherwise(
        F.trim(F.lower(F.col("category")))
    )
)

# job: 80.98% null + 1 literal "none" value
# null/\N/none/n/a/empty → "Unknown"
df = df.withColumn("job",
    F.when(
        F.col("job").isNull() | F.trim(F.lower(F.col("job"))).isin(NULL_MARKERS + ["none", "null", "n/a"]),
        F.lit("Unknown")
    ).otherwise(
        F.trim(F.col("job"))  # preserve case — job titles are display values
    )
)

# characters: 51.22% null + JSON bracket format ["Self"] → strip to "Self"
# Step A: replace null/\N → "Unknown"
# Step B: strip leading [" and trailing "] from valid values
# Note: some entries have multiple characters ["Hero","Villain"] → kept as-is after strip

# WHY regexp_replace only on characters column?
#   characters is the only column with JSON-like formatting in the raw data.
#   Profiling showed values like ["Self"], ["Blacksmith"], ["Assistant"].
#   These brackets are artifacts of IMDb storing character names as a JSON
#   array string. Without stripping them, Power BI dashboards would show
#   ["Self"] instead of Self — unusable for analysts.
#   No other column has this structured formatting issue.

df = df.withColumn("characters",
    F.when(
        F.col("characters").isNull() | F.trim(F.lower(F.col("characters"))).isin(NULL_MARKERS),
        F.lit("Unknown")
    ).otherwise(
        # Strip leading [" and trailing "]
        F.regexp_replace(
            F.regexp_replace(
                F.trim(F.col("characters")),
                r'^\["', ""      # remove leading ["
            ),
            r'"\]$', ""          # remove trailing "]
        )
    )
)

# ── STEP 6: Add Silver audit column ──────────────────────────────────
df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
       .withColumn("loaded_by", F.current_user()) \
       .withColumn("source_table", F.lit("bronze_title_principals"))

# ── STEP 7: Select final columns in correct order ────────────────────
df_silver = df.select(
    "tconst",       # string  — FK to silver_title_basics (part of composite PK)
    "ordering",     # BIGINT  — ordering within title (part of composite PK)
    "nconst",       # string  — FK to silver_name_basics
    "category",     # string  — lower (actor, director, producer...)
    "job",          # string  — display case or "Unknown" (80.98% Unknown)
    "characters",   # string  — JSON stripped or "Unknown" (51.22% Unknown)
    "ingestion_timestamp",
    "source_file",  # from Bronze: "title.principals.tsv.gz"
    "source_table", # Silver addition: "bronze_title_principals"
    "loaded_by"
)
# ── STEP 8: Write to Silver ───────────────────────────────────────────

# WHY repartition(200, tconst)?
#
# REASON 1 — Small File Problem:
#   Spark writes one file per partition. With 98M rows and default settings,
#   you risk hundreds of tiny files in the Delta table. Small files hurt query
#   performance because every read has to open each file individually.
#   repartition(200) controls the exact number of output files written.
#
# REASON 2 — Co-location for Gold Joins:
#   By repartitioning on tconst, all rows for the same title land in the
#   same partition. When Gold builds dim_principals and joins this table to
#   silver_title_basics on tconst, Spark does a partition-local join instead
#   of shuffling 98M rows across the cluster. This is called partition pruning.
#
# WHY only this table and not others?
#   title_principals is 98M rows — 48% of the entire dataset.
#   The other 6 Silver tables are small enough that Spark handles
#   partitioning automatically without issues. Explicit repartition
#   on small tables adds overhead without benefit.
#
# WHY regexp_replace only on characters column?
#   characters is the only column with JSON-like formatting in the raw data.
#   Profiling showed values like ["Self"], ["Blacksmith"], ["Assistant"].
#   These brackets are artifacts of IMDb storing character names as a JSON
#   array string. Without stripping them, Power BI dashboards would show
#   ["Self"] instead of Self — unusable for analysts.
#   No other column has this structured formatting issue.

df_silver.repartition(200, F.col("tconst")) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("imdb_final_project.silver_title_principals")

# ── STEP 9: Validate ─────────────────────────────────────────────────
silver_table = spark.table("imdb_final_project.silver_title_principals")
silver_count = silver_table.count()
print(f"\nBronze row count : {bronze_count}")
print(f"Silver row count : {silver_count}")
print(f"Rows dropped     : {bronze_count - rejected_count - silver_count}")
# Expected: dropped = 5
assert silver_count == bronze_count - rejected_count, "Row count mismatch: Bronze vs Silver"

print("\nSilver schema (verify BIGINT on ordering):")
silver_table.printSchema()
print("Silver top 5 rows:")
silver_table.show(5, truncate=False)

# Spot check: confirm JSON bracket stripping worked
print("Characters sample — should NOT contain [\" prefix:")
silver_table.filter(F.col("characters") != "Unknown") \
            .select("characters").show(10, truncate=False)

# Spot check: job null rate
print("job = 'Unknown' count (expect ~81% of rows):")
print(silver_table.filter(F.col("job") == "Unknown").count())