# NOTEBOOK 6: silver_title_ratings

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
df = spark.table("imdb_final_project.bronze_title_ratings")
bronze_count = df.count()
print(f"Bronze row count : {bronze_count}")
print("Bronze schema:")
df.printSchema()
# print("Bronze top 5 rows:")
# df.show(5, truncate=False)

# root
#  |-- tconst: string (nullable = true)
#  |-- averageRating: string (nullable = true)
#  |-- numVotes: string (nullable = true)
#  |-- ingestion_timestamp: timestamp (nullable = true)
#  |-- source_file: string (nullable = true)
#  |-- loaded_by: string (nullable = true)

# ── STEP 2: Log rejected rows BEFORE dropping ────────────────────────
# Rule: drop rows where tconst is null — cannot link rating to a title without FK
# Per profiling: 0 nulls expected here — Alteryx confirmed clean
rejected_df = df.filter(
    F.col("tconst").isNull() | 
    (F.trim(F.col("tconst")) == "") | 
    F.trim(F.col("tconst")).isin(NULL_MARKERS)
)
rejected_count = rejected_df.count()
print(f"Rejected row count (null or empty tconst): {rejected_count}")

if rejected_count > 0:
    rejected_df \
        .withColumn("pk_value", F.coalesce(F.col("tconst"), F.lit("NULL_TCONST"))) \
        .withColumn("source_table", F.lit("bronze_title_ratings")) \
        .withColumn("rejection_reason", F.lit("null_or_empty_tconst")) \
        .withColumn("rejection_timestamp", F.current_timestamp()) \
        .select("pk_value", "source_table", "rejection_reason", "rejection_timestamp") \
        .write.format("delta").mode("append") \
        .saveAsTable(REJECTED_TABLE)
    print(f"Logged {rejected_count:,} rejected rows → {REJECTED_TABLE}")
else:
    print("No rejected rows — skipping log")

# ── STEP 3: Filter to valid rows ─────────────────────────────────────
df = df.filter(
    F.col("tconst").isNotNull() & 
    (F.trim(F.col("tconst")) != "") & 
    ~F.trim(F.col("tconst")).isin(NULL_MARKERS)  # ← ~ means NOT in null markers
)
# REJECT condition  →  isNull | == "" | isin(NULL_MARKERS)
# KEEP condition    →  isNotNull & != "" & ~isin(NULL_MARKERS)

# ── STEP 4: Type casting ─────────────────────────────────────────────
# averageRating → FLOAT (values from 1.0 to 10.0, 91 distinct values per profiling)
df = df.withColumn("averageRating",
    F.col("averageRating").cast(FloatType())
)

# numVotes → INT (additive measure — total votes cast per title)
df = df.withColumn("numVotes",
    F.col("numVotes").cast(IntegerType())
)

# ── STEP 5: String standardization ───────────────────────────────────

# ── STEP 6: Add Silver audit column ──────────────────────────────────
df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
       .withColumn("loaded_by", F.current_user()) \
       .withColumn("source_table", F.lit("bronze_title_ratings"))

# ── STEP 7: Select final columns in correct order ────────────────────
df_silver = df.select(
    "tconst",           # string  — PK and FK to silver_title_basics
    "averageRating",    # FLOAT   — 1.0 to 10.0 (91 distinct values)
    "numVotes",         # INT     — total votes cast
    "ingestion_timestamp",
    "source_file",      # from Bronze: "title.ratings.tsv.gz"
    "source_table",     # Silver addition: "bronze_title_ratings"
    "loaded_by"
    )

# ── STEP 8: Write to Silver ───────────────────────────────────────────
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("imdb_final_project.silver_title_ratings")

# ── STEP 9: Validate ─────────────────────────────────────────────────
silver_table = spark.table("imdb_final_project.silver_title_ratings")
silver_count = silver_table.count()
print(f"\nBronze row count : {bronze_count}")
print(f"Silver row count : {silver_count}")
print(f"Rows dropped     : {bronze_count - rejected_count - silver_count}")
# Expected: 0 dropped
assert silver_count == bronze_count - rejected_count, "Row count mismatch: Bronze vs Silver"

print("\nSilver schema (verify FLOAT on averageRating, INT on numVotes):")
silver_table.printSchema()
print("Silver top 5 rows:")
silver_table.show(5, truncate=False)

# Spot check: rating range should be 1.0–10.0
print("Rating range check:")
silver_table.agg(
    F.min("averageRating").alias("min_rating"),
    F.max("averageRating").alias("max_rating"),
    F.avg("averageRating").alias("avg_rating")
).show()

# Spot check: null check on both measure columns
print("Null check on averageRating and numVotes (both should be 0):")
silver_table.filter(F.col("averageRating").isNull() | F.col("numVotes").isNull()).count()
