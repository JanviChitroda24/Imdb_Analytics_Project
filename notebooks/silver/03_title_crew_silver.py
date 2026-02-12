# NOTEBOOK 3: silver_title_crew

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
df = spark.table("imdb_final_project.bronze_title_crew")
bronze_count = df.count()
print(f"Bronze row count : {bronze_count}")
print("Bronze schema:")
df.printSchema()
# print("Bronze top 5 rows:")
# df.show(5, truncate=False)

# ── STEP 2: Log and count rejected rows BEFORE dropping ──────────────────────
rejected_df = df.filter(F.col("tconst").isNull())
rejected_count = rejected_df.count()
print(f"Rejected row count (null tconst): {rejected_count}")

if rejected_count > 0:
    rejected_df \
        .withColumn("pk_value", F.col("tconst")) \
        .withColumn("source_table", F.lit("bronze_title_crew")) \
        .withColumn("rejection_reason", F.lit("null_primary_key_tconst")) \
        .withColumn("rejection_timestamp", F.current_timestamp()) \
        .select("pk_value", "source_table", "rejection_reason", "rejection_timestamp") \
        .write.format("delta").mode("append") \
        .saveAsTable(REJECTED_TABLE)
    print(f"Logged {rejected_count:,} rejected rows → {REJECTED_TABLE}")
else:
    print("No rejected rows — skipping log")
 
# ── STEP 3: Drop rows with bad data ───────────────────────────────────────────
df = df.filter(F.col("tconst").isNotNull())

# ── STEP 4: Type casting ──────────────────────────────────────────────────────
# no columns to cast

# ── STEP 5: String standardization ───────────────────────────────────────────
# directors: comma-separated nconst IDs (e.g. "nm0005690,nm0721526")
# null/\N → "Unknown" — Alteryx already replaced \N but defensive check retained
# NOT exploded here — explosion happens at Gold when building gold_bridge_title_crew
# Why keep as comma-separated? Silver's job = clean. Gold's job = model.
df = df.withColumn("directors",
            F.when(F.col("directors").isNull() | F.trim(F.col("directors")).isin(NULL_MARKERS),
                   F.lit("Unknown")
                ).otherwise(
                    F.trim(F.lower(F.col("directors")))
                )           
        )
df = df.withColumn("writers",
            F.when(F.col("writers").isNull() | F.trim(F.col("writers")).isin(NULL_MARKERS),
                   F.lit("Unknown")
                ).otherwise(
                    F.trim(F.lower(F.col("writers")))
                )           
        )

# ── STEP 6: Add Silver audit columns ────────────────────────────────────────
df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("loaded_by", F.current_user()) \
        .withColumn("source_table", F.lit("bronze_title_crew"))

# ── STEP 7: Select final columns ─────────────────────────────────────────────
df_silver = df.select(
    "tconst",
    "directors",
    "writers",
    "ingestion_timestamp",
    "source_file",
    "source_table",
    "loaded_by"
)

# ── STEP 8: Write to Silver ───────────────────────────────────────────────────
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("imdb_final_project.silver_title_crew")

# ── STEP 9: Validate ──────────────────────────────────────────────────────────
silver_table = spark.table("imdb_final_project.silver_title_crew")
silver_count = silver_table.count()
print(f"\nBronze row count : {bronze_count}")
print(f"Silver row count : {silver_count}")
print(f"Rows dropped     : {bronze_count - rejected_count - silver_count}")
assert silver_count == bronze_count - rejected_count, "Row count mismatch: Bronze vs Silver"
 
# Type checks
print("\nSilver schema (verify INT types):")
silver_table.printSchema()
print("Silver top 5 rows:")
silver_table.show(5, truncate=False)