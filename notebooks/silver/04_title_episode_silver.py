# NOTEBOOK 4: silver_title_episode

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


# Bronze row count : 9514619
# Bronze schema:
# root
#  |-- tconst: string (nullable = true)
#  |-- parentTconst: string (nullable = true)
#  |-- seasonNumber: string (nullable = true)
#  |-- episodeNumber: string (nullable = true)
#  |-- ingestion_timestamp: timestamp (nullable = true)
#  |-- source_file: string (nullable = true)
#  |-- loaded_by: string (nullable = true)

# ── STEP 2: Log rejected rows BEFORE dropping ────────────────────────
rejected_df = df.filter(
    F.col("tconst").isNull() | F.col("parentTconst").isNull()
)
rejected_count = rejected_df.count()
print(f"Rejected row count (null tconst or parentTconst): {rejected_count}")

if rejected_count > 0:
    rejected_df \
        .withColumn("pk_value", F.coalesce(F.col("tconst"), F.lit("NULL_TCONST"))) \
        .withColumn("source_table", F.lit("bronze_title_episode")) \
        .withColumn("rejection_reason", F.lit("null_tconst_or_parentTconst")) \
        .withColumn("rejection_timestamp", F.current_timestamp()) \
        .select("pk_value", "source_table", "rejection_reason", "rejection_timestamp") \
        .write.format("delta").mode("append") \
        .saveAsTable(REJECTED_TABLE)
    print(f"Logged {rejected_count:,} rejected rows → {REJECTED_TABLE}")
else:
    print("No rejected rows — skipping log")

# ── STEP 3: Filter to valid rows ─────────────────────────────────────
df = df.filter(F.col("tconst").isNotNull() & F.col("parentTconst").isNotNull())
# Step 2 REJECT condition  →  use |  (reject if ANY key is null)
# Step 3 KEEP condition    →  use &  (keep only if ALL keys are valid)

# ── STEP 4: Type casting ─────────────────────────────────────────────
# seasonNumber → INT, null/empty/\N → -9999
# (20.58% null — episodes without season info; sentinel preserves the row)
df = df.withColumn("seasonNumber",
        F.when(
            F.col("seasonNumber").isNull() | F.trim(F.col("seasonNumber")).isin(NULL_MARKERS), F.lit(-9999)
        ).otherwise(
            F.trim(F.col("seasonNumber")).cast(IntegerType())
        )
)
# episodeNumber → INT, null/empty/\N → -9999
# (20.58% null — same episodes that lack season number also lack episode number)
df = df.withColumn("episodeNumber",
        F.when(
            F.col("episodeNumber").isNull() | F.trim(F.col("episodeNumber")).isin(NULL_MARKERS), F.lit(-9999)
        ).otherwise(
            F.trim(F.col("episodeNumber")).cast(IntegerType())
        )
)

# ── STEP 5: String standardization ───────────────────────────────────

# ── STEP 6: Add Silver audit column ──────────────────────────────────
df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
       .withColumn("loaded_by", F.current_user()) \
       .withColumn("source_table", F.lit("bronze_title_episode"))

# ── STEP 7: Select final columns in correct order ────────────────────
df_silver = df.select(
    "tconst",           # string  — PK (episode identifier)
    "parentTconst",     # string  — FK to silver_title_basics (parent series)
    "seasonNumber",     # INT     — -9999 if null
    "episodeNumber",    # INT     — -9999 if null
    "ingestion_timestamp",
    "source_file",      # from Bronze: "title.episode.tsv.gz"
    "source_table",     # Silver addition: "bronze_title_episode"
    "loaded_by"
)

# ── STEP 8: Write to Silver ───────────────────────────────────────────
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("imdb_final_project.silver_title_episode")

# ── STEP 9: Validate ─────────────────────────────────────────────────
silver_table = spark.table("imdb_final_project.silver_title_episode")
silver_count = silver_table.count()
print(f"\nBronze row count : {bronze_count}")
print(f"Silver row count : {silver_count}")
print(f"Rows dropped     : {bronze_count - rejected_count - silver_count}")
assert silver_count == bronze_count - rejected_count, "Row count mismatch: Bronze vs Silver"

print("\nSilver schema (verify INT types on season/episode):")
silver_table.printSchema()
print("Silver top 5 rows:")
silver_table.show(5, truncate=False)