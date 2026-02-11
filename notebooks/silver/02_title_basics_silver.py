# NOTEBOOK: silver_title_basics

# imports 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, FloatType

spark = SparkSession.builder.getOrCreate()

NULL_MARKERS = ["none", "None", "NONE", "\\N", ""]
REJECTED_TABLE = "imdb_final_project.silver_rejected_rows"


# ── STEP 1: Read from Bronze ──────────────────────────────────────────
df = spark.table("imdb_final_project.bronze_title_basics")
bronze_count = df.count()
print(f"Bronze row count for bronze_title_basics : {bronze_count}")
print("Printing schema for bronze_title_basics: ")
df.printSchema()
print("Printing top 5 rows:")
df.show(5, truncate=False)

# ── STEP 2: Log and count rejected rows BEFORE dropping ──────────────────────
# Rule: drop rows where tconst is null — tconst is the central FK for the entire model
rejected_df = df.filter(F.col("tconst").isNull())
rejected_count = rejected_df.count()
print(f"Rejected row count (null tconst): {rejected_count}")

if rejected_count > 0:
    rejected_df \
        .withColumn("pk_value", F.col("tconst")) \
        .withColumn("source_table", F.lit("bronze_title_basics")) \
        .withColumn("rejection_reason", F.lit("null_primary_key_tconst")) \
        .withColumn("rejection_timestamp", F.current_timestamp()) \
        .select("pk_value", "source_table", "rejection_reason", "rejection_timestamp") \
        .write.format("delta").mode("append") \
        .saveAsTable(REJECTED_TABLE)
    print(f"Logged {rejected_count:,} rejected rows → {REJECTED_TABLE}")
else:
    print("No rejected rows — skipping log")
    
# ── STEP 3: Filter to valid rows ──────────────────────────────────────────────
df = df.filter(F.col("tconst").isNotNull())

# ── STEP 4: Type casting ──────────────────────────────────────────────────────
# startYear → INT, null/\N → -9999
df = df.withColumn("startYear",
                   F.when(
                       F.col("startYear").isNull() | F.trim(F.col("startYear")).isin(NULL_MARKERS) , 
                       F.lit(-9999)
                   ).otherwise(
                       F.trim(F.col("startYear")).cast(IntegerType())
                   )
        )

# endYear → INT, null/\N → -9999 (98.75% null — only series that have ended)
df = df.withColumn("endYear",
                   F.when(
                       F.col("endYear").isNull() | F.trim(F.col("endYear")).isin(NULL_MARKERS) , 
                       F.lit(-9999)
                   ).otherwise(
                       F.trim(F.col("endYear")).cast(IntegerType())
                   )
        )

# runtimeMinutes → INT, null → -9999 (64.12% null — keep row, sentinel for missing)
df = df.withColumn("runtimeMinutes",
                   F.when(
                       F.col("runtimeMinutes").isNull() | F.trim(F.col("runtimeMinutes")).isin(NULL_MARKERS) , 
                       F.lit(-9999)
                   ).otherwise(
                       F.trim(F.col("runtimeMinutes")).cast(IntegerType())
                   )
        )

# isAdult → INT (0 or 1 only, no nulls per profiling)
df = df.withColumn("isAdult",
                   F.when(
                       F.col("isAdult").isNull(), F.lit(0)
                   ).otherwise(
                       F.col("isAdult").cast(IntegerType())
                   )
        )

# ── STEP 5: String standardization ───────────────────────────────────────────
# titleType: trim + lower (categorical — 11 distinct values e.g. movie, short, tvseries)
df = df.withColumn("titleType",
    F.when(
        F.col("titleType").isNull() | F.trim(F.lower(F.col("titleType"))).isin(NULL_MARKERS),
        F.lit("Unknown")
    ).otherwise(
        F.trim(F.lower(F.col("titleType")))
    )
)
 
# primaryTitle: initcap + trim (display title — preserve proper case)
df = df.withColumn("primaryTitle",
    F.when(
        F.col("primaryTitle").isNull() | F.trim(F.lower(F.col("primaryTitle"))).isin(NULL_MARKERS),
        F.lit("Unknown")
    ).otherwise(
        F.initcap(F.trim(F.col("primaryTitle")))
    )
)
 
# originalTitle: initcap + trim (display title — same logic as primaryTitle)
df = df.withColumn("originalTitle",
    F.when(
        F.col("originalTitle").isNull() | F.trim(F.lower(F.col("originalTitle"))).isin(NULL_MARKERS),
        F.lit("Unknown")
    ).otherwise(
        F.initcap(F.trim(F.col("originalTitle")))
    )
)

# genres: trim only — stays comma-separated at Silver (e.g. "Action,Comedy,Drama")
# Explosion to 1 row per genre happens at Gold when building gold_bridge_title_genre
# Why not explode here? Silver's job is to clean, not model. Gold defines the structure.
df = df.withColumn("genres",
    F.when(
        F.col("genres").isNull() | F.trim(F.lower(F.col("genres"))).isin(NULL_MARKERS),
        F.lit("Unknown")
    ).otherwise(
        F.trim(F.col("genres"))
    )
)

# ── STEP 6: Add Silver audit columns ─────────────────────────────────────────
df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
       .withColumn("loaded_by", F.current_user()) \
       .withColumn("source_table", F.lit("bronze_title_basics"))
# source_file preserved from Bronze as-is ("title.basics.tsv.gz")

# ── STEP 7: Select final columns ─────────────────────────────────────────────
df_silver = df.select(
    "tconst",               # string  — PK, natural key
    "titleType",            # string  — lower (movie, short, tvseries...)
    "primaryTitle",         # string  — initcap
    "originalTitle",        # string  — initcap
    "isAdult",              # INT     — 0 or 1
    "startYear",            # INT     — -9999 if null
    "endYear",              # INT     — -9999 if null (98.75% will be -9999)
    "runtimeMinutes",       # INT     — -9999 if null (64.12% will be -9999)
    "genres",               # string  — comma-separated, NOT exploded at Silver
    "ingestion_timestamp",
    "source_file",          # from Bronze: "title.basics.tsv.gz"
    "source_table",         # Silver addition: "bronze_title_basics"
    "loaded_by"
)
 
# ── STEP 8: Write to Silver ───────────────────────────────────────────────────
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("imdb_final_project.silver_title_basics")
 
# ── STEP 9: Validate ──────────────────────────────────────────────────────────
silver_table = spark.table("imdb_final_project.silver_title_basics")
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
 
# Spot check: confirm sentinel values applied correctly
print("Null check — runtimeMinutes = -9999 (should be ~64% of rows):")
silver_table.filter(F.col("runtimeMinutes") == -9999).count()
 