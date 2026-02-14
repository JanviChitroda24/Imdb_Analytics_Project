# NOTEBOOK 7: silver_title_akas

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
df = spark.table("imdb_final_project.bronze_title_akas")
bronze_count = df.count()
print(f"Bronze row count : {bronze_count}")
print("Bronze schema:")
df.printSchema()
# print("Bronze top 5 rows:")
# df.show(5, truncate=False)

# root
#  |-- titleId: string (nullable = true)
#  |-- ordering: string (nullable = true)
#  |-- title: string (nullable = true)
#  |-- region: string (nullable = true)
#  |-- language: string (nullable = true)
#  |-- types: string (nullable = true)
#  |-- attributes: string (nullable = true)
#  |-- isOriginalTitle: string (nullable = true)
#  |-- ingestion_timestamp: timestamp (nullable = true)
#  |-- source_file: string (nullable = true)
#  |-- loaded_by: string (nullable = true)


# ── STEP 2: Log rejected rows BEFORE dropping ────────────────────────
# Rule: drop rows where tconst is null — cannot link rating to a title without FK
# Per profiling: 0 nulls expected here — Alteryx confirmed clean
rejected_df = df.filter(F.col("titleId").isNull())
rejected_count = rejected_df.count()
print(f"Rejected row count (null titleId): {rejected_count}")

if rejected_count > 0:
    rejected_df \
        .withColumn("pk_value", F.col("titleId")) \
        .withColumn("source_table", F.lit("bronze_title_akas")) \
        .withColumn("rejection_reason", F.lit("null_primary_key_titleId")) \
        .withColumn("rejection_timestamp", F.current_timestamp()) \
        .select("pk_value", "source_table", "rejection_reason", "rejection_timestamp") \
        .write.format("delta").mode("append") \
        .saveAsTable(REJECTED_TABLE)
    print(f"Logged {rejected_count:,} rejected rows → {REJECTED_TABLE}")
else:
    print("No rejected rows — skipping log")

# ── STEP 3: Filter to valid rows ─────────────────────────────────────
df = df.filter(F.col("titleId").isNotNull())

# ── STEP 4: Type casting ─────────────────────────────────────────────
# ordering → BIGINT (your Databricks schema shows bigint for this column)
df = df.withColumn("ordering", F.col("ordering").cast(IntegerType()))

# isOriginalTitle → INT (0 or 1 only, no nulls per profiling)
df = df.withColumn("isOriginalTitle",
        F.when(F.col("isOriginalTitle").isNull(), F.lit(0))
            .otherwise(F.col("isOriginalTitle").cast(IntegerType()))
    )

# ── STEP 5: String standardization ───────────────────────────────────
# title: initcap + trim (display title — localized name, needs proper case)
df = df.withColumn("title",
        F.when(
            F.col("title").isNull() | F.trim(F.lower(F.col("title"))).isin(NULL_MARKERS),
            F.lit("Unknown")
        ).otherwise(
                F.initcap(F.trim(F.col("title")))
                )
    )

# region: upper + trim (ISO 3166 codes are uppercase — "US", "DE", "JP")
# 22.43% null → "Unknown" — maps to Unknown dim key in Gold
df = df.withColumn("region",
    F.when(
        F.col("region").isNull() | F.trim(F.lower(F.col("region"))).isin(NULL_MARKERS),
        F.lit("Unknown")
    ).otherwise(
        F.upper(F.trim(F.col("region")))
    )
)

# language: lower + trim (ISO 639 codes are lowercase — "en", "ja", "fr")
# 32.97% null → "Unknown" — maps to Unknown dim key in Gold
df = df.withColumn("language",
    F.when(
        F.col("language").isNull() | F.trim(F.lower(F.col("language"))).isin(NULL_MARKERS),
        F.lit("Unknown")
    ).otherwise(
        F.lower(F.trim(F.col("language")))
    )
)

# types: lower + trim (69.31% null — carry to Silver, not modeled in Gold)
df = df.withColumn("types",
    F.when(
        F.col("types").isNull() | F.trim(F.lower(F.col("types"))).isin(NULL_MARKERS),
        F.lit("Unknown")
    ).otherwise(
        F.trim(F.lower(F.col("types")))
    )
)

# attributes: 99.44% null — carry to Silver, NOT modeled in Gold
df = df.withColumn("attributes",
    F.when(
        F.col("attributes").isNull() | F.trim(F.lower(F.col("attributes"))).isin(NULL_MARKERS),
        F.lit("Unknown"))
     .otherwise(F.trim(F.col("attributes")))
)

# ── STEP 6: Add Silver audit column ──────────────────────────────────
df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
       .withColumn("loaded_by", F.current_user()) \
       .withColumn("source_table", F.lit("bronze_title_akas"))

# ── STEP 7: Select final columns in correct order ────────────────────
df_silver = df.select(
    "titleId",          # string  — FK to tconst in title_basics (part of composite PK)
    "ordering",         # INT     — ordering per title (part of composite PK)
    "title",            # string  — localized title text, initcap
    "region",           # string  — ISO 3166 code upper ("US","DE") or "Unknown"
    "language",         # string  — ISO 639 code lower ("en","ja") or "Unknown"
    "types",            # string  — lower or "Unknown" (not modeled in Gold)
    "attributes",       # string  — mostly "Unknown" (99.44% null, NOT in Gold)
    "isOriginalTitle",  # INT     — 0 or 1
    "ingestion_timestamp",
    "source_file",      # from Bronze: "title.akas.tsv.gz"
    "source_table",     # Silver addition: "bronze_title_akas"
    "loaded_by"
)

# ── STEP 8: Write to Silver ───────────────────────────────────────────
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("imdb_final_project.silver_title_akas")

# ── STEP 9: Validate ──────────────────────────────────────────────────────────
silver_table = spark.table("imdb_final_project.silver_title_akas")
silver_count = silver_table.count()
print(f"\nBronze row count : {bronze_count}")
print(f"Silver row count : {silver_count}")
print(f"Rows dropped     : {bronze_count - rejected_count - silver_count}")
assert silver_count == bronze_count - rejected_count, "Row count mismatch: Bronze vs Silver"

# ── Schema check ──────────────────────────────────────────────────────────────
print("\nSilver schema (verify INT on ordering and isOriginalTitle):")
silver_table.printSchema()
print("Silver top 5 rows:")
silver_table.show(5, truncate=False)

# ── Spot check 1: ordering and isOriginalTitle cast correctly ─────────────────
# Both should be INT — if still string the cast failed
print("\nSpot check 1 — ordering distinct values (should be small integers 1-262):")
silver_table.select("ordering").distinct().orderBy("ordering").show(10)

print("Spot check 2 — isOriginalTitle distinct values (should be ONLY 0 and 1):")
silver_table.select("isOriginalTitle").distinct().show()

# ── Spot check 2: region null handling ───────────────────────────────────────
# 22.43% of rows should have region = "Unknown"
# That is approximately 12.4M rows out of 55.3M
region_unknown_count = silver_table.filter(F.col("region") == "Unknown").count()
region_unknown_pct   = round((region_unknown_count / silver_count) * 100, 2)
print(f"\nSpot check 3 — region = 'Unknown' count : {region_unknown_count:,}")
print(f"                region = 'Unknown' pct   : {region_unknown_pct}% (expect ~22.43%)")

# ── Spot check 3: language null handling ─────────────────────────────────────
# 32.97% of rows should have language = "Unknown"
# That is approximately 18.2M rows out of 55.3M
language_unknown_count = silver_table.filter(F.col("language") == "Unknown").count()
language_unknown_pct   = round((language_unknown_count / silver_count) * 100, 2)
print(f"\nSpot check 4 — language = 'Unknown' count : {language_unknown_count:,}")
print(f"                language = 'Unknown' pct   : {language_unknown_pct}% (expect ~32.97%)")

# ── Spot check 4: region is uppercase ────────────────────────────────────────
# No region should contain lowercase letters — all ISO 3166 codes are uppercase
lowercase_regions = silver_table.filter(
    (F.col("region") != "Unknown") &
    (F.col("region") != F.upper(F.col("region")))
).count()
print(f"\nSpot check 5 — lowercase region codes (should be 0): {lowercase_regions:,}")

# ── Spot check 5: language is lowercase ──────────────────────────────────────
# No language should contain uppercase letters — all ISO 639 codes are lowercase
uppercase_languages = silver_table.filter(
    (F.col("language") != "Unknown") &
    (F.col("language") != F.lower(F.col("language")))
).count()
print(f"\nSpot check 6 — uppercase language codes (should be 0): {uppercase_languages:,}")

# ── Spot check 6: attributes is almost entirely Unknown ──────────────────────
# 99.44% null in source → should be ~99.44% "Unknown" in Silver
attributes_unknown_count = silver_table.filter(F.col("attributes") == "Unknown").count()
attributes_unknown_pct   = round((attributes_unknown_count / silver_count) * 100, 2)
print(f"\nSpot check 7 — attributes = 'Unknown' pct : {attributes_unknown_pct}% (expect ~99.44%)")