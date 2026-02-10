# SOURCE: bronze_name_basics
# TARGET: silver_name_basics

# imports 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, FloatType

spark = SparkSession.builder.getOrCreate()

NULL_MARKERS = ["none", "None", "NONE", "\\N", ""]
REJECTED_TABLE = "imdb_final_project.silver_rejected_rows"

# NOTEBOOK: silver_name_basics

# ── STEP 1: Read from Bronze ──────────────────────────────────────────
df = spark.table("imdb_final_project.bronze_name_basics")
bronze_count = df.count()
print(f"Bronze row count : {bronze_count}")
print("Printing current schema:")
df.printSchema()
print("Printing top 5 rows:")
df.show(5, truncate=False)

# ── STEP 2: Log rejected rows BEFORE dropping ────────────────────────
# Rule: only drop rows where nconst is null (can't join anywhere without PK)
rejected_df = df.filter(F.col("nconst").isNull())
rejected_count = rejected_df.count()
print(f"Rejected row count (null nconst): {rejected_count}")

if rejected_count > 0:
    rejected_df \
        .withColumn("pk_value", F.col("nconst")) \
        .withColumn("source_table", F.lit("bronze_name_basics")) \
        .withColumn("rejection_reason", F.lit("null_primary_key_nconst")) \
        .withColumn("rejection_timestamp", F.current_timestamp()) \
        .select("pk_value", "source_table", "rejection_reason", "rejection_timestamp") \
        .write.format("delta").mode("append") \
        .saveAsTable(REJECTED_TABLE)
    print(f"Logged {rejected_count:,} rejected rows → {REJECTED_TABLE}")
else:
    print("No rejected rows — skipping log")
    
# ── STEP 3: Filter to valid rows ─────────────────────────────────────
df = df.filter(F.col("nconst").isNotNull())

# ── STEP 4: Type casting ─────────────────────────────────────────────
# birthYear: cast string → INT, null/\N → -9999
df = df.withColumn("birthYear", 
                   F.when(
                        F.col("birthYear").isNull() | (F.col("birthYear") == "\\N") | (F.col("birthYear") == ""),
                        F.lit(-9999)
                   ).otherwise(
                       F.col("birthYear").cast(IntegerType())
                       )
                )
                   
# deathYear: cast string → INT, null/\N → -9999
df = df.withColumn("deathYear",
                   F.when(
                       F.col("deathYear").isNull() | (F.col("deathYear") == "\\N") | (F.col("deathYear") == ""),
                       F.lit(-9999)
                       ).otherwise(
                           F.col("deathYear").cast(IntegerType())
                       )
                   )

# ── STEP 5: String standardization ───────────────────────────────────
# primaryName: trim, Standardize Case (initcap), replace null/"none"/"\\N" → "Unknown"
df = df.withColumn("primaryName",
                   F.when(
                       F.col("primaryName").isNull() | F.trim(F.col("primaryName")).isin(NULL_MARKERS) , 
                       F.lit("Unknown")
                   ).otherwise(
                       F.trim(F.initcap(F.col("primaryName")))
                   )
        )

# You don't explode in Silver because Silver's job is to clean, not to model. Explosion is a modeling decision, and modeling belongs to Gold.

# primaryProfession: trim, Standardize Case (lower), replace null/"none"/"\\N" → "Unknown"
df = df.withColumn("primaryProfession",
                    F.when(
                        F.col("primaryProfession").isNull() | F.trim(F.col("primaryProfession")).isin(NULL_MARKERS) ,
                        F.lit("Unknown")
                    ).otherwise(
                        F.trim(F.lower(F.col("primaryProfession")))
                    )
        )

# knownForTitles: trim, Standardize Case (lower), replace Null/"none"/"\\N" → "Unknown"
df = df.withColumn("knownForTitles",
                    F.when(
                        F.col("knownForTitles").isNull() |  F.trim(F.col("knownForTitles")).isin(NULL_MARKERS),
                        F.lit("Unknown")
                    ).otherwise(
                        F.trim(F.lower(F.col("knownForTitles")))
                    )
        )

# ── STEP 6: Add Silver audit column ──────────────────────────────────
df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("loaded_by", F.current_user()) \
    .withColumn("source_table", F.lit("bronze_name_basics"))

# ── STEP 7: Select final columns in correct order ────────────────────
df_silver = df.select(
    "nconst",
    "primaryName",
    "birthYear",       # INT
    "deathYear",       # INT
    "primaryProfession",
    "knownForTitles",
    "ingestion_timestamp",
    "source_file",      # "name.basics.tsv.gz" — original file lineage
    "source_table",     # "bronze_name_basics" — Bronze table lineage
    "loaded_by"
)

# ── STEP 8: Write to Silver ───────────────────────────────────────────
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("imdb_final_project.silver_name_basics")


# ── STEP 9: Validate ─────────────────────────────────────────────────
silver_table = spark.table("imdb_final_project.silver_name_basics")
silver_count = silver_table.count()
print(f"Silver row count : {silver_count}")
assert silver_count == bronze_count - rejected_count, "Row count mismatch between Bronze and Silver"
print(f"Rows dropped: {bronze_count - rejected_count - silver_count}")

print("Printng silver table schema: ")
silver_table.printSchema()

print("Printing top 5 rows:")
silver_table.show(5, truncate=False)