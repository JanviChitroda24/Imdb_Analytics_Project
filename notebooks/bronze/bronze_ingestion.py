# =============================================================================
# IMDB ANALYTICS PLATFORM
# BRONZE LAYER — Raw Ingestion Notebook
# =============================================================================
# PURPOSE:
#   Land all 7 cleaned IMDb CSVs into Delta tables exactly as received.
#   No transformations. No type casting. No filtering.
#   This is the immutable landing zone — our source of truth.
#
# DESIGN DECISIONS (Interview-Ready):
#   1. All columns loaded as STRING — Bronze is schema-on-read. Type casting
#      happens in Silver. This protects us if IMDb changes a column format.
#   2. Audit columns added at ingestion (ingestion_timestamp, source_file,
#      loaded_by) — ensures every row is traceable to when and where it arrived.
#   3. Delta format used — supports ACID transactions, time travel, and
#      schema evolution. If we need to reprocess, we can query prior versions.
#   4. saveAsTable with overwrite — Bronze is a full refresh. We don't
#      incrementally append Bronze because the source files are full snapshots.
#   5. Row count validation after each load — catches silent failures where
#      a file loads 0 rows or partial rows without raising an error.
#
# LAYER: Bronze
# AUTHOR: Janvi Chitroda
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

spark = SparkSession.builder.appName("IMDb_Bronze_Ingestion").getOrCreate()

# =============================================================================
# CONFIGURATION
# =============================================================================

# Base path where your cleaned CSVs are stored in DBFS
# Update this path to match your Databricks FileStore location
BASE_PATH = "dbfs:/FileStore/imdb/cleaned/"

# Target database (catalog schema in Databricks Unity Catalog or legacy hive)
DATABASE = "imdb_final_project"

# Pipeline metadata for audit columns
LOADED_BY = "bronze_ingestion_notebook"

# Dataset registry — maps source file to bronze table name
DATASETS = [
    {
        "source_file": "Cleaned_name_basics.csv",
        "bronze_table": "bronze_name_basics",
        "description": "Cast & crew personnel details"
    },
    {
        "source_file": "Cleaned_title_basics.csv",
        "bronze_table": "bronze_title_basics",
        "description": "Title metadata and genres"
    },
    {
        "source_file": "Cleaned_title_akas.csv",
        "bronze_table": "bronze_title_akas",
        "description": "Localized title names (multi-language/region)"
    },
    {
        "source_file": "Cleaned_title_crew.csv",
        "bronze_table": "bronze_title_crew",
        "description": "Directors and writers per title"
    },
    {
        "source_file": "Cleaned_title_episode.csv",
        "bronze_table": "bronze_title_episode",
        "description": "Series to episode relationships"
    },
    {
        "source_file": "Cleaned_title_principals.csv",
        "bronze_table": "bronze_title_principals",
        "description": "Principal cast and crew credits per title"
    },
    {
        "source_file": "Cleaned_title_ratings.csv",
        "bronze_table": "bronze_title_ratings",
        "description": "Average ratings and vote counts"
    },
]

# =============================================================================
# VALIDATION LOG — Stores row count results for reconciliation report
# =============================================================================

validation_log = []

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def load_bronze_table(source_file, bronze_table, description):
    """
    Loads a single cleaned CSV into a Bronze Delta table.

    Design decisions:
    - inferSchema=False: All columns treated as STRING. Bronze is schema-on-read.
      We never want Bronze to silently cast a value and mask a data quality issue.
    - header=True: CSVs have headers from the Alteryx cleaning step.
    - Audit columns added AFTER read — keeps them separate from source columns.
    - overwrite mode — Bronze is a full refresh per pipeline run.

    Args:
        source_file (str): CSV filename in BASE_PATH
        bronze_table (str): Target Delta table name
        description (str): Human-readable table description for logging
    """

    print(f"\n{'='*60}")
    print(f"Loading: {source_file} → {DATABASE}.{bronze_table}")
    print(f"Description: {description}")
    print(f"{'='*60}")

    file_path = BASE_PATH + source_file

    # ------------------------------------------------------------------
    # STEP 1: READ CSV
    # All columns come in as strings — no type inference at Bronze.
    # This is intentional. Type casting is a Silver responsibility.
    # ------------------------------------------------------------------
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("quote", '"') \
        .option("escape", '"') \
        .load(file_path)

    source_row_count = df.count()
    print(f"  Source rows read from CSV   : {source_row_count:,}")

    # ------------------------------------------------------------------
    # STEP 2: ADD AUDIT COLUMNS
    # Every Bronze row gets 3 audit columns:
    #   - ingestion_timestamp: when this pipeline run executed
    #   - source_file: which file this row came from (traceability)
    #   - loaded_by: which notebook/process loaded it
    #
    # Interview answer: "Audit columns at Bronze give us full lineage.
    # If a bad row surfaces in Gold 3 months later, we can trace it
    # back to exactly which source file and pipeline run it came from."
    # ------------------------------------------------------------------
    df = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit(source_file)) \
        .withColumn("loaded_by", lit(LOADED_BY))

    # ------------------------------------------------------------------
    # STEP 3: WRITE TO DELTA TABLE
    # overwrite: Bronze tables are full refresh — each pipeline run
    # replaces the prior load. No incremental logic at Bronze.
    # Delta format: ACID transactions + time travel capability.
    # ------------------------------------------------------------------
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{DATABASE}.{bronze_table}")

    print(f"  Written to Delta table      : {DATABASE}.{bronze_table}")

    # ------------------------------------------------------------------
    # STEP 4: ROW COUNT VALIDATION
    # Re-read from the Delta table to confirm rows landed correctly.
    # We compare source CSV rows vs Delta table rows.
    # A mismatch here means the write failed silently — which Delta
    # can do if schema enforcement kicks in unexpectedly.
    # ------------------------------------------------------------------
    bronze_row_count = spark.table(f"{DATABASE}.{bronze_table}").count()
    match = "✅ MATCH" if source_row_count == bronze_row_count else "❌ MISMATCH"

    print(f"  Bronze Delta rows confirmed : {bronze_row_count:,}")
    print(f"  Row count validation        : {match}")

    if source_row_count != bronze_row_count:
        print(f"  ⚠️  WARNING: Expected {source_row_count:,} rows but got {bronze_row_count:,}")

    # Log result for reconciliation report
    validation_log.append({
        "table": f"{DATABASE}.{bronze_table}",
        "source_file": source_file,
        "source_rows": source_row_count,
        "bronze_rows": bronze_row_count,
        "status": match,
        "loaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    return bronze_row_count


def print_schema_preview(bronze_table):
    """
    Prints schema and sample rows from a Bronze table.
    Useful for verifying all columns landed as STRING.
    """
    print(f"\n  Schema preview — {bronze_table}:")
    spark.table(f"{DATABASE}.{bronze_table}").printSchema()
    print(f"\n  Sample rows (top 3):")
    spark.table(f"{DATABASE}.{bronze_table}").show(3, truncate=True)


# =============================================================================
# MAIN EXECUTION — Load All 7 Bronze Tables
# =============================================================================

print("\n" + "="*60)
print("  IMDb Bronze Layer Ingestion — START")
print(f"  Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

# Create database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
print(f"\nDatabase ready: {DATABASE}")

# Load each dataset
for dataset in DATASETS:
    load_bronze_table(
        source_file=dataset["source_file"],
        bronze_table=dataset["bronze_table"],
        description=dataset["description"]
    )

# =============================================================================
# ROW COUNT RECONCILIATION REPORT
# =============================================================================
# This report is your Bronze validation artifact.
# Compare these numbers against the profiling summary to confirm
# row counts are consistent with what was profiled.
#
# Expected approximate counts (from profiling):
#   bronze_name_basics     ~14.9M  (profiled: 15.1M — delta from Alteryx cleaning)
#   bronze_title_akas      ~54.2M  (profiled: 55.3M)
#   bronze_title_basics    ~12.1M  (profiled: 12.3M)
#   bronze_title_crew      ~12.1M  (profiled: 12.3M)
#   bronze_title_episode   ~9.3M   (profiled: 9.5M)
#   bronze_title_principals~96.2M  (profiled: 98M)
#   bronze_title_ratings   ~1.6M   (profiled: 1.6M)
#
# The differences between profiled counts and Bronze counts are explained by
# Alteryx pre-cleaning (e.g., rows with null PKs were removed in Alteryx).
# These dropped rows should be documented in your validation log.
# =============================================================================

print("\n\n" + "="*60)
print("  BRONZE ROW COUNT RECONCILIATION REPORT")
print("="*60)
print(f"  {'Table':<35} {'Source CSV':>12} {'Bronze Delta':>13} {'Status'}")
print(f"  {'-'*35} {'-'*12} {'-'*13} {'-'*10}")

total_source = 0
total_bronze = 0

for entry in validation_log:
    table_short = entry["table"].replace(f"{DATABASE}.", "")
    print(f"  {table_short:<35} {entry['source_rows']:>12,} {entry['bronze_rows']:>13,}  {entry['status']}")
    total_source += entry["source_rows"]
    total_bronze += entry["bronze_rows"]

print(f"  {'-'*35} {'-'*12} {'-'*13}")
print(f"  {'TOTAL':<35} {total_source:>12,} {total_bronze:>13,}")
print(f"\n  Run completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

# =============================================================================
# OPTIONAL: SCHEMA PREVIEW
# Uncomment to print schema + sample rows for any table you want to inspect.
# Useful when verifying that columns landed as strings and audit columns exist.
# =============================================================================

# print_schema_preview("bronze_name_basics")
# print_schema_preview("bronze_title_ratings")

# =============================================================================
# OPTIONAL: PERSIST VALIDATION LOG AS A DELTA TABLE
# In production, you'd write this log to a dedicated validation table
# so it can be queried later and tracked across pipeline runs.
# =============================================================================

# validation_df = spark.createDataFrame(validation_log)
# validation_df.write.format("delta").mode("append") \
#     .saveAsTable(f"{DATABASE}._bronze_validation_log")

print("\n✅ Bronze layer ingestion complete.")
print("   Next step: Run Silver transformation notebooks.")