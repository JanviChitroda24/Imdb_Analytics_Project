# =============================================================================
# GOLD LAYER — PHASE 2: SCD DIMENSIONS
# IMDb Analytics Platform
# Author: Janvi Chitroda
#
# Tables Built:
#   1. gold_dim_name   — SCD Type-1  (14.9M rows from silver_name_basics)
#   2. gold_dim_title  — SCD Type-2  (12.1M rows from silver_title_basics)
#
# Why Phase 2 after Phase 1?
#   DIM_Name and DIM_Title produce NameKey and TitleKey surrogate keys.
#   Every bridge table and fact table in Phase 3/4/5 needs these keys.
#   They must exist before any downstream table can be built.
#
# SCD TYPE-1 (DIM_Name):
#   Overwrite on change. No history kept.
#   Rationale: If a person's name or birth year changes in IMDb, we
#   just update it. We don't need to know what it was before.
#
# SCD TYPE-2 (DIM_Title):
#   Keep full history on change. New row added, old row closed.
#   Rationale: If a title's type, runtime, or adult flag changes,
#   analysts need to know which version was current when a rating
#   was recorded. History matters here.
#
# Surrogate Keys: decimal(10,0) via row_number() — deterministic.
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DecimalType, StructType, StructField,
    StringType, LongType, IntegerType
)

CATALOG   = "imdb_final_project"
LOADED_BY = "gold_phase2_notebook"
NOW       = F.current_timestamp()

# --------------------------------------------------------------------------- #
# HELPER: validate after every write                                          #
# --------------------------------------------------------------------------- #
def validate_write(table_name, expected_min, expected_max):
    count = spark.table(f"{CATALOG}.{table_name}").count()
    status = "✅ PASS" if expected_min <= count <= expected_max else "❌ FAIL"
    print(f"{status} | {table_name}: {count:,} rows (expected {expected_min:,}–{expected_max:,})")
    if count == 0:
        raise ValueError(f"CRITICAL: {table_name} wrote 0 rows. Pipeline halted.")
    return count


# ============================================================================= #
#  TABLE 1: gold_dim_name                                                       #
#  Source  : silver_name_basics                                                 #
#  SCD     : Type-1 — overwrite on change, no history                          #
#  Grain   : 1 row per unique person (nconst)                                   #
#  Rows    : ~14.9M                                                             #
#                                                                               #
#  Why Type-1 for names?                                                        #
#  If Fred Astaire's birth year gets corrected in IMDb, we just update          #
#  the record. No analyst needs to know what the wrong birth year was.          #
#  Name corrections and birth/death year updates are data fixes,                #
#  not meaningful business events worth tracking historically.                  #
#                                                                               #
#  Key design decision — deduplicate on nconst BEFORE assigning surrogate key: #
#  silver_name_basics has 1 row per profession per person (exploded).           #
#  We need 1 row per person in DIM_Name. So we dedup on nconst first,          #
#  taking the first occurrence of each person's attributes.                     #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_dim_name (SCD Type-1)...")
print("="*60)

silver_name = spark.table(f"{CATALOG}.silver_name_basics")

print(f"silver_name_basics row count: {silver_name.count():,}")
print("Sample:")
silver_name.show(3, truncate=False)

# silver_name_basics has multiple rows per person because primaryProfession
# was not exploded in Silver — BUT nconst is still unique per row in Silver
# (Alteryx kept one row per person, profession is one comma-separated string)
# We still deduplicate as a safety measure

# Window to pick one row per nconst — ordered by nconst for determinism
w_dedup = Window.partitionBy("nconst").orderBy("nconst")

name_deduped = (
    silver_name
    .filter(F.col("nconst").isNotNull())
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f"After dedup on nconst: {name_deduped.count():,} rows")

# Assign surrogate key
# Order by nconst for deterministic key assignment across pipeline runs
w_key = Window.orderBy("nconst")

gold_dim_name = (
    name_deduped
    .withColumn("NameKey", F.row_number().over(w_key).cast(DecimalType(10, 0)))
    .withColumn("ModifiedDate", NOW)
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select(
        F.col("NameKey"),
        F.col("nconst").alias("NCONST"),
        F.col("primaryName").alias("PrimaryName"),
        F.col("birthYear").cast(IntegerType()).alias("BirthYear"),
        F.col("deathYear").cast(IntegerType()).alias("DeathYear"),
        F.col("ModifiedDate"),
        F.col("loaded_by"),
        F.col("load_timestamp")
    )
)

gold_dim_name.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_dim_name")

validate_write("gold_dim_name", expected_min=14_000_000, expected_max=16_000_000)

# Validation checks
print("\nNull checks:")
null_namekey = spark.table(f"{CATALOG}.gold_dim_name").filter(F.col("NameKey").isNull()).count()
null_nconst  = spark.table(f"{CATALOG}.gold_dim_name").filter(F.col("NCONST").isNull()).count()
print(f"  Null NameKey:  {null_namekey} (expected: 0)")
print(f"  Null NCONST:   {null_nconst}  (expected: 0)")

# Duplicate surrogate key check
total     = spark.table(f"{CATALOG}.gold_dim_name").count()
distinct  = spark.table(f"{CATALOG}.gold_dim_name").select("NameKey").distinct().count()
print(f"  Duplicate NameKeys: {total - distinct} (expected: 0)")

print("\nSample (notice -9999 for missing birth/death years):")
spark.table(f"{CATALOG}.gold_dim_name").show(10, truncate=False)

# Sentinel value check — birthYear should have -9999 for unknowns
sentinel_birth = spark.table(f"{CATALOG}.gold_dim_name") \
    .filter(F.col("BirthYear") == -9999).count()
print(f"  Rows with BirthYear = -9999: {sentinel_birth:,} (expected: large number ~95% of rows)")


# ============================================================================= #
#  TABLE 2: gold_dim_title                                                      #
#  Source  : silver_title_basics                                                #
#  SCD     : Type-2 — keep history when tracked columns change                 #
#  Grain   : 1 row per title VERSION (not just per title)                       #
#  Rows    : ~12.1M (initial load — all rows are Version 1 / current)          #
#                                                                               #
#  Why Type-2 for titles?                                                       #
#  A title's TitleType, RuntimeMinutes, IsAdult, or ReleaseYear can be         #
#  corrected in IMDb over time. If a rating was recorded when a title was       #
#  classified as "movie" but later reclassified as "tvMovie", we need to        #
#  know which classification was current at the time of the rating.            #
#  Type-2 preserves this history with EffectiveDate/EndDate/IsCurrent.         #
#                                                                               #
#  SCD Type-2 columns:                                                          #
#    EffectiveDate — when this version became active  (current load timestamp) #
#    EndDate       — when this version was superseded (9999-12-31 = still open)#
#    IsCurrent     — boolean flag: True = this is the active version           #
#                                                                               #
#  On initial load: ALL rows are current (IsCurrent=True, EndDate=9999-12-31)  #
#  On future incremental loads: changed rows get EndDate set to today,         #
#  IsCurrent set to False, and a new row inserted with IsCurrent=True.         #
#                                                                               #
#  Key design decision — silver_title_basics has genres as comma-separated     #
#  string. DIM_Title does NOT store genres — that goes to bridge_title_genre.  #
#  DIM_Title stores only the title-level attributes, not multi-value fields.   #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_dim_title (SCD Type-2)...")
print("="*60)

silver_basics = spark.table(f"{CATALOG}.silver_title_basics")

print(f"silver_title_basics row count: {silver_basics.count():,}")
print("Sample:")
silver_basics.show(3, truncate=False)

# Deduplicate on tconst — silver may have multiple rows per title
# because genres column has comma-separated values meaning one title
# can appear multiple times. We take one row per tconst for DIM_Title.
# (genres goes to bridge_title_genre in Phase 3, not here)
w_dedup_title = Window.partitionBy("tconst").orderBy("tconst")

title_deduped = (
    silver_basics
    .filter(F.col("tconst").isNotNull())
    .withColumn("rn", F.row_number().over(w_dedup_title))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f"After dedup on tconst: {title_deduped.count():,} rows")

# Assign surrogate key — ordered by tconst for determinism
w_key_title = Window.orderBy("tconst")

# SCD Type-2 tracking columns:
# EffectiveDate = now (when this version was loaded)
# EndDate       = 9999-12-31 (open-ended = still current)
# IsCurrent     = True (all rows are current on initial load)
OPEN_END_DATE = F.to_timestamp(F.lit("9999-12-31"), "yyyy-MM-dd")

gold_dim_title = (
    title_deduped
    .withColumn("TitleKey", F.row_number().over(w_key_title).cast(DecimalType(10, 0)))
    # SCD Type-2 tracking columns
    .withColumn("EffectiveDate", NOW)
    .withColumn("EndDate", OPEN_END_DATE)
    .withColumn("IsCurrent", F.lit(True))
    .withColumn("CreatedDate", NOW)
    .withColumn("ModifiedDate", NOW)
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select(
        F.col("TitleKey"),
        F.col("tconst").alias("Tconst"),
        F.col("titleType").alias("TitleType"),
        F.col("primaryTitle").alias("PrimaryTitle"),
        F.col("originalTitle").alias("OriginalTitle"),
        F.col("isAdult").cast(IntegerType()).alias("IsAdult"),
        F.col("startYear").cast(IntegerType()).alias("ReleaseYear"),
        F.col("endYear").cast(IntegerType()).alias("EndYear"),
        F.col("runtimeMinutes").cast(IntegerType()).alias("RuntimeMinutes"),
        # SCD Type-2 columns
        F.col("EffectiveDate"),
        F.col("EndDate"),
        F.col("IsCurrent"),
        F.col("CreatedDate"),
        F.col("ModifiedDate"),
        F.col("loaded_by"),
        F.col("load_timestamp")
    )
)

gold_dim_title.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_dim_title")

validate_write("gold_dim_title", expected_min=11_000_000, expected_max=13_000_000)

# Validation checks
print("\nNull checks:")
null_titlekey = spark.table(f"{CATALOG}.gold_dim_title").filter(F.col("TitleKey").isNull()).count()
null_tconst   = spark.table(f"{CATALOG}.gold_dim_title").filter(F.col("Tconst").isNull()).count()
print(f"  Null TitleKey: {null_titlekey} (expected: 0)")
print(f"  Null Tconst:   {null_tconst}  (expected: 0)")

# All rows should be current on initial load
not_current = spark.table(f"{CATALOG}.gold_dim_title").filter(F.col("IsCurrent") == False).count()
print(f"  Rows where IsCurrent=False: {not_current} (expected: 0 on initial load)")

# Duplicate surrogate key check
total_t    = spark.table(f"{CATALOG}.gold_dim_title").count()
distinct_t = spark.table(f"{CATALOG}.gold_dim_title").select("TitleKey").distinct().count()
print(f"  Duplicate TitleKeys: {total_t - distinct_t} (expected: 0)")

# Sentinel value checks
sentinel_runtime = spark.table(f"{CATALOG}.gold_dim_title") \
    .filter(F.col("RuntimeMinutes") == -9999).count()
sentinel_year = spark.table(f"{CATALOG}.gold_dim_title") \
    .filter(F.col("ReleaseYear") == -9999).count()
print(f"  Rows with RuntimeMinutes=-9999: {sentinel_runtime:,}")
print(f"  Rows with ReleaseYear=-9999:    {sentinel_year:,}")

print("\nTitle type distribution:")
spark.table(f"{CATALOG}.gold_dim_title") \
    .groupBy("TitleType") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show(truncate=False)

print("\nSample rows:")
spark.table(f"{CATALOG}.gold_dim_title").show(5, truncate=False)

print("\nSCD Type-2 columns sample (all should be IsCurrent=True on initial load):")
spark.table(f"{CATALOG}.gold_dim_title") \
    .select("TitleKey", "Tconst", "TitleType", "EffectiveDate", "EndDate", "IsCurrent") \
    .show(5, truncate=False)


# ============================================================================= #
#  FINAL VALIDATION — Phase 2 Summary                                           #
# ============================================================================= #
print("\n" + "="*60)
print("GOLD PHASE 2 — FINAL VALIDATION SUMMARY")
print("="*60)

phase2_tables = [
    ("gold_dim_name",  14_000_000, 16_000_000),
    ("gold_dim_title", 11_000_000, 13_000_000),
]

all_pass = True
for table, min_r, max_r in phase2_tables:
    count = spark.table(f"{CATALOG}.{table}").count()
    status = "✅ PASS" if min_r <= count <= max_r else "❌ FAIL"
    print(f"  {status} | {table}: {count:,} rows")
    if status == "❌ FAIL":
        all_pass = False

print()
if all_pass:
    print("🎉 Phase 2 complete. Ready for Phase 3 (Bridge Tables).")
else:
    print("⚠️  One or more tables failed. Review before proceeding.")

# ============================================================================= #
#  INTERVIEW CHEAT SHEET — Phase 2                                              #
# ============================================================================= #
print("""
Interview Cheat Sheet — Phase 2:

SCD Type-1 (DIM_Name):
  - Overwrite on change — no history kept
  - Rationale: name/birth year corrections are data fixes, not business events
  - ModifiedDate tells you when the record was last updated

SCD Type-2 (DIM_Title):
  - New row on change — full history preserved
  - Tracked columns: TitleType, IsAdult, RuntimeMinutes, ReleaseYear
  - EffectiveDate = when this version became active
  - EndDate = 9999-12-31 means still current (open-ended)
  - IsCurrent = True/False flag for easy filtering in Power BI
  - On initial load: ALL rows are current (IsCurrent=True)

Why DIM_Title doesn't store genres:
  - genres is a multi-value field (up to 3 per title)
  - Storing it in DIM_Title would violate 1NF
  - bridge_title_genre handles the many-to-many relationship

Why dedup on tconst before assigning TitleKey:
  - silver_title_basics can have multiple rows per tconst
    because genres are comma-separated (one title = multiple genre combos)
  - DIM_Title needs exactly 1 row per title
  - dedup picks one row per tconst, genres go to bridge in Phase 3

Key numbers:
  gold_dim_name:  ~14.9M rows (1 per person)
  gold_dim_title: ~12.1M rows (1 per title on initial load)
""")