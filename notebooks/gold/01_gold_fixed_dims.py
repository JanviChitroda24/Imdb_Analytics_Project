# =============================================================================
# GOLD LAYER — PHASE 1: FIXED DIMENSIONS (CORRECTED v2)
# IMDb Analytics Platform
# Author: Janvi Chitroda
#
# KEY FIX in this version:
#   silver_title_basics.genres and silver_name_basics.primaryProfession
#   are still comma-separated strings (Alteryx did NOT explode them).
#   We must split() + explode() here before building the dimensions.
#
#   Example: "Action,Comedy,Drama" → 3 separate rows: Action | Comedy | Drama
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, StructType, StructField, StringType, LongType

CATALOG   = "imdb_final_project"
LOADED_BY = "gold_phase1_notebook"
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
#  TABLE 1: gold_dim_genre                                                      #
#  Source  : silver_title_basics.genres                                         #
#  FIX     : genres is still "Action,Comedy,Drama" — must split + explode first #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_dim_genre...")
print("="*60)

silver_basics = spark.table(f"{CATALOG}.silver_title_basics")

# Diagnose first — show what the genres column actually looks like
print("Sample genres values from Silver:")
silver_basics.select("genres").filter(F.col("genres").isNotNull()).show(5, truncate=False)

genre_df = (
    silver_basics
    .select(F.explode(F.split(F.col("genres"), ",")).alias("GenreName"))
    .withColumn("GenreName", F.trim(F.col("GenreName")))
    .filter(F.col("GenreName").isNotNull() & (F.col("GenreName") != ""))
    .distinct()
    .orderBy("GenreName")
)

print(f"Distinct genres after explode: {genre_df.count()}")
genre_df.show(30, truncate=False)

w1 = Window.orderBy("GenreName")

gold_dim_genre = (
    genre_df
    .withColumn("GenreKey", F.row_number().over(w1).cast(DecimalType(10, 0)))
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select("GenreKey", "GenreName", "loaded_by", "load_timestamp")
)

gold_dim_genre.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_dim_genre")

validate_write("gold_dim_genre", expected_min=20, expected_max=35)
spark.table(f"{CATALOG}.gold_dim_genre").show(30, truncate=False)


# ============================================================================= #
#  TABLE 2: gold_dim_profession                                                 #
#  Source  : silver_name_basics.primaryProfession                               #
#  FIX     : still "actor,producer,director" — must split + explode first       #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_dim_profession...")
print("="*60)

silver_name = spark.table(f"{CATALOG}.silver_name_basics")

# Diagnose first
print("Sample primaryProfession values from Silver:")
silver_name.select("primaryProfession").filter(F.col("primaryProfession").isNotNull()).show(5, truncate=False)

profession_df = (
    silver_name
    .select(F.explode(F.split(F.col("primaryProfession"), ",")).alias("Profession"))
    .withColumn("Profession", F.trim(F.col("Profession")))
    .filter(F.col("Profession").isNotNull() & (F.col("Profession") != ""))
    .distinct()
    .orderBy("Profession")
)

print(f"Distinct professions after explode: {profession_df.count()}")
profession_df.show(50, truncate=False)

w2 = Window.orderBy("Profession")

gold_dim_profession = (
    profession_df
    .withColumn("ProfessionKey", F.row_number().over(w2).cast(DecimalType(10, 0)))
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select("ProfessionKey", "Profession", "loaded_by", "load_timestamp")
)

gold_dim_profession.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_dim_profession")

validate_write("gold_dim_profession", expected_min=40, expected_max=55)
spark.table(f"{CATALOG}.gold_dim_profession").show(50, truncate=False)


# ============================================================================= #
#  TABLE 3: gold_dim_crew                                                       #
#  Source  : HARDCODED — exactly 2 values (director / writer)                  #
#  No changes needed — was already passing                                      #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_dim_crew...")
print("="*60)

crew_schema = StructType([
    StructField("CrewKey", LongType(), False),
    StructField("Crew_Role", StringType(), False)
])

gold_dim_crew = (
    spark.createDataFrame([(1, "director"), (2, "writer")], schema=crew_schema)
    .withColumn("CrewKey", F.col("CrewKey").cast(DecimalType(10, 0)))
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select("CrewKey", "Crew_Role", "loaded_by", "load_timestamp")
)

gold_dim_crew.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_dim_crew")

validate_write("gold_dim_crew", expected_min=2, expected_max=2)
spark.table(f"{CATALOG}.gold_dim_crew").show(truncate=False)


# ============================================================================= #
#  TABLE 4: gold_dim_region                                                     #
#  Source  : ref_region_codes                                                   #
#  No changes needed — was already passing                                      #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_dim_region...")
print("="*60)

region_ref = (
    spark.table(f"{CATALOG}.ref_region_codes")
    .select("RegionCode", "RegionDescription")
    .filter(F.col("IsHistorical") == "0")
    .filter(F.col("RegionCode").isNotNull())
    .filter(F.trim(F.col("RegionCode")) != "")
    .distinct()
    .orderBy("RegionCode")
)

w3 = Window.orderBy("RegionCode")

region_with_keys = (
    region_ref
    .withColumn("RegionKey", F.row_number().over(w3).cast(DecimalType(10, 0)))
    .select("RegionKey", "RegionCode", "RegionDescription")
)

unknown_region = spark.createDataFrame(
    [(-9999, "Unknown", "Unknown")],
    schema=["RegionKey", "RegionCode", "RegionDescription"]
).withColumn("RegionKey", F.col("RegionKey").cast(DecimalType(10, 0)))

gold_dim_region = (
    region_with_keys
    .union(unknown_region)
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
)

gold_dim_region.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_dim_region")

validate_write("gold_dim_region", expected_min=200, expected_max=300)
sentinel_region = spark.table(f"{CATALOG}.gold_dim_region") \
    .filter(F.col("RegionCode") == "Unknown").count()
print(f"Unknown sentinel: {sentinel_region} row (expected: 1)")
spark.table(f"{CATALOG}.gold_dim_region").orderBy("RegionCode").show(5, truncate=False)


# ============================================================================= #
#  TABLE 5: gold_dim_language                                                   #
#  Source  : ref_language_codes                                                 #
#  No changes needed — was already passing                                      #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_dim_language...")
print("="*60)

language_ref = (
    spark.table(f"{CATALOG}.ref_language_codes")
    .select("LanguageCode", "LanguageDescription")
    .filter(F.col("LanguageCode").isNotNull())
    .filter(F.trim(F.col("LanguageCode")) != "")
    .distinct()
    .orderBy("LanguageCode")
)

w4 = Window.orderBy("LanguageCode")

language_with_keys = (
    language_ref
    .withColumn("LanguageKey", F.row_number().over(w4).cast(DecimalType(10, 0)))
    .select("LanguageKey", "LanguageCode", "LanguageDescription")
)

unknown_language = spark.createDataFrame(
    [(-9999, "Unknown", "Unknown")],
    schema=["LanguageKey", "LanguageCode", "LanguageDescription"]
).withColumn("LanguageKey", F.col("LanguageKey").cast(DecimalType(10, 0)))

gold_dim_language = (
    language_with_keys
    .union(unknown_language)
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
)

gold_dim_language.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_dim_language")

validate_write("gold_dim_language", expected_min=150, expected_max=220)
sentinel_lang = spark.table(f"{CATALOG}.gold_dim_language") \
    .filter(F.col("LanguageCode") == "Unknown").count()
print(f"Unknown sentinel: {sentinel_lang} row (expected: 1)")
spark.table(f"{CATALOG}.gold_dim_language").orderBy("LanguageCode").show(5, truncate=False)


# ============================================================================= #
#  FINAL VALIDATION — Phase 1 Summary                                           #
# ============================================================================= #
print("\n" + "="*60)
print("GOLD PHASE 1 — FINAL VALIDATION SUMMARY")
print("="*60)

phase1_tables = [
    ("gold_dim_genre",       20,  35),
    ("gold_dim_profession",  40,  55),
    ("gold_dim_crew",         2,   2),
    ("gold_dim_region",     200, 300),
    ("gold_dim_language",   150, 220),
]

all_pass = True
for table, min_r, max_r in phase1_tables:
    count = spark.table(f"{CATALOG}.{table}").count()
    status = "✅ PASS" if min_r <= count <= max_r else "❌ FAIL"
    print(f"  {status} | {table}: {count:,} rows")
    if status == "❌ FAIL":
        all_pass = False

print()
if all_pass:
    print("🎉 All Phase 1 tables validated. Ready for Phase 2 (SCD Dimensions).")
else:
    print("⚠️  One or more tables failed validation. Review before proceeding to Phase 2.")