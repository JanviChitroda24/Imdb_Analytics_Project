# =============================================================================
# GOLD LAYER — PHASE 3: BRIDGE TABLES
# IMDb Analytics Platform
# Author: Janvi Chitroda
#
# Tables Built (in order):
#   1. gold_bridge_title_genre  — Title ↔ Genre       (from silver_title_basics)
#   2. gold_bridge_profession   — Person ↔ Profession  (from silver_name_basics)
#   3. gold_bridge_title_crew   — Title ↔ Crew ↔ Person (from silver_title_crew)
#   4. gold_bridge_akas         — Title ↔ Region ↔ Language (from silver_title_akas)
#
# Why bridge tables?
#   All four relationships are many-to-many:
#   - One title can have multiple genres, one genre belongs to many titles
#   - One person can have multiple professions, one profession has many people
#   - One title can have multiple directors/writers, one person directs many titles
#   - One title can have many regional releases, one region has many titles
#   Bridge tables are the standard relational solution for M:N relationships.
#   Without them you'd need arrays in dimension columns — not queryable in SQL.
#
# Key pattern for ALL bridge tables:
#   1. Read Silver (which has natural keys like tconst, nconst)
#   2. Explode multi-value fields where needed
#   3. Join to Gold dimensions to get surrogate keys (TitleKey, NameKey etc.)
#   4. Assign bridge-specific surrogate key
#   5. Write and validate
#
# Dependencies (must run AFTER):
#   Phase 1: gold_dim_genre, gold_dim_profession, gold_dim_crew,
#            gold_dim_region, gold_dim_language
#   Phase 2: gold_dim_title, gold_dim_name
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

CATALOG   = "imdb_final_project"
LOADED_BY = "gold_phase3_notebook"
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


# --------------------------------------------------------------------------- #
# PRE-LOAD: Read all dimension tables once upfront                            #
# We'll join these to Silver tables to get surrogate keys                     #
# --------------------------------------------------------------------------- #
print("Loading dimension lookup tables...")

dim_title      = spark.table(f"{CATALOG}.gold_dim_title") \
                      .filter(F.col("IsCurrent") == True) \
                      .select("TitleKey", "Tconst")

dim_name       = spark.table(f"{CATALOG}.gold_dim_name") \
                      .select("NameKey", "NCONST")

dim_genre      = spark.table(f"{CATALOG}.gold_dim_genre") \
                      .select("GenreKey", "GenreName")

dim_profession = spark.table(f"{CATALOG}.gold_dim_profession") \
                      .select("ProfessionKey", "Profession")

dim_crew       = spark.table(f"{CATALOG}.gold_dim_crew") \
                      .select("CrewKey", "Crew_Role")

dim_region     = spark.table(f"{CATALOG}.gold_dim_region") \
                      .select("RegionKey", F.upper(F.col("RegionCode")).alias("RegionCode"))

dim_language   = spark.table(f"{CATALOG}.gold_dim_language") \
                      .select("LanguageKey", F.lower(F.col("LanguageCode")).alias("LanguageCode"))

print("✅ All dimension tables loaded.")
print(f"  dim_title:      {dim_title.count():,} rows")
print(f"  dim_name:       {dim_name.count():,} rows")
print(f"  dim_genre:      {dim_genre.count():,} rows")
print(f"  dim_profession: {dim_profession.count():,} rows")
print(f"  dim_crew:       {dim_crew.count():,} rows")
print(f"  dim_region:     {dim_region.count():,} rows")
print(f"  dim_language:   {dim_language.count():,} rows")


# ============================================================================= #
#  TABLE 1: gold_bridge_title_genre                                             #
#  Source  : silver_title_basics.genres (comma-separated)                      #
#  Grain   : 1 row per title-genre combination                                 #
#  Rows    : ~19M (12.3M titles × avg 1.63 genres each)                        #
#                                                                               #
#  Example:                                                                     #
#  tt0000001 | "Action,Comedy,Drama"                                            #
#  becomes 3 bridge rows:                                                       #
#  TitleKey=1, GenreKey=1 (Action),  GenreOrder=0                              #
#  TitleKey=1, GenreKey=6 (Comedy),  GenreOrder=1                              #
#  TitleKey=1, GenreKey=9 (Drama),   GenreOrder=2                              #
#                                                                               #
#  GenreOrder: position of genre in the original comma-separated string        #
#  (0-indexed). Useful for identifying "primary genre" = position 0.           #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_bridge_title_genre...")
print("="*60)

silver_basics = spark.table(f"{CATALOG}.silver_title_basics")

# Step 1: Split genres and explode — with position tracking for GenreOrder
# posexplode gives both the position (index) and the value
title_genre_exploded = (
    silver_basics
    .select("tconst", "genres")
    .filter(
        F.col("tconst").isNotNull() &
        F.col("genres").isNotNull() &
        (F.col("genres") != "Unknown") &
        (F.trim(F.col("genres")) != "")
    )
    .withColumn("genres_array", F.split(F.col("genres"), ","))
    # posexplode returns (pos, col) — pos is the 0-based position in the array
    .select(
        F.col("tconst"),
        F.posexplode(F.col("genres_array")).alias("GenreOrder", "GenreName")
    )
    .withColumn("GenreName", F.trim(F.col("GenreName")))
    .filter(F.col("GenreName") != "")
)

print(f"Exploded title-genre pairs: {title_genre_exploded.count():,}")

# Step 2: Join to dim_title to get TitleKey
# Left join so we can detect any tconst not found in dim_title
title_genre_with_titlekey = (
    title_genre_exploded
    .join(dim_title, title_genre_exploded.tconst == dim_title.Tconst, "left")
)

# Check for orphaned tconsts (titles not in dim_title)
orphan_titles = title_genre_with_titlekey.filter(F.col("TitleKey").isNull()).count()
print(f"Orphaned tconsts (not in dim_title): {orphan_titles} (expected: 0)")

# Step 3: Join to dim_genre to get GenreKey
title_genre_with_keys = (
    title_genre_with_titlekey
    .join(dim_genre, title_genre_with_titlekey.GenreName == dim_genre.GenreName, "left")
)
# You need two joins because you have two FKs to resolve. 
# There's no way to do both in one join because they come from two completely different dimension tables.


orphan_genres = title_genre_with_keys.filter(F.col("GenreKey").isNull()).count()
print(f"Orphaned GenreNames (not in dim_genre): {orphan_genres} (expected: 0)")

# Step 4: Assign bridge surrogate key and select final columns
w1 = Window.orderBy("TitleKey", "GenreOrder")

gold_bridge_title_genre = (
    title_genre_with_keys
    .filter(F.col("TitleKey").isNotNull() & F.col("GenreKey").isNotNull())
    .withColumn("TitleGenreKey", F.row_number().over(w1).cast(DecimalType(10, 0)))
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select(
        F.col("TitleGenreKey"),
        F.col("TitleKey"),
        F.col("GenreKey"),
        F.col("GenreOrder"),
        F.col("loaded_by"),
        F.col("load_timestamp")
    )
)

gold_bridge_title_genre.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_bridge_title_genre")

validate_write("gold_bridge_title_genre", expected_min=15_000_000, expected_max=22_000_000)
print("Sample:")
spark.table(f"{CATALOG}.gold_bridge_title_genre").show(10, truncate=False)

# Genre distribution check
print("Genre distribution (top 10):")
spark.table(f"{CATALOG}.gold_bridge_title_genre") \
    .join(dim_genre, "GenreKey") \
    .groupBy("GenreName") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show(10, truncate=False)


# After Step 1 — explode:
# tconst   | GenreOrder | GenreName
# ---------|------------|----------
# tt0000001|     0      | Documentary
# tt0000001|     1      | Short
# tt0000002|     0      | Animation
# We have natural keys (tconst, GenreName) but no surrogate keys yet. The bridge table needs surrogate keys — TitleKey and GenreKey — not natural keys.

# After Step 2 — join to dim_title:
# tconst   | GenreOrder | GenreName   | TitleKey
# ---------|------------|-------------|----------
# tt0000001|     0      | Documentary |    1
# tt0000001|     1      | Short       |    1
# tt0000002|     0      | Animation   |    2
# Now we have TitleKey but GenreKey is still missing.

# After Step 3 — join to dim_genre:
# tconst   | GenreOrder | GenreName   | TitleKey | GenreKey
# ---------|------------|-------------|----------|----------
# tt0000001|     0      | Documentary |    1     |    8
# tt0000001|     1      | Short       |    1     |    23
# tt0000002|     0      | Animation   |    2     |    4
# Now we have both surrogate keys. The bridge table is ready to write.

# The short answer:
# Each join resolves exactly one natural key → surrogate key lookup:
# Step 2: tconst    → TitleKey     (lookup in dim_title)
# Step 3: GenreName → GenreKey     (lookup in dim_genre)
# You need two joins because you have two FKs to resolve. There's no way to do both in one join because they come from two completely different dimension tables.


# ============================================================================= #
#  TABLE 2: gold_bridge_profession                                              #
#  Source  : silver_name_basics.primaryProfession (comma-separated)            #
#  Grain   : 1 row per person-profession combination                           #
#  Rows    : ~20M (14.9M people × avg 1.39 professions each)                   #
#                                                                               #
#  IsPrimary flag:                                                              #
#  The first profession in the comma-separated string (position 0) is          #
#  considered the primary profession. All others are secondary.                #
#  This satisfies Business Requirement 1: "Get primary and secondary           #
#  professions for any personnel."                                              #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_bridge_profession...")
print("="*60)

silver_name = spark.table(f"{CATALOG}.silver_name_basics")

# Step 1: Explode primaryProfession with position tracking
name_profession_exploded = (
    silver_name
    .select("nconst", "primaryProfession")
    .filter(
        F.col("nconst").isNotNull() &
        F.col("primaryProfession").isNotNull() &
        (F.col("primaryProfession") != "Unknown") &
        (F.trim(F.col("primaryProfession")) != "")
    )
    .withColumn("profession_array", F.split(F.col("primaryProfession"), ","))
    .select(
        F.col("nconst"),
        F.posexplode(F.col("profession_array")).alias("pos", "Profession")
    )
    .withColumn("Profession", F.trim(F.col("Profession")))
    .withColumn("IsPrimary", F.when(F.col("pos") == 0, 1).otherwise(0))
    .filter(F.col("Profession") != "")
)

print(f"Exploded person-profession pairs: {name_profession_exploded.count():,}")

# Step 2: Join to dim_name to get NameKey
profession_with_namekey = (
    name_profession_exploded
    .join(dim_name, name_profession_exploded.nconst == dim_name.NCONST, "left")
)

orphan_names = profession_with_namekey.filter(F.col("NameKey").isNull()).count()
print(f"Orphaned nconsts (not in dim_name): {orphan_names} (expected: 0)")

# Step 3: Join to dim_profession to get ProfessionKey
profession_with_keys = (
    profession_with_namekey
    .join(dim_profession,
          profession_with_namekey.Profession == dim_profession.Profession,
          "left")
)

orphan_professions = profession_with_keys.filter(F.col("ProfessionKey").isNull()).count()
print(f"Orphaned Professions (not in dim_profession): {orphan_professions} (expected: 0)")

# Step 4: Assign bridge surrogate key
w2 = Window.orderBy("NameKey", "pos")

gold_bridge_profession = (
    profession_with_keys
    .filter(F.col("NameKey").isNotNull() & F.col("ProfessionKey").isNotNull())
    .withColumn("titleProfessionKey", F.row_number().over(w2).cast(DecimalType(10, 0)))
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select(
        F.col("titleProfessionKey"),
        F.col("ProfessionKey"),
        F.col("NameKey"),
        F.col("IsPrimary"),
        F.col("loaded_by"),
        F.col("load_timestamp")
    )
)

gold_bridge_profession.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_bridge_profession")

validate_write("gold_bridge_profession", expected_min=15_000_000, expected_max=25_000_000)
print("Sample:")
spark.table(f"{CATALOG}.gold_bridge_profession").show(10, truncate=False)

# IsPrimary distribution
primary_count   = spark.table(f"{CATALOG}.gold_bridge_profession").filter(F.col("IsPrimary") == 1).count()
secondary_count = spark.table(f"{CATALOG}.gold_bridge_profession").filter(F.col("IsPrimary") == 0).count()
print(f"Primary professions:   {primary_count:,}")
print(f"Secondary professions: {secondary_count:,}")


# ============================================================================= #
#  TABLE 3: gold_bridge_title_crew                                              #
#  Source  : silver_title_crew (directors and writers columns)                 #
#  Grain   : 1 row per title-crew_role-person combination                      #
#  Rows    : ~25-30M (after exploding directors + writers)                      #
#                                                                               #
#  silver_title_crew has:                                                       #
#  tconst | directors (comma-sep nconsts) | writers (comma-sep nconsts)        #
#                                                                               #
#  We process directors and writers SEPARATELY then UNION them,                #
#  adding Crew_Role = "director" or "writer" so we can join to dim_crew.       #
#                                                                               #
#  Watch out for "Unknown" values
#  in directors/writers. We must filter these out before joining to dim_name.  #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_bridge_title_crew...")
print("="*60)

silver_crew = spark.table(f"{CATALOG}.silver_title_crew")

print(f"silver_title_crew row count: {silver_crew.count():,}")
print("Sample:")
silver_crew.show(3, truncate=False)

# Step 1a: Explode directors
directors_exploded = (
    silver_crew
    .select("tconst", F.col("directors").alias("nconst_raw"))
    .filter(
        F.col("tconst").isNotNull() &
        F.col("directors").isNotNull() &
        (F.col("directors") != "Unknown") &
        (F.trim(F.col("directors")) != "")
    )
    .withColumn("nconst_array", F.split(F.col("nconst_raw"), ","))
    .select(
        F.col("tconst"),
        F.explode(F.col("nconst_array")).alias("nconst")
    )
    .withColumn("nconst", F.trim(F.col("nconst")))
    .withColumn("Crew_Role", F.lit("director"))
    .filter(
        F.col("nconst").isNotNull() &
        (F.col("nconst") != "Unknown") &
        (F.col("nconst") != r"\N") &
        (F.trim(F.col("nconst")) != "")
    )
)

print(f"Director rows after explode: {directors_exploded.count():,}")

# Step 1b: Explode writers
writers_exploded = (
    silver_crew
    .select("tconst", F.col("writers").alias("nconst_raw"))
    .filter(
        F.col("tconst").isNotNull() &
        F.col("writers").isNotNull() &
        (F.col("writers") != "Unknown") &
        (F.trim(F.col("writers")) != "")
    )
    .withColumn("nconst_array", F.split(F.col("nconst_raw"), ","))
    .select(
        F.col("tconst"),
        F.explode(F.col("nconst_array")).alias("nconst")
    )
    .withColumn("nconst", F.trim(F.col("nconst")))
    .withColumn("Crew_Role", F.lit("writer"))
    .filter(
        F.col("nconst").isNotNull() &
        (F.col("nconst") != "Unknown") &
        (F.col("nconst") != r"\N") &
        (F.trim(F.col("nconst")) != "")
    )
)

print(f"Writer rows after explode: {writers_exploded.count():,}")

# Step 1c: Union directors and writers
crew_combined = directors_exploded.union(writers_exploded)
print(f"Combined crew rows: {crew_combined.count():,}")

# Step 2: Join to dim_title to get TitleKey
crew_with_titlekey = (
    crew_combined
    .join(dim_title, crew_combined.tconst == dim_title.Tconst, "left")
)

orphan_crew_titles = crew_with_titlekey.filter(F.col("TitleKey").isNull()).count()
print(f"Orphaned tconsts (not in dim_title): {orphan_crew_titles}")

# Step 3: Join to dim_name to get NameKey
crew_with_namekey = (
    crew_with_titlekey
    .join(dim_name, crew_with_titlekey.nconst == dim_name.NCONST, "left")
)

orphan_crew_names = crew_with_namekey.filter(F.col("NameKey").isNull()).count()
print(f"Orphaned nconsts (not in dim_name): {orphan_crew_names}")

# Step 4: Join to dim_crew to get CrewKey
crew_with_crewkey = (
    crew_with_namekey
    .join(dim_crew, crew_with_namekey.Crew_Role == dim_crew.Crew_Role, "left")
)

# Step 5: Assign bridge surrogate key
w3 = Window.orderBy("TitleKey", "CrewKey", "NameKey")

gold_bridge_title_crew = (
    crew_with_crewkey
    .filter(
        F.col("TitleKey").isNotNull() &
        F.col("NameKey").isNotNull() &
        F.col("CrewKey").isNotNull()
    )
    .withColumn("titleCrewKey", F.row_number().over(w3).cast(DecimalType(10, 0)))
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select(
        F.col("titleCrewKey"),
        F.col("CrewKey"),
        F.col("TitleKey"),
        F.col("NameKey"),
        F.col("loaded_by"),
        F.col("load_timestamp")
    )
)

gold_bridge_title_crew.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_bridge_title_crew")

validate_write("gold_bridge_title_crew", expected_min=10_000_000, expected_max=35_000_000)
print("Sample:")
spark.table(f"{CATALOG}.gold_bridge_title_crew").show(10, truncate=False)

# Director vs Writer count
director_count = spark.table(f"{CATALOG}.gold_bridge_title_crew") \
    .join(dim_crew, "CrewKey") \
    .filter(F.col("Crew_Role") == "director").count()
writer_count = spark.table(f"{CATALOG}.gold_bridge_title_crew") \
    .join(dim_crew, "CrewKey") \
    .filter(F.col("Crew_Role") == "writer").count()
print(f"Director rows: {director_count:,}")
print(f"Writer rows:   {writer_count:,}")


# ============================================================================= #
#  TABLE 4: gold_bridge_akas                                                    #
#  Source  : silver_title_akas                                                  #
#  Grain   : 1 row per title-region-language alternate title                   #
#  Rows    : ~54M (largest bridge table)                                        #
#                                                                               #
#  This is the most straightforward bridge — no exploding needed.              #
#  silver_title_akas already has 1 row per alternate title.                    #
#  We just need to:                                                             #
#    1. Join tconst → TitleKey                                                  #
#    2. Join region code → RegionKey  (Unknown maps to -9999 sentinel)         #
#    3. Join language code → LanguageKey (Unknown maps to -9999 sentinel)      #
#                                                                               #
#  The Unknown sentinels we created in Phase 1 are used HERE.                  #
#  Any row with region="Unknown" gets RegionKey=-9999.                         #
#  Any row with language="Unknown" gets LanguageKey=-9999.                     #
# ============================================================================= #

print("\n" + "="*60)
print("Building gold_bridge_akas...")
print("="*60)
 
silver_akas = (
    spark.table(f"{CATALOG}.silver_title_akas")
    .withColumn("region_normalized",   F.upper(F.col("region")))
    .withColumn("language_normalized", F.lower(F.col("language")))
)
 
print(f"silver_title_akas row count: {silver_akas.count():,}")
 
print("Sample normalized silver values:")
silver_akas.select(
    "titleId", "region", "region_normalized", "language", "language_normalized"
).show(5, truncate=False)
 
# Step 1: Join to dim_title
akas_with_titlekey = (
    silver_akas
    .filter(F.col("titleId").isNotNull())
    .join(dim_title, silver_akas.titleId == dim_title.Tconst, "left")
)
 
orphan_akas = akas_with_titlekey.filter(F.col("TitleKey").isNull()).count()
print(f"Orphaned titleIds: {orphan_akas:,} (expected: small number)")
 
# Step 2: Join to dim_region (already uppercased in pre-load)
akas_with_regionkey = (
    akas_with_titlekey
    .join(
        dim_region,
        akas_with_titlekey.region_normalized == dim_region.RegionCode,
        "left"
    )
    .withColumn("RegionKey",
        F.when(F.col("RegionKey").isNull(), F.lit(-9999).cast(DecimalType(10, 0)))
         .otherwise(F.col("RegionKey"))
    )
)
 
matched_region   = akas_with_regionkey.filter(F.col("RegionKey") != -9999).count()
unmatched_region = akas_with_regionkey.filter(F.col("RegionKey") == -9999).count()
print(f"Region matched:   {matched_region:,}")
print(f"Region unmatched: {unmatched_region:,} (expected ~22% of 55M = ~12.4M)")
 
# Step 3: Join to dim_language (already lowercased in pre-load)
akas_with_languagekey = (
    akas_with_regionkey
    .join(
        dim_language,
        akas_with_regionkey.language_normalized == dim_language.LanguageCode,
        "left"
    )
    .withColumn("LanguageKey",
        F.when(F.col("LanguageKey").isNull(), F.lit(-9999).cast(DecimalType(10, 0)))
         .otherwise(F.col("LanguageKey"))
    )
)
 
matched_language   = akas_with_languagekey.filter(F.col("LanguageKey") != -9999).count()
unmatched_language = akas_with_languagekey.filter(F.col("LanguageKey") == -9999).count()
print(f"Language matched:   {matched_language:,}")
print(f"Language unmatched: {unmatched_language:,} (expected ~33% of 55M = ~18.2M)")
 
# Step 4: Assign bridge surrogate key
w4 = Window.orderBy("TitleKey", "RegionKey", "LanguageKey")
 
gold_bridge_akas = (
    akas_with_languagekey
    .filter(F.col("TitleKey").isNotNull())
    .withColumn("TitleAkasKey", F.row_number().over(w4).cast(DecimalType(10, 0)))
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select(
        F.col("TitleAkasKey"),
        F.col("TitleKey"),
        F.col("RegionKey"),
        F.col("LanguageKey"),
        F.col("title").alias("AkasTitle"),
        F.col("isOriginalTitle").cast("int").alias("IsOriginalTitle"),
        F.col("loaded_by"),
        F.col("load_timestamp")
    )
)
 
gold_bridge_akas.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_bridge_akas")
 
validate_write("gold_bridge_akas", expected_min=45_000_000, expected_max=60_000_000)
 
# Key sentinel validation
total            = spark.table(f"{CATALOG}.gold_bridge_akas").count()
unknown_region   = spark.table(f"{CATALOG}.gold_bridge_akas").filter(F.col("RegionKey")   == -9999).count()
unknown_language = spark.table(f"{CATALOG}.gold_bridge_akas").filter(F.col("LanguageKey") == -9999).count()
region_pct       = round(unknown_region   / total * 100, 1)
language_pct     = round(unknown_language / total * 100, 1)
 
print(f"\nSentinel validation:")
print(f"  Unknown RegionKey (-9999):   {unknown_region:,} ({region_pct}%)  — expected ~22%")
print(f"  Unknown LanguageKey (-9999): {unknown_language:,} ({language_pct}%) — expected ~33%")
print(f"  {'✅ Region join working' if region_pct < 50 else '❌ Region join still broken'}")
print(f"  {'✅ Language join working' if language_pct < 50 else '❌ Language join still broken'}")
 
spark.table(f"{CATALOG}.gold_bridge_akas").show(5, truncate=False)


# ============================================================================= #
#  FINAL VALIDATION — Phase 3 Summary                                           #
# ============================================================================= #
print("\n" + "="*60)
print("GOLD PHASE 3 — FINAL VALIDATION SUMMARY")
print("="*60)

phase3_tables = [
    ("gold_bridge_title_genre", 15_000_000, 22_000_000),
    ("gold_bridge_profession",  15_000_000, 25_000_000),
    ("gold_bridge_title_crew",  10_000_000, 35_000_000),
    ("gold_bridge_akas",        45_000_000, 60_000_000),
]

all_pass = True
for table, min_r, max_r in phase3_tables:
    count = spark.table(f"{CATALOG}.{table}").count()
    status = "✅ PASS" if min_r <= count <= max_r else "❌ FAIL"
    print(f"  {status} | {table}: {count:,} rows")
    if status == "❌ FAIL":
        all_pass = False

print()
if all_pass:
    print("🎉 Phase 3 complete. Ready for Phase 4 (DIM_Principals + Fact Tables).")
else:
    print("⚠️  One or more tables failed. Review before proceeding.")

print("""
Interview Cheat Sheet — Phase 3:

Why bridge tables instead of arrays in dimension columns?
  Arrays in SQL columns are not queryable without UNNEST/LATERAL JOIN.
  Bridge tables allow standard SQL JOINs, GROUP BY, and COUNT operations.
  e.g. "Find all Action movies" = JOIN bridge_title_genre WHERE GenreName='Action'
  With arrays you'd need: WHERE ARRAY_CONTAINS(genres, 'Action') — not portable.

Why posexplode instead of explode for genre and profession?
  posexplode returns both position AND value.
  Position 0 = primary genre / primary profession.
  This lets us answer BR#1: "Get PRIMARY and secondary professions for any person."
  Regular explode loses the position information.

Why union directors and writers before joining to dims?
  Both are nconst lists pointing to the same dim_name table.
  Unioning first means we do ONE join to dim_title and ONE join to dim_name
  instead of two separate joins per role. Cleaner and more efficient.

Why does bridge_akas not need exploding?
  silver_title_akas already has 1 row per alternate title (its natural grain).
  No comma-separated values to split. The bridge just needs FK lookups.

Why -9999 sentinel matters in bridge_akas:
  22% of akas rows have no region, 33% have no language.
  Without the sentinel, these rows would have NULL FKs and be excluded
  from any JOIN — silently losing 12M+ rows from your analysis.

Why left join with exploded data as the driving table:
    The driving table (left side) should always be what you're building — in this case, title-genre pairs. A left join preserves all pairs and brings in TitleKey where it matches. If TitleKey comes back NULL, that tconst is an orphan — we can see it, count it, log it, then filter it out before writing.

    Why not the other way around:
    dim_title LEFT JOIN exploded asks "give me all titles with their genres" — produces NULL genre rows for titles with no genres. Wrong grain, wrong purpose.

    Why not inner join:
    Silently drops orphans without telling you. Left join makes data quality issues visible.
    One line to remember:

    Left side = what you're building. Right side = the lookup. Nulls after join = orphans to investigate.

    This same pattern applies to every join in Phase 3 — genre, profession, crew, akas. Always the Silver/exploded data on the left, dimension on the right.

""")