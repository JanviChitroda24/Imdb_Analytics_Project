# =============================================================================
# GOLD LAYER — PHASE 4: DIM_PRINCIPALS + FACT TABLES
# IMDb Analytics Platform
# Author: Janvi Chitroda
#
# Tables Built (in order):
#   1. gold_dim_principals    — SCD Type-1  (from silver_title_principals)
#   2. gold_fact_title_ratings — Fact table  (from silver_title_ratings)
#   3. gold_fact_episodes      — Fact table  (from silver_title_episode)
#
# Dependency order:
#   DIM_Principals must be built before Fact tables because it resolves
#   the NameKey and TitleKey FKs needed downstream.
#   Both fact tables need TitleKey from gold_dim_title.
#
# Why DIM_Principals is a dimension and not a bridge:
#   A bridge table links two existing dimensions (M:N).
#   DIM_Principals has its own attributes — Ordering, Category, Job, Characters.
#   It's a dependent dimension: it exists because of the relationship between
#   a title and a person, but it carries descriptive attributes of its own.
#   That makes it a dimension, not a pure bridge.
#
# SCD Types:
#   DIM_Principals  → Type-1 (job/characters change, history not needed)
#   FACT tables     → No SCD (facts are immutable — you don't version a rating)
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, IntegerType

CATALOG   = "imdb_final_project"
LOADED_BY = "gold_phase4_notebook"
NOW       = F.current_timestamp()

# --------------------------------------------------------------------------- #
# HELPER                                                                      #
# --------------------------------------------------------------------------- #
def validate_write(table_name, expected_min, expected_max):
    count = spark.table(f"{CATALOG}.{table_name}").count()
    status = "✅ PASS" if expected_min <= count <= expected_max else "❌ FAIL"
    print(f"{status} | {table_name}: {count:,} rows (expected {expected_min:,}–{expected_max:,})")
    if count == 0:
        raise ValueError(f"CRITICAL: {table_name} wrote 0 rows. Pipeline halted.")
    return count


# --------------------------------------------------------------------------- #
# PRE-LOAD: dimension lookup tables needed for FK resolution                  #
# --------------------------------------------------------------------------- #
print("Loading dimension lookup tables...")

dim_title = (
    spark.table(f"{CATALOG}.gold_dim_title")
    .filter(F.col("IsCurrent") == True)
    .select("TitleKey", "Tconst")
)

dim_name = (
    spark.table(f"{CATALOG}.gold_dim_name")
    .select("NameKey", "NCONST")
)

print(f"  dim_title: {dim_title.count():,} rows")
print(f"  dim_name:  {dim_name.count():,} rows")
print("✅ Dimension tables loaded.")


# ============================================================================= #
#  TABLE 1: gold_dim_principals                                                 #
#  Source  : silver_title_principals                                            #
#  SCD     : Type-1 — overwrite on change                                      #
#  Grain   : 1 row per title-person-ordering combination                        #
#  Rows    : ~96M (largest table in Gold layer)                                 #
#                                                                               #
#  Why SCD Type-1?                                                              #
#  If a person's job title or character name gets corrected in IMDb, we just   #
#  overwrite it. Analysts don't need to know what the wrong value was.         #
#  Historical tracking of job/character corrections has no business value.     #
#                                                                               #
#  Why is this a dimension and not a bridge?                                   #
#  It carries its own descriptive attributes: Ordering, Category, Job,         #
#  Characters. A pure bridge only has FK columns + a surrogate key.            #
#  DIM_Principals has meaningful attributes that analysts query directly        #
#  e.g. "find all actors in this title" uses Category, not just the FK.        #
#                                                                               #
#  characters column cleaning:                                                  #
#  Silver stores characters as JSON-like strings: '["Self"]', '["Blacksmith"]' #
#  We strip the brackets and quotes to get clean values: Self, Blacksmith      #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_dim_principals (SCD Type-1)...")
print("="*60)

silver_principals = spark.table(f"{CATALOG}.silver_title_principals")

print(f"silver_title_principals row count: {silver_principals.count():,}")
print("Sample:")
silver_principals.show(3, truncate=False)

# Step 1: Clean the characters column
# Silver stores: '["Self"]' or '["Tony Stark","Iron Man"]'
# We want:        'Self'     or  'Tony Stark,Iron Man'
# Strip: ["  "]  and replace internal ","  with clean comma
principals_cleaned = (
    silver_principals
    .filter(
        F.col("tconst").isNotNull() &
        F.col("nconst").isNotNull()
    )
    .withColumn(
        "characters_clean",
        F.when(
            F.col("characters").isin("Unknown", r"\N") | F.col("characters").isNull(),
            F.lit("Unknown")
        ).otherwise(
            # Remove ["  and  "] brackets and quotes
            F.regexp_replace(
                F.regexp_replace(F.col("characters"), r'^\["', ""),
                r'"\]$', ""
            )
        )
    )
)

print("\nSample characters after cleaning:")
principals_cleaned.select("characters", "characters_clean").show(10, truncate=False)

# Step 2: Join to dim_title to get TitleKey
principals_with_titlekey = (
    principals_cleaned
    .join(dim_title, principals_cleaned.tconst == dim_title.Tconst, "left")
)

orphan_titles = principals_with_titlekey.filter(F.col("TitleKey").isNull()).count()
print(f"Orphaned tconsts: {orphan_titles:,} (expected: small number)")

# Step 3: Join to dim_name to get NameKey
principals_with_namekey = (
    principals_with_titlekey
    .join(dim_name, principals_with_titlekey.nconst == dim_name.NCONST, "left")
)

orphan_names = principals_with_namekey.filter(F.col("NameKey").isNull()).count()
print(f"Orphaned nconsts: {orphan_names:,} (expected: small number)")

# Step 4: Assign surrogate key
# Natural key for principals is (tconst + ordering) — composite PK from source
# We order by TitleKey then Ordering for determinism
w_principals = Window.orderBy("TitleKey", "ordering")

gold_dim_principals = (
    principals_with_namekey
    .filter(
        F.col("TitleKey").isNotNull() &
        F.col("NameKey").isNotNull()
    )
    .withColumn("PrincipalKey", F.row_number().over(w_principals).cast(DecimalType(10, 0)))
    .withColumn("ModifiedDate", NOW)
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select(
        F.col("PrincipalKey"),
        F.col("NameKey"),
        F.col("TitleKey"),
        F.col("ordering").cast(IntegerType()).alias("Ordering"),
        F.col("category").alias("Category"),
        F.col("job").alias("Job"),
        F.col("characters_clean").alias("Characters"),
        F.col("ModifiedDate"),
        F.col("loaded_by"),
        F.col("load_timestamp")
    )
)

gold_dim_principals.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_dim_principals")

validate_write("gold_dim_principals", expected_min=90_000_000, expected_max=100_000_000)

# Validation checks
print("\nNull checks:")
null_pk  = spark.table(f"{CATALOG}.gold_dim_principals").filter(F.col("PrincipalKey").isNull()).count()
null_nk  = spark.table(f"{CATALOG}.gold_dim_principals").filter(F.col("NameKey").isNull()).count()
null_tk  = spark.table(f"{CATALOG}.gold_dim_principals").filter(F.col("TitleKey").isNull()).count()
print(f"  Null PrincipalKey: {null_pk} (expected: 0)")
print(f"  Null NameKey:      {null_nk} (expected: 0)")
print(f"  Null TitleKey:     {null_tk} (expected: 0)")

# Category distribution — good data quality check
print("\nCategory distribution:")
spark.table(f"{CATALOG}.gold_dim_principals") \
    .groupBy("Category") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show(15, truncate=False)

print("\nSample rows:")
spark.table(f"{CATALOG}.gold_dim_principals").show(5, truncate=False)


# ============================================================================= #
#  TABLE 2: gold_fact_title_ratings                                             #
#  Source  : silver_title_ratings                                               #
#  Grain   : 1 row per title (one rating per title)                             #
#  Rows    : ~1.6M                                                              #
#                                                                               #
#  Why only 1.6M rows when there are 12.3M titles?                             #
#  Only 13% of IMDb titles have been rated. The remaining 87% exist in         #
#  dim_title but have no entry here. This is expected and important to         #
#  communicate to Power BI users — rating visuals only cover rated titles.     #
#                                                                               #
#  Measures in this fact table:                                                 #
#    AverageRating — continuous float (1.0 to 10.0), 91 distinct values        #
#    NumVotes      — integer, additive metric                                   #
#                                                                               #
#  No SCD on fact tables — facts are immutable measurements. If a rating       #
#  changes, you insert a new row or update in place. For this project          #
#  we do a full overwrite on each load (same as other tables).                 #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_fact_title_ratings...")
print("="*60)

silver_ratings = spark.table(f"{CATALOG}.silver_title_ratings")

print(f"silver_title_ratings row count: {silver_ratings.count():,}")
print("Sample:")
silver_ratings.show(3, truncate=False)

# Join to dim_title to get TitleKey
ratings_with_titlekey = (
    silver_ratings
    .filter(F.col("tconst").isNotNull())
    .join(dim_title, silver_ratings.tconst == dim_title.Tconst, "left")
)

orphan_ratings = ratings_with_titlekey.filter(F.col("TitleKey").isNull()).count()
print(f"Orphaned tconsts: {orphan_ratings:,} (expected: small number)")

# Assign surrogate key
w_ratings = Window.orderBy("TitleKey")

gold_fact_title_ratings = (
    ratings_with_titlekey
    .filter(F.col("TitleKey").isNotNull())
    .withColumn("RatingKey", F.row_number().over(w_ratings).cast(DecimalType(10, 0)))
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select(
        F.col("RatingKey"),
        F.col("TitleKey"),
        F.col("averageRating").cast("float").alias("AverageRating"),
        F.col("numVotes").cast(IntegerType()).alias("NumVotes"),
        F.col("loaded_by"),
        F.col("load_timestamp")
    )
)

gold_fact_title_ratings.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_fact_title_ratings")

validate_write("gold_fact_title_ratings", expected_min=1_500_000, expected_max=1_800_000)

# Rating distribution check
print("\nRating distribution summary:")
spark.table(f"{CATALOG}.gold_fact_title_ratings") \
    .agg(
        F.min("AverageRating").alias("MinRating"),
        F.max("AverageRating").alias("MaxRating"),
        F.round(F.avg("AverageRating"), 2).alias("AvgRating"),
        F.sum("NumVotes").alias("TotalVotes"),
        F.count("*").alias("TotalRatedTitles")
    ).show(truncate=False)

# Coverage check — what % of dim_title has a rating
total_titles = spark.table(f"{CATALOG}.gold_dim_title").filter(F.col("IsCurrent") == True).count()
rated_titles = spark.table(f"{CATALOG}.gold_fact_title_ratings").count()
coverage_pct = round(rated_titles / total_titles * 100, 1)
print(f"Rating coverage: {rated_titles:,} out of {total_titles:,} titles ({coverage_pct}%)")
print(f"Expected: ~13% based on profiling")

print("\nSample rows:")
spark.table(f"{CATALOG}.gold_fact_title_ratings").show(5, truncate=False)


# ============================================================================= #
#  TABLE 3: gold_fact_episodes                                                  #
#  Source  : silver_title_episode                                               #
#  Grain   : 1 row per episode (each episode of each TV series)                #
#  Rows    : ~9.3M                                                              #
#                                                                               #
#  silver_title_episode has two tconst columns:                                 #
#    tconst       — the episode itself (e.g. tt1234567)                        #
#    parentTconst — the series it belongs to (e.g. tt9876543)                  #
#                                                                               #
#  Both need to be resolved to TitleKey via dim_title:                         #
#    tconst       → EpisodeTitleKey  (the episode's key)                       #
#    parentTconst → SeriesTitleKey   (the parent series' key)                  #
#                                                                               #
#  Two joins to the same dim_title table — we alias dim_title twice            #
#  to avoid column name conflicts.                                              #
#                                                                               #
#  Sentinel values:                                                             #
#    SeasonNumber  = -9999 if unknown (from Silver)                            #
#    EpisodeNumber = -9999 if unknown (from Silver)                            #
# ============================================================================= #
print("\n" + "="*60)
print("Building gold_fact_episodes...")
print("="*60)

silver_episode = spark.table(f"{CATALOG}.silver_title_episode")

print(f"silver_title_episode row count: {silver_episode.count():,}")
print("Sample:")
silver_episode.show(3, truncate=False)

# We need TWO joins to dim_title:
# 1. tconst       → EpisodeTitleKey
# 2. parentTconst → SeriesTitleKey
# Alias dim_title twice to avoid column collision

dim_title_episode = dim_title.select(
    F.col("TitleKey").alias("EpisodeTitleKey"),
    F.col("Tconst").alias("episode_tconst")
)

dim_title_series = dim_title.select(
    F.col("TitleKey").alias("SeriesTitleKey"),
    F.col("Tconst").alias("series_tconst")
)

# Step 1: Join episode tconst → EpisodeTitleKey
episodes_with_episodekey = (
    silver_episode
    .filter(
        F.col("tconst").isNotNull() &
        F.col("parentTconst").isNotNull()
    )
    .join(
        dim_title_episode,
        silver_episode.tconst == dim_title_episode.episode_tconst,
        "left"
    )
)

orphan_episodes = episodes_with_episodekey.filter(F.col("EpisodeTitleKey").isNull()).count()
print(f"Orphaned episode tconsts: {orphan_episodes:,} (expected: small number)")

# Step 2: Join parentTconst → SeriesTitleKey
episodes_with_both_keys = (
    episodes_with_episodekey
    .join(
        dim_title_series,
        episodes_with_episodekey.parentTconst == dim_title_series.series_tconst,
        "left"
    )
)

orphan_series = episodes_with_both_keys.filter(F.col("SeriesTitleKey").isNull()).count()
print(f"Orphaned parentTconsts: {orphan_series:,} (expected: small number)")

# Step 3: Assign surrogate key
w_episodes = Window.orderBy("SeriesTitleKey", "EpisodeTitleKey")

gold_fact_episodes = (
    episodes_with_both_keys
    .filter(
        F.col("EpisodeTitleKey").isNotNull() &
        F.col("SeriesTitleKey").isNotNull()
    )
    .withColumn("EpisodeKey", F.row_number().over(w_episodes).cast(DecimalType(10, 0)))
    .withColumn("loaded_by", F.lit(LOADED_BY))
    .withColumn("load_timestamp", NOW)
    .select(
        F.col("EpisodeKey"),
        F.col("EpisodeTitleKey").alias("TitleKey"),
        F.col("SeriesTitleKey"),
        F.col("seasonNumber").cast(IntegerType()).alias("SeasonNumber"),
        F.col("episodeNumber").cast(IntegerType()).alias("EpisodeNumber"),
        F.col("loaded_by"),
        F.col("load_timestamp")
    )
)

gold_fact_episodes.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.gold_fact_episodes")

validate_write("gold_fact_episodes", expected_min=8_000_000, expected_max=10_000_000)

# Season distribution check
print("\nTop 10 seasons by episode count:")
spark.table(f"{CATALOG}.gold_fact_episodes") \
    .filter(F.col("SeasonNumber") > 0) \
    .groupBy("SeasonNumber") \
    .count() \
    .orderBy("SeasonNumber") \
    .show(10, truncate=False)

# Sentinel check
unknown_season  = spark.table(f"{CATALOG}.gold_fact_episodes").filter(F.col("SeasonNumber")  == -9999).count()
unknown_episode = spark.table(f"{CATALOG}.gold_fact_episodes").filter(F.col("EpisodeNumber") == -9999).count()
total_ep = spark.table(f"{CATALOG}.gold_fact_episodes").count()
print(f"Episodes with unknown SeasonNumber  (-9999): {unknown_season:,}  ({round(unknown_season/total_ep*100,1)}%)")
print(f"Episodes with unknown EpisodeNumber (-9999): {unknown_episode:,} ({round(unknown_episode/total_ep*100,1)}%)")
print(f"Expected: ~20% from profiling")

print("\nSample rows:")
spark.table(f"{CATALOG}.gold_fact_episodes").show(5, truncate=False)


# ============================================================================= #
#  FINAL VALIDATION — Phase 4 Summary                                           #
# ============================================================================= #
print("\n" + "="*60)
print("GOLD PHASE 4 — FINAL VALIDATION SUMMARY")
print("="*60)

phase4_tables = [
    ("gold_dim_principals",    90_000_000, 100_000_000),
    ("gold_fact_title_ratings", 1_500_000,   1_800_000),
    ("gold_fact_episodes",      8_000_000,  10_000_000),
]

all_pass = True
for table, min_r, max_r in phase4_tables:
    count = spark.table(f"{CATALOG}.{table}").count()
    status = "✅ PASS" if min_r <= count <= max_r else "❌ FAIL"
    print(f"  {status} | {table}: {count:,} rows")
    if status == "❌ FAIL":
        all_pass = False

print()
if all_pass:
    print("🎉 Phase 4 complete. Full Gold layer is built!")
    print("\nComplete Gold layer summary:")
    all_tables = [
        ("gold_dim_genre",           28,          35),
        ("gold_dim_profession",       40,          55),
        ("gold_dim_crew",              2,           2),
        ("gold_dim_region",          200,         300),
        ("gold_dim_language",        150,         220),
        ("gold_dim_name",     14_000_000,  16_000_000),
        ("gold_dim_title",    11_000_000,  13_000_000),
        ("gold_dim_principals",90_000_000,100_000_000),
        ("gold_bridge_title_genre", 15_000_000, 22_000_000),
        ("gold_bridge_profession",  15_000_000, 25_000_000),
        ("gold_bridge_title_crew",  10_000_000, 35_000_000),
        ("gold_bridge_akas",        45_000_000, 60_000_000),
        ("gold_fact_title_ratings",  1_500_000,  1_800_000),
        ("gold_fact_episodes",       8_000_000, 10_000_000),
    ]
    print(f"\n{'Table':<35} {'Rows':>15}")
    print("-" * 52)
    for table, _, _ in all_tables:
        count = spark.table(f"{CATALOG}.{table}").count()
        print(f"  {table:<33} {count:>15,}")
else:
    print("⚠️  One or more tables failed. Review before proceeding.")

print("""
Interview Cheat Sheet — Phase 4:

Why DIM_Principals is a dimension not a bridge:
  A bridge has only FK columns + surrogate key.
  DIM_Principals has its own attributes: Ordering, Category, Job, Characters.
  Analysts query these directly — "find all actors" uses Category attribute.

Why SCD Type-1 for DIM_Principals:
  Job titles and character names get corrected over time.
  No analyst needs to know what the wrong job title was before correction.
  Corrections are data quality fixes, not business events worth historizing.

Why characters needed cleaning:
  Silver stores characters as JSON arrays: '["Self"]', '["Tony Stark"]'
  We strip the brackets/quotes to get clean string values.
  Power BI cannot filter on '["Self"]' but can filter on 'Self'.

Why FACT_Title_Ratings has only 1.6M rows vs 12.3M titles:
  Only 13% of IMDb titles have been rated by users.
  The other 87% exist in dim_title but have no rating record.
  Always filter by TitleType in Power BI to avoid misleading averages.

Why FACT_Episodes needs two joins to dim_title:
  Each episode has TWO title references:
    tconst → the episode itself (EpisodeTitleKey)
    parentTconst → the parent series (SeriesTitleKey)
  Both are tconst values pointing to different rows in dim_title.
  We alias dim_title twice to resolve both without column collision.

Key numbers:
  gold_dim_principals:    ~96M rows  (largest Gold table)
  gold_fact_title_ratings: ~1.6M rows (only 13% of titles rated)
  gold_fact_episodes:      ~9.3M rows (~20% have unknown season/episode)
""")