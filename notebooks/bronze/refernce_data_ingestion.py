# ============================================================
# Load Reference CSVs → Delta Tables
# Run this ONCE after uploading the CSVs via the UI
# ============================================================

CATALOG = "imdb_final_project"
BASE_PATH = "/Volumes/workspace/imdb_final_project/raw_data/"

# ── Language Codes ───────────────────────────────────────────
df_lang = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(f"{BASE_PATH}/language_codes.csv")

# Verify it looks right before saving
print(f"Language rows: {df_lang.count()}")
df_lang.show(5)

df_lang.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.ref_language_codes")

# ── Region Codes ─────────────────────────────────────────────
df_region = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(f"{BASE_PATH}/region_codes.csv")

print(f"Region rows: {df_region.count()}")
df_region.show(5)

df_region.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.ref_region_codes")

print("✅ Both reference tables created successfully.")