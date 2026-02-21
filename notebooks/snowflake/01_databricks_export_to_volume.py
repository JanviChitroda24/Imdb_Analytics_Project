# databricks workspace
import time

CATALOG = "workspace.imdb_final_project"
EXPORT_PATH = "/Volumes/workspace/imdb_final_project/gold_parquet"

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.imdb_final_project.gold_parquet")
spark.sql("SHOW VOLUMES IN workspace.imdb_final_project").show()

GOLD_TABLES = [
    ("gold_dim_genre",           "dim_genre"),
    ("gold_dim_profession",      "dim_profession"),
    ("gold_dim_crew",            "dim_crew"),
    ("gold_dim_region",          "dim_region"),
    ("gold_dim_language",        "dim_language"),
    ("gold_dim_name",            "dim_name"),
    ("gold_dim_title",           "dim_title"),
    ("gold_dim_principals",      "dim_principals"),
    ("gold_fact_title_ratings",  "fact_title_ratings"),
    ("gold_fact_episodes",       "fact_episodes"),
    ("gold_bridge_title_genre",  "bridge_title_genre"),
    ("gold_bridge_profession",   "bridge_profession"),
    ("gold_bridge_title_crew",   "bridge_title_crew"),
    ("gold_bridge_akas",         "bridge_akas"),
]

print("=" * 60)
print("Exporting Gold Tables as Parquet")
print("=" * 60)

for source_table, export_name in GOLD_TABLES:
    full_table = f"{CATALOG}.{source_table}"
    output_path = f"{EXPORT_PATH}/{export_name}"
    
    print(f"\nExporting: {full_table}")
    start = time.time()
    
    df = spark.table(full_table)
    row_count = df.count()
    print(f"  Rows: {row_count:,}")
    
    if row_count > 50_000_000:
        num_files = 10
    elif row_count > 10_000_000:
        num_files = 4
    elif row_count > 1_000_000:
        num_files = 2
    else:
        num_files = 1
    
    print(f"  Files: {num_files}")
    
    df.repartition(num_files) \
        .write \
        .mode("overwrite") \
        .parquet(output_path)
    
    elapsed = time.time() - start
    print(f"  Done ({elapsed:.1f}s)")

print("\n" + "=" * 60)
print("All exports complete!")
print("=" * 60)

