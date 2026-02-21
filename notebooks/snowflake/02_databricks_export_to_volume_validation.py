import os

EXPORT_PATH = "/Volumes/workspace/imdb_final_project/gold_parquet"

GOLD_TABLES = [
    "dim_genre", "dim_profession", "dim_crew", "dim_region", "dim_language",
    "dim_name", "dim_title", "dim_principals", "fact_title_ratings",
    "fact_episodes", "bridge_title_genre", "bridge_profession",
    "bridge_title_crew", "bridge_akas"
]

print(f"{'Table':<35} {'Files':>6} {'Size (MB)':>12}")
print("-" * 55)

total_mb = 0
for export_name in GOLD_TABLES:
    path = f"{EXPORT_PATH}/{export_name}"
    try:
        files = [f for f in os.listdir(path) if f.endswith(".parquet")]
        size_mb = sum(os.path.getsize(os.path.join(path, f)) for f in files) / (1024 * 1024)
        total_mb += size_mb
        print(f"  {export_name:<33} {len(files):>6} {size_mb:>11.1f}")
    except Exception as e:
        print(f"  {export_name:<33} ERROR: {e}")

print("-" * 55)
print(f"  {'TOTAL':<33} {'':>6} {total_mb:>11.1f}")


# Next step — download the Parquet files. In Databricks, go to Catalog (left sidebar) → expand workspace → imdb_final_project → click Volumes → click gold_parquet. You should see the 14 folders. You can download each folder's files from there.