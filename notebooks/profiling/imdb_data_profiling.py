# ============================================================
# IMDb Data Profiling Script
# ============================================================
# Purpose: Profile all 7 IMDb datasets to document row counts,
#          null percentages, data types, cardinality, and
#          multi-value field patterns.
#
# Output:
#   - HTML profiling reports (one per dataset)  → docs/data_profiling/
#   - Summary markdown file                     → docs/data_profiling/profiling_summary.md
#
# Usage:
#   Option A (Local Python):
#       pip install pandas ydata-profiling
#       python profiling/imdb_data_profiling.py
#
#   Option B (Databricks):
#       Copy cells into a Databricks notebook
#       %pip install ydata-profiling
#       Then run each cell
#
# Note: For large files, the script uses minimal mode in
#       ydata-profiling to avoid memory issues.
# ============================================================

import os
import pandas as pd
from datetime import datetime


# ============================================================
# STEP 1: LOAD AND BASIC PROFILING (Manual — always runs)
# ============================================================

def load_dataset(dataset_name, config):
    """Load a single IMDb TSV dataset into a pandas DataFrame."""
    filepath = os.path.join(RAW_DATA_DIR, config["file"])

    if not os.path.exists(filepath):
        filepath_gz = filepath + ".gz"
        if os.path.exists(filepath_gz):
            filepath = filepath_gz
            print(f"  📦 Loading compressed file: {filepath_gz}")
        else:
            print(f"  ❌ File not found: {filepath} (or .gz)")
            return None

    print(f"  📂 Loading {filepath}...")

    df = pd.read_csv(
        filepath,
        sep="\t",
        na_values=["\\N"],
        low_memory=False,
        dtype=str,
        quoting=3,
    )

    print(f"  ✅ Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


def profile_dataset_manual(dataset_name, df, config):
    """Generate manual profiling stats for a dataset."""

    stats = {
        "dataset": dataset_name,
        "description": config["description"],
        "file": config["file"],
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": {},
        "multi_value_analysis": {},
    }

    for col in df.columns:
        col_stats = {}
        col_stats["pandas_dtype"] = str(df[col].dtype)
        null_count = df[col].isna().sum()
        col_stats["null_count"] = int(null_count)
        col_stats["null_percentage"] = round((null_count / len(df)) * 100, 2)
        col_stats["unique_count"] = int(df[col].nunique())
        col_stats["cardinality_ratio"] = round(
            (df[col].nunique() / len(df)) * 100, 2
        )
        non_null_vals = df[col].dropna().head(5).tolist()
        col_stats["sample_values"] = non_null_vals

        if df[col].dtype == "object":
            temp_col = df[col].str.strip()
            empty_count = (temp_col == "").sum()
            none_count = (temp_col.str.lower() == "none").sum()
            unknown_count = (temp_col.str.lower() == "unknown").sum()
            col_stats["empty_string_count"] = int(empty_count)
            col_stats["none_literal_count"] = int(none_count)
            col_stats["unknown_literal_count"] = int(unknown_count)

        if df[col].dtype == "object":
            sample = df[col].dropna().head(1000)
            numeric_count = pd.to_numeric(sample, errors="coerce").notna().sum()
            col_stats["likely_numeric"] = numeric_count > (len(sample) * 0.8)

        stats["columns"][col] = col_stats

    for mv_col in config.get("multi_value_columns", []):
        if mv_col in df.columns:
            sep = config["multi_value_separator"]
            non_null = df[mv_col].dropna()
            value_counts = non_null.str.split(sep).str.len()

            mv_stats = {
                "min_values_per_row": int(value_counts.min()) if len(value_counts) > 0 else 0,
                "max_values_per_row": int(value_counts.max()) if len(value_counts) > 0 else 0,
                "avg_values_per_row": round(value_counts.mean(), 2) if len(value_counts) > 0 else 0,
                "total_distinct_values": int(
                    non_null.str.split(sep).explode().nunique()
                ),
                "top_10_values": (
                    non_null.str.split(sep)
                    .explode()
                    .value_counts()
                    .head(10)
                    .to_dict()
                ),
            }

            stats["multi_value_analysis"][mv_col] = mv_stats

    pk = config.get("primary_key")
    ck = config.get("composite_key")

    if pk:
        pk_nulls = df[pk].isna().sum()
        pk_dupes = df[pk].duplicated().sum()
        stats["primary_key"] = {
            "column": pk,
            "null_count": int(pk_nulls),
            "duplicate_count": int(pk_dupes),
            "is_valid_pk": pk_nulls == 0 and pk_dupes == 0,
        }
    elif ck:
        ck_nulls = df[ck].isna().any(axis=1).sum()
        ck_dupes = df.duplicated(subset=ck).sum()
        stats["primary_key"] = {
            "column": ck,
            "null_count": int(ck_nulls),
            "duplicate_count": int(ck_dupes),
            "is_valid_pk": ck_nulls == 0 and ck_dupes == 0,
        }

    return stats


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    print("=" * 60)
    print("IMDb Data Profiling — Starting")
    print("=" * 60)

    all_stats = {}

    for dataset_name, config in DATASETS.items():
        print(f"\n{'─' * 50}")
        print(f"📁 Processing: {dataset_name}")
        print(f"{'─' * 50}")

        df = load_dataset(dataset_name, config)
        if df is None:
            continue

        stats = profile_dataset_manual(dataset_name, df, config)
        all_stats[dataset_name] = stats

        del df

    print(f"\n{'=' * 60}")
    print("✅ Basic profiling complete!")
    print(f"{'=' * 60}")


if __name__ == "__main__":

    ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    RAW_DATA_DIR = os.path.join(ROOT_DIR, "data")
    OUTPUT_DIR = os.path.join(ROOT_DIR, "docs", "profiling")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    DATASETS = {
        "name_basics": {
            "file": "name.basics.tsv",
            "description": "Cast & crew personnel details",
            "primary_key": "nconst",
            "multi_value_columns": ["primaryProfession", "knownForTitles"],
            "multi_value_separator": ",",
            "null_marker": "\\N",
        },
        "title_basics": {
            "file": "title.basics.tsv",
            "description": "Title metadata and genres",
            "primary_key": "tconst",
            "multi_value_columns": ["genres"],
            "multi_value_separator": ",",
            "null_marker": "\\N",
        },
        "title_akas": {
            "file": "title.akas.tsv",
            "description": "Localized title names (multi-language/region)",
            "primary_key": None,
            "composite_key": ["titleId", "ordering"],
            "multi_value_columns": [],
            "multi_value_separator": None,
            "null_marker": "\\N",
        },
        "title_crew": {
            "file": "title.crew.tsv",
            "description": "Directors and writers per title",
            "primary_key": "tconst",
            "multi_value_columns": ["directors", "writers"],
            "multi_value_separator": ",",
            "null_marker": "\\N",
        },
        "title_episode": {
            "file": "title.episode.tsv",
            "description": "Series ↔ episode relationships",
            "primary_key": "tconst",
            "multi_value_columns": [],
            "multi_value_separator": None,
            "null_marker": "\\N",
        },
        "title_principals": {
            "file": "title.principals.tsv",
            "description": "Principal cast/crew credits per title",
            "primary_key": None,
            "composite_key": ["tconst", "ordering"],
            "multi_value_columns": [],
            "multi_value_separator": None,
            "null_marker": "\\N",
        },
        "title_ratings": {
            "file": "title.ratings.tsv",
            "description": "Average ratings and vote counts",
            "primary_key": "tconst",
            "multi_value_columns": [],
            "multi_value_separator": None,
            "null_marker": "\\N",
        },
    }
    main()