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
        # Try .gz version
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
        na_values=["\\N"],       # IMDb uses \N for null
        low_memory=False,
        dtype=str,               # Load everything as string first
        quoting=3,               # QUOTE_NONE — IMDb TSVs have no quoting
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

    # ----- Per-Column Analysis -----
    for col in df.columns:
        col_stats = {}

        # Data type (as loaded — all string, but detect actual type)
        col_stats["pandas_dtype"] = str(df[col].dtype)

        # Null analysis (remember: \N was already converted to NaN by na_values)
        null_count = df[col].isna().sum()
        col_stats["null_count"] = int(null_count)
        col_stats["null_percentage"] = round((null_count / len(df)) * 100, 2)

        # Cardinality
        col_stats["unique_count"] = int(df[col].nunique())
        col_stats["cardinality_ratio"] = round(
            (df[col].nunique() / len(df)) * 100, 2
        )

        # Sample values (first 5 non-null)
        non_null_vals = df[col].dropna().head(5).tolist()
        col_stats["sample_values"] = non_null_vals

        # Check for "none", "unknown", empty strings
        if df[col].dtype == "object":
            temp_col = df[col].str.strip()
            empty_count = (temp_col == "").sum()
            none_count = (temp_col.str.lower() == "none").sum()
            unknown_count = (temp_col.str.lower() == "unknown").sum()
            col_stats["empty_string_count"] = int(empty_count)
            col_stats["none_literal_count"] = int(none_count)
            col_stats["unknown_literal_count"] = int(unknown_count)

        # Detect if numeric
        if df[col].dtype == "object":
            sample = df[col].dropna().head(1000)
            numeric_count = pd.to_numeric(sample, errors="coerce").notna().sum()
            col_stats["likely_numeric"] = numeric_count > (len(sample) * 0.8)

        stats["columns"][col] = col_stats

    # ----- Multi-Value Field Analysis -----
    for mv_col in config.get("multi_value_columns", []):
        if mv_col in df.columns:
            sep = config["multi_value_separator"]
            non_null = df[mv_col].dropna()

            # Count values per cell
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

    # ----- Primary Key Validation -----
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
# STEP 2: YDATA-PROFILING (HTML Reports)
# ============================================================

def generate_ydata_report(dataset_name, df, config):
    """Generate ydata-profiling HTML report (minimal mode for large datasets)."""
    try:
        from ydata_profiling import ProfileReport
    except ImportError:
        print("  ⚠️  ydata-profiling not installed. Skipping HTML report.")
        print("     Install with: pip install ydata-profiling")
        return

    print(f"  📊 Generating ydata-profiling report for {dataset_name}...")

    # Use minimal=True for large datasets (>1M rows) to avoid OOM
    minimal = len(df) > 1_000_000

    if minimal:
        print(f"  ⚡ Using MINIMAL mode ({len(df):,} rows is large)")
        # Sample for profiling to keep it manageable
        sample_size = min(500_000, len(df))
        df_sample = df.sample(n=sample_size, random_state=42)
        title_suffix = f" (sampled {sample_size:,}/{len(df):,} rows)"
    else:
        df_sample = df
        title_suffix = ""

    profile = ProfileReport(
        df_sample,
        title=f"IMDb Profiling: {dataset_name}{title_suffix}",
        minimal=minimal,
        explorative=True,
        correlations=None if minimal else {"auto": {"calculate": True}},
    )

    output_path = os.path.join(OUTPUT_DIR, f"{dataset_name}_profile.html")
    profile.to_file(output_path)
    print(f"  ✅ Saved: {output_path}")


# ============================================================
# STEP 3: GENERATE SUMMARY MARKDOWN
# ============================================================

def generate_summary_markdown(all_stats):
    """Generate a comprehensive profiling summary as markdown."""

    lines = []
    lines.append("# IMDb Data Profiling Summary")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    lines.append("---\n")

    # ---- Overview Table ----
    lines.append("## Dataset Overview\n")
    lines.append("| # | Dataset | File | Rows | Columns | Description |")
    lines.append("|---|---------|------|------|---------|-------------|")
    total_rows = 0
    for i, (name, stats) in enumerate(all_stats.items(), 1):
        lines.append(
            f"| {i} | `{name}` | `{stats['file']}` | "
            f"{stats['row_count']:,} | {stats['column_count']} | "
            f"{stats['description']} |"
        )
        total_rows += stats["row_count"]
    lines.append(f"\n**Total rows across all datasets: {total_rows:,}**\n")

    # ---- Per-Dataset Deep Dive ----
    for name, stats in all_stats.items():
        lines.append(f"\n---\n")
        lines.append(f"## {name}\n")
        lines.append(f"**File:** `{stats['file']}`  ")
        lines.append(f"**Rows:** {stats['row_count']:,}  ")
        lines.append(f"**Columns:** {stats['column_count']}\n")

        # Primary Key Validation
        if "primary_key" in stats:
            pk = stats["primary_key"]
            status = "✅ VALID" if pk["is_valid_pk"] else "❌ INVALID"
            lines.append(f"**Primary Key:** `{pk['column']}` → {status}")
            if not pk["is_valid_pk"]:
                lines.append(f"  - Null count: {pk['null_count']:,}")
                lines.append(f"  - Duplicate count: {pk['duplicate_count']:,}")
            lines.append("")

        # Column-Level Stats
        lines.append("### Column Analysis\n")
        lines.append(
            "| Column | Null Count | Null % | Unique Count | "
            "Cardinality % | Likely Numeric | Sample Values |"
        )
        lines.append(
            "|--------|-----------|--------|-------------|"
            "--------------|----------------|---------------|"
        )
        for col, cs in stats["columns"].items():
            sample = ", ".join([str(v) for v in cs.get("sample_values", [])[:3]])
            numeric = cs.get("likely_numeric", "N/A")
            lines.append(
                f"| `{col}` | {cs['null_count']:,} | {cs['null_percentage']}% | "
                f"{cs['unique_count']:,} | {cs['cardinality_ratio']}% | "
                f"{numeric} | {sample} |"
            )

        # Empty String / "none" Literal Check
        has_issues = False
        for col, cs in stats["columns"].items():
            empty = cs.get("empty_string_count", 0)
            none_lit = cs.get("none_literal_count", 0)
            if empty > 0 or none_lit > 0:
                if not has_issues:
                    lines.append("\n### ⚠️ Data Quality Flags\n")
                    lines.append("| Column | Empty Strings | 'none' Literals |")
                    lines.append("|--------|--------------|-----------------|")
                    has_issues = True
                lines.append(f"| `{col}` | {empty:,} | {none_lit:,} |")

        # Multi-Value Analysis
        if stats["multi_value_analysis"]:
            lines.append("\n### Multi-Value Field Analysis\n")
            lines.append(
                "| Column | Min Values | Max Values | Avg Values | "
                "Distinct Values | Top 3 Values |"
            )
            lines.append(
                "|--------|-----------|-----------|-----------|"
                "----------------|--------------|"
            )
            for mv_col, mv in stats["multi_value_analysis"].items():
                top3 = list(mv["top_10_values"].keys())[:3]
                top3_str = ", ".join(top3)
                lines.append(
                    f"| `{mv_col}` | {mv['min_values_per_row']} | "
                    f"{mv['max_values_per_row']} | {mv['avg_values_per_row']} | "
                    f"{mv['total_distinct_values']:,} | {top3_str} |"
                )

            lines.append("\n**Why this matters:** These multi-value fields need to be "
                         "`exploded` in the Silver layer to create proper bridge table "
                         "relationships in the Gold layer.\n")

    # ---- Row Count Reference Table ----
    lines.append("\n---\n")
    lines.append("## Row Count Reference (for Bronze Layer Validation)\n")
    lines.append("Use this table to validate Bronze ingestion matches source:\n")
    lines.append("| Dataset | Source Row Count | Bronze Row Count | Match? |")
    lines.append("|---------|----------------|-----------------|--------|")
    for name, stats in all_stats.items():
        lines.append(
            f"| `{name}` | {stats['row_count']:,} | _fill after Bronze load_ | ⬜ |"
        )

    # ---- Key Findings / Cleaning Decisions ----
    lines.append("\n---\n")
    lines.append("## Key Findings & Cleaning Decisions\n")
    lines.append("Based on profiling, document your cleaning decisions here:\n")
    lines.append("| Finding | Decision | Layer | Justification |")
    lines.append("|---------|----------|-------|---------------|")
    lines.append("| `\\N` used as null marker across all files | Replace with proper NULL on load | Bronze/Silver | IMDb convention; pandas na_values handles this |")
    lines.append("| `birthYear`/`deathYear` have nulls | Replace NULL → -9999 (sentinel) | Silver | Preserves row for joins; filterable in Gold |")
    lines.append("| `genres` is comma-separated | Explode to 1 row per genre | Silver | Required for bridge_title_genre in Gold |")
    lines.append("| `primaryProfession` is comma-separated | Explode to 1 row per profession | Silver | Required for bridge_profession in Gold |")
    lines.append("| `directors`/`writers` are comma-separated nconst lists | Explode to 1 row per person | Silver | Required for bridge_title_crew in Gold |")
    lines.append("| `runtimeMinutes` has nulls | Replace NULL → -9999 (sentinel) | Silver | Keeps row available for non-runtime queries |")
    lines.append("| `primaryName` may contain 'none' literal | Flag and replace → 'Unknown' | Silver | Invalid name; don't drop row |")
    lines.append("| `tconst`/`nconst` nulls in principals | Drop rows and log to rejected table | Silver | Cannot establish FK without key |")
    lines.append("| _Add more as you discover them..._ | | | |")

    return "\n".join(lines)


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

        # Load
        df = load_dataset(dataset_name, config)
        if df is None:
            continue

        # Manual profiling
        stats = profile_dataset_manual(dataset_name, df, config)
        all_stats[dataset_name] = stats

        # ydata-profiling HTML report
        generate_ydata_report(dataset_name, df, config)

        # Free memory
        del df

    # Generate summary markdown
    print(f"\n{'─' * 50}")
    print("📝 Generating profiling summary markdown...")
    summary_md = generate_summary_markdown(all_stats)

    summary_path = os.path.join(OUTPUT_DIR, "profiling_summary.md")
    with open(summary_path, "w") as f:
        f.write(summary_md)
    print(f"✅ Saved: {summary_path}")

    print(f"\n{'=' * 60}")
    print("✅ Profiling complete!")
    print(f"   HTML reports: {OUTPUT_DIR}/<dataset>_profile.html")
    print(f"   Summary:      {summary_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":

    # -----------------------------------------------------------
    # CONFIGURATION — Update these paths to match your setup
    # -----------------------------------------------------------

    # Project root (IMDB_Project) so paths are consistent
    ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    # Where your raw .tsv files are stored (unzipped)
    RAW_DATA_DIR = os.path.join(ROOT_DIR, "data")

    # Where profiling outputs will be saved
    OUTPUT_DIR = os.path.join(ROOT_DIR, "docs", "profiling")

    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # -----------------------------------------------------------
    # DATASET DEFINITIONS
    # -----------------------------------------------------------
    # Each entry: (filename, description, expected_key, multi_value_columns)

    DATASETS = {
        "name_basics": {
            "file": "name.basics.tsv",
            "description": "Cast & crew personnel details",
            "primary_key": "nconst",
            "multi_value_columns": ["primaryProfession", "knownForTitles"],
            "multi_value_separator": ",",
            "null_marker": "\\N",  # IMDb uses \N for nulls
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
            "primary_key": None,  # Composite: titleId + ordering
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
            "primary_key": None,  # Composite: tconst + ordering
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
