# IMDb Analytics Platform

## Overview

An end-to-end data engineering project that builds a scalable analytics platform on top of IMDb's non-commercial datasets. The platform ingests, profiles, cleanses, transforms, and models ~7 raw TSV datasets into a query-optimized star schema, following the **Medallion Architecture (Bronze → Silver → Gold)**, and serves curated data to interactive dashboards for business intelligence.

---

## Problem Statement

IMDb provides rich, publicly available datasets covering movies, TV series, cast, crew, ratings, and regional release information. However, the raw data is:

- **Fragmented** — spread across 7 separate TSV files with no unified key structure  
- **Dirty** — contains nulls (`\N`), inconsistent formatting, and denormalized multi-value fields (e.g., comma-separated genres, pipe-delimited professions)  
- **Unmodeled** — no schema design exists for analytical queries; raw files are not joinable without transformation  
- **Missing context** — region and language codes lack human-readable descriptions  

This makes it impossible for analysts to directly answer business questions like *"Who are the top-rated directors by genre?"* or *"How does episode count correlate with series ratings?"* without significant manual effort.

---

## Objective

Design and implement a **production-style data pipeline** that:

1. **Ingests** raw IMDb datasets into a Bronze layer (raw, immutable landing zone)  
2. **Cleans & Validates** data in a Silver layer (standardized, deduplicated, type-cast, null-handled)  
3. **Models** curated, business-ready tables in a Gold layer (star schema with facts & dimensions)  
4. **Loads** Gold tables into **Snowflake** for performant analytical queries  
5. **Visualizes** key business metrics via **Power BI** dashboards  
6. **Ensures traceability** — every row in the Gold layer can be traced back to its source with audit columns (source file, load timestamp, loaded_by)

---

## Architecture

```
Raw TSV Files (IMDb)
        │
        ▼
┌──────────────────┐
│   BRONZE LAYER   │  ← Raw ingestion, no transformation, schema-on-read
│   (Landing Zone) │     Databricks / Azure Data Factory
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   SILVER LAYER   │  ← Data cleansing, type casting, null handling,
│   (Staging)      │     deduplication, validation checks, column unification
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│    GOLD LAYER    │  ← Star schema (Facts + Dimensions), business logic,
│  (Integration)   │     aggregations, enriched with region/language descriptions
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│    SNOWFLAKE     │  ← Optimized warehouse for analytical queries
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│    POWER BI      │  ← Dashboards & Visualizations
└──────────────────┘
```

---

## Datasets

Find the datasets online at: https://developer.imdb.com/non-commercial-datasets/

| # | File | Description | Key Fields |
|---|------|-------------|------------|
| 1 | `name.basics.tsv.gz` | Cast & crew personnel details | nconst, primaryName, birthYear, primaryProfession, knownForTitles |
| 2 | `title.basics.tsv.gz` | Title metadata and genres | tconst, titleType, primaryTitle, startYear, runtimeMinutes, genres |
| 3 | `title.akas.tsv.gz` | Localized title names (multi-language) | titleId, ordering, title, region, language |
| 4 | `title.crew.tsv.gz` | Directors and writers per title | tconst, directors, writers |
| 5 | `title.episode.tsv.gz` | Series ↔ episode relationships | tconst, parentTconst, seasonNumber, episodeNumber |
| 6 | `title.principals.tsv.gz` | Principal cast/crew credits | tconst, nconst, category, job, characters |
| 7 | `title.ratings.tsv.gz` | Average ratings and vote counts | tconst, averageRating, numVotes |

**Supplementary Reference Data:**
- **Region Codes** → Country descriptions (ISO 3166) — https://help.imdb.com/article/contribution/other-submission-guides/country-codes/G99K4LFRMSC37DCN#
- **Language Codes** → Language descriptions (ISO 639) — https://en.wikipedia.org/wiki/List_of_ISO_639_language_codes

---

## Business Requirements

The platform enables business analysts to:

| # | Requirement | Gold Layer Table(s) Involved |
|---|-------------|------------------------------|
| 1 | Get primary and secondary professions for any personnel | dim_person |
| 2 | Find personnel with multiple professions | dim_person |
| 3 | List genres for a title; find titles by genre | dim_genre, bridge_title_genre |
| 4 | Find all titles released in a given year | dim_title / fact_title |
| 5 | Track and analyze movie runtime trends by release year | fact_title |
| 6 | Classify and list adult vs. non-adult titles | dim_title |
| 7 | List all languages associated with a title | dim_language, fact_title_aka |
| 8 | List all regions where a title was released | dim_region, fact_title_aka |
| 9 | Identify directors and writers per title (popularity ranking) | bridge_title_director, bridge_title_writer |
| 10 | Get episode count per season for a given series | fact_episode |
| 11 | Find all cast/crew types for a given title | fact_principals |
| 12 | List jobs involved in a given title | fact_principals |
| 13 | List characters in a given title | fact_principals |
| 14 | Find top-rated movies by year and genre | fact_title + dim_genre + fact_ratings |

---

## Data Quality & Validation Strategy

- **Row count reconciliation** at every layer transition (Bronze → Silver → Gold)
- **Null handling** — `\N` values identified, categorized, and treated (drop with justification or default)
- **Deduplication** — duplicate detection on natural keys
- **Schema enforcement** — type casting and constraint checks at Silver layer
- **Dropped row logging** — every filtered/dropped row is documented with reason before exclusion
- **Audit columns** on every table:
  - `source_file` — origin dataset
  - `load_timestamp` — when the row was loaded
  - `loaded_by` — pipeline or user identifier

---

## Dimensional Model (Gold Layer — Star Schema)

### Fact Tables
- `fact_title_ratings` — ratings and vote metrics per title
- `fact_principals` — cast/crew assignments per title
- `fact_episode` — episode-level data linked to parent series
- `fact_title_aka` — regional/language release information

### Dimension Tables
- `dim_title` — title metadata (type, year, runtime, adult flag)
- `dim_person` — personnel details (name, birth year, professions)
- `dim_genre` — genre lookup
- `dim_region` — region code + description
- `dim_language` — language code + description
- `dim_date` — date dimension (optional, for time-based analysis)

### Bridge Tables
- `bridge_title_genre` — many-to-many: title ↔ genre
- `bridge_title_director` — many-to-many: title ↔ director
- `bridge_title_writer` — many-to-many: title ↔ writer

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Ingestion & Orchestration | Azure Data Factory |
| Processing & Transformation | Databricks (PySpark) |
| Data Warehouse | Snowflake |
| Visualization | Power BI |
| Version Control | Git / GitHub |
| Data Profiling | Python (ydata-profiling) |
| Documentation | Mapping Document (Excel), Data Profiling Reports |

---

## Dashboards (Power BI)

1. **Movie Insights** — top-rated movies, runtime distribution, adult vs. non-adult breakdown
2. **Crew Analysis** — most prolific directors, writers, actors; crew diversity per title
3. **Rating Trends** — rating distribution over years, genre-wise rating comparison
4. **Series Analysis** — season/episode count vs. ratings correlation
5. **Regional Release Analysis** — geographic distribution of title releases, language coverage

---

## Project Deliverables

- [ ] Data Profiling Report (ydata-profiling)
- [ ] Source-to-Target Mapping Document
- [ ] Dimensional Model (ER Diagram)
- [ ] Databricks Notebooks (Bronze, Silver, Gold pipelines)
- [ ] Azure Data Factory Pipeline Definitions
- [ ] Snowflake DDL & Load Scripts
- [ ] Power BI Dashboard (.pbix)
- [ ] Row Count Reconciliation Report
- [ ] Git Repository with team contributions

---

## How to Run

### 1) Setup
1. Install Python 3.9+  
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### 2) Download IMDb datasets
1. Download the `.tsv.gz` files from the IMDb non-commercial datasets page  
2. Unzip them into `data/raw/` so the files look like:
   - `data/raw/name.basics.tsv`
   - `data/raw/title.basics.tsv`
   - `data/raw/title.akas.tsv`
   - `data/raw/title.crew.tsv`
   - `data/raw/title.episode.tsv`
   - `data/raw/title.principals.tsv`
   - `data/raw/title.ratings.tsv`

## Author

**Janvi Chitroda**  
MS Information Systems — Northeastern University  
[LinkedIn](https://linkedin.com/in/janvi-chitroda) | [GitHub](https://github.com/JanviChitroda24)
