# IMDb Data Profiling Summary

**Generated:** 2026-02-28 17:59:25

---

## Dataset Overview

| # | Dataset | File | Rows | Columns | Description |
|---|---------|------|------|---------|-------------|
| 1 | `name_basics` | `name.basics.tsv` | 15,122,182 | 6 | Cast & crew personnel details |
| 2 | `title_basics` | `title.basics.tsv` | 12,323,467 | 9 | Title metadata and genres |
| 3 | `title_akas` | `title.akas.tsv` | 55,294,765 | 8 | Localized title names (multi-language/region) |
| 4 | `title_crew` | `title.crew.tsv` | 12,325,801 | 3 | Directors and writers per title |
| 5 | `title_episode` | `title.episode.tsv` | 9,514,619 | 4 | Series ↔ episode relationships |
| 6 | `title_principals` | `title.principals.tsv` | 98,073,271 | 6 | Principal cast/crew credits per title |
| 7 | `title_ratings` | `title.ratings.tsv` | 1,641,397 | 3 | Average ratings and vote counts |

**Total rows across all datasets: 204,295,502**


---

## name_basics

**File:** `name.basics.tsv`  
**Rows:** 15,122,182  
**Columns:** 6

**Primary Key:** `nconst` → ✅ VALID

### Column Analysis

| Column | Null Count | Null % | Unique Count | Cardinality % | Likely Numeric | Sample Values |
|--------|-----------|--------|-------------|--------------|----------------|---------------|
| `nconst` | 0 | 0.0% | 15,122,182 | 100.0% | False | nm0000001, nm0000002, nm0000003 |
| `primaryName` | 77 | 0.0% | 11,536,665 | 76.29% | False | Fred Astaire, Lauren Bacall, Brigitte Bardot |
| `birthYear` | 14,455,133 | 95.59% | 562 | 0.0% | True | 1899, 1924, 1934 |
| `deathYear` | 14,866,668 | 98.31% | 508 | 0.0% | True | 1987, 2014, 2025 |
| `primaryProfession` | 3,031,612 | 20.05% | 26,002 | 0.17% | False | actor,miscellaneous,producer, actress,miscellaneous,soundtrack, actress,music_department,producer |
| `knownForTitles` | 1,794,731 | 11.87% | 6,235,649 | 41.24% | False | tt0072308,tt0050419,tt0027125,tt0025164, tt0037382,tt0075213,tt0038355,tt0117057, tt0057345,tt0049189,tt0056404,tt0054452 |

### Multi-Value Field Analysis

| Column | Min Values | Max Values | Avg Values | Distinct Values | Top 3 Values |
|--------|-----------|-----------|-----------|----------------|--------------|
| `primaryProfession` | 1 | 3 | 1.39 | 46 | actor, actress, producer |
| `knownForTitles` | 1 | 4 | 1.84 | 2,210,851 | tt0407423, tt4202558, tt0123338 |

**Why this matters:** These multi-value fields need to be `exploded` in the Silver layer to create proper bridge table relationships in the Gold layer.


---

## title_basics

**File:** `title.basics.tsv`  
**Rows:** 12,323,467  
**Columns:** 9

**Primary Key:** `tconst` → ✅ VALID

### Column Analysis

| Column | Null Count | Null % | Unique Count | Cardinality % | Likely Numeric | Sample Values |
|--------|-----------|--------|-------------|--------------|----------------|---------------|
| `tconst` | 0 | 0.0% | 12,323,467 | 100.0% | False | tt0000001, tt0000002, tt0000003 |
| `titleType` | 0 | 0.0% | 11 | 0.0% | False | short, short, short |
| `primaryTitle` | 25 | 0.0% | 5,514,248 | 44.75% | False | Carmencita, Le clown et ses chiens, Poor Pierrot |
| `originalTitle` | 25 | 0.0% | 5,541,670 | 44.97% | False | Carmencita, Le clown et ses chiens, Pauvre Pierrot |
| `isAdult` | 0 | 0.0% | 2 | 0.0% | True | 0, 0, 0 |
| `startYear` | 1,454,135 | 11.8% | 153 | 0.0% | True | 1894, 1892, 1892 |
| `endYear` | 12,169,828 | 98.75% | 98 | 0.0% | True | 1947, 1945, 1955 |
| `runtimeMinutes` | 7,901,398 | 64.12% | 1,001 | 0.01% | True | 1, 5, 5 |
| `genres` | 533,300 | 4.33% | 2,381 | 0.02% | False | Documentary,Short, Animation,Short, Animation,Comedy,Romance |

### Multi-Value Field Analysis

| Column | Min Values | Max Values | Avg Values | Distinct Values | Top 3 Values |
|--------|-----------|-----------|-----------|----------------|--------------|
| `genres` | 1 | 3 | 1.63 | 28 | Drama, Comedy, Talk-Show |

**Why this matters:** These multi-value fields need to be `exploded` in the Silver layer to create proper bridge table relationships in the Gold layer.


---

## title_akas

**File:** `title.akas.tsv`  
**Rows:** 55,294,765  
**Columns:** 8

**Primary Key:** `['titleId', 'ordering']` → ✅ VALID

### Column Analysis

| Column | Null Count | Null % | Unique Count | Cardinality % | Likely Numeric | Sample Values |
|--------|-----------|--------|-------------|--------------|----------------|---------------|
| `titleId` | 0 | 0.0% | 12,318,832 | 22.28% | False | tt0000001, tt0000001, tt0000001 |
| `ordering` | 0 | 0.0% | 262 | 0.0% | True | 1, 2, 3 |
| `title` | 47 | 0.0% | 7,802,435 | 14.11% | False | Carmencita, Carmencita, Carmencita |
| `region` | 12,403,891 | 22.43% | 250 | 0.0% | False | DE, US, HU |
| `language` | 18,230,097 | 32.97% | 110 | 0.0% | False | ja, ja, ja |
| `types` | 38,324,503 | 69.31% | 23 | 0.0% | False | original, imdbDisplay, imdbDisplay |
| `attributes` | 54,982,937 | 99.44% | 184 | 0.0% | False | literal title, literal title, literal English title |
| `isOriginalTitle` | 0 | 0.0% | 2 | 0.0% | True | 1, 0, 0 |

---

## title_crew

**File:** `title.crew.tsv`  
**Rows:** 12,325,801  
**Columns:** 3

**Primary Key:** `tconst` → ✅ VALID

### Column Analysis

| Column | Null Count | Null % | Unique Count | Cardinality % | Likely Numeric | Sample Values |
|--------|-----------|--------|-------------|--------------|----------------|---------------|
| `tconst` | 0 | 0.0% | 12,325,801 | 100.0% | False | tt0000001, tt0000002, tt0000003 |
| `directors` | 5,425,527 | 44.02% | 999,627 | 8.11% | False | nm0005690, nm0721526, nm0721526 |
| `writers` | 6,035,964 | 48.97% | 1,485,561 | 12.05% | False | nm0721526, nm0085156, nm0721526 |

### Multi-Value Field Analysis

| Column | Min Values | Max Values | Avg Values | Distinct Values | Top 3 Values |
|--------|-----------|-----------|-----------|----------------|--------------|
| `directors` | 1 | 528 | 1.34 | 882,095 | nm8467983, nm1203430, nm1966600 |
| `writers` | 1 | 1393 | 2.33 | 1,122,941 | nm6352729, nm0914844, nm0633202 |

**Why this matters:** These multi-value fields need to be `exploded` in the Silver layer to create proper bridge table relationships in the Gold layer.


---

## title_episode

**File:** `title.episode.tsv`  
**Rows:** 9,514,619  
**Columns:** 4

**Primary Key:** `tconst` → ✅ VALID

### Column Analysis

| Column | Null Count | Null % | Unique Count | Cardinality % | Likely Numeric | Sample Values |
|--------|-----------|--------|-------------|--------------|----------------|---------------|
| `tconst` | 0 | 0.0% | 9,514,619 | 100.0% | False | tt0031458, tt0041951, tt0042816 |
| `parentTconst` | 0 | 0.0% | 232,954 | 2.45% | False | tt32857063, tt0041038, tt0989125 |
| `seasonNumber` | 1,958,005 | 20.58% | 331 | 0.0% | True | 1, 1, 3 |
| `episodeNumber` | 1,958,005 | 20.58% | 15,929 | 0.17% | True | 9, 17, 42 |

---

## title_principals

**File:** `title.principals.tsv`  
**Rows:** 98,073,271  
**Columns:** 6

**Primary Key:** `['tconst', 'ordering']` → ✅ VALID

### Column Analysis

| Column | Null Count | Null % | Unique Count | Cardinality % | Likely Numeric | Sample Values |
|--------|-----------|--------|-------------|--------------|----------------|---------------|
| `tconst` | 0 | 0.0% | 11,144,615 | 11.36% | False | tt0000001, tt0000001, tt0000001 |
| `ordering` | 0 | 0.0% | 75 | 0.0% | True | 1, 2, 3 |
| `nconst` | 0 | 0.0% | 6,990,414 | 7.13% | False | nm1588970, nm0005690, nm0005690 |
| `category` | 0 | 0.0% | 13 | 0.0% | False | self, director, producer |
| `job` | 79,424,536 | 80.98% | 46,611 | 0.05% | False | producer, director of photography, producer |
| `characters` | 50,229,917 | 51.22% | 4,450,719 | 4.54% | False | ["Self"], ["Blacksmith"], ["Assistant"] |

### ⚠️ Data Quality Flags

| Column | Empty Strings | 'none' Literals |
|--------|--------------|-----------------|
| `job` | 0 | 1 |

---

## title_ratings

**File:** `title.ratings.tsv`  
**Rows:** 1,641,397  
**Columns:** 3

**Primary Key:** `tconst` → ✅ VALID

### Column Analysis

| Column | Null Count | Null % | Unique Count | Cardinality % | Likely Numeric | Sample Values |
|--------|-----------|--------|-------------|--------------|----------------|---------------|
| `tconst` | 0 | 0.0% | 1,641,397 | 100.0% | False | tt0000001, tt0000002, tt0000003 |
| `averageRating` | 0 | 0.0% | 91 | 0.01% | True | 5.7, 5.5, 6.5 |
| `numVotes` | 0 | 0.0% | 24,242 | 1.48% | True | 2194, 309, 2298 |

---

## Row Count Reference (for Bronze Layer Validation)

Use this table to validate Bronze ingestion matches source:

| Dataset | Source Row Count | Bronze Row Count | Match? |
|---------|----------------|-----------------|--------|
| `name_basics` | 15,122,182 | _fill after Bronze load_ | ⬜ |
| `title_basics` | 12,323,467 | _fill after Bronze load_ | ⬜ |
| `title_akas` | 55,294,765 | _fill after Bronze load_ | ⬜ |
| `title_crew` | 12,325,801 | _fill after Bronze load_ | ⬜ |
| `title_episode` | 9,514,619 | _fill after Bronze load_ | ⬜ |
| `title_principals` | 98,073,271 | _fill after Bronze load_ | ⬜ |
| `title_ratings` | 1,641,397 | _fill after Bronze load_ | ⬜ |

---

## Key Findings & Cleaning Decisions

Based on profiling, document your cleaning decisions here:

| Finding | Decision | Layer | Justification |
|---------|----------|-------|---------------|
| `\N` used as null marker across all files | Replace with proper NULL on load | Bronze/Silver | IMDb convention; pandas na_values handles this |
| `birthYear`/`deathYear` have nulls | Replace NULL → -9999 (sentinel) | Silver | Preserves row for joins; filterable in Gold |
| `genres` is comma-separated | Explode to 1 row per genre | Silver | Required for bridge_title_genre in Gold |
| `primaryProfession` is comma-separated | Explode to 1 row per profession | Silver | Required for bridge_profession in Gold |
| `directors`/`writers` are comma-separated nconst lists | Explode to 1 row per person | Silver | Required for bridge_title_crew in Gold |
| `runtimeMinutes` has nulls | Replace NULL → -9999 (sentinel) | Silver | Keeps row available for non-runtime queries |
| `primaryName` may contain 'none' literal | Flag and replace → 'Unknown' | Silver | Invalid name; don't drop row |
| `tconst`/`nconst` nulls in principals | Drop rows and log to rejected table | Silver | Cannot establish FK without key |
| _Add more as you discover them..._ | | | |