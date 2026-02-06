# IMDb Data Profiling — Analysis & Findings

> **Purpose:** This document analyzes the profiling results from all 7 IMDb datasets (204M+ total rows) and documents the key findings, data quality issues, risks, and cleaning decisions that will drive Bronze → Silver → Gold transformations.

---

## Executive Summary

Profiling was run across 7 IMDb datasets totaling **204,295,502 rows**. The data is heavily sparse — several columns have 50-98% NULL rates, multi-value fields are stored as comma-separated strings requiring explosion, and the `\N` marker is used universally for NULLs. All primary keys validated successfully (no nulls, no duplicates), which is a strong foundation. However, significant transformation work is needed in the Silver layer to make this data analytically useful.

---

## Dataset Scale Overview

| Dataset | Rows | % of Total | Risk Level |
|---------|------|-----------|------------|
| `title_principals` | 98,073,271 | **48.0%** | 🔴 High — largest table; drives performance decisions |
| `title_akas` | 55,294,765 | **27.1%** | 🟠 Medium — large with high null rates in region/language |
| `name_basics` | 15,122,182 | **7.4%** | 🟡 Low — manageable but 95% null on birthYear |
| `title_crew` | 12,325,801 | **6.0%** | 🟡 Low — but multi-value explode will multiply rows significantly |
| `title_basics` | 12,323,467 | **6.0%** | 🟡 Low — core dimension; 64% null on runtimeMinutes |
| `title_episode` | 9,514,619 | **4.7%** | 🟢 Clean — straightforward with minor null handling |
| `title_ratings` | 1,641,397 | **0.8%** | 🟢 Cleanest — zero nulls, simple structure |
| **TOTAL** | **204,295,502** | **100%** | |

**Key Insight:** `title_principals` alone is 98M rows — nearly half the entire dataset. Any transformation on this table (joins, lookups) must be performance-optimized. This is the table most likely to cause Spark OOM errors or long runtimes if not partitioned correctly.

---

## Finding 1: Primary Keys Are Clean Across All Datasets

| Dataset | Primary Key | Nulls | Duplicates | Status |
|---------|------------|-------|------------|--------|
| name_basics | `nconst` | 0 | 0 | ✅ Valid |
| title_basics | `tconst` | 0 | 0 | ✅ Valid |
| title_akas | `titleId` + `ordering` | 0 | 0 | ✅ Valid |
| title_crew | `tconst` | 0 | 0 | ✅ Valid |
| title_episode | `tconst` | 0 | 0 | ✅ Valid |
| title_principals | `tconst` + `ordering` | 0 | 0 | ✅ Valid |
| title_ratings | `tconst` | 0 | 0 | ✅ Valid |

**Why This Matters:** Clean primary keys mean we can trust referential integrity from the source. No deduplication is needed at Bronze. Surrogate key generation in Gold can proceed without worrying about source key collisions.

**Interview Angle:** If asked "Did you encounter any data quality issues with keys?", the answer is: *"All 7 datasets had valid primary keys with zero nulls and zero duplicates. This gave us confidence to build surrogate keys in Gold directly from natural keys without deduplication logic."*

---

## Finding 2: Extreme Null Sparsity in Critical Columns

Several columns have NULL rates above 50%, which directly impacts how we model and query the data.

| Dataset | Column | Null % | Impact |
|---------|--------|--------|--------|
| name_basics | `birthYear` | **95.59%** | Only ~667K out of 15M people have a birth year |
| name_basics | `deathYear` | **98.31%** | Expected — most people are alive; but also most just don't have data |
| title_basics | `endYear` | **98.75%** | Only applies to series that have ended |
| title_basics | `runtimeMinutes` | **64.12%** | Major gap — runtime analysis only possible for ~4.4M titles |
| title_principals | `job` | **80.98%** | Most principal credits don't have a specific job description |
| title_principals | `characters` | **51.22%** | About half the credits don't list character names |
| title_crew | `directors` | **44.02%** | ~5.4M titles have no director listed |
| title_crew | `writers` | **48.97%** | ~6M titles have no writer listed |
| title_akas | `types` | **69.31%** | Most alternate titles don't have a type classification |
| title_akas | `attributes` | **99.44%** | Almost entirely empty — minimal analytical value |
| title_akas | `language` | **32.97%** | 1 in 3 alternate titles has no language code |
| title_akas | `region` | **22.43%** | 1 in 5 alternate titles has no region code |
| title_episode | `seasonNumber` | **20.58%** | ~2M episodes lack season information |
| title_episode | `episodeNumber` | **20.58%** | Same ~2M episodes lack episode numbers |

### Decisions Driven by This Finding

**For `birthYear` (95.59% null):**
- Replace NULL → -9999 (sentinel value)
- Rationale: If we use NULL, inner joins on birth year would drop 95% of personnel. Sentinel value preserves the row and allows filtering in dashboards (WHERE birthYear != -9999)
- Alternative considered: Keep as NULL and use LEFT JOINs everywhere — rejected because it forces every downstream query to handle NULLs explicitly

**For `runtimeMinutes` (64.12% null):**
- Replace NULL → -9999 (sentinel value)
- Rationale: Runtime analysis is a business requirement. We need to keep all title rows available for other queries (genre analysis, rating analysis) even if runtime is missing
- Dashboard impact: Runtime visuals must filter out -9999 values to avoid skewing averages

**For `job` (80.98% null) and `characters` (51.22% null):**
- Replace NULL → "Unknown"
- Rationale: These are descriptive attributes, not keys. "Unknown" is more readable than NULL in dashboards and doesn't require special handling in BI tools

**For `attributes` (99.44% null):**
- Keep in Silver but do NOT model in Gold
- Rationale: A column that is 99.44% empty provides almost no analytical value. Including it in Gold would add storage cost with no business benefit

---

## Finding 3: Multi-Value Fields Require Explosion

Four source columns contain comma-separated values that must be split into individual rows in the Silver layer. This is the most transformation-heavy aspect of the pipeline.

| Dataset | Column | Distinct Values | Avg Per Row | Max Per Row | Silver Row Multiplier |
|---------|--------|----------------|-------------|-------------|----------------------|
| name_basics | `primaryProfession` | 46 | 1.39 | 3 | ~1.4x on non-null rows |
| title_basics | `genres` | 28 | 1.63 | 3 | ~1.6x on non-null rows |
| title_crew | `directors` | 882,095 | 1.34 | **528** | ~1.3x average, but extreme outliers |
| title_crew | `writers` | 1,122,941 | 2.33 | **1,393** | ~2.3x average, but extreme outliers |
| name_basics | `knownForTitles` | 2,210,851 | 1.84 | 4 | Not exploded (reference only) |

### Critical Observations

**`directors` has a max of 528 values per row.** This means one title has 528 directors listed. After explosion, this single row becomes 528 rows. This is almost certainly a data quality issue in the source — a documentary or compilation with an unusually large credits list. We should investigate and potentially cap or flag extreme outliers.

**`writers` has a max of 1,393 values per row.** Even more extreme. One title claims to have 1,393 writers. This will cause a massive row explosion in Silver. We need to either cap this or handle it as an expected edge case.

**`genres` is bounded (28 distinct, max 3 per title).** This is clean and predictable. The bridge_title_genre table will be well-behaved.

**`primaryProfession` is bounded (46 distinct, max 3 per person).** Also clean. The bridge_profession table will be manageable.

### Estimated Silver Row Counts After Explosion

| Table | Bronze Rows | Est. Silver Rows | Reason |
|-------|------------|-----------------|--------|
| silver_name_basics | 15,122,182 | ~16.8M | Profession explode (1.39x on ~12M non-null rows) |
| silver_title_basics | 12,323,467 | ~19.2M | Genre explode (1.63x on ~11.8M non-null rows) |
| silver_title_crew | 12,325,801 | ~25-30M | Director + writer explode combined |
| silver_title_akas | 55,294,765 | ~55.3M | No explosion — 1:1 pass-through |
| silver_title_episode | 9,514,619 | ~9.5M | No explosion — 1:1 pass-through |
| silver_title_principals | 98,073,271 | ~98M | No explosion — 1:1 pass-through |
| silver_title_ratings | 1,641,397 | ~1.6M | No explosion — 1:1 pass-through |

**Interview Angle:** If asked "Why did row counts increase between Bronze and Silver?", the answer is: *"Multi-value fields like genres and professions were stored as comma-separated strings in the source. I exploded them in Silver to normalize the data — for example, a title with 'Action,Comedy,Drama' becomes 3 rows in Silver, one per genre. This was necessary because the Gold layer uses bridge tables for many-to-many relationships, which require one row per relationship pair."*

---

## Finding 4: Cardinality Analysis Reveals Dimension vs Fact Patterns

Cardinality (unique count vs total rows) helps identify which columns are suitable for dimensions vs facts.

### Low Cardinality → Dimension Candidates

| Column | Unique Values | Good Dimension? |
|--------|--------------|----------------|
| `genres` (exploded) | 28 | ✅ Perfect — small, fixed lookup table |
| `primaryProfession` (exploded) | 46 | ✅ Perfect — small, fixed lookup table |
| `titleType` | 11 | ✅ Could be a dimension, but kept as attribute in dim_title |
| `isAdult` | 2 | ✅ Boolean flag — attribute in dim_title |
| `category` (principals) | 13 | ✅ Small set — actor, director, producer, etc. |
| `region` | 250 | ✅ Reference dimension (ISO 3166) |
| `language` | 110 | ✅ Reference dimension (ISO 639) |

### High Cardinality → Fact/Key Candidates

| Column | Unique Values | Role |
|--------|--------------|------|
| `nconst` | 15,122,182 | Natural key for personnel (100% unique) |
| `tconst` (title_basics) | 12,323,467 | Natural key for titles (100% unique) |
| `primaryName` | 11,536,665 | Attribute — not unique (name collisions exist) |
| `averageRating` | 91 | Measure — continuous values from 1.0 to 10.0 |
| `numVotes` | 24,242 | Measure — additive metric |

**Key Insight:** `primaryName` has 11.5M unique values out of 15.1M rows — meaning ~3.6M names are shared by multiple people. This confirms that `nconst` (not name) must be used as the key. Name alone is not sufficient for uniqueness.

---

## Finding 5: Data Quality Flags

### 5.1 'none' Literal in title_principals.job

Profiling detected **1 row** where `job` contains the literal string "none" instead of being NULL. While this is a minor issue (1 row out of 98M), it signals that data quality checks should look for string literals like "none", "null", "n/a", "N/A", and "" (empty string) in addition to actual NULLs.

**Decision:** In Silver, standardize all of these variants → "Unknown" for string columns.

### 5.2 primaryName Has 77 NULLs

Out of 15.1M personnel records, 77 have no name at all. These are valid records (they have an nconst) but lack a primary name.

**Decision:** Replace NULL primaryName → "Unknown" in Silver. Do not drop these rows — they may still appear in bridge tables and fact tables.

### 5.3 title and originalTitle Have 25 NULLs Each

25 titles in title_basics have no primaryTitle or originalTitle. Again, tiny numbers but the pipeline must handle them.

**Decision:** Replace NULL → "Unknown" in Silver.

### 5.4 characters Column Contains JSON-like Arrays

Sample values for `characters` in title_principals show: `["Self"]`, `["Blacksmith"]`, `["Assistant"]`. This is not a simple string — it's a JSON array stored as a string. Silver layer must strip the brackets and quotes to extract clean character names.

**Decision:** In Silver, parse the JSON array and extract character names. If multiple characters, keep as comma-separated or explode depending on Gold requirements.

---

## Finding 6: Cross-Dataset Join Feasibility

The dimensional model requires joining datasets together. Profiling confirms the join keys exist and align:

| Join | Left Table | Right Table | Join Key | Left Unique | Right Unique | Join Type |
|------|-----------|-------------|----------|-------------|-------------|-----------|
| Title → Ratings | title_basics (12.3M) | title_ratings (1.6M) | `tconst` | 12.3M | 1.6M | LEFT JOIN — only 13% of titles have ratings |
| Title → Episodes | title_basics (12.3M) | title_episode (9.5M) | `tconst` | 12.3M | 9.5M | LEFT JOIN — episodes are subset of titles |
| Title → Akas | title_basics (12.3M) | title_akas (55.3M) | `tconst`/`titleId` | 12.3M | 12.3M unique titleIds | LEFT JOIN — many akas per title |
| Title → Crew | title_basics (12.3M) | title_crew (12.3M) | `tconst` | 12.3M | 12.3M | 1:1 — same set of titles |
| Title → Principals | title_basics (12.3M) | title_principals (98M) | `tconst` | 12.3M | 11.1M unique tconsts | LEFT JOIN — ~10 principals per title avg |
| Person → Principals | name_basics (15.1M) | title_principals (98M) | `nconst` | 15.1M | 7.0M unique nconsts | LEFT JOIN — not all people are in principals |

### Key Observations

**Only 13% of titles have ratings.** This means `gold_fact_title_ratings` will have 1.6M rows, not 12.3M. Dashboards filtering by rating will only show a fraction of all titles. This should be communicated to business users.

**title_principals averages ~10 rows per title** (98M rows / 11.1M unique tconsts). This is the fan-out that makes this table so large. Gold dim_principals will inherit this volume.

**Not all people appear in principals.** 15.1M people exist in name_basics, but only 7.0M unique nconsts appear in title_principals. The remaining ~8M people are known to IMDb but don't have principal credits. They may appear in crew (directors/writers) or have other minor roles.

---

## Finding 7: title_akas Specific Issues

### Region Nulls (22.43%) and Language Nulls (32.97%)

The bridge_akas table in Gold links titles to regions and languages. With 22% of regions and 33% of languages being NULL, we need a strategy:

**Decision:** Create an "Unknown" entry in both `gold_dim_region` (RegionKey = -9999) and `gold_dim_language` (LanguageKey = -9999). Map all NULL region/language values to these Unknown keys. This preserves referential integrity while clearly marking missing data.

### attributes Column (99.44% null)

This column provides almost no information. Out of 55M rows, only ~310K have an attribute value, and those are mostly "literal title" or "literal English title."

**Decision:** Carry through to Silver for completeness, but do NOT model as a Gold column. Storage cost is not justified for a 99.44% empty field.

---

## Finding 8: title_crew Outlier Risk

The `directors` column has a **max of 528 values per row** and `writers` has a **max of 1,393 values per row**. After explosion in Silver, these extreme rows will generate hundreds or thousands of rows each.

### Risk Assessment

A single title with 1,393 writers will produce 1,393 rows in `silver_title_crew` and 1,393 rows in `gold_bridge_title_crew`. While this is technically correct (the data says what it says), it could:

- Skew "top writers" analysis (writers on this title get counted once each)
- Create performance hotspots during joins
- Indicate data quality issues in the source

### Recommendation

- **Do not drop or cap these rows** — they represent real IMDb data
- **Flag them** — add a column or log entry noting titles with >50 directors or >100 writers
- **Investigate** — query these specific titles to understand if they're compilations, anthologies, or data errors
- **Filter in dashboards** — Power BI visuals should have reasonable top-N limits to avoid outlier distortion

---

## Summary of All Cleaning Decisions

| # | Finding | Column(s) | Decision | Layer | Sentinel/Default |
|---|---------|-----------|----------|-------|-----------------|
| 1 | `\N` null marker | All columns | Handled by read options (nullValue="\\N") | Bronze | N/A |
| 2 | birthYear 95.59% null | birthYear, deathYear | NULL → -9999 | Silver | -9999 |
| 3 | runtimeMinutes 64.12% null | runtimeMinutes | NULL → -9999 | Silver | -9999 |
| 4 | startYear/endYear nulls | startYear, endYear | NULL → -9999 | Silver | -9999 |
| 5 | seasonNumber/episodeNumber 20.58% null | seasonNumber, episodeNumber | NULL → -1 | Silver | -1 |
| 6 | primaryName 77 nulls | primaryName | NULL → "Unknown" | Silver | "Unknown" |
| 7 | primaryTitle/originalTitle 25 nulls | primaryTitle, originalTitle | NULL → "Unknown" | Silver | "Unknown" |
| 8 | job 80.98% null | job | NULL → "Unknown" | Silver | "Unknown" |
| 9 | characters 51.22% null | characters | NULL → "Unknown"; strip JSON brackets | Silver | "Unknown" |
| 10 | region 22.43% null | region | NULL → "Unknown" → maps to Unknown dim key | Silver/Gold | "Unknown" |
| 11 | language 32.97% null | language | NULL → "Unknown" → maps to Unknown dim key | Silver/Gold | "Unknown" |
| 12 | genres comma-separated | genres | Explode → 1 row per genre | Silver | NULL rows excluded |
| 13 | primaryProfession comma-separated | primaryProfession | Explode → 1 row per profession | Silver | NULL rows excluded |
| 14 | directors comma-separated | directors | Explode → 1 row per director + add Crew_Role | Silver | NULL rows excluded |
| 15 | writers comma-separated | writers | Explode → 1 row per writer + add Crew_Role | Silver | NULL rows excluded |
| 16 | attributes 99.44% null | attributes | Carry to Silver; do NOT model in Gold | Silver | Keep as NULL |
| 17 | job has 1 'none' literal | job | Standardize 'none'/'null'/'n/a'/'' → "Unknown" | Silver | "Unknown" |
| 18 | characters has JSON brackets | characters | Strip `["` and `"]` to extract clean names | Silver | "Unknown" |
| 19 | directors max 528 per title | directors | Allow but flag titles with >50 | Silver | Log outliers |
| 20 | writers max 1393 per title | writers | Allow but flag titles with >100 | Silver | Log outliers |

---

## Profiling Numbers to Remember for Interviews

These are the exact numbers interviewers might ask about:

| Metric | Value | Why It Matters |
|--------|-------|---------------|
| Total rows across all datasets | **204,295,502** | Scale of the project |
| Largest table | `title_principals` at **98M rows** | Performance bottleneck |
| Smallest table | `title_ratings` at **1.6M rows** | Cleanest dataset |
| Highest null rate | `attributes` at **99.44%** | Decision to exclude from Gold |
| Most critical null | `birthYear` at **95.59%** | Drives sentinel value decision |
| Distinct genres | **28** | Small, fixed dimension |
| Distinct professions | **46** | Small, fixed dimension |
| Distinct directors | **882,095** | Large — not a dimension; lives in bridge |
| Max directors per title | **528** | Outlier risk flag |
| Max writers per title | **1,393** | Outlier risk flag |
| Titles with ratings | **1.6M out of 12.3M (13%)** | Only 13% of titles are rated |
| Avg principals per title | **~10** | Explains why principals is 98M rows |

---