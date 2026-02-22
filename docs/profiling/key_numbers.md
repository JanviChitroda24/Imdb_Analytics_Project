markdown# IMDb Profiling — Key Numbers to Remember

## Round 1 — Row Counts

| Dataset | Rows | Columns |
|---------|------|---------|
| name.basics | 15,122,182 | 6 |
| title.basics | 12,323,467 | 9 |
| title.akas | 55,294,765 | 8 |
| title.crew | 12,325,801 | 3 |
| title.episode | 9,514,619 | 4 |
| title.principals | 98,073,271 | 6 |
| title.ratings | 1,641,397 | 3 |
| **TOTAL** | **204,295,502** | |

**Q: Which is the largest table?**
title_principals — 98M rows (48% of all data). Performance bottleneck.

**Q: Which is the smallest and cleanest?**
title_ratings — 1.6M rows, zero nulls across all 3 columns.

---

## Round 2 — Null Rates

| Column | Dataset | Null Rate |
|--------|---------|-----------|
| birthYear | name_basics | 95.59% |
| deathYear | name_basics | 98.31% |
| endYear | title_basics | 98.75% |
| runtimeMinutes | title_basics | 64.12% |
| job | title_principals | 80.98% |
| characters | title_principals | 51.22% |
| attributes | title_akas | 99.44% |
| language | title_akas | 32.97% |
| region | title_akas | 22.43% |
| directors | title_crew | 44.02% |
| writers | title_crew | 48.97% |
| seasonNumber | title_episode | 20.58% |
| episodeNumber | title_episode | 20.58% |

---

## Round 3 — Multi-Value Fields

| Column | Dataset | Distinct Values | Avg Per Row | Max Per Row |
|--------|---------|----------------|-------------|-------------|
| genres | title_basics | **28** | 1.63 | 3 |
| primaryProfession | name_basics | **46** | 1.39 | 3 |
| directors | title_crew | 882,095 | 1.34 | **528** |
| writers | title_crew | 1,122,941 | 2.33 | **1,393** |

**Q: How many distinct genres?** 28

**Q: Max directors on one title?** 528 (extreme outlier — likely a compilation)

**Q: Max writers on one title?** 1,393 (extreme outlier — flagged but not dropped)

---

## Round 4 — Relationships

**Q: What percentage of titles have ratings?**
1.6M ÷ 12.3M = **13%**
Only 1 in 8 titles has a rating.
FACT_TITLE_RATINGS will have 1.6M rows, NOT 12.3M.

**Q: Average principals per title — and why does it matter?**
98M ÷ 11.1M unique tconsts in principals = **~10 principals per title**
This explains why title_principals is 98M rows.
Divide by 11.1M (unique tconsts IN principals), not 12.3M (all titles).

---

## Cleaning Decisions — Quick Reference

| Column | Finding | Decision | Sentinel |
|--------|---------|----------|---------|
| birthYear, deathYear | 95-98% null | NULL → -9999 | -9999 |
| runtimeMinutes, startYear, endYear | High null | NULL → -9999 | -9999 |
| seasonNumber, episodeNumber | 20% null | NULL → -1 | -1 |
| primaryName, primaryTitle | Small null + 'none' literal | NULL/'none' → 'Unknown' | 'Unknown' |
| job, characters | 51-81% null | NULL → 'Unknown' | 'Unknown' |
| region, language | 22-33% null | NULL → 'Unknown' → Unknown dim key | 'Unknown' |
| genres | comma-separated | EXPLODE → 1 row per genre | Drop NULLs |
| primaryProfession | comma-separated | EXPLODE → 1 row per profession | Drop NULLs |
| directors, writers | comma-separated | EXPLODE → 1 row each + Crew_Role | Drop NULLs |
| characters | JSON array ["Name"] | Strip brackets → clean string | 'Unknown' |
| attributes | 99.44% null | Keep Silver, exclude Gold | NULL |
| tconst/nconst nulls | 5 rows in principals | DROP + log to rejected table | N/A |