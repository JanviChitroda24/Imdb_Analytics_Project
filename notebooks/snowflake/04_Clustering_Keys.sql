-- ============================================================================
-- IMDb Analytics Platform — Step 1: Apply Clustering Keys
-- Run this in Snowflake Worksheets (app.snowflake.com)
-- Author: Janvi Chitroda
-- ============================================================================

USE DATABASE IMDB_ANALYTICS;
USE SCHEMA GOLD;

-- ============================================================================
-- APPLY CLUSTERING KEYS (7 tables)
-- Why: Snowflake stores data in micro-partitions. Clustering organizes rows
--      so queries skip irrelevant partitions (partition pruning).
-- ============================================================================

-- 1. DIM_TITLE (12.3M rows) — Most dashboards filter by year + content type
ALTER TABLE DIM_TITLE CLUSTER BY (ReleaseYear, TitleType);

-- 2. FACT_TITLE_RATINGS (1.6M rows) — Every ratings query joins on TitleKey
ALTER TABLE FACT_TITLE_RATINGS CLUSTER BY (TitleKey);

-- 3. DIM_PRINCIPALS (98M rows) — Largest table. "Cast for title X" scans by TitleKey
ALTER TABLE DIM_PRINCIPALS CLUSTER BY (TitleKey);

-- 4. BRIDGE_AKAS (55M rows) — Regional release queries start from a title
ALTER TABLE BRIDGE_AKAS CLUSTER BY (TitleKey);

-- 5. BRIDGE_TITLE_GENRE (19M rows) — Dual access: title→genre and genre→title
ALTER TABLE BRIDGE_TITLE_GENRE CLUSTER BY (TitleKey, GenreKey);

-- 6. BRIDGE_TITLE_CREW (24M rows) — Director/writer lookups start from a title
ALTER TABLE BRIDGE_TITLE_CREW CLUSTER BY (TitleKey);

-- 7. FACT_EPISODES (9.5M rows) — "All episodes of series X" filters by parent series
ALTER TABLE FACT_EPISODES CLUSTER BY (SeriesTitleKey);


-- ============================================================================
-- VERIFY CLUSTERING APPLIED
-- ============================================================================

SELECT TABLE_NAME, CLUSTERING_KEY
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'GOLD'
  AND CLUSTERING_KEY IS NOT NULL
ORDER BY TABLE_NAME;

-- Expected output: 7 rows with clustering keys shown
-- DIM_PRINCIPALS     | LINEAR(TitleKey)
-- DIM_TITLE          | LINEAR(ReleaseYear, TitleType)
-- BRIDGE_AKAS        | LINEAR(TitleKey)
-- BRIDGE_TITLE_CREW  | LINEAR(TitleKey)
-- BRIDGE_TITLE_GENRE | LINEAR(TitleKey, GenreKey)
-- FACT_EPISODES      | LINEAR(SeriesTitleKey)
-- FACT_TITLE_RATINGS | LINEAR(TitleKey)