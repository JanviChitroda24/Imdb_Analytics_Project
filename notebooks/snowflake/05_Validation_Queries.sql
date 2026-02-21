-- ============================================================================
-- IMDb Analytics Platform — Step 2: Snowflake Validation Queries
-- Run this in Snowflake Worksheets after clustering
-- Author: Janvi Chitroda
-- ============================================================================

-- Check 1 — Row Counts: "Did all 204M+ rows actually make it from Databricks to Snowflake?" 
        -- We compare the row count of each Snowflake table against the known Databricks Gold count. 
        -- If any number is off, rows were silently dropped during the load.
-- Check 2 — PK Uniqueness: "Does every table have a truly unique primary key?" 
        -- If duplicates snuck in, your joins will produce incorrect results 
        --     — a dashboard showing "top 10 movies" might double-count a title. This catches that.
-- Check 3 — Null PKs: "Are any primary keys NULL?" 
        -- A null primary key means a row exists but can't be joined to anything 
        -- — it's invisible to your dashboards. This should never happen.
-- Check 4 — FK Integrity: "Does every foreign key point to a real row in its parent dimension?" 
        -- For example, if a row in FACT_TITLE_RATINGS has TitleKey=99999 
        --     but no such TitleKey exists in DIM_TITLE, that's an orphan record 
        --     — it'll disappear from any query that joins to DIM_TITLE. 
        --     This check catches broken relationships across all 15 FK connections in your star schema.


USE DATABASE IMDB_ANALYTICS;
USE SCHEMA GOLD;

-- ============================================================================
-- CHECK 1: ROW COUNT VALIDATION (14 tables)
-- Compare against expected counts from Databricks Gold layer
-- ============================================================================

SELECT 'DIM_GENRE'           AS table_name, COUNT(*) AS row_count, 29         AS expected FROM DIM_GENRE
UNION ALL
SELECT 'DIM_PROFESSION',                    COUNT(*),              47         FROM DIM_PROFESSION
UNION ALL
SELECT 'DIM_CREW',                          COUNT(*),              2          FROM DIM_CREW
UNION ALL
SELECT 'DIM_REGION',                        COUNT(*),              245        FROM DIM_REGION
UNION ALL
SELECT 'DIM_LANGUAGE',                      COUNT(*),              184        FROM DIM_LANGUAGE
UNION ALL
SELECT 'DIM_NAME',                          COUNT(*),              15122182   FROM DIM_NAME
UNION ALL
SELECT 'DIM_TITLE',                         COUNT(*),              12323467   FROM DIM_TITLE
UNION ALL
SELECT 'DIM_PRINCIPALS',                    COUNT(*),              98060957   FROM DIM_PRINCIPALS
UNION ALL
SELECT 'FACT_TITLE_RATINGS',                COUNT(*),              1641397    FROM FACT_TITLE_RATINGS
UNION ALL
SELECT 'FACT_EPISODES',                     COUNT(*),              9512634    FROM FACT_EPISODES
UNION ALL
SELECT 'BRIDGE_TITLE_GENRE',                COUNT(*),              19175783   FROM BRIDGE_TITLE_GENRE
UNION ALL
SELECT 'BRIDGE_PROFESSION',                 COUNT(*),              16847080   FROM BRIDGE_PROFESSION
UNION ALL
SELECT 'BRIDGE_TITLE_CREW',                 COUNT(*),              23883164   FROM BRIDGE_TITLE_CREW
UNION ALL
SELECT 'BRIDGE_AKAS',                       COUNT(*),              55294741   FROM BRIDGE_AKAS
ORDER BY table_name;

-- All row_count values should match expected. If any mismatch → investigate.


-- ============================================================================
-- CHECK 2: PRIMARY KEY UNIQUENESS (no duplicates)
-- ============================================================================

SELECT 'DIM_GENRE'       AS table_name, COUNT(*) - COUNT(DISTINCT GenreKey)       AS duplicate_pks FROM DIM_GENRE
UNION ALL
SELECT 'DIM_PROFESSION',                COUNT(*) - COUNT(DISTINCT ProfessionKey)  FROM DIM_PROFESSION
UNION ALL
SELECT 'DIM_CREW',                      COUNT(*) - COUNT(DISTINCT CrewKey)        FROM DIM_CREW
UNION ALL
SELECT 'DIM_REGION',                    COUNT(*) - COUNT(DISTINCT RegionKey)      FROM DIM_REGION
UNION ALL
SELECT 'DIM_LANGUAGE',                  COUNT(*) - COUNT(DISTINCT LanguageKey)    FROM DIM_LANGUAGE
UNION ALL
SELECT 'DIM_NAME',                      COUNT(*) - COUNT(DISTINCT NameKey)        FROM DIM_NAME
UNION ALL
SELECT 'DIM_TITLE',                     COUNT(*) - COUNT(DISTINCT TitleKey)       FROM DIM_TITLE
UNION ALL
SELECT 'DIM_PRINCIPALS',               COUNT(*) - COUNT(DISTINCT PrincipalKey)   FROM DIM_PRINCIPALS
UNION ALL
SELECT 'FACT_TITLE_RATINGS',            COUNT(*) - COUNT(DISTINCT RatingKey)      FROM FACT_TITLE_RATINGS
UNION ALL
SELECT 'FACT_EPISODES',                 COUNT(*) - COUNT(DISTINCT EpisodeKey)     FROM FACT_EPISODES
UNION ALL
SELECT 'BRIDGE_TITLE_GENRE',            COUNT(*) - COUNT(DISTINCT TitleGenreKey)  FROM BRIDGE_TITLE_GENRE
UNION ALL
SELECT 'BRIDGE_PROFESSION',             COUNT(*) - COUNT(DISTINCT TitleProfessionKey) FROM BRIDGE_PROFESSION
UNION ALL
SELECT 'BRIDGE_TITLE_CREW',             COUNT(*) - COUNT(DISTINCT TitleCrewKey)   FROM BRIDGE_TITLE_CREW
UNION ALL
SELECT 'BRIDGE_AKAS',                   COUNT(*) - COUNT(DISTINCT TitleAkasKey)   FROM BRIDGE_AKAS
ORDER BY table_name;

-- Expected: ALL duplicate_pks = 0


-- ============================================================================
-- CHECK 3: NULL PRIMARY KEY CHECK
-- ============================================================================

SELECT 'DIM_GENRE'       AS table_name, COUNT_IF(GenreKey IS NULL)       AS null_pks FROM DIM_GENRE
UNION ALL
SELECT 'DIM_PROFESSION',                COUNT_IF(ProfessionKey IS NULL)  FROM DIM_PROFESSION
UNION ALL
SELECT 'DIM_CREW',                      COUNT_IF(CrewKey IS NULL)        FROM DIM_CREW
UNION ALL
SELECT 'DIM_REGION',                    COUNT_IF(RegionKey IS NULL)      FROM DIM_REGION
UNION ALL
SELECT 'DIM_LANGUAGE',                  COUNT_IF(LanguageKey IS NULL)    FROM DIM_LANGUAGE
UNION ALL
SELECT 'DIM_NAME',                      COUNT_IF(NameKey IS NULL)        FROM DIM_NAME
UNION ALL
SELECT 'DIM_TITLE',                     COUNT_IF(TitleKey IS NULL)       FROM DIM_TITLE
UNION ALL
SELECT 'DIM_PRINCIPALS',               COUNT_IF(PrincipalKey IS NULL)   FROM DIM_PRINCIPALS
UNION ALL
SELECT 'FACT_TITLE_RATINGS',            COUNT_IF(RatingKey IS NULL)      FROM FACT_TITLE_RATINGS
UNION ALL
SELECT 'FACT_EPISODES',                 COUNT_IF(EpisodeKey IS NULL)     FROM FACT_EPISODES
UNION ALL
SELECT 'BRIDGE_TITLE_GENRE',            COUNT_IF(TitleGenreKey IS NULL)  FROM BRIDGE_TITLE_GENRE
UNION ALL
SELECT 'BRIDGE_PROFESSION',             COUNT_IF(TitleProfessionKey IS NULL) FROM BRIDGE_PROFESSION
UNION ALL
SELECT 'BRIDGE_TITLE_CREW',             COUNT_IF(TitleCrewKey IS NULL)   FROM BRIDGE_TITLE_CREW
UNION ALL
SELECT 'BRIDGE_AKAS',                   COUNT_IF(TitleAkasKey IS NULL)   FROM BRIDGE_AKAS
ORDER BY table_name;

-- Expected: ALL null_pks = 0


-- ============================================================================
-- CHECK 4: FOREIGN KEY INTEGRITY (no orphan records)
-- Every FK in a fact/bridge table must exist in the referenced dimension
-- ============================================================================

-- FACT_TITLE_RATINGS → DIM_TITLE
SELECT 'FACT_TITLE_RATINGS → DIM_TITLE' AS fk_check,
       COUNT(*) AS orphan_count
FROM FACT_TITLE_RATINGS f
LEFT JOIN DIM_TITLE d ON f.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL

UNION ALL

-- FACT_EPISODES → DIM_TITLE (TitleKey = episode)
SELECT 'FACT_EPISODES.TitleKey → DIM_TITLE',
       COUNT(*)
FROM FACT_EPISODES f
LEFT JOIN DIM_TITLE d ON f.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL

UNION ALL

-- FACT_EPISODES → DIM_TITLE (SeriesTitleKey)
SELECT 'FACT_EPISODES.SeriesTitleKey → DIM_TITLE',
       COUNT(*)
FROM FACT_EPISODES f
LEFT JOIN DIM_TITLE d ON f.SeriesTitleKey = d.TitleKey
WHERE d.TitleKey IS NULL

UNION ALL

-- BRIDGE_TITLE_GENRE → DIM_TITLE
SELECT 'BRIDGE_TITLE_GENRE → DIM_TITLE',
       COUNT(*)
FROM BRIDGE_TITLE_GENRE b
LEFT JOIN DIM_TITLE d ON b.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL

UNION ALL

-- BRIDGE_TITLE_GENRE → DIM_GENRE
SELECT 'BRIDGE_TITLE_GENRE → DIM_GENRE',
       COUNT(*)
FROM BRIDGE_TITLE_GENRE b
LEFT JOIN DIM_GENRE d ON b.GenreKey = d.GenreKey
WHERE d.GenreKey IS NULL

UNION ALL

-- BRIDGE_PROFESSION → DIM_NAME
SELECT 'BRIDGE_PROFESSION → DIM_NAME',
       COUNT(*)
FROM BRIDGE_PROFESSION b
LEFT JOIN DIM_NAME d ON b.NameKey = d.NameKey
WHERE d.NameKey IS NULL

UNION ALL

-- BRIDGE_PROFESSION → DIM_PROFESSION
SELECT 'BRIDGE_PROFESSION → DIM_PROFESSION',
       COUNT(*)
FROM BRIDGE_PROFESSION b
LEFT JOIN DIM_PROFESSION d ON b.ProfessionKey = d.ProfessionKey
WHERE d.ProfessionKey IS NULL

UNION ALL

-- BRIDGE_TITLE_CREW → DIM_TITLE
SELECT 'BRIDGE_TITLE_CREW → DIM_TITLE',
       COUNT(*)
FROM BRIDGE_TITLE_CREW b
LEFT JOIN DIM_TITLE d ON b.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL

UNION ALL

-- BRIDGE_TITLE_CREW → DIM_CREW
SELECT 'BRIDGE_TITLE_CREW → DIM_CREW',
       COUNT(*)
FROM BRIDGE_TITLE_CREW b
LEFT JOIN DIM_CREW d ON b.CrewKey = d.CrewKey
WHERE d.CrewKey IS NULL

UNION ALL

-- BRIDGE_TITLE_CREW → DIM_NAME
SELECT 'BRIDGE_TITLE_CREW → DIM_NAME',
       COUNT(*)
FROM BRIDGE_TITLE_CREW b
LEFT JOIN DIM_NAME d ON b.NameKey = d.NameKey
WHERE d.NameKey IS NULL

UNION ALL

-- BRIDGE_AKAS → DIM_TITLE
SELECT 'BRIDGE_AKAS → DIM_TITLE',
       COUNT(*)
FROM BRIDGE_AKAS b
LEFT JOIN DIM_TITLE d ON b.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL

UNION ALL

-- BRIDGE_AKAS → DIM_REGION
SELECT 'BRIDGE_AKAS → DIM_REGION',
       COUNT(*)
FROM BRIDGE_AKAS b
LEFT JOIN DIM_REGION d ON b.RegionKey = d.RegionKey
WHERE d.RegionKey IS NULL

UNION ALL

-- BRIDGE_AKAS → DIM_LANGUAGE
SELECT 'BRIDGE_AKAS → DIM_LANGUAGE',
       COUNT(*)
FROM BRIDGE_AKAS b
LEFT JOIN DIM_LANGUAGE d ON b.LanguageKey = d.LanguageKey
WHERE d.LanguageKey IS NULL

UNION ALL

-- DIM_PRINCIPALS → DIM_TITLE
SELECT 'DIM_PRINCIPALS → DIM_TITLE',
       COUNT(*)
FROM DIM_PRINCIPALS p
LEFT JOIN DIM_TITLE d ON p.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL

UNION ALL

-- DIM_PRINCIPALS → DIM_NAME
SELECT 'DIM_PRINCIPALS → DIM_NAME',
       COUNT(*)
FROM DIM_PRINCIPALS p
LEFT JOIN DIM_NAME d ON p.NameKey = d.NameKey
WHERE d.NameKey IS NULL

ORDER BY fk_check;

-- Expected: ALL orphan_count = 0
-- If any > 0, there are broken foreign key references to investigate