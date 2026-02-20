-- ============================================================================
-- IMDb Analytics Platform — Snowflake DDL
-- Gold Layer: 8 Dimensions + 2 Facts + 4 Bridges = 14 Tables
-- Author: Janvi Chitroda
-- Generated: 2026-04-04
-- ============================================================================

-- ============================================================================
-- STEP 1: DATABASE & SCHEMA SETUP
-- ============================================================================

CREATE DATABASE IF NOT EXISTS IMDB_ANALYTICS;
USE DATABASE IMDB_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

-- ============================================================================
-- STEP 2: DIMENSION TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- DIM_GENRE (Fixed — No SCD) — 29 rows
-- Grain: One row per distinct genre
-- Source: Distinct genres from silver_title_basics
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_GENRE (
    GenreKey        NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    GenreName       VARCHAR(100)    NOT NULL,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ
)
COMMENT = 'Fixed dimension: 28 IMDb genres + Unknown. No SCD — genre names do not change.';


-- ----------------------------------------------------------------------------
-- DIM_PROFESSION (Fixed — No SCD) — 47 rows
-- Grain: One row per distinct profession
-- Source: Distinct professions from silver_name_basics
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_PROFESSION (
    ProfessionKey   NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    Profession      VARCHAR(200)    NOT NULL,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ
)
COMMENT = 'Fixed dimension: 47 IMDb professions. No SCD — profession labels do not change.';


-- ----------------------------------------------------------------------------
-- DIM_CREW (Fixed — No SCD) — 2 rows
-- Grain: One row per crew role type
-- Values: 1=director, 2=writer
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_CREW (
    CrewKey         NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    Crew_Role       VARCHAR(50)     NOT NULL,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ
)
COMMENT = 'Fixed dimension: 2 crew roles (director, writer). No SCD.';


-- ----------------------------------------------------------------------------
-- DIM_REGION (Fixed — No SCD) — 245 rows + Unknown sentinel
-- Grain: One row per ISO 3166 country/region code
-- Source: ISO 3166 reference data
-- Sentinel: RegionKey = -9999 for unknown/missing regions
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_REGION (
    RegionKey           NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    RegionCode          VARCHAR(10)     NOT NULL,
    RegionDescription   VARCHAR(200)    NOT NULL,
    loaded_by           VARCHAR(200),
    load_timestamp      TIMESTAMP_NTZ
)
COMMENT = 'Fixed dimension: ISO 3166 region codes. RegionKey=-9999 is Unknown sentinel for missing regions in BRIDGE_AKAS.';


-- ----------------------------------------------------------------------------
-- DIM_LANGUAGE (Fixed — No SCD) — 184 rows + Unknown sentinel
-- Grain: One row per ISO 639 language code
-- Source: ISO 639 reference data
-- Sentinel: LanguageKey = -9999 for unknown/missing languages
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_LANGUAGE (
    LanguageKey         NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    LanguageCode        VARCHAR(10)     NOT NULL,
    LanguageDescription VARCHAR(200)    NOT NULL,
    loaded_by           VARCHAR(200),
    load_timestamp      TIMESTAMP_NTZ
)
COMMENT = 'Fixed dimension: ISO 639 language codes. LanguageKey=-9999 is Unknown sentinel for missing languages in BRIDGE_AKAS.';


-- ----------------------------------------------------------------------------
-- DIM_NAME (SCD Type-1) — 15,122,182 rows
-- Grain: One row per unique person (nconst)
-- Source: silver_name_basics
-- SCD-1: Overwrite on change (no history needed for name/birth/death)
-- Sentinel: BirthYear/DeathYear = -9999 for unknown
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_NAME (
    NameKey         NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    NCONST          VARCHAR(20)     NOT NULL,
    PrimaryName     VARCHAR(500)    NOT NULL,
    BirthYear       NUMBER(10,0)    NOT NULL    DEFAULT -9999,
    DeathYear       NUMBER(10,0)    NOT NULL    DEFAULT -9999,
    ModifiedDate    TIMESTAMP_NTZ   NOT NULL,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ
)
COMMENT = 'SCD Type-1 dimension: personnel/cast/crew. BirthYear/DeathYear=-9999 means unknown. Overwrite on change — no history needed.';


-- ----------------------------------------------------------------------------
-- DIM_TITLE (SCD Type-2) — 12,323,467 rows
-- Grain: One row per title version (multiple versions if SCD-2 triggered)
-- Source: silver_title_basics
-- SCD-2: Tracks history for TitleType, IsAdult, RuntimeMinutes changes
-- Sentinel: ReleaseYear/EndYear/RuntimeMinutes = -9999 for unknown
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_TITLE (
    TitleKey        NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    Tconst          VARCHAR(20)     NOT NULL,
    TitleType       VARCHAR(50)     NOT NULL,
    PrimaryTitle    VARCHAR(1000)   NOT NULL,
    OriginalTitle   VARCHAR(1000)   NOT NULL,
    IsAdult         NUMBER(1,0)     NOT NULL    DEFAULT 0,
    ReleaseYear     NUMBER(10,0)    NOT NULL    DEFAULT -9999,
    EndYear         NUMBER(10,0)    NOT NULL    DEFAULT -9999,
    RuntimeMinutes  NUMBER(10,0)    NOT NULL    DEFAULT -9999,
    EffectiveDate   TIMESTAMP_NTZ   NOT NULL,
    EndDate         TIMESTAMP_NTZ   NOT NULL    DEFAULT '9999-12-31 00:00:00'::TIMESTAMP_NTZ,
    IsCurrent       BOOLEAN         NOT NULL    DEFAULT TRUE,
    CreatedDate     TIMESTAMP_NTZ   NOT NULL,
    ModifiedDate    TIMESTAMP_NTZ   NOT NULL,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ
)
COMMENT = 'SCD Type-2 dimension: title metadata. IsCurrent=TRUE for active version. EndDate=9999-12-31 for current records. Central hub — referenced by almost all facts and bridges.';


-- ----------------------------------------------------------------------------
-- DIM_PRINCIPALS (SCD Type-1) — 98,060,957 rows
-- Grain: One row per person-title-ordering combination
-- Source: silver_title_principals joined with DIM_NAME and DIM_TITLE
-- SCD-1: Overwrite — job/characters corrections don't need history
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_PRINCIPALS (
    PrincipalKey    NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    NameKey         NUMBER(10,0)    NOT NULL,
    TitleKey        NUMBER(10,0)    NOT NULL,
    Ordering        NUMBER(10,0)    NOT NULL,
    Category        VARCHAR(100)    NOT NULL,
    Job             VARCHAR(1000)   NOT NULL    DEFAULT 'Unknown',
    Characters      VARCHAR(2000)   NOT NULL    DEFAULT 'Unknown',
    ModifiedDate    TIMESTAMP_NTZ   NOT NULL,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ,
    CONSTRAINT fk_principals_name   FOREIGN KEY (NameKey)  REFERENCES GOLD.DIM_NAME(NameKey),
    CONSTRAINT fk_principals_title  FOREIGN KEY (TitleKey) REFERENCES GOLD.DIM_TITLE(TitleKey)
)
COMMENT = 'SCD Type-1 dimension: cast/crew credits per title. Largest table (~98M rows). Job/Characters default to Unknown when missing.';


-- ============================================================================
-- STEP 3: FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- FACT_TITLE_RATINGS — 1,641,397 rows
-- Grain: One row per rated title
-- Source: silver_title_ratings joined with DIM_TITLE
-- Note: Only 13% of titles have ratings (1.6M out of 12.3M)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_TITLE_RATINGS (
    RatingKey       NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    TitleKey        NUMBER(10,0)    NOT NULL,
    AverageRating   FLOAT           NOT NULL,
    NumVotes        NUMBER(10,0)    NOT NULL,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ,
    CONSTRAINT fk_ratings_title FOREIGN KEY (TitleKey) REFERENCES GOLD.DIM_TITLE(TitleKey)
)
COMMENT = 'Fact table: title ratings. Only ~13% of titles are rated. AverageRating range 1.0-10.0.';


-- ----------------------------------------------------------------------------
-- FACT_EPISODES — 9,512,634 rows
-- Grain: One row per episode
-- Source: silver_title_episode joined with DIM_TITLE (twice: episode + parent)
-- Sentinel: SeasonNumber/EpisodeNumber = -9999 for unknown
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_EPISODES (
    EpisodeKey      NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    TitleKey        NUMBER(10,0)    NOT NULL,
    SeriesTitleKey  NUMBER(10,0)    NOT NULL,
    SeasonNumber    NUMBER(10,0)    NOT NULL    DEFAULT -9999,
    EpisodeNumber   NUMBER(10,0)    NOT NULL    DEFAULT -9999,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ,
    CONSTRAINT fk_episodes_title    FOREIGN KEY (TitleKey)       REFERENCES GOLD.DIM_TITLE(TitleKey),
    CONSTRAINT fk_episodes_series   FOREIGN KEY (SeriesTitleKey) REFERENCES GOLD.DIM_TITLE(TitleKey)
)
COMMENT = 'Fact table: episode structure. TitleKey=episode, SeriesTitleKey=parent series. SeasonNumber/EpisodeNumber=-9999 means unknown.';


-- ============================================================================
-- STEP 4: BRIDGE TABLES (Many-to-Many Relationships)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- BRIDGE_TITLE_GENRE — 19,175,783 rows
-- Links: Title ↔ Genre (multiple genres per title, max 3)
-- Source: Exploded genres from silver_title_basics
-- GenreOrder: 0 = primary genre (first listed)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.BRIDGE_TITLE_GENRE (
    TitleGenreKey   NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    TitleKey        NUMBER(10,0)    NOT NULL,
    GenreKey        NUMBER(10,0)    NOT NULL,
    GenreOrder      NUMBER(10,0)    NOT NULL    DEFAULT 0,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ,
    CONSTRAINT fk_titlegenre_title  FOREIGN KEY (TitleKey) REFERENCES GOLD.DIM_TITLE(TitleKey),
    CONSTRAINT fk_titlegenre_genre  FOREIGN KEY (GenreKey) REFERENCES GOLD.DIM_GENRE(GenreKey)
)
COMMENT = 'Bridge: title-genre many-to-many. GenreOrder=0 is primary genre. Max 3 genres per title.';


-- ----------------------------------------------------------------------------
-- BRIDGE_PROFESSION — 16,847,080 rows
-- Links: Person ↔ Profession (multiple professions per person, max 3)
-- Source: Exploded primaryProfession from silver_name_basics
-- IsPrimary: 1 = first-listed profession, 0 = secondary
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.BRIDGE_PROFESSION (
    titleProfessionKey  NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    ProfessionKey       NUMBER(10,0)    NOT NULL,
    NameKey             NUMBER(10,0)    NOT NULL,
    IsPrimary           NUMBER(1,0)     NOT NULL    DEFAULT 0,
    loaded_by           VARCHAR(200),
    load_timestamp      TIMESTAMP_NTZ,
    CONSTRAINT fk_profession_prof   FOREIGN KEY (ProfessionKey) REFERENCES GOLD.DIM_PROFESSION(ProfessionKey),
    CONSTRAINT fk_profession_name   FOREIGN KEY (NameKey)       REFERENCES GOLD.DIM_NAME(NameKey)
)
COMMENT = 'Bridge: person-profession many-to-many. IsPrimary=1 for first-listed profession. Max 3 professions per person.';


-- ----------------------------------------------------------------------------
-- BRIDGE_TITLE_CREW — 23,883,164 rows
-- Links: Title ↔ Crew Role ↔ Person (directors/writers per title)
-- Source: Exploded directors/writers from silver_title_crew
-- CrewKey: 1=director, 2=writer (FK to DIM_CREW)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.BRIDGE_TITLE_CREW (
    titleCrewKey    NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    CrewKey         NUMBER(10,0)    NOT NULL,
    TitleKey        NUMBER(10,0)    NOT NULL,
    NameKey         NUMBER(10,0)    NOT NULL,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ,
    CONSTRAINT fk_titlecrew_crew    FOREIGN KEY (CrewKey)  REFERENCES GOLD.DIM_CREW(CrewKey),
    CONSTRAINT fk_titlecrew_title   FOREIGN KEY (TitleKey) REFERENCES GOLD.DIM_TITLE(TitleKey),
    CONSTRAINT fk_titlecrew_name    FOREIGN KEY (NameKey)  REFERENCES GOLD.DIM_NAME(NameKey)
)
COMMENT = 'Bridge: title-crew many-to-many. CrewKey=1 for directors, 2 for writers. Outlier titles may have 500+ directors or 1300+ writers.';


-- ----------------------------------------------------------------------------
-- BRIDGE_AKAS — 55,294,741 rows
-- Links: Title ↔ Region ↔ Language (regional title variations)
-- Source: silver_title_akas joined with DIM_TITLE, DIM_REGION, DIM_LANGUAGE
-- Sentinel: RegionKey/LanguageKey = -9999 for Unknown
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.BRIDGE_AKAS (
    TitleAkasKey    NUMBER(10,0)    NOT NULL    PRIMARY KEY,
    TitleKey        NUMBER(10,0)    NOT NULL,
    RegionKey       NUMBER(10,0)    NOT NULL    DEFAULT -9999,
    LanguageKey     NUMBER(10,0)    NOT NULL    DEFAULT -9999,
    AkasTitle       VARCHAR(2000)   NOT NULL,
    IsOriginalTitle NUMBER(1,0)     NOT NULL    DEFAULT 0,
    loaded_by       VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ,
    CONSTRAINT fk_akas_title    FOREIGN KEY (TitleKey)    REFERENCES GOLD.DIM_TITLE(TitleKey),
    CONSTRAINT fk_akas_region   FOREIGN KEY (RegionKey)   REFERENCES GOLD.DIM_REGION(RegionKey),
    CONSTRAINT fk_akas_language FOREIGN KEY (LanguageKey)  REFERENCES GOLD.DIM_LANGUAGE(LanguageKey)
)
COMMENT = 'Bridge: title localization. RegionKey/LanguageKey=-9999 maps to Unknown sentinel in reference dims. Largest bridge at 55M rows.';


-- ============================================================================
-- STEP 5: CLUSTERING KEYS (Performance Optimization)
-- ============================================================================

-- DIM_TITLE: Most queries filter by ReleaseYear and/or TitleType
ALTER TABLE GOLD.DIM_TITLE CLUSTER BY (ReleaseYear, TitleType);

-- FACT_TITLE_RATINGS: Joins to DIM_TITLE via TitleKey
ALTER TABLE GOLD.FACT_TITLE_RATINGS CLUSTER BY (TitleKey);

-- DIM_PRINCIPALS: Largest dimension (~98M); queried by TitleKey
ALTER TABLE GOLD.DIM_PRINCIPALS CLUSTER BY (TitleKey);

-- BRIDGE_AKAS: Largest bridge (55M); queried by TitleKey
ALTER TABLE GOLD.BRIDGE_AKAS CLUSTER BY (TitleKey);

-- BRIDGE_TITLE_GENRE: Frequently filtered by GenreKey for genre-based queries
ALTER TABLE GOLD.BRIDGE_TITLE_GENRE CLUSTER BY (TitleKey, GenreKey);

-- BRIDGE_TITLE_CREW: Queried by TitleKey for director/writer lookups
ALTER TABLE GOLD.BRIDGE_TITLE_CREW CLUSTER BY (TitleKey);

-- FACT_EPISODES: Queried by SeriesTitleKey to find episodes of a series
ALTER TABLE GOLD.FACT_EPISODES CLUSTER BY (SeriesTitleKey);


-- ============================================================================
-- STEP 6: VERIFICATION QUERIES (Run after data load)
-- ============================================================================

-- 6a. Row count validation — compare against Databricks Gold counts
SELECT 'DIM_GENRE'           AS table_name, COUNT(*) AS row_count FROM GOLD.DIM_GENRE
UNION ALL SELECT 'DIM_PROFESSION',    COUNT(*) FROM GOLD.DIM_PROFESSION
UNION ALL SELECT 'DIM_CREW',          COUNT(*) FROM GOLD.DIM_CREW
UNION ALL SELECT 'DIM_REGION',        COUNT(*) FROM GOLD.DIM_REGION
UNION ALL SELECT 'DIM_LANGUAGE',      COUNT(*) FROM GOLD.DIM_LANGUAGE
UNION ALL SELECT 'DIM_NAME',          COUNT(*) FROM GOLD.DIM_NAME
UNION ALL SELECT 'DIM_TITLE',         COUNT(*) FROM GOLD.DIM_TITLE
UNION ALL SELECT 'DIM_PRINCIPALS',    COUNT(*) FROM GOLD.DIM_PRINCIPALS
UNION ALL SELECT 'FACT_TITLE_RATINGS',COUNT(*) FROM GOLD.FACT_TITLE_RATINGS
UNION ALL SELECT 'FACT_EPISODES',     COUNT(*) FROM GOLD.FACT_EPISODES
UNION ALL SELECT 'BRIDGE_TITLE_GENRE',COUNT(*) FROM GOLD.BRIDGE_TITLE_GENRE
UNION ALL SELECT 'BRIDGE_PROFESSION', COUNT(*) FROM GOLD.BRIDGE_PROFESSION
UNION ALL SELECT 'BRIDGE_TITLE_CREW', COUNT(*) FROM GOLD.BRIDGE_TITLE_CREW
UNION ALL SELECT 'BRIDGE_AKAS',       COUNT(*) FROM GOLD.BRIDGE_AKAS
ORDER BY table_name;

-- Expected row counts (from Databricks Gold layer):
-- DIM_GENRE:            29
-- DIM_PROFESSION:       47
-- DIM_CREW:             2
-- DIM_REGION:           245
-- DIM_LANGUAGE:         184
-- DIM_NAME:             15,122,182
-- DIM_TITLE:            12,323,467
-- DIM_PRINCIPALS:       98,060,957
-- FACT_TITLE_RATINGS:   1,641,397
-- FACT_EPISODES:        9,512,634
-- BRIDGE_TITLE_GENRE:   19,175,783
-- BRIDGE_PROFESSION:    16,847,080
-- BRIDGE_TITLE_CREW:    23,883,164
-- BRIDGE_AKAS:          55,294,741


-- 6b. Primary key uniqueness check
SELECT 'DIM_GENRE'           AS table_name, COUNT(*) AS total, COUNT(DISTINCT GenreKey)           AS distinct_keys, CASE WHEN COUNT(*) = COUNT(DISTINCT GenreKey) THEN 'PASS' ELSE 'FAIL' END AS pk_check FROM GOLD.DIM_GENRE
UNION ALL
SELECT 'DIM_NAME',          COUNT(*), COUNT(DISTINCT NameKey),          CASE WHEN COUNT(*) = COUNT(DISTINCT NameKey) THEN 'PASS' ELSE 'FAIL' END FROM GOLD.DIM_NAME
UNION ALL
SELECT 'DIM_TITLE',         COUNT(*), COUNT(DISTINCT TitleKey),         CASE WHEN COUNT(*) = COUNT(DISTINCT TitleKey) THEN 'PASS' ELSE 'FAIL' END FROM GOLD.DIM_TITLE
UNION ALL
SELECT 'DIM_PRINCIPALS',    COUNT(*), COUNT(DISTINCT PrincipalKey),     CASE WHEN COUNT(*) = COUNT(DISTINCT PrincipalKey) THEN 'PASS' ELSE 'FAIL' END FROM GOLD.DIM_PRINCIPALS
UNION ALL
SELECT 'FACT_TITLE_RATINGS', COUNT(*), COUNT(DISTINCT RatingKey),       CASE WHEN COUNT(*) = COUNT(DISTINCT RatingKey) THEN 'PASS' ELSE 'FAIL' END FROM GOLD.FACT_TITLE_RATINGS
UNION ALL
SELECT 'FACT_EPISODES',     COUNT(*), COUNT(DISTINCT EpisodeKey),       CASE WHEN COUNT(*) = COUNT(DISTINCT EpisodeKey) THEN 'PASS' ELSE 'FAIL' END FROM GOLD.FACT_EPISODES;


-- 6c. Referential integrity — orphan check (FK → PK existence)
-- These should all return 0 rows if integrity holds

-- FACT_TITLE_RATINGS → DIM_TITLE
SELECT COUNT(*) AS orphan_ratings
FROM GOLD.FACT_TITLE_RATINGS f
LEFT JOIN GOLD.DIM_TITLE d ON f.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL;

-- FACT_EPISODES → DIM_TITLE (episode)
SELECT COUNT(*) AS orphan_episode_titles
FROM GOLD.FACT_EPISODES f
LEFT JOIN GOLD.DIM_TITLE d ON f.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL;

-- FACT_EPISODES → DIM_TITLE (parent series)
SELECT COUNT(*) AS orphan_series_titles
FROM GOLD.FACT_EPISODES f
LEFT JOIN GOLD.DIM_TITLE d ON f.SeriesTitleKey = d.TitleKey
WHERE d.TitleKey IS NULL;

-- DIM_PRINCIPALS → DIM_NAME
SELECT COUNT(*) AS orphan_principals_name
FROM GOLD.DIM_PRINCIPALS p
LEFT JOIN GOLD.DIM_NAME n ON p.NameKey = n.NameKey
WHERE n.NameKey IS NULL;

-- DIM_PRINCIPALS → DIM_TITLE
SELECT COUNT(*) AS orphan_principals_title
FROM GOLD.DIM_PRINCIPALS p
LEFT JOIN GOLD.DIM_TITLE d ON p.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL;

-- BRIDGE_TITLE_GENRE → DIM_TITLE
SELECT COUNT(*) AS orphan_genre_title
FROM GOLD.BRIDGE_TITLE_GENRE bg
LEFT JOIN GOLD.DIM_TITLE d ON bg.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL;

-- BRIDGE_TITLE_GENRE → DIM_GENRE
SELECT COUNT(*) AS orphan_genre_genre
FROM GOLD.BRIDGE_TITLE_GENRE bg
LEFT JOIN GOLD.DIM_GENRE g ON bg.GenreKey = g.GenreKey
WHERE g.GenreKey IS NULL;

-- BRIDGE_PROFESSION → DIM_NAME
SELECT COUNT(*) AS orphan_prof_name
FROM GOLD.BRIDGE_PROFESSION bp
LEFT JOIN GOLD.DIM_NAME n ON bp.NameKey = n.NameKey
WHERE n.NameKey IS NULL;

-- BRIDGE_PROFESSION → DIM_PROFESSION
SELECT COUNT(*) AS orphan_prof_profession
FROM GOLD.BRIDGE_PROFESSION bp
LEFT JOIN GOLD.DIM_PROFESSION p ON bp.ProfessionKey = p.ProfessionKey
WHERE p.ProfessionKey IS NULL;

-- BRIDGE_TITLE_CREW → DIM_TITLE
SELECT COUNT(*) AS orphan_crew_title
FROM GOLD.BRIDGE_TITLE_CREW bc
LEFT JOIN GOLD.DIM_TITLE d ON bc.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL;

-- BRIDGE_TITLE_CREW → DIM_NAME
SELECT COUNT(*) AS orphan_crew_name
FROM GOLD.BRIDGE_TITLE_CREW bc
LEFT JOIN GOLD.DIM_NAME n ON bc.NameKey = n.NameKey
WHERE n.NameKey IS NULL;

-- BRIDGE_TITLE_CREW → DIM_CREW
SELECT COUNT(*) AS orphan_crew_crew
FROM GOLD.BRIDGE_TITLE_CREW bc
LEFT JOIN GOLD.DIM_CREW c ON bc.CrewKey = c.CrewKey
WHERE c.CrewKey IS NULL;

-- BRIDGE_AKAS → DIM_TITLE
SELECT COUNT(*) AS orphan_akas_title
FROM GOLD.BRIDGE_AKAS ba
LEFT JOIN GOLD.DIM_TITLE d ON ba.TitleKey = d.TitleKey
WHERE d.TitleKey IS NULL;

-- BRIDGE_AKAS → DIM_REGION
SELECT COUNT(*) AS orphan_akas_region
FROM GOLD.BRIDGE_AKAS ba
LEFT JOIN GOLD.DIM_REGION r ON ba.RegionKey = r.RegionKey
WHERE r.RegionKey IS NULL;

-- BRIDGE_AKAS → DIM_LANGUAGE
SELECT COUNT(*) AS orphan_akas_language
FROM GOLD.BRIDGE_AKAS ba
LEFT JOIN GOLD.DIM_LANGUAGE l ON ba.LanguageKey = l.LanguageKey
WHERE l.LanguageKey IS NULL;


-- 6d. Null primary key check (should all return 0)
SELECT 'DIM_GENRE'            AS tbl, COUNT(*) AS null_pks FROM GOLD.DIM_GENRE            WHERE GenreKey      IS NULL
UNION ALL SELECT 'DIM_PROFESSION',     COUNT(*) FROM GOLD.DIM_PROFESSION     WHERE ProfessionKey IS NULL
UNION ALL SELECT 'DIM_CREW',           COUNT(*) FROM GOLD.DIM_CREW           WHERE CrewKey       IS NULL
UNION ALL SELECT 'DIM_REGION',         COUNT(*) FROM GOLD.DIM_REGION         WHERE RegionKey     IS NULL
UNION ALL SELECT 'DIM_LANGUAGE',       COUNT(*) FROM GOLD.DIM_LANGUAGE       WHERE LanguageKey   IS NULL
UNION ALL SELECT 'DIM_NAME',           COUNT(*) FROM GOLD.DIM_NAME           WHERE NameKey       IS NULL
UNION ALL SELECT 'DIM_TITLE',          COUNT(*) FROM GOLD.DIM_TITLE          WHERE TitleKey      IS NULL
UNION ALL SELECT 'DIM_PRINCIPALS',     COUNT(*) FROM GOLD.DIM_PRINCIPALS     WHERE PrincipalKey  IS NULL
UNION ALL SELECT 'FACT_TITLE_RATINGS', COUNT(*) FROM GOLD.FACT_TITLE_RATINGS WHERE RatingKey     IS NULL
UNION ALL SELECT 'FACT_EPISODES',      COUNT(*) FROM GOLD.FACT_EPISODES      WHERE EpisodeKey    IS NULL
UNION ALL SELECT 'BRIDGE_TITLE_GENRE', COUNT(*) FROM GOLD.BRIDGE_TITLE_GENRE WHERE TitleGenreKey IS NULL
UNION ALL SELECT 'BRIDGE_PROFESSION',  COUNT(*) FROM GOLD.BRIDGE_PROFESSION  WHERE titleProfessionKey IS NULL
UNION ALL SELECT 'BRIDGE_TITLE_CREW',  COUNT(*) FROM GOLD.BRIDGE_TITLE_CREW  WHERE titleCrewKey  IS NULL
UNION ALL SELECT 'BRIDGE_AKAS',        COUNT(*) FROM GOLD.BRIDGE_AKAS        WHERE TitleAkasKey  IS NULL;


-- ============================================================================
-- STEP 7: SAMPLE ANALYTICAL QUERIES (Performance Testing)
-- ============================================================================

-- Business Requirement #14: Top 10 rated movies in 2023 by genre
SELECT
    d.PrimaryTitle,
    d.ReleaseYear,
    g.GenreName,
    f.AverageRating,
    f.NumVotes
FROM GOLD.FACT_TITLE_RATINGS f
JOIN GOLD.DIM_TITLE d           ON f.TitleKey = d.TitleKey
JOIN GOLD.BRIDGE_TITLE_GENRE bg ON d.TitleKey = bg.TitleKey AND bg.GenreOrder = 0
JOIN GOLD.DIM_GENRE g           ON bg.GenreKey = g.GenreKey
WHERE d.ReleaseYear = 2023
  AND d.TitleType = 'movie'
  AND d.IsCurrent = TRUE
  AND f.NumVotes >= 1000
ORDER BY f.AverageRating DESC
LIMIT 10;

-- Business Requirement #9: Directors for a given title
SELECT
    d.PrimaryTitle,
    n.PrimaryName   AS DirectorName,
    c.Crew_Role
FROM GOLD.BRIDGE_TITLE_CREW bc
JOIN GOLD.DIM_TITLE d   ON bc.TitleKey = d.TitleKey
JOIN GOLD.DIM_NAME n    ON bc.NameKey  = n.NameKey
JOIN GOLD.DIM_CREW c    ON bc.CrewKey  = c.CrewKey
WHERE d.Tconst = 'tt0111161'  -- The Shawshank Redemption
  AND d.IsCurrent = TRUE
ORDER BY c.Crew_Role, n.PrimaryName;

-- Business Requirement #10: Episode count per season for a series
SELECT
    d.PrimaryTitle  AS SeriesTitle,
    e.SeasonNumber,
    COUNT(*)        AS EpisodeCount
FROM GOLD.FACT_EPISODES e
JOIN GOLD.DIM_TITLE d ON e.SeriesTitleKey = d.TitleKey
WHERE d.Tconst = 'tt0903747'  -- Breaking Bad
  AND d.IsCurrent = TRUE
  AND e.SeasonNumber != -9999
GROUP BY d.PrimaryTitle, e.SeasonNumber
ORDER BY e.SeasonNumber;


-- Why a separate schema for Gold?
--     In production you'd also have BRONZE and SILVER schemas in the same database for full Medallion separation, 
--     but since we only load the curated Gold layer into Snowflake (Bronze and Silver live in Databricks Delta), 
--     a single GOLD schema is sufficient.

