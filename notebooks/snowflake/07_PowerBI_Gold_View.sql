USE DATABASE IMDB_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS GOLD_PBI;
USE SCHEMA GOLD_PBI;

-- ============================================================
-- Views for Power BI — keep all columns, fix timestamp precision
-- Truncate TIMESTAMP_NTZ to millisecond precision (Power BI compatible)
-- ============================================================

CREATE OR REPLACE VIEW GOLD_PBI.V_DIM_GENRE AS
SELECT GenreKey, GenreName,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.DIM_GENRE;

CREATE OR REPLACE VIEW GOLD_PBI.V_DIM_PROFESSION AS
SELECT ProfessionKey, Profession,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.DIM_PROFESSION;

CREATE OR REPLACE VIEW GOLD_PBI.V_DIM_CREW AS
SELECT CrewKey, Crew_Role,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.DIM_CREW;

CREATE OR REPLACE VIEW GOLD_PBI.V_DIM_REGION AS
SELECT RegionKey, RegionCode, RegionDescription,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.DIM_REGION;

CREATE OR REPLACE VIEW GOLD_PBI.V_DIM_LANGUAGE AS
SELECT LanguageKey, LanguageCode, LanguageDescription,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.DIM_LANGUAGE;

CREATE OR REPLACE VIEW GOLD_PBI.V_DIM_NAME AS
SELECT NameKey, NCONST, PrimaryName, BirthYear, DeathYear,
       ModifiedDate::TIMESTAMP_NTZ(3) AS ModifiedDate,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.DIM_NAME;

CREATE OR REPLACE VIEW GOLD_PBI.V_DIM_TITLE AS
SELECT TitleKey, Tconst, TitleType, PrimaryTitle, OriginalTitle,
       IsAdult, ReleaseYear, EndYear, RuntimeMinutes,
       EffectiveDate::TIMESTAMP_NTZ(3) AS EffectiveDate,
       EndDate::TIMESTAMP_NTZ(3) AS EndDate,
       IsCurrent,
       CreatedDate::TIMESTAMP_NTZ(3) AS CreatedDate,
       ModifiedDate::TIMESTAMP_NTZ(3) AS ModifiedDate,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.DIM_TITLE;

CREATE OR REPLACE VIEW GOLD_PBI.V_DIM_PRINCIPALS AS
SELECT PrincipalKey, NameKey, TitleKey, Ordering, Category, Job, Characters,
       ModifiedDate::TIMESTAMP_NTZ(3) AS ModifiedDate,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.DIM_PRINCIPALS;

CREATE OR REPLACE VIEW GOLD_PBI.V_FACT_TITLE_RATINGS AS
SELECT RatingKey, TitleKey, AverageRating, NumVotes,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.FACT_TITLE_RATINGS;

CREATE OR REPLACE VIEW GOLD_PBI.V_FACT_EPISODES AS
SELECT EpisodeKey, TitleKey, SeriesTitleKey, SeasonNumber, EpisodeNumber,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.FACT_EPISODES;

CREATE OR REPLACE VIEW GOLD_PBI.V_BRIDGE_TITLE_GENRE AS
SELECT TitleGenreKey, TitleKey, GenreKey, GenreOrder,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.BRIDGE_TITLE_GENRE;

CREATE OR REPLACE VIEW GOLD_PBI.V_BRIDGE_PROFESSION AS
SELECT titleProfessionKey, ProfessionKey, NameKey, IsPrimary,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.BRIDGE_PROFESSION;

CREATE OR REPLACE VIEW GOLD_PBI.V_BRIDGE_TITLE_CREW AS
SELECT titleCrewKey, CrewKey, TitleKey, NameKey,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.BRIDGE_TITLE_CREW;

CREATE OR REPLACE VIEW GOLD_PBI.V_BRIDGE_AKAS AS
SELECT TitleAkasKey, TitleKey, RegionKey, LanguageKey, AkasTitle, IsOriginalTitle,
       loaded_by, load_timestamp::TIMESTAMP_NTZ(3) AS load_timestamp
FROM GOLD.BRIDGE_AKAS;

-- Verify all 14 views created
SELECT TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'GOLD_PBI'
ORDER BY TABLE_NAME;