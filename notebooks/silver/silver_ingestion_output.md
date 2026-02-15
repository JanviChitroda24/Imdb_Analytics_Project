# NOTEBOOK: silver_name_basics

Bronze row count : 15122182
Printing current schema:
root
 |-- nconst: string (nullable = true)
 |-- primaryName: string (nullable = true)
 |-- birthYear: string (nullable = true)
 |-- deathYear: string (nullable = true)
 |-- primaryProfession: string (nullable = true)
 |-- knownForTitles: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Printing top 5 rows:
+---------+---------------+---------+---------+---------------------------------+---------------------------------------+--------------------------+------------------+-------------------------+
|nconst   |primaryName    |birthYear|deathYear|primaryProfession                |knownForTitles                         |ingestion_timestamp       |source_file       |loaded_by                |
+---------+---------------+---------+---------+---------------------------------+---------------------------------------+--------------------------+------------------+-------------------------+
|nm0000001|Fred Astaire   |1899     |1987     |actor,miscellaneous,producer     |tt0072308,tt0050419,tt0027125,tt0025164|2026-03-20 10:44:34.607658|name.basics.tsv.gz|bronze_ingestion_notebook|
|nm0000002|Lauren Bacall  |1924     |2014     |actress,miscellaneous,soundtrack |tt0037382,tt0075213,tt0038355,tt0117057|2026-03-20 10:44:34.607658|name.basics.tsv.gz|bronze_ingestion_notebook|
|nm0000003|Brigitte Bardot|1934     |2025     |actress,music_department,producer|tt0057345,tt0049189,tt0056404,tt0054452|2026-03-20 10:44:34.607658|name.basics.tsv.gz|bronze_ingestion_notebook|
|nm0000004|John Belushi   |1949     |1982     |actor,writer,music_department    |tt0072562,tt0077975,tt0080455,tt0078723|2026-03-20 10:44:34.607658|name.basics.tsv.gz|bronze_ingestion_notebook|
|nm0000005|Ingmar Bergman |1918     |2007     |writer,director,actor            |tt0050986,tt0069467,tt0050976,tt0083922|2026-03-20 10:44:34.607658|name.basics.tsv.gz|bronze_ingestion_notebook|
+---------+---------------+---------+---------+---------------------------------+---------------------------------------+--------------------------+------------------+-------------------------+
only showing top 5 rows
Rejected row count (null nconst): 0
No rejected rows — skipping log
Silver row count : 15122182
Rows dropped: 0
Printng silver table schema: 
root
 |-- nconst: string (nullable = true)
 |-- primaryName: string (nullable = true)
 |-- birthYear: integer (nullable = true)
 |-- deathYear: integer (nullable = true)
 |-- primaryProfession: string (nullable = true)
 |-- knownForTitles: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- source_table: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Printing top 5 rows:
+----------+----------------------+---------+---------+------------------------------------------+---------------------+--------------------------+------------------+------------------+---------------------------+
|nconst    |primaryName           |birthYear|deathYear|primaryProfession                         |knownForTitles       |ingestion_timestamp       |source_file       |source_table      |loaded_by                  |
+----------+----------------------+---------+---------+------------------------------------------+---------------------+--------------------------+------------------+------------------+---------------------------+
|nm17776393|Zsombor Szmejkál      |-9999    |-9999    |actor,assistant_director                  |tt38682579,tt38682286|2026-03-20 11:10:10.202633|name.basics.tsv.gz|bronze_name_basics|chitroda.j@northeastern.edu|
|nm17776394|Rozi Bíró             |-9999    |-9999    |actor,make_up_department                  |tt38682286           |2026-03-20 11:10:10.202633|name.basics.tsv.gz|bronze_name_basics|chitroda.j@northeastern.edu|
|nm17776395|Emilia Ruusa Limberger|-9999    |-9999    |actor                                     |tt38682286           |2026-03-20 11:10:10.202633|name.basics.tsv.gz|bronze_name_basics|chitroda.j@northeastern.edu|
|nm17776396|Maxim Sámuel Rözge    |-9999    |-9999    |actor                                     |tt38682286           |2026-03-20 11:10:10.202633|name.basics.tsv.gz|bronze_name_basics|chitroda.j@northeastern.edu|
|nm17776397|Bálint Kövesi         |-9999    |-9999    |actor,assistant_director,camera_department|tt38682579,tt38682286|2026-03-20 11:10:10.202633|name.basics.tsv.gz|bronze_name_basics|chitroda.j@northeastern.edu|
+----------+----------------------+---------+---------+------------------------------------------+---------------------+--------------------------+------------------+------------------+---------------------------+
only showing top 5 rows


-------------------------


# NOTEBOOK: silver_title_basics

Bronze row count for bronze_title_basics : 12323467
Printing schema for bronze_title_basics: 
root
 |-- tconst: string (nullable = true)
 |-- titleType: string (nullable = true)
 |-- primaryTitle: string (nullable = true)
 |-- originalTitle: string (nullable = true)
 |-- isAdult: string (nullable = true)
 |-- startYear: string (nullable = true)
 |-- endYear: string (nullable = true)
 |-- runtimeMinutes: string (nullable = true)
 |-- genres: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Printing top 5 rows:
+---------+---------+----------------------+----------------------+-------+---------+-------+--------------+------------------------+--------------------------+-------------------+-------------------------+
|tconst   |titleType|primaryTitle          |originalTitle         |isAdult|startYear|endYear|runtimeMinutes|genres                  |ingestion_timestamp       |source_file        |loaded_by                |
+---------+---------+----------------------+----------------------+-------+---------+-------+--------------+------------------------+--------------------------+-------------------+-------------------------+
|tt0000001|short    |Carmencita            |Carmencita            |0      |1894     |NULL   |1             |Documentary,Short       |2026-03-20 10:45:24.657138|title.basics.tsv.gz|bronze_ingestion_notebook|
|tt0000002|short    |Le clown et ses chiens|Le clown et ses chiens|0      |1892     |NULL   |5             |Animation,Short         |2026-03-20 10:45:24.657138|title.basics.tsv.gz|bronze_ingestion_notebook|
|tt0000003|short    |Poor Pierrot          |Pauvre Pierrot        |0      |1892     |NULL   |5             |Animation,Comedy,Romance|2026-03-20 10:45:24.657138|title.basics.tsv.gz|bronze_ingestion_notebook|
|tt0000004|short    |Un bon bock           |Un bon bock           |0      |1892     |NULL   |12            |Animation,Short         |2026-03-20 10:45:24.657138|title.basics.tsv.gz|bronze_ingestion_notebook|
|tt0000005|short    |Blacksmith Scene      |Blacksmith Scene      |0      |1893     |NULL   |1             |Short                   |2026-03-20 10:45:24.657138|title.basics.tsv.gz|bronze_ingestion_notebook|
+---------+---------+----------------------+----------------------+-------+---------+-------+--------------+------------------------+--------------------------+-------------------+-------------------------+
only showing top 5 rows
Rejected row count (null tconst): 0
+------+---------+------------+-------------+-------+---------+-------+--------------+------+-------------------+-----------+---------+
|tconst|titleType|primaryTitle|originalTitle|isAdult|startYear|endYear|runtimeMinutes|genres|ingestion_timestamp|source_file|loaded_by|
+------+---------+------------+-------------+-------+---------+-------+--------------+------+-------------------+-----------+---------+
+------+---------+------------+-------------+-------+---------+-------+--------------+------+-------------------+-----------+---------+


Bronze row count : 12323467
Silver row count : 12323467
Rows dropped     : 0

Silver schema (verify INT types):
root
 |-- tconst: string (nullable = true)
 |-- titleType: string (nullable = true)
 |-- primaryTitle: string (nullable = true)
 |-- originalTitle: string (nullable = true)
 |-- isAdult: integer (nullable = true)
 |-- startYear: integer (nullable = true)
 |-- endYear: integer (nullable = true)
 |-- runtimeMinutes: integer (nullable = true)
 |-- genres: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- source_table: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Silver top 5 rows:
+----------+---------+-----------------+-----------------+-------+---------+-------+--------------+------------+--------------------------+-------------------+-------------------+---------------------------+
|tconst    |titleType|primaryTitle     |originalTitle    |isAdult|startYear|endYear|runtimeMinutes|genres      |ingestion_timestamp       |source_file        |source_table       |loaded_by                  |
+----------+---------+-----------------+-----------------+-------+---------+-------+--------------+------------+--------------------------+-------------------+-------------------+---------------------------+
|tt23807082|tvepisode|Episode #1.236   |Episode #1.236   |0      |2022     |-9999  |-9999         |Comedy,Drama|2026-03-20 10:59:32.776646|title.basics.tsv.gz|bronze_title_basics|chitroda.j@northeastern.edu|
|tt23807084|tvepisode|Episode #1.237   |Episode #1.237   |0      |2022     |-9999  |-9999         |Comedy,Drama|2026-03-20 10:59:32.776646|title.basics.tsv.gz|bronze_title_basics|chitroda.j@northeastern.edu|
|tt23807086|tvepisode|Episode #1.238   |Episode #1.238   |0      |2022     |-9999  |-9999         |Comedy,Drama|2026-03-20 10:59:32.776646|title.basics.tsv.gz|bronze_title_basics|chitroda.j@northeastern.edu|
|tt23807088|tvepisode|Episode #1.241   |Episode #1.241   |0      |2022     |-9999  |-9999         |Comedy,Drama|2026-03-20 10:59:32.776646|title.basics.tsv.gz|bronze_title_basics|chitroda.j@northeastern.edu|
|tt2380709 |tvepisode|Monster Tea Party|Monster Tea Party|0      |2012     |-9999  |13            |Animation   |2026-03-20 10:59:32.776646|title.basics.tsv.gz|bronze_title_basics|chitroda.j@northeastern.edu|
+----------+---------+-----------------+-----------------+-------+---------+-------+--------------+------------+--------------------------+-------------------+-------------------+---------------------------+
only showing top 5 rows
Null check — runtimeMinutes = -9999 (should be ~64% of rows):
7901398


-----------------------------


# NOTEBOOK 3: silver_title_crew


Bronze row count : 12325801
Bronze schema:
root
 |-- tconst: string (nullable = true)
 |-- directors: string (nullable = true)
 |-- writers: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Rejected row count (null tconst): 0
No rejected rows — skipping log

Bronze row count : 12325801
Silver row count : 12325801
Rows dropped     : 0

Silver schema (verify INT types):
root
 |-- tconst: string (nullable = true)
 |-- directors: string (nullable = true)
 |-- writers: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- source_table: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Silver top 5 rows:
+---------+---------+---------+--------------------------+-----------------+-----------------+---------------------------+
|tconst   |directors|writers  |ingestion_timestamp       |source_file      |source_table     |loaded_by                  |
+---------+---------+---------+--------------------------+-----------------+-----------------+---------------------------+
|tt0000001|nm0005690|Unknown  |2026-03-20 11:13:53.330043|title.crew.tsv.gz|bronze_title_crew|chitroda.j@northeastern.edu|
|tt0000002|nm0721526|Unknown  |2026-03-20 11:13:53.330043|title.crew.tsv.gz|bronze_title_crew|chitroda.j@northeastern.edu|
|tt0000003|nm0721526|nm0721526|2026-03-20 11:13:53.330043|title.crew.tsv.gz|bronze_title_crew|chitroda.j@northeastern.edu|
|tt0000004|nm0721526|Unknown  |2026-03-20 11:13:53.330043|title.crew.tsv.gz|bronze_title_crew|chitroda.j@northeastern.edu|
|tt0000005|nm0005690|Unknown  |2026-03-20 11:13:53.330043|title.crew.tsv.gz|bronze_title_crew|chitroda.j@northeastern.edu|
+---------+---------+---------+--------------------------+-----------------+-----------------+---------------------------+
only showing top 5 rows


--------------------------


# NOTEBOOK 4: silver_title_episode


Bronze row count : 9514619
Bronze schema:
root
 |-- tconst: string (nullable = true)
 |-- parentTconst: string (nullable = true)
 |-- seasonNumber: string (nullable = true)
 |-- episodeNumber: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Rejected row count (null tconst or parentTconst): 0
No rejected rows — skipping log

Bronze row count : 9514619
Silver row count : 9514619
Rows dropped     : 0

Silver schema (verify INT types on season/episode):
root
 |-- tconst: string (nullable = true)
 |-- parentTconst: string (nullable = true)
 |-- seasonNumber: integer (nullable = true)
 |-- episodeNumber: integer (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- source_table: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Silver top 5 rows:
+---------+------------+------------+-------------+--------------------------+--------------------+--------------------+---------------------------+
|tconst   |parentTconst|seasonNumber|episodeNumber|ingestion_timestamp       |source_file         |source_table        |loaded_by                  |
+---------+------------+------------+-------------+--------------------------+--------------------+--------------------+---------------------------+
|tt0031458|tt32857063  |-9999       |-9999        |2026-03-20 11:26:51.377555|title.episode.tsv.gz|bronze_title_episode|chitroda.j@northeastern.edu|
|tt0041951|tt0041038   |1           |9            |2026-03-20 11:26:51.377555|title.episode.tsv.gz|bronze_title_episode|chitroda.j@northeastern.edu|
|tt0042816|tt0989125   |1           |17           |2026-03-20 11:26:51.377555|title.episode.tsv.gz|bronze_title_episode|chitroda.j@northeastern.edu|
|tt0042889|tt0989125   |-9999       |-9999        |2026-03-20 11:26:51.377555|title.episode.tsv.gz|bronze_title_episode|chitroda.j@northeastern.edu|
|tt0043426|tt0040051   |3           |42           |2026-03-20 11:26:51.377555|title.episode.tsv.gz|bronze_title_episode|chitroda.j@northeastern.edu|
+---------+------------+------------+-------------+--------------------------+--------------------+--------------------+---------------------------+
only showing top 5 rows


-----------------------------


# NOTEBOOK 5: silver_title_principals


Bronze row count : 98073271
Bronze schema:
root
 |-- tconst: string (nullable = true)
 |-- ordering: string (nullable = true)
 |-- nconst: string (nullable = true)
 |-- category: string (nullable = true)
 |-- job: string (nullable = true)
 |-- characters: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Rejected row count (null tconst or nconst): 0
No rejected rows — skipping log

Bronze row count : 98073271
Silver row count : 98073271
Rows dropped     : 0

Silver schema (verify BIGINT on ordering):
root
 |-- tconst: string (nullable = true)
 |-- ordering: long (nullable = true)
 |-- nconst: string (nullable = true)
 |-- category: string (nullable = true)
 |-- job: string (nullable = true)
 |-- characters: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- source_table: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Silver top 5 rows:
+---------+--------+---------+--------+--------+--------------+--------------------------+-----------------------+-----------------------+---------------------------+
|tconst   |ordering|nconst   |category|job     |characters    |ingestion_timestamp       |source_file            |source_table           |loaded_by                  |
+---------+--------+---------+--------+--------+--------------+--------------------------+-----------------------+-----------------------+---------------------------+
|tt0000658|1       |nm0169871|director|Unknown |Unknown       |2026-03-20 11:38:11.423684|title.principals.tsv.gz|bronze_title_principals|chitroda.j@northeastern.edu|
|tt0000839|1       |nm0294276|director|director|Unknown       |2026-03-20 11:38:11.423684|title.principals.tsv.gz|bronze_title_principals|chitroda.j@northeastern.edu|
|tt0000839|2       |nm0378408|producer|producer|Unknown       |2026-03-20 11:38:11.423684|title.principals.tsv.gz|bronze_title_principals|chitroda.j@northeastern.edu|
|tt0001170|1       |nm0001908|actor   |Unknown |Frank Morrison|2026-03-20 11:38:11.423684|title.principals.tsv.gz|bronze_title_principals|chitroda.j@northeastern.edu|
|tt0001170|2       |nm0930290|actress |Unknown |Faro Nan      |2026-03-20 11:38:11.423684|title.principals.tsv.gz|bronze_title_principals|chitroda.j@northeastern.edu|
+---------+--------+---------+--------+--------+--------------+--------------------------+-----------------------+-----------------------+---------------------------+
only showing top 5 rows
Characters sample — should NOT contain [" prefix:
+-----------------------+
|characters             |
+-----------------------+
|Bertrand               |
|Laure                  |
|The Major              |
|The Widow (unconfirmed)|
|The Widow's Daughter   |
|The Lawyer             |
|The Nun                |
|The Major's Friend     |
|The Priest             |
|The Butler             |
+-----------------------+
only showing top 10 rows
job = 'Unknown' count (expect ~81% of rows):
79424537


----------------------------------------------


# NOTEBOOK 6: silver_title_ratings

Bronze row count : 1641397
Bronze schema:
root
 |-- tconst: string (nullable = true)
 |-- averageRating: string (nullable = true)
 |-- numVotes: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Rejected row count (null or empty tconst): 0
No rejected rows — skipping log

Bronze row count : 1641397
Silver row count : 1641397
Rows dropped     : 0

Silver schema (verify FLOAT on averageRating, INT on numVotes):
root
 |-- tconst: string (nullable = true)
 |-- averageRating: float (nullable = true)
 |-- numVotes: integer (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- source_table: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Silver top 5 rows:
+---------+-------------+--------+-------------------------+--------------------+--------------------+---------------------------+
|tconst   |averageRating|numVotes|ingestion_timestamp      |source_file         |source_table        |loaded_by                  |
+---------+-------------+--------+-------------------------+--------------------+--------------------+---------------------------+
|tt0000001|5.7          |2194    |2026-03-20 11:56:44.66248|title.ratings.tsv.gz|bronze_title_ratings|chitroda.j@northeastern.edu|
|tt0000002|5.5          |309     |2026-03-20 11:56:44.66248|title.ratings.tsv.gz|bronze_title_ratings|chitroda.j@northeastern.edu|
|tt0000003|6.5          |2298    |2026-03-20 11:56:44.66248|title.ratings.tsv.gz|bronze_title_ratings|chitroda.j@northeastern.edu|
|tt0000004|5.1          |196     |2026-03-20 11:56:44.66248|title.ratings.tsv.gz|bronze_title_ratings|chitroda.j@northeastern.edu|
|tt0000005|6.2          |3031    |2026-03-20 11:56:44.66248|title.ratings.tsv.gz|bronze_title_ratings|chitroda.j@northeastern.edu|
+---------+-------------+--------+-------------------------+--------------------+--------------------+---------------------------+
only showing top 5 rows
Rating range check:
+----------+----------+-----------------+
|min_rating|max_rating|       avg_rating|
+----------+----------+-----------------+
|       1.0|      10.0|6.965824118545316|
+----------+----------+-----------------+

Null check on averageRating and numVotes (both should be 0):
0


----------------------------------
# NOTEBOOK 7: silver_title_akas

Bronze row count : 55294765
Bronze schema:
root
 |-- titleId: string (nullable = true)
 |-- ordering: string (nullable = true)
 |-- title: string (nullable = true)
 |-- region: string (nullable = true)
 |-- language: string (nullable = true)
 |-- types: string (nullable = true)
 |-- attributes: string (nullable = true)
 |-- isOriginalTitle: string (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Rejected row count (null titleId): 0
No rejected rows — skipping log

Bronze row count : 55294765
Silver row count : 55294765
Rows dropped     : 0

Silver schema (verify INT on ordering and isOriginalTitle):
root
 |-- titleId: string (nullable = true)
 |-- ordering: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- region: string (nullable = true)
 |-- language: string (nullable = true)
 |-- types: string (nullable = true)
 |-- attributes: string (nullable = true)
 |-- isOriginalTitle: integer (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_file: string (nullable = true)
 |-- source_table: string (nullable = true)
 |-- loaded_by: string (nullable = true)

Silver top 5 rows:
+---------+--------+---------------+------+--------+-------+----------+---------------+--------------------------+-----------------+-----------------+---------------------------+
|titleId  |ordering|title          |region|language|types  |attributes|isOriginalTitle|ingestion_timestamp       |source_file      |source_table     |loaded_by                  |
+---------+--------+---------------+------+--------+-------+----------+---------------+--------------------------+-----------------+-----------------+---------------------------+
|tt5681338|2       |Episódio #1.167|PT    |pt      |Unknown|Unknown   |0              |2026-03-20 12:34:10.522026|title.akas.tsv.gz|bronze_title_akas|chitroda.j@northeastern.edu|
|tt5681338|3       |एपिसोड #1.167  |IN    |hi      |Unknown|Unknown   |0              |2026-03-20 12:34:10.522026|title.akas.tsv.gz|bronze_title_akas|chitroda.j@northeastern.edu|
|tt5681338|4       |Épisode #1.167 |FR    |fr      |Unknown|Unknown   |0              |2026-03-20 12:34:10.522026|title.akas.tsv.gz|bronze_title_akas|chitroda.j@northeastern.edu|
|tt5681338|5       |Episodio #1.167|IT    |it      |Unknown|Unknown   |0              |2026-03-20 12:34:10.522026|title.akas.tsv.gz|bronze_title_akas|chitroda.j@northeastern.edu|
|tt5681338|6       |Folge #1.167   |DE    |de      |Unknown|Unknown   |0              |2026-03-20 12:34:10.522026|title.akas.tsv.gz|bronze_title_akas|chitroda.j@northeastern.edu|
+---------+--------+---------------+------+--------+-------+----------+---------------+--------------------------+-----------------+-----------------+---------------------------+
only showing top 5 rows

Spot check 1 — ordering distinct values (should be small integers 1-262):
+--------+
|ordering|
+--------+
|       1|
|       2|
|       3|
|       4|
|       5|
|       6|
|       7|
|       8|
|       9|
|      10|
+--------+
only showing top 10 rows
Spot check 2 — isOriginalTitle distinct values (should be ONLY 0 and 1):
+---------------+
|isOriginalTitle|
+---------------+
|              0|
|              1|
+---------------+


Spot check 3 — region = 'Unknown' count : 12,403,683
                region = 'Unknown' pct   : 22.43% (expect ~22.43%)

Spot check 4 — language = 'Unknown' count : 18,230,097
                language = 'Unknown' pct   : 32.97% (expect ~32.97%)

Spot check 5 — lowercase region codes (should be 0): 0

Spot check 6 — uppercase language codes (should be 0): 0

Spot check 7 — attributes = 'Unknown' pct : 99.44% (expect ~99.44%)

