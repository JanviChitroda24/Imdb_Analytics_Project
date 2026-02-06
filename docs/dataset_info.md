# IMDb Datasets — Overview & Key Relationships

---

## The 7 Datasets — Plain Language Summary

### 1. name_basics (15.1M rows)
This is your people table. Every person who has ever been credited on IMDb lives here — actors, directors, producers, writers, camera operators, everyone. Each person gets a unique `nconst` (like nm0000001). It tells you their name, birth year, death year, what professions they have (comma-separated, up to 3), and which titles they're known for. The catch is that 95% of people don't have a birth year listed, so most of this table is sparse on biographical data.

### 2. title_basics (12.3M rows)
This is your titles table — the core of everything. Every movie, TV series, short film, TV episode, video game, etc. gets a unique `tconst` (like tt0000001). It has the title type (movie, tvSeries, short, etc.), primary and original title names, whether it's adult content, release year, end year (for series), runtime in minutes, and genres (comma-separated, up to 3). The big gap here is that 64% of titles have no runtime listed.

### 3. title_akas (55.3M rows)
This is your localization table. For every title, it stores all the different names that title goes by in different countries and languages. So "The Dark Knight" might have entries for its German name, Japanese name, French name, etc. Each row is one title + one regional variation. It has the region code, language code, and whether it's the original title. This is why it's so large — one title can have dozens of regional names. About 22% of rows are missing the region code and 33% are missing the language code.

### 4. title_crew (12.3M rows)
This is your directors and writers table. It's a simple 3-column table: one `tconst`, a comma-separated list of director `nconst` values, and a comma-separated list of writer `nconst` values. It has exactly one row per title. The tricky part is that 44% of titles have no director listed and 49% have no writer. And the outliers are wild — one title has 528 directors and another has 1,393 writers. After explosion in Silver, this table will roughly double or triple in size.

### 5. title_episode (9.5M rows)
This is your series-episode linkage table. It connects an episode (its own `tconst`) to its parent series (`parentTconst`), and tells you which season and episode number it is. So if "Breaking Bad S03E05" exists, this table maps that episode's tconst back to Breaking Bad's series tconst, with seasonNumber=3 and episodeNumber=5. About 20% of episodes are missing season and episode numbers — likely specials, pilots, or poorly cataloged content.

### 6. title_principals (98M rows — the monster)
This is your cast and crew credits table and it's by far the largest dataset at 98M rows. For each title, it lists the principal (most important) credits — typically the top 10 or so people. Each row has the title, the person, their ordering (billing position), their category (actor, director, producer, cinematographer, etc.), their specific job description, and the character they played. The `job` column is 81% null and `characters` is 51% null because not every credit type has a job title or character name. Also, `characters` stores values as JSON arrays like `["Self"]` which needs cleaning.

### 7. title_ratings (1.6M rows — the cleanest)
This is your ratings table. Dead simple — one row per title, with the average rating (1.0 to 10.0 scale) and the number of votes. Zero nulls anywhere. The important thing to know is that only 1.6M out of 12.3M titles have ratings — that's just 13%. So most titles on IMDb have never been rated, which means any rating-based analysis only covers a small slice of the full catalog.

---

## The Two Keys That Connect Everything

### tconst — Title Constant

Every title on IMDb — movie, TV series, episode, short film, video game — gets a unique identifier called `tconst`. It follows the format `tt` + number (e.g., `tt0111161` = The Shawshank Redemption). There are **12.3 million unique tconsts** in the dataset. This is the **central join key** of the entire data model — it appears in all 7 datasets either as a primary key or foreign key. In the Gold layer, `tconst` maps to a surrogate `TitleKey`.

### nconst — Name Constant

Every person on IMDb — actor, director, writer, producer, crew member — gets a unique identifier called `nconst`. It follows the format `nm` + number (e.g., `nm0000138` = Leonardo DiCaprio). There are **15.1 million unique nconsts** in the dataset. This key connects people to titles. In the Gold layer, `nconst` maps to a surrogate `NameKey`.

### How They Work Together

`tconst` and `nconst` together answer **"who worked on what."** The `title_principals` and `title_crew` tables are where these two keys meet — linking people to the titles they contributed to.

---

## Detailed Dataset Breakdown

### 1. name_basics — The People Table
**15.1M rows** — Every person ever credited on IMDb.

Stores name, birth/death year, up to 3 professions (comma-separated), and up to 4 known titles. The big catch: 95.6% of people have no birth year listed.

| Key Column | Role | Description |
|-----------|------|-------------|
| `nconst` | **PK** | Unique person identifier |
| `knownForTitles` | Embedded FKs | Comma-separated `tconst` values (not formally a FK, used for reference) |

---

### 2. title_basics — The Titles Table (Central Hub)
**12.3M rows** — Every movie, series, episode, short, and game on IMDb.

The core dimension. Stores title type, name, adult flag, release year, runtime, and genres (comma-separated, up to 3). Runtime is 64% null. Every other dataset joins back to this one.

| Key Column | Role | Description |
|-----------|------|-------------|
| `tconst` | **PK** | Unique title identifier — the central key of the entire model |

Referenced as FK by: `title_akas`, `title_crew`, `title_episode`, `title_principals`, `title_ratings`

---

### 3. title_akas — The Localization Table
**55.3M rows** — Regional and language variations of every title.

Stores how a title is named in different countries and languages. One title can have dozens of regional names, which is why this is the second-largest table. 22% of regions and 33% of languages are null.

| Key Column | Role | Description |
|-----------|------|-------------|
| `titleId` + `ordering` | **Composite PK** | titleId is the title, ordering is the sequence number for that title's variants |
| `titleId` | **FK → title_basics.tconst** | Links each regional name back to its parent title |
| `region` | **FK → ISO 3166 reference** | Region code looked up against dim_region in Gold |
| `language` | **FK → ISO 639 reference** | Language code looked up against dim_language in Gold |

---

### 4. title_crew — The Directors & Writers Table
**12.3M rows** — Which directors and writers worked on each title.

A simple 3-column table: one tconst, a comma-separated list of director nconsts, and a comma-separated list of writer nconsts. One row per title. 44% have no director, 49% have no writer. Outliers: one title has 528 directors, another has 1,393 writers.

| Key Column | Role | Description |
|-----------|------|-------------|
| `tconst` | **PK** | One row per title |
| `directors` | **Embedded FKs → name_basics.nconst** | Comma-separated nconst values — exploded in Silver |
| `writers` | **Embedded FKs → name_basics.nconst** | Comma-separated nconst values — exploded in Silver |

---

### 5. title_episode — The Series-Episode Linkage Table
**9.5M rows** — Connects episodes to their parent series.

Maps each episode (which has its own tconst) to its parent series (parentTconst), with season and episode numbers. 20.6% of episodes are missing season/episode numbers — likely specials or poorly cataloged content.

| Key Column | Role | Description |
|-----------|------|-------------|
| `tconst` | **PK** | The episode's own unique identifier |
| `parentTconst` | **FK → title_basics.tconst** | The parent series this episode belongs to |

This is the only table where `tconst` appears **twice** in different roles — once as the episode itself and once as the parent series.

---

### 6. title_principals — The Cast & Crew Credits Table (The Monster)
**98M rows** — Principal cast and crew credits for each title.

The largest dataset by far (48% of all data). For each title, it lists the top ~10 most important credits — actors, directors, producers, cinematographers, etc. Each row has the person, their billing order, their category, their job title, and the character they played. `job` is 81% null, `characters` is 51% null and stores values as JSON arrays like `["Self"]`.

| Key Column | Role | Description |
|-----------|------|-------------|
| `tconst` + `ordering` | **Composite PK** | tconst is the title, ordering is the billing position (1st billed, 2nd billed, etc.) |
| `tconst` | **FK → title_basics.tconst** | Links credit to its title |
| `nconst` | **FK → name_basics.nconst** | Links credit to the person |

This is the **primary intersection table** between people and titles. It's where `tconst` and `nconst` directly meet.

---

### 7. title_ratings — The Ratings Table (The Cleanest)
**1.6M rows** — Average rating and vote count per title.

The simplest and cleanest dataset. Zero nulls anywhere. One row per rated title with the average score (1.0–10.0) and total number of votes. The key thing to know: only **1.6M out of 12.3M titles (13%) have ratings**. So rating-based analysis only covers a small slice of the full catalog.

| Key Column | Role | Description |
|-----------|------|-------------|
| `tconst` | **PK** | One row per rated title |
| `tconst` | **FK → title_basics.tconst** | Links rating to its title |

---

## How All 7 Datasets Connect

```
                        name_basics
                        (15.1M people)
                         PK: nconst
                             │
                     ┌───────┴────────┐
                     │                │
                     ▼                ▼
              title_principals    title_crew
              (98M credits)       (12.3M titles)
              FK: nconst          FK: directors (nconst list)
              FK: tconst          FK: writers (nconst list)
                     │            PK: tconst
                     │                │
                     ▼                ▼
                ┌─────────── title_basics ───────────┐
                │         (12.3M titles)             │
                │          PK: tconst                │
                │     THE CENTRAL HUB                │
                │                                    │
         ┌──────┼──────────┬────────────┐            │
         ▼      ▼          ▼            ▼            ▼
   title_akas  title_episode  title_ratings    (genres →
   (55.3M)     (9.5M)         (1.6M)          bridge_title_genre)
   FK: titleId FK: tconst     FK: tconst
               FK: parentTconst
```

### Join Summary

| From | To | Join Key | Relationship | Notes |
|------|----|----------|-------------|-------|
| title_basics | title_ratings | tconst | 1:1 (partial) | Only 13% of titles have ratings |
| title_basics | title_episode | tconst | 1:many | One series → many episodes |
| title_basics | title_akas | tconst = titleId | 1:many | One title → many regional names |
| title_basics | title_crew | tconst | 1:1 | Every title has exactly one crew row |
| title_basics | title_principals | tconst | 1:many | One title → ~10 principal credits |
| name_basics | title_principals | nconst | 1:many | One person → many credits |
| name_basics | title_crew | nconst (embedded) | 1:many | One person can direct/write many titles |

---

## Quick Reference for Interviews

**"What's tconst?"** — IMDb's unique identifier for every title. Format: tt + number. 12.3M unique values. It's the central join key — every dataset connects through it.

**"What's nconst?"** — IMDb's unique identifier for every person. Format: nm + number. 15.1M unique values. It connects people to their credits and roles.

**"How do they relate?"** — `title_principals` is the main intersection table where tconst meets nconst — it says "person X worked on title Y in role Z." `title_crew` also connects them but stores nconsts as comma-separated lists that need to be exploded.

**"Which table is the central hub?"** — `title_basics`. Every other table references it through tconst. In the Gold layer, this becomes `gold_dim_title` with a surrogate `TitleKey` that all facts and bridges join to.