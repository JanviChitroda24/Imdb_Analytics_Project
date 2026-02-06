# Understanding the Source-to-Target Mapping Document

---

## What Is It?

A source-to-target mapping document is the **blueprint of the entire data pipeline**. It traces every single column's journey from the raw source file all the way through to the final Gold layer table. For each column, it documents: where it comes from, what data type it starts as, what transformations happen to it at each layer, what it becomes in the target, how nulls are handled, and what key role it plays.

Think of it like an architect's drawing before construction begins — you don't write a single line of PySpark code without this document being finalized first.

---

## Why Do We Need It?

### 1. It Forces You to Plan Before You Code

Without it, transformation decisions get made on the fly inside notebooks, which leads to inconsistency. For example, how do you handle NULL `birthYear`? If you don't decide upfront, one developer might drop the row while another replaces it with -9999. The mapping document is the single source of truth.

### 2. It's Your Validation Contract

When you build Bronze → Silver → Gold, you check your code against this document. If the mapping says `startYear` gets renamed to `ReleaseYear` in Gold with type INT and SCD Type-2, that's exactly what your Gold notebook should produce. Any deviation is a bug.

### 3. It Documents the "Why" Behind Every Decision

The "Null Handling" and "Notes" columns capture rationale. For instance, why -9999 instead of NULL for missing years? Because sentinel values preserve the row for downstream joins — a NULL would get dropped in an inner join and you'd silently lose data. That's the kind of reasoning interviewers want to hear.

### 4. It Maps Multi-Value Field Explosions

The mapping shows that `genres` (comma-separated in the source) gets exploded in Silver to 1 row per genre, then feeds two Gold tables: `gold_dim_genre` for distinct genre lookup and `gold_bridge_title_genre` for the many-to-many relationship. Without this documented, someone reading your code wouldn't understand why row counts increase between Bronze and Silver.

---

## What's In the Mapping Document?

### Sheet Structure

The Excel file contains **9 sheets**:

| Sheet | What It Covers |
|-------|---------------|
| `name_basics` | Personnel data — profession explosion, surrogate key generation |
| `title_basics` | Title metadata — genre explosion, SCD-2 columns added |
| `title_akas` | Localized titles — region/language FK lookups to reference dimensions |
| `title_crew` | Directors & writers — explosion + derived `Crew_Role` column |
| `title_episode` | Series-episode links — season/episode sentinel values |
| `title_principals` | Cast/crew credits — category, job, characters null standardization |
| `title_ratings` | Ratings — type casting (string → float/int), FK validation |
| `reference_data` | ISO region + language codes → Gold reference dimensions |
| `Legend` | Color key (yellow = PK, green = FK, pink = multi-value explode) |

### The 18 Columns (4 Groups)

Each sheet traces columns through 4 stages:

**Source (Columns 1–3):** Where the data comes from — source file, source column name, original data type.

**Bronze (Columns 4–6):** Landing zone — Bronze table name, column name (same as source), data type (always string — no changes in Bronze).

**Silver (Columns 7–10):** Transformation layer — Silver table name, column name (may be renamed), target data type after casting, and the exact transformation logic applied.

**Gold (Columns 11–14):** Dimensional model — Gold table(s) it feeds, final column name, final data type, and Gold-specific logic (surrogate keys, lookups, SCD rules).

**Metadata (Columns 15–18):** Key type (PK/FK/blank), SCD type (Type-1/Type-2/Fixed), null handling strategy, and notes with business context.

### Color Coding

| Color | Meaning |
|-------|---------|
| 🟨 Yellow | Primary Key column |
| 🟩 Green | Foreign Key column |
| 🩷 Pink | Multi-value field that requires EXPLODE transformation |

---

## Why Plan Gold Before Writing Any Code?

The dimensional model (Gold layer) is your **destination**. Every transformation in Bronze and Silver exists to serve the Gold layer. If you don't know where you're going, you'll make wrong decisions along the way.

Concrete example: In `title.crew.tsv`, the `directors` column is a comma-separated list of nconst values. At Silver, you need to decide — do I explode this into 1 row per director, or keep it as-is? The only way to answer that is to know that in Gold, you need a `bridge_title_crew` table with one row per title-director combination. If you didn't plan Gold first, you might skip the explode in Silver and then have to backtrack later.

Same with `primaryProfession` in `name.basics` — you explode it in Silver because you know Gold needs a `bridge_profession` table. Without that Gold plan, you wouldn't know to add the `IsPrimary` flag either.

**Phase 1 planning** means knowing the target schema at a high level — which tables exist, what their grain is, and what feeds them. You don't need to finalize exact DDL or SCD merge logic yet. That comes when you actually build Gold.

The mapping document is a **living document** — you update it as you discover things during build. But having it from day one means your transformations are purposeful, not random.

---

## How the Mapping Document Was Planned — Step by Step

### Step 1: Start From the Source (What Do I Have?)

This is where profiling comes in. Before touching the mapping document, I looked at the profiling results and listed out every column across all 7 datasets. For each column I noted: what's the column name, what data type does it appear to be, how many nulls does it have, and is it a single value or comma-separated multi-value field.

For example, profiling told me that `title_basics.genres` is a comma-separated string with 28 distinct values, max 3 per row, and 4.33% null. That single fact drives multiple decisions downstream.

### Step 2: Start From the Destination (What Do I Need?)

This is where the dimensional model comes in. Before deciding what transformations to apply, I looked at the business requirements and asked: what Gold tables do I need to answer these questions?

For example, the business requirement says "Get the list of genres for a given title and identify movies based on a given genre." That tells me I need a way to query titles by genre efficiently. A comma-separated string won't work for that — I need a proper `dim_genre` lookup table and a `bridge_title_genre` table connecting titles to genres. Now I know that `genres` must be exploded somewhere in the pipeline.

So the mapping document is planned **from both ends simultaneously** — source on the left, Gold on the right, and you figure out the middle (Bronze and Silver) based on what transformation is needed to get from one to the other.

### Step 3: Define What Each Layer Does (The Rules)

Before filling in the columns, I established clear rules for each layer:

**Bronze rules:** No transformations at all. Every column comes in exactly as it is in the source. Data types stay as strings. The only additions are audit columns (`ingestion_timestamp`, `source_file`). This layer exists so you always have an immutable copy of the raw data.

**Silver rules:** This is where cleaning and standardization happen. Type casting (string → int/float), null handling (replace `\N` with sentinel values or "Unknown"), multi-value field explosion, trimming whitespace, filtering invalid rows. The goal is to make data "Gold-ready" — clean, typed, and normalized.

**Gold rules:** This is where the dimensional model gets built. Surrogate keys are assigned, dimensions are created from distinct values, bridge tables are populated from exploded Silver data, fact tables are built with FK references to dimensions, and SCD tracking columns are added.

### Step 4: Walk Through Each Column One by One

With the rules established, I went through every column in every dataset and asked a series of questions:

**"What is this column?"** — Is it a key, an attribute, a measure, or a multi-value field?

**"Does it have nulls?"** — Profiling tells me. If yes, what's the right strategy? For numeric fields like `birthYear`, I chose -9999 as a sentinel because NULL would break inner joins. For string fields like `job`, I chose "Unknown" because it's more readable in dashboards.

**"Does it need type casting?"** — Everything comes in as string from IMDb. I check if profiling flagged it as "likely numeric" and decide the target type (INT, FLOAT, etc.).

**"Is it a multi-value field?"** — If yes, it needs to be exploded in Silver. Then I trace where the exploded values go — typically a dimension table (for distinct values) and a bridge table (for the relationships).

**"Which Gold table does it feed?"** — This is the destination. A column might feed one table (simple pass-through) or multiple tables (like `genres` feeding both `dim_genre` and `bridge_title_genre`).

**"What key role does it play?"** — PK, FK, or just a regular attribute? This determines the color coding and validation requirements.

**"What SCD type applies?"** — Only relevant for dimension tables. I ask: "If this value changes in the source tomorrow, do I need to keep the old version?" If yes → Type-2. If no → Type-1. If it never changes → Fixed.

### Step 5: Handle the Generated Columns

Not every Gold column exists in the source. Some are created during the pipeline:

**Surrogate keys** like `TitleKey`, `NameKey` — created in Gold using `row_number()`. They don't exist in source, Bronze, or Silver. In the mapping, marked as "— (generated)" in the source columns.

**SCD-2 tracking columns** like `EffectiveDate`, `EndDate`, `IsCurrent` — added to Type-2 dimensions in Gold. Generated, not sourced.

**Derived columns** like `Crew_Role` — created in Silver when exploding `title_crew`. The source has separate `directors` and `writers` columns. In Silver, I union them into a single structure and add a `Crew_Role` column to track which one it was. This column doesn't exist in the source but is critical for Gold.

**Audit columns** like `ingestion_timestamp`, `source_file`, `ModifiedDate` — added at various layers for traceability.

### Step 6: Organize Into Sheets

Each source file gets its own sheet because the transformations are different per dataset. Reference data (ISO region/language codes) gets a separate sheet because it doesn't flow through Bronze/Silver — it loads directly to Gold. The Legend sheet explains the color coding so anyone reading the document can understand it without asking.

---

## Concrete Example: Tracing `genres` Through the Entire Mapping

**Source:** `title.basics.tsv` → column `genres` → type is string → sample value: "Action,Comedy,Drama"

**Bronze:** Loaded as-is into `bronze_title_basics.genres` as string. No changes. This is just raw landing.

**Silver:** This is where the action happens. The comma-separated string is exploded — "Action,Comedy,Drama" becomes 3 separate rows. So `silver_title_basics` will have 3 rows for this one title, each with a single genre value. Row count increases ~1.6x for the whole table.

**Gold:** The exploded values feed TWO Gold tables. First, all distinct genre names (28 of them) become `gold_dim_genre` with a surrogate `GenreKey`. Second, the title-genre pairs become `gold_bridge_title_genre` with `TitleKey` + `GenreKey` + `GenreOrder`. This bridge table is what allows the query "find all Action movies" to work efficiently.

**Key type:** Not a PK or FK in source — but in Gold, `GenreKey` becomes a FK in the bridge table.

**SCD type:** Fixed — "Comedy" will always be "Comedy." Genre names don't change.

**Null handling:** 4.33% of titles have no genres. These rows are excluded from the bridge table (you can't create a relationship to a genre that doesn't exist), but the title itself still exists in `dim_title`.

That's one column. Multiply this thinking by every column across all 7 datasets and you have the complete mapping document.

---

## How to Talk About This in an Interview

> *"Before writing any pipeline code, I created a source-to-target mapping document that traced every column from the 7 raw IMDb TSV files through Bronze, Silver, and Gold layers. It documented data type conversions, null handling strategies, multi-value field explode logic, surrogate key assignments, and SCD types for each dimension. This served as my validation contract — after building each layer, I verified the output matched the mapping. It also captured design rationale like why I chose sentinel values over NULLs for missing years, and why certain fields were exploded in Silver rather than Gold. The mapping was a living document — I updated it as profiling revealed data patterns I hadn't anticipated."*

---

## Document Location

```
imdb-analytics-platform/
└── docs/
    ├── mapping_document.xlsx                  ← The mapping document itself
    ├── mapping_document_explained.md          ← This file
    └── how_to_plan_mapping_document.md        ← Detailed planning process
```