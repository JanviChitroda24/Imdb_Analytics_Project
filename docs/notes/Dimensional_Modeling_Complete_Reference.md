# Dimensional Modeling — Complete Reference Guide

> A comprehensive reference for learning dimensional modeling from first principles, building production data warehouses, and preparing for data engineering and analytics interviews.

---

## Table of Contents

1. [What Is Dimensional Modeling?](#1-what-is-dimensional-modeling)
2. [Why Not Just Use a Normalized Database?](#2-why-not-just-use-a-normalized-database)
3. [The Kimball Method](#3-the-kimball-method)
4. [Schema Types](#4-schema-types)
5. [The Building Blocks — Facts and Dimensions](#5-the-building-blocks--facts-and-dimensions)
6. [Fact Tables — Complete Reference](#6-fact-tables--complete-reference)
7. [Dimension Tables — Complete Reference](#7-dimension-tables--complete-reference)
8. [The Decision Framework — Which Column Goes Where?](#8-the-decision-framework--which-column-goes-where)
9. [Grain — The Most Important Concept](#9-grain--the-most-important-concept)
10. [Types of Facts](#10-types-of-facts)
11. [Types of Dimensions](#11-types-of-dimensions)
12. [Slowly Changing Dimensions (SCD)](#12-slowly-changing-dimensions-scd)
13. [Bridge Tables — Handling Many-to-Many Relationships](#13-bridge-tables--handling-many-to-many-relationships)
14. [Surrogate Keys vs Natural Keys](#14-surrogate-keys-vs-natural-keys)
15. [The Dimensional Modeling Process — Step by Step](#15-the-dimensional-modeling-process--step-by-step)
16. [Common Modeling Mistakes](#16-common-modeling-mistakes)
17. [Schema Comparison — When to Use What](#17-schema-comparison--when-to-use-what)
18. [Interview Questions and Model Answers](#18-interview-questions-and-model-answers)

---

## 1. What Is Dimensional Modeling?

Dimensional modeling is a design technique for ***organizing data*** in a database to make analytical queries fast, intuitive, and business-friendly.

It was developed by **Ralph Kimball** in the 1990s and remains the dominant approach for building data warehouses and analytical systems today.

The core idea is simple: separate your data into two types of tables.

- **Fact tables** — record things that happened ***(events, measurements, transactions)***
- **Dimension tables** — describe the context around what happened (who, what, where, when, why)

An analyst asking "what were the total sales by region last quarter?" needs:
- The sales numbers (fact)
- The region names (dimension)
- The time period (dimension)

That's dimensional modeling in its simplest form.

---

## 2. Why Not Just Use a Normalized Database?

A **normalized (3NF) database** is designed to eliminate data redundancy. Every piece of information lives in exactly one place. Foreign keys link everything together. It is the correct design for transactional systems (OLTP — Online Transaction Processing).

An **analytical database** (OLAP — Online Analytical Processing) has different requirements. Analysts need to:

- Join many tables to answer a single business question
- Aggregate millions of rows quickly
- Filter on multiple attributes simultaneously
- Understand the data without deep knowledge of the schema

A 3NF schema forces analysts to write complex multi-table joins just to answer basic questions. A simple question like "show me revenue by product category by month" might require joining 8 tables in a normalized schema. In a dimensional model it requires joining 3.

### The trade-off table

| Concern | Normalized (3NF) | Dimensional (Star Schema) |
|---|---|---|
| Storage efficiency | ✅ Minimal redundancy | ❌ Some redundancy |
| Write performance | ✅ Fast inserts/updates | ❌ Slower writes |
| Query simplicity | ❌ Complex multi-joins | ✅ Simple, predictable joins |
| Query performance | ❌ Many joins slow reads | ✅ Fewer joins, fast reads |
| Business readability | ❌ Hard to understand | ✅ Intuitive for analysts |
| Aggregation speed | ❌ Slow on large data | ✅ Optimized for aggregation |

**Use 3NF for:** transactional systems, source systems, operational databases, anywhere data is written frequently and read in small chunks.

**Use dimensional modeling for:** data warehouses, analytics platforms, BI dashboards, anywhere data is read frequently in large aggregations.

---

## 3. The Kimball Method

Ralph Kimball's approach to dimensional modeling is built on four key principles.

### Principle 1 — Design from business requirements, not source systems

You start with the questions the business needs to answer — not with the tables you happen to have. The business questions determine what your grain is, what your facts are, and what dimensions you need. The source data is just the raw material.

### Principle 2 — Use a bus architecture

All fact tables in the warehouse share conformed dimensions. This means `dim_date`, `dim_customer`, `dim_product` are defined once and reused across every fact table that needs them. This allows analysts to drill across different fact tables using the same dimension attributes without inconsistency.

A **conformed dimension** is a dimension that means the same thing to every fact table that references it. "Customer" means the same thing in fact_orders and fact_returns — they point to the same dim_customer table.

### Principle 3 — Favor denormalized dimensions

Kimball explicitly tells you to break 3NF rules in your dimension tables. Put as many descriptive attributes as possible directly in the dimension row, even if it creates some redundancy. The goal is to allow analysts to filter and group on any attribute in one join, not two or three.

For example, if a product belongs to a category which belongs to a department, a normalized model has three tables. Kimball says: put category name and department name directly on the product row in dim_product. One join gets everything.

### Principle 4 — Drill-down capability

The model should support drilling from summary to detail. An analyst should be able to start at "total revenue by year" and drill into "revenue by quarter" then "revenue by month" then "revenue by day" — all through the same date dimension, no redesign required.

---

## 4. Schema Types

### Star Schema

The most common dimensional model pattern. One fact table at the center, dimension tables radiating out like points on a star. Every dimension joins directly to the fact table — no joins between dimensions.

```
                   dim_date
                      |
dim_region — fact_sales — dim_product
                      |
                 dim_customer
```

**Characteristics:**
- Simple, flat dimensions (denormalized)
- Maximum 2 joins to answer any question (fact → dimension)
- Best query performance
- Easiest for analysts and BI tools to understand
- Some storage redundancy in dimensions

**Use when:** query performance and analyst usability are the priority. This is the default choice for most data warehouses.

### Snowflake Schema

An extension of the star schema where dimension tables are normalized into sub-tables. A product dimension might link to a category table which links to a department table.

```
                   dim_date
                      |
dim_region — fact_sales — dim_product — dim_category — dim_department
                      |
                 dim_customer
```

**Characteristics:**
- Normalized dimensions (less redundancy)
- More joins required for queries (3, 4, 5 hops instead of 2)
- Slower query performance
- More complex for analysts
- Better storage efficiency

**Use when:** dimension tables are enormous and storage cost matters, or when attribute hierarchies are complex and frequently updated.

**Kimball's view:** avoid snowflaking unless you have a compelling reason. The storage savings rarely justify the query complexity cost.

### Galaxy Schema (Fact Constellation)

Multiple fact tables share the same dimension tables. Also called a fact constellation.

```
dim_date ——— fact_sales ——— dim_product
    |                           |
fact_inventory ————————— dim_warehouse
```

**Characteristics:**
- Multiple fact tables in the same model
- Shared (conformed) dimensions
- Supports drill-across queries between fact tables
- More complex to design

**Use when:** you have multiple business processes that share common dimensions. Most enterprise data warehouses are galaxy schemas — they just have many star schemas sharing conformed dimensions.

### One Big Table (OBT)

Everything in a single flat table — all facts and dimensions denormalized into one. Common in modern data lakehouses and tools like BigQuery.

**Characteristics:**
- Zero joins required
- Extreme storage redundancy
- Fastest possible query on a single subject
- Breaks down with complex multi-subject queries

**Use when:** you have a single, well-defined analytical use case and storage is cheap. Not suitable as a general warehouse design.

---

## 5. The Building Blocks — Facts and Dimensions

### The fundamental question

Every column in your data warehouse answers one of two questions:

**"What happened, and how much?"** → Fact table
**"What was it about?"** → Dimension table

Facts record events. Dimensions describe context.

### A concrete example

A retail sales transaction:

| Data | Type | Why |
|---|---|---|
| Sale amount: $49.99 | Fact measure | A measurement produced by the event |
| Quantity sold: 3 | Fact measure | A measurement produced by the event |
| Product name: "Running Shoes" | Dimension attribute | Describes the product, not the event |
| Category: "Footwear" | Dimension attribute | Describes the product |
| Store city: "Boston" | Dimension attribute | Describes the store |
| Sale date: 2024-03-15 | Dimension attribute | Context for when it happened |
| Customer age: 34 | Dimension attribute | Describes the customer |

The $49.99 and quantity 3 only exist because a sale happened. The product name, category, city, date — those exist independently of any particular sale.

---

## 6. Fact Tables — Complete Reference

### Definition

A fact table records a measurable business event at a specific grain. It contains:
1. **Foreign keys** — pointing to dimension tables that provide context
2. **Measures** — the numeric values being measured
3. **Optionally** — a degenerate dimension (a natural key with no associated dimension table, like an order number)

### The 6 rules for fact tables

**Rule 1 — Declare the grain before writing any columns**

The grain answers: "what does one row in this table represent?" This is the single most important design decision. Every column you add must be true at exactly that grain.

Wrong: adding a column that's only true at a higher level of aggregation.
Wrong: adding a column that requires joining to a dimension first.

Grain declaration examples:
- "One row per individual sales transaction line item"
- "One row per customer per day (account balance snapshot)"
- "One row per title that has been rated"
- "One row per episode of a TV series"

**Rule 2 — Measures must be additive or semi-additive**

*Fully additive:* can SUM meaningfully across all dimensions.
Example: `revenue`, `quantity_sold`, `num_votes`

*Semi-additive:* can SUM across some dimensions but not time.
Example: `account_balance` (summing across accounts is fine, summing across days is wrong — you'd double-count)

*Non-additive:* cannot SUM at all — only AVG, ratio, or count.
Example: `average_rating`, `profit_margin_pct`

Non-additive measures still belong in fact tables. You just use AVG or calculate ratios instead of SUM.

**Rule 3 — Fact tables are narrow**

A fact table should have: surrogate FKs + a small number of measures + optionally degenerate dimensions. If you find yourself adding lots of text description columns, they belong in a dimension.

**Rule 4 — Fact tables are tall**

Facts accumulate over time. They are the largest tables in your warehouse. A fact table with millions or billions of rows is normal and expected.

**Rule 5 — Measures must be produced by the event, not inherent to an entity**

If a value exists the moment an entity exists (a product has a price, a movie has a runtime) it may belong in a dimension. If a value is generated by an event occurring (a sale happened, a rating was submitted) it belongs in the fact table.

**Rule 6 — Do not put descriptive text in fact tables**

Store the FK to dim_customer, not the customer's name. Store the FK to dim_product, not the product name. Descriptive text in fact tables creates redundancy, inconsistency, and kills query performance.

### Degenerate dimensions

A degenerate dimension is a natural key that lives in the fact table with no associated dimension table. Common examples: order number, invoice number, ticket number, transaction ID.

These are identifiers that have no additional attributes worth storing in a separate table — the ID itself is the only useful value. They are kept in the fact table for filtering and grouping.

```
fact_sales
- SaleKey (PK)
- DateKey (FK → dim_date)
- CustomerKey (FK → dim_customer)
- ProductKey (FK → dim_product)
- OrderNumber (degenerate dimension — no dim_order table needed)
- Quantity
- Revenue
- Discount
```

---

## 7. Dimension Tables — Complete Reference

### Definition

A dimension table describes an entity — a person, place, product, time period, or concept. It provides the context that makes fact table measurements meaningful. Analysts use dimension attributes to filter, group, label, and slice fact data.

### The 6 rules for dimension tables

**Rule 1 — Dimensions describe entities, not events**

An entity is a thing that exists independently of any measurement event. A customer exists before any purchase. A product exists before any sale. A date exists regardless of what happens on it.

**Rule 2 — Dimensions are wide**

Deliberately violate 3NF. Put as many descriptive attributes as possible directly on the dimension row. You want analysts to be able to answer "give me all sales where the product is in the Electronics category, from a store in the Northeast region, to a customer aged 25-34" with a single join to each of three dimensions — not a chain of 6 joins through normalized lookup tables.

**Rule 3 — Dimension attributes are used to filter, group, and label — not to aggregate**

The mental test: would you ever write `SUM(this_column)` in a meaningful query?
- `SUM(revenue)` → meaningful → fact measure
- `SUM(product_category)` → meaningless → dimension attribute
- `SUM(customer_age)` → meaningless → dimension attribute
- `AVG(customer_age)` → meaningful → still a dimension attribute (you GROUP BY it and COUNT/AVG, not aggregate the column itself)

**Rule 4 — One row per entity per grain**

dim_customer has one row per customer. dim_product has one row per product. No fan-out. If a customer appears twice, something is wrong.

Exception: SCD Type-2 dimensions have one row per version of the entity, but still one current active row per entity.

**Rule 5 — Use surrogate keys as primary keys**

Never use the natural key (customer_id from the source system, product_sku, tconst) as the PK of a dimension. Use a system-generated integer surrogate key. Reasons explained in section 14.

**Rule 6 — Attributes belong in one canonical dimension**

A customer's city appears once — in dim_customer. Not in fact_sales, not in dim_store. One canonical location, one join to get it. This prevents inconsistency when the city name changes.

### The role-playing dimension pattern

The same dimension is used multiple times in a single fact table to play different roles.

Example: an airline fact table has a departure airport and an arrival airport. Both use the same dim_airport dimension, but with different FK column names.

```
fact_flight
- FlightKey (PK)
- DepartureDateKey (FK → dim_date)
- ArrivalDateKey (FK → dim_date)         ← same dim_date, different role
- DepartureAirportKey (FK → dim_airport)
- ArrivalAirportKey (FK → dim_airport)   ← same dim_airport, different role
- FlightDurationMinutes
- NumPassengers
```

---

## 8. The Decision Framework — Which Column Goes Where?

Use this decision process for every column in your source data.

### Step 1 — Is it a key or identifier?

If it is a natural key from a source system (customer_id, product_sku, order_number):
- If it has associated attributes worth storing → becomes a surrogate PK in a dimension
- If it has no useful attributes beyond itself → stays in the fact table as a degenerate dimension

### Step 2 — Is it produced by an event, or inherent to an entity?

**Produced by an event** (only exists because something happened):
- A sale amount exists because a sale occurred → fact measure
- A rating exists because someone voted → fact measure
- A login duration exists because a login session occurred → fact measure

**Inherent to an entity** (exists the moment the entity exists):
- A product's color exists the moment the product exists → dimension attribute
- A customer's birth year exists the moment the customer record exists → dimension attribute
- A movie's runtime exists the moment the movie is catalogued → dimension attribute

### Step 3 — Is it additive across rows?

Can you meaningfully aggregate it across multiple rows?

| Question | Result |
|---|---|
| SUM across all dimensions? | Fully additive → fact measure |
| SUM across some dimensions but not time? | Semi-additive → fact measure |
| Cannot SUM, only AVG or ratio? | Non-additive → still fact measure |
| Makes no sense to aggregate? | Dimension attribute |

**Important:** "Can I AVG this?" is not the deciding question. You can `AVG(birth_year)` but that doesn't make birth_year a fact measure. The deciding question is: ***was this value produced by a measurement event?***

### Step 4 — Does it vary independently from the entity?

If the same entity can have different values for this column at different events or time periods, and you care about those differences → fact measure or SCD-tracked dimension attribute.

If the value is fixed to the entity and changes only when the entity itself is updated → dimension attribute.

### Step 5 — Would it make sense in a fact table at your declared grain?

Every column must be true at the grain. If your grain is "one row per order line item" and you're considering adding "total order revenue", that's wrong — total order revenue is true at the order level, not the line item level. It belongs in a separate, higher-grain fact or as an aggregation.

### Quick reference card

| Column characteristic | Where it goes |
|---|---|
| Produced by event, numeric, additive | Fact measure |
| Produced by event, numeric, non-additive | Fact measure (use AVG/ratio) |
| Intrinsic to entity, descriptive text | Dimension attribute |
| Intrinsic to entity, numeric, not event-driven | Dimension attribute |
| Natural key with no useful attributes | Degenerate dimension (stays in fact) |
| Natural key with attributes | Surrogate key PK in dimension |
| Boolean flag classifying an entity | Dimension attribute |
| Boolean flag indicating event occurred | Fact measure (or degenerate dim) |

---

## 9. Grain — The Most Important Concept

### What grain means

Grain is the single, precise declaration of what one row in a fact table represents. It is the atomic unit of measurement in your model.

Before writing a single column name, you must answer: ***"If I pick up one row from this table and hold it up, what exactly is it?"***

### Why grain matters

**Wrong grain = wrong answers.** If your grain is ambiguous or inconsistently applied:
- Aggregations double-count (you get 2x the actual revenue)
- Joins fan out unexpectedly (one-to-one becomes one-to-many)
- Analysts can't trust the numbers

### Grain examples across domains

| Domain | Fact Table | Grain |
|---|---|---|
| Retail | fact_sales | One row per order line item |
| Banking | fact_transactions | One row per account transaction |
| Banking | fact_account_balance | One row per account per day |
| Healthcare | fact_claims | One row per claim line |
| Web analytics | fact_page_views | One row per page view event |
| HR | fact_payroll | One row per employee per pay period |
| IMDb project | fact_title_ratings | One row per rated title |
| IMDb project | fact_episodes | One row per episode |

### Choosing the right grain

Always choose the **lowest available grain** that answers the business questions. You can always aggregate up from detail to summary — you cannot disaggregate from summary to detail.

If a business requirement asks "how many transactions per customer per day?" and your grain is "one row per customer per month", you cannot answer that question. If your grain is "one row per transaction", you can answer both the daily and monthly question by aggregating.

**The exception:** when the source data only provides summary data (like IMDb ratings — only aggregated averages, no individual votes), your grain is limited by what exists.

### Grain and the dimension relationship

Once you declare a grain, it constrains which dimensions are valid. Every dimension you add must be directly and fully determined by the grain.

For grain = "one row per order line item":
- ✅ product dimension — every line item has exactly one product
- ✅ customer dimension — every line item has exactly one customer
- ✅ date dimension — every line item has exactly one order date
- ❌ store region — only if every line item maps to exactly one region; if a store covers multiple regions this would be wrong

---

## 10. Types of Facts

### Transaction fact table

Records a discrete event at the moment it occurs. Once written, rows are never updated — new events create new rows. The most common fact table type.

**Characteristics:**
- One row per event (purchase, click, login, vote)
- Append-only — new rows added, old rows not changed
- Can have many rows quickly
- Point-in-time accuracy

**Examples:** sales transactions, web page views, login events, payment events

```
fact_sales
- SaleKey (PK)
- DateKey (FK)
- CustomerKey (FK)
- ProductKey (FK)
- Quantity
- Revenue
- Discount
```

### Periodic snapshot fact table

Takes a regular snapshot of a metric at fixed time intervals — daily, weekly, monthly. Even if nothing changed, a new row is created for each period. Used when you need to track metrics over time.

**Characteristics:**
- One row per entity per time period
- Rows are always inserted (even if metrics unchanged)
- Supports trend analysis and time-series comparison
- Semi-additive measures are common (balances, headcount)

**Examples:** daily account balances, weekly inventory levels, monthly active users, monthly employee headcount

```
fact_account_balance_daily
- SnapshotKey (PK)
- AccountKey (FK)
- DateKey (FK)
- ClosingBalance        ← semi-additive (SUM across accounts OK, not across dates)
- NumTransactionsToday  ← fully additive
- InterestAccrued       ← fully additive
```

### Accumulating snapshot fact table

Tracks a single entity as it moves through a defined pipeline or lifecycle. One row per entity, updated in place as it progresses through stages. Includes multiple date FKs — one per milestone.

**Characteristics:**
- One row per entity (one row per order, one row per application)
- Row is updated as entity moves through stages
- Multiple date foreign keys (order_date, ship_date, delivery_date)
- Lag measures (days between stages) are common
- Relatively rare — used only for pipeline/workflow processes

**Examples:** order fulfillment pipeline, loan application lifecycle, insurance claim processing, job application stages

```
fact_order_fulfillment
- OrderKey (PK)
- CustomerKey (FK)
- ProductKey (FK)
- OrderDateKey (FK → dim_date)
- ShipDateKey (FK → dim_date)      ← NULL until shipped
- DeliveryDateKey (FK → dim_date)  ← NULL until delivered
- OrderAmount
- DaysToShip                       ← lag measure, updated when shipped
- DaysToDeliver                    ← lag measure, updated when delivered
- CurrentStage                     ← 'ordered', 'shipped', 'delivered'
```

### Factless fact table

A fact table with no measures — only foreign keys. Records that an event occurred, or captures a relationship between entities, without any numeric measurement.

**Use cases:**

*Coverage:* "which students are enrolled in which courses?" — the enrollment itself is the fact.
*Event occurrence:* "which promotions were active on which dates?" — presence in the table means it happened.

```
fact_student_enrollment
- EnrollmentKey (PK)
- StudentKey (FK)
- CourseKey (FK)
- TermKey (FK)
- EnrollmentDateKey (FK)
(no measures — the row itself means the student is enrolled)
```

---

## 11. Types of Dimensions

### Conformed dimension

A dimension shared across multiple fact tables in the warehouse. Defined once, used everywhere. This is what allows drill-across queries — comparing metrics from different fact tables along the same dimension.

**Example:** dim_date is used by fact_sales, fact_inventory, fact_web_traffic, fact_support_tickets. Because they all share the same dim_date, an analyst can compare sales revenue vs support ticket volume by month in a single query.

The defining rule: a conformed dimension has the same keys, same attribute names, and same attribute values across every fact table that references it.

### Junk dimension

A collection of miscellaneous low-cardinality flags and indicators that don't belong to any specific dimension but clutter the fact table. Rather than having 10 flag columns in the fact table, you create one junk dimension containing all combinations.

**Example:** in a banking transaction, you might have flags like:
- `is_international` (Y/N)
- `is_card_present` (Y/N)
- `is_recurring` (Y/N)
- `channel` (web/mobile/branch/atm)

These form a junk dimension. All combinations are pre-built (2 × 2 × 2 × 4 = 32 rows) and the fact table gets a single `TransactionContextKey`.

**When to use:** when you have 3+ low-cardinality flags/indicators that don't belong to an existing dimension.

### Outrigger dimension

A dimension that references another dimension — a secondary dimension hanging off a primary dimension. Rare in Kimball modeling (it creates a partial snowflake) but sometimes justified.

**Example:** dim_employee has a FK to dim_department. dim_department is an outrigger.

**When to use:** only when the secondary dimension is very large, changes frequently, and must be kept separate to avoid redundancy. Otherwise, flatten it.

### Role-playing dimension

A single physical dimension table used multiple times in the same fact table, each time playing a different role. Each role gets an aliased view or is referenced through a different FK column name.

**Example:** dim_date used as order_date, ship_date, and delivery_date in fact_order_fulfillment.

### Degenerate dimension

A dimension attribute (usually a natural key) that lives in the fact table with no associated dimension table. It has no additional attributes worth storing separately.

**Examples:** order number, invoice number, ticket number, transaction ID, check number.

### Static / fixed dimension

A dimension whose values never or rarely change. Populated once and left alone. No SCD tracking needed.

**Examples:** dim_genre (28 genres), dim_country (ISO 3166 country codes), dim_language, dim_product_category (if categories are stable).

### Date dimension

The most universal dimension in any warehouse. Every fact table in the warehouse connects to it. Pre-populated with one row per day for a range of years. Contains every possible time attribute an analyst might need — day of week, week number, month, quarter, year, is_holiday, is_weekend, fiscal_period, etc.

Never rely on SQL date functions for time-based analysis. Pre-compute every time attribute in the date dimension so analysts can filter and group without writing DATEPART() or EXTRACT() every time.

```
dim_date
- DateKey (PK — integer, format YYYYMMDD e.g. 20240315)
- FullDate (date)
- DayOfWeek (1-7)
- DayName ('Monday')
- DayOfMonth (1-31)
- DayOfYear (1-366)
- WeekNumber (1-53)
- MonthNumber (1-12)
- MonthName ('March')
- Quarter (1-4)
- QuarterName ('Q1')
- Year (2024)
- IsWeekend (boolean)
- IsHoliday (boolean)
- HolidayName (nullable)
- FiscalYear
- FiscalQuarter
- FiscalPeriod
```

---

## 12. Slowly Changing Dimensions (SCD)

Dimension attributes change over time. A customer moves to a new city. A product gets reclassified to a new category. An employee gets promoted. How you handle these changes determines your ability to answer historical questions accurately.

There are six SCD types, though Types 1, 2, and 3 are by far the most common in practice.

### SCD Type 0 — Retain original (never change)

Attributes never change once set. Original value is always preserved, even if the source changes.

**Use when:** the attribute is historical fact that should never be overwritten — date of birth, original enrollment date, founding year of a company.

**Implementation:** no special logic needed. Simply ignore any changes to the source value.

### SCD Type 1 — Overwrite (no history)

When an attribute changes, overwrite the old value with the new value. No history is kept. The dimension always reflects the current state.

**Use when:** the old value was wrong (data correction) or historical accuracy for this attribute doesn't matter for your business questions.

**Examples:**
- Correcting a misspelled name
- Updating a phone number or email address
- Fixing an incorrect birth year

**Implementation:** simple UPDATE statement on the dimension row.

```sql
UPDATE dim_customer
SET email = 'new_email@example.com',
    modified_date = CURRENT_TIMESTAMP
WHERE customer_id = 'C001'
```

**Drawback:** any historical fact rows that referenced this customer now "see" the new attribute value, even if the fact occurred before the change. The history is rewritten.

### SCD Type 2 — Add new row (full history)

When an attribute changes, insert a new row for the new version and mark the old row as expired. Full history is preserved. Fact rows point to the dimension version that was current at the time of the event.

**Use when:** you need to accurately answer "what did we know at the time?" questions. When historical accuracy matters.

**Examples:**
- Customer moves to a new city (sales should reflect region at time of purchase)
- Product reclassified to a new category (historical sales should still show old category)
- Title type changes (content classification must be historically accurate)

**Required columns:**
- `effective_date` — when this version became active
- `end_date` — when this version expired (set to far-future date like 9999-12-31 for current version)
- `is_current` — boolean flag, true for the currently active version

```
dim_customer (SCD Type-2 example)
CustomerKey | CustomerID | Name          | City     | EffectiveDate | EndDate    | IsCurrent
1           | C001       | Alice Johnson | Boston   | 2020-01-01    | 2023-05-14 | false
47          | C001       | Alice Johnson | New York | 2023-05-15    | 9999-12-31 | true
```

A fact row from 2021 pointing to CustomerKey=1 correctly shows Alice lived in Boston. A fact row from 2024 pointing to CustomerKey=47 correctly shows New York.

**The SCD Type-2 merge logic (UPSERT pattern):**

```sql
-- Step 1: Identify changed records
SELECT source.* 
FROM source_table source
JOIN dim_customer target ON source.customer_id = target.customer_id
WHERE target.is_current = true
AND (source.city != target.city OR source.name != target.name)

-- Step 2: Expire old version
UPDATE dim_customer
SET end_date = CURRENT_DATE - 1,
    is_current = false
WHERE customer_id IN (changed records)
AND is_current = true

-- Step 3: Insert new version
INSERT INTO dim_customer
SELECT new_surrogate_key, customer_id, name, city, 
       CURRENT_DATE, '9999-12-31', true
FROM changed records
```

**Drawbacks:**
- Table grows over time (one row per version per entity)
- Queries need to filter on `is_current = true` or specify a date range
- Joins must be on surrogate key (not natural key) to get the right version

### SCD Type 3 — Add new column (limited history)

Add a new column to store the previous value alongside the current value. Only one prior version is tracked.

**Use when:** you need to compare "before and after" for a single known change, not track arbitrary history.

```
dim_customer
CustomerKey | Name  | CurrentCity | PreviousCity | CityChangeDate
1           | Alice | New York     | Boston       | 2023-05-15
```

**Drawbacks:**
- Only tracks one change (if Alice moves again, Boston is lost)
- Schema changes required if more attributes need this treatment
- Rarely used in practice — Type 1 or Type 2 usually more appropriate

### SCD Type 4 — History table

Maintain a separate history table that stores all previous versions of the dimension, while the main dimension table always contains only the current version.

```
dim_customer (current state only)
CustomerKey | CustomerID | Name  | City

dim_customer_history (all changes)
HistoryKey | CustomerID | Name  | City | ChangedDate | ChangedBy
```

**Use when:** you want fast queries on current state (no is_current filter needed) but still need to access full history occasionally.

### SCD Type 6 — Hybrid (Type 1 + 2 + 3 combined)

Combines Types 1, 2, and 3. Maintains full row history (Type 2) AND carries the current attribute value on every historical row (Type 1) AND stores the original value in a separate column (Type 3).

```
dim_customer
CustomerKey | CustomerID | City     | CurrentCity | OriginalCity | EffDate    | EndDate    | IsCurrent
1           | C001       | Boston   | New York     | Boston       | 2020-01-01 | 2023-05-14 | false
47          | C001       | New York  | New York     | Boston       | 2023-05-15 | 9999-12-31 | true
```

**Use when:** you need historical accuracy (where was the customer when they bought?) AND current state for trend analysis (where do they live now?) AND original value for segmentation (where were they when they first joined?).

**In practice:** Type 6 is complex to implement and maintain. Only use it if you genuinely need all three capabilities.

### SCD decision guide

| Situation | Recommended SCD |
|---|---|
| Data correction (old value was wrong) | Type 1 |
| Change happened, history doesn't matter | Type 1 |
| Change happened, history matters for facts | Type 2 |
| Comparing current vs one prior value | Type 3 |
| Need fast current-state queries + occasional history | Type 4 |
| Need all three: historical accuracy + current state + original | Type 6 |
| Value never changes | Type 0 |

---

## 13. Bridge Tables — Handling Many-to-Many Relationships

### The problem

In relational modeling, a many-to-many relationship cannot be stored directly between two tables — you need a third table to resolve it.

In dimensional modeling, many-to-many relationships are very common:
- One movie can have multiple genres
- One person can have multiple professions
- One order can span multiple shipping addresses (hypothetically)
- One title can be released in multiple regions and languages

### The wrong approach

Storing comma-separated values in a single column:

```
title_basics
tconst | genres
tt001  | "Action,Drama,Thriller"
```

This fails because:
- You cannot join on `genres` to a genre dimension
- Filtering requires `LIKE '%Drama%'` which is slow and error-prone
- You cannot count titles per genre efficiently
- You cannot build a genre dimension from it without splitting and exploding first

### The right approach — bridge table

```
dim_genre                   bridge_title_genre              dim_title
GenreKey | GenreName         TitleGenreKey | TitleKey | GenreKey    TitleKey | Title
1        | Action            1             | 101      | 1           101      | The Dark Knight
2        | Drama             2             | 101      | 2           102      | Inception
3        | Thriller          3             | 101      | 3
                             4             | 102      | 1
                             5             | 102      | 2
```

Now "The Dark Knight" has 3 genre rows in the bridge, and "Inception" has 2. Every genre is a proper FK join. Counting titles per genre is a simple GROUP BY.

### Bridge table design rules

**Rule 1 — The bridge resolves the many-to-many**
It contains one row per valid combination of the two (or more) entities being related.

**Rule 2 — The bridge contains only FKs and optional relationship attributes**
No descriptive attributes of the entities themselves — those stay in their respective dimension tables.

**Optional bridge attributes:**
- Ordering/sequence (which genre is primary?)
- Is_primary flag (first profession vs secondary?)
- Effective dates (if the relationship has a validity period)
- Weight/strength (confidence score, percentage allocation)

**Rule 3 — Bridge tables need their own surrogate PK**
Every table needs a PK. The bridge gets its own surrogate key in addition to the FKs.

**Rule 4 — The grain of a bridge is one row per valid relationship pair**

```
bridge_title_genre
- TitleGenreKey (PK, surrogate)
- TitleKey (FK → dim_title)
- GenreKey (FK → dim_genre)
- GenreOrder (0 = primary, 1 = secondary, 2 = tertiary)
```

### Multi-hop bridges (three-way relationships)

Sometimes you need to model a relationship between three entities simultaneously.

```
bridge_title_crew
- TitleCrewKey (PK)
- TitleKey (FK → dim_title)     — which title
- NameKey (FK → dim_name)       — which person
- CrewKey (FK → dim_crew)       — what role (director/writer)
```

One row means: this specific person played this specific crew role on this specific title.

---

## 14. Surrogate Keys vs Natural Keys

### Natural keys

Natural keys are identifiers that come from the source system. Examples: `customer_id` from your CRM, `product_sku` from inventory, `tconst` from IMDb, social security number, email address.

**Problems with using natural keys as dimension PKs:**

1. **Source system changes break your warehouse.** If the CRM changes customer_id format from integers to GUIDs, every fact table FK becomes invalid.

2. **String keys slow down joins.** Comparing "nm0000001" == "nm0000001" is slower than comparing 1 == 1. At 98 million rows, this matters.

3. **SCD Type-2 is impossible.** When Alice Johnson moves cities, you need a new row for the new version. If the PK is her natural customer_id (which doesn't change), you can't have two rows with the same PK. You need a separate surrogate key to distinguish versions.

4. **Integration across source systems is impossible.** If you merge two CRMs where both have a customer_id = 1000, you have a collision. Surrogate keys are generated by your warehouse and are immune to source conflicts.

### Surrogate keys

Surrogate keys are system-generated integers (or decimal integers) with no business meaning. They are assigned by the warehouse when a new dimension row is created.

**Benefits:**
- Integer joins are fast regardless of row count
- Completely independent from source system changes
- Support SCD Type-2 versioning (different surrogate keys for different versions of the same entity)
- No collisions when integrating multiple source systems

**Why decimal(10,0) specifically?**

Some warehouse platforms (like Databricks with ER/Studio-generated DDL) use `decimal(10,0)` as a convention. It provides range up to 9,999,999,999 and avoids auto-increment limitations in distributed systems.

In PySpark/Databricks you generate surrogate keys with:
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = df.withColumn(
    "SurrogateKey",
    F.row_number().over(Window.orderBy("natural_key")).cast("decimal(10,0)")
)
```

Or with `monotonically_increasing_id()` for truly distributed generation (though this produces non-consecutive integers).

### The natural key still matters

Keep the natural key as a regular attribute column in the dimension — not the PK, but still stored. You need it to:
- Look up which surrogate key corresponds to a given source entity
- Join incoming fact data (which arrives with natural keys) to find the right surrogate key
- Debug and trace data lineage

---

## 15. The Dimensional Modeling Process — Step by Step

This is the Kimball Bus Architecture process. Follow it in order.

### Step 1 — Identify business processes

A business process is an operational activity that produces measurable results. Each business process typically becomes one or more fact tables.

Examples:
- Retail sales process → fact_sales
- Order fulfillment → fact_order_fulfillment
- Website clickstream → fact_page_views
- Customer support → fact_support_tickets

Don't model the entire enterprise in one pass. Identify the highest priority business process and model that first.

### Step 2 — Declare the grain

For your chosen business process, declare the grain of the fact table. Be as specific as possible. Write it as a single sentence:

*"One row per order line item in the sales transaction system."*
*"One row per rated title in the IMDb dataset."*
*"One row per episode as recorded in the series-episode relationship."*

### Step 3 — Identify the dimensions

With the grain declared, ask: *"For a single row at this grain, what context does an analyst need?"*

For grain = "one row per order line item":
- What product was sold? → dim_product
- Who bought it? → dim_customer
- When did it happen? → dim_date
- Where was it sold? → dim_store
- Which employee processed it? → dim_employee

Every dimension you add must be directly determined by the grain. If "store region" requires knowing the store first, then joining to a region table, that's either an attribute on dim_store (flatten it) or an outrigger (rarely justified).

### Step 4 — Identify the facts (measures)

With the grain and dimensions identified, ask: *"What numeric values are measured at this grain?"*

For grain = "one row per order line item":
- Quantity sold
- Unit price
- Extended revenue (quantity × price)
- Discount amount
- Cost of goods

Apply the additivity test to each. Confirm each measure is produced by the event (not inherent to a product or customer).

### Step 5 — Define the physical schema

For each dimension, determine:
- Which columns from the source feeds into this dimension
- What transformations are needed (type casting, null handling, standardization)
- What SCD type applies
- What the surrogate key generation strategy is

For each fact table:
- Which source tables feed it
- How to join source data to dimension surrogate keys
- What transformations apply to measures
- What the grain validation looks like

### Step 6 — Document the bus matrix

A bus matrix maps every fact table (rows) against every dimension (columns). A checkmark means that fact table uses that dimension. This is your enterprise integration map.

```
                    dim_date  dim_customer  dim_product  dim_store  dim_employee
fact_sales             ✓          ✓             ✓           ✓           ✓
fact_inventory         ✓                        ✓           ✓
fact_web_traffic       ✓          ✓
fact_support_tickets   ✓          ✓                                     ✓
```

This matrix tells you which dimensions need to be conformed (shared across multiple fact tables) and which are fact-specific.

---

## 16. Common Modeling Mistakes

### Mistake 1 — Getting the grain wrong

Mixing rows of different granularity in the same fact table. This causes double-counting and unpredictable aggregation behavior.

**Example of wrong grain:** having both order-level and line-item-level rows in the same fact table because some orders have one line and some have multiple.

**Fix:** always pick the lowest grain and aggregate up if needed.

### Mistake 2 — Putting descriptive text in fact tables

Storing product_name, customer_city, or category_description directly in the fact table instead of using FK references to dimensions.

**Consequence:** the same description might be spelled differently across rows. When the category name changes, you have to update millions of fact rows. Query performance degrades.

### Mistake 3 — Snowflaking dimensions unnecessarily

Normalizing dim_product into dim_product + dim_category + dim_department to save storage.

**Consequence:** every query that needs category requires two joins instead of one. BI tools get confused. Analysts write more complex SQL.

**Rule:** flatten unless you have a compelling storage or maintenance reason not to.

### Mistake 4 — Using natural keys as dimension PKs

Using `customer_id` from the source system as the PK of dim_customer.

**Consequence:** SCD Type-2 is impossible. Source system changes break your warehouse. Cross-system integration collides.

### Mistake 5 — No declared grain

Building a fact table without explicitly declaring what one row represents.

**Consequence:** the table grows in unpredictable ways as new data arrives. Different analysts interpret the table differently. Aggregations silently double-count.

### Mistake 6 — Fact tables without any measures

A fact table with only foreign keys and no measures. This is usually a bridge table trying to be a fact table, or a design that needs rethinking.

**Exception:** factless fact tables (enrollment, coverage, event occurrence) are valid — see section 10.

### Mistake 7 — Wrong SCD type

Using SCD Type-1 when the business needs historical accuracy.

**Example:** a retailer uses SCD Type-1 on dim_customer's region. When a customer moves from Boston to New York, all their historical purchases now show New York. The company incorrectly reports New York as their top market, when Boston was actually the top market.

**Fix:** identify which dimension attributes need historical tracking at design time, not after the data is wrong.

### Mistake 8 — Missing date dimension

Storing dates as raw strings or timestamps in the fact table instead of FK references to a pre-built dim_date.

**Consequence:** every time-based query requires `DATEPART()`, `EXTRACT()`, `TRUNC()` — expensive and inconsistent. Fiscal year logic gets duplicated across every query. Is_holiday is never available.

---

## 17. Schema Comparison — When to Use What

| Criteria | Star Schema | Snowflake | Galaxy / Constellation | OBT |
|---|---|---|---|---|
| Query simplicity | ✅ Simplest | ❌ More complex | ✅ Good | ✅ Simplest |
| Query performance | ✅ Fastest | ❌ Slower | ✅ Fast | ✅ Fastest |
| Storage efficiency | ❌ Some redundancy | ✅ Minimal redundancy | ❌ Some redundancy | ❌ High redundancy |
| BI tool compatibility | ✅ Best | ⚠️ Moderate | ✅ Good | ✅ Good |
| Maintenance complexity | ✅ Low | ❌ Higher | ⚠️ Moderate | ✅ Low |
| Multi-subject queries | ⚠️ One subject | ⚠️ One subject | ✅ Multiple subjects | ❌ Single subject |
| Analyst usability | ✅ Highest | ❌ Lower | ✅ High | ✅ High |
| When to use | Default choice | Large dimensions with complex hierarchies | Enterprise warehouse with multiple processes | Single-subject analytics at scale |

**The practical decision tree:**

1. Is this a single business process with one fact table? → **Star schema**
2. Do you have multiple fact tables sharing dimensions? → **Galaxy schema** (collection of star schemas)
3. Are your dimension tables extremely large (100M+ rows) with deep hierarchies? → Consider **snowflake** for those dimensions only
4. Is this a single, well-defined analytical subject with no complex joins needed? → **OBT** may be appropriate

In practice, most modern data warehouses are **galaxy schemas** — multiple star schemas sharing conformed dimensions. The Kimball Bus Architecture is the organizing principle.

---

## 18. Interview Questions and Model Answers

### Foundational questions

**Q: What is the difference between a fact table and a dimension table?**

> A fact table records measurable events — transactions, ratings, clicks — and contains numeric measures you can aggregate. A dimension table describes the context around those events — who, what, where, when — and contains attributes you filter and group by. The fact table is tall and narrow; dimensions are shorter and wide. They join together through surrogate foreign keys.

**Q: What is grain and why does it matter?**

> Grain is the declaration of what one row in a fact table represents. It's the single most important design decision in dimensional modeling because every column you add must be true at exactly that grain. Wrong grain causes double-counting, unexpected join fan-out, and analysts losing trust in the numbers. You always declare grain before writing any column names.

**Q: Why use a star schema instead of a normalized 3NF schema for analytics?**

> A normalized schema is designed for write performance and storage efficiency — correct for transactional systems. For analytics, analysts need to join many attributes to answer business questions. A star schema denormalizes dimensions so analysts get all context about an entity in one join, not five. The trade-off is some storage redundancy for significantly simpler, faster queries and better BI tool compatibility.

**Q: What is a conformed dimension?**

> A conformed dimension is a dimension table shared across multiple fact tables in the warehouse, with identical keys, column names, and values. dim_date used by both fact_sales and fact_web_traffic is conformed — this allows analysts to drill across both fact tables using the same date attributes. Conformed dimensions are the foundation of Kimball's Bus Architecture.

### SCD questions

**Q: What are the different types of slowly changing dimensions?**

> The most common are Types 1, 2, and 3. Type 1 overwrites the old value — use it for corrections or when history doesn't matter. Type 2 inserts a new row for each change, preserving full history with effective date, end date, and is_current columns — use it when historical accuracy matters for fact analysis. Type 3 adds a column for the previous value — use it when you need a before/after comparison for a single known attribute change. Type 6 combines all three for maximum flexibility at higher implementation cost.

**Q: When would you choose SCD Type-2 over Type-1?**

> SCD Type-2 is necessary when a dimension attribute changes and you need historical fact rows to reflect the attribute value as it was at the time of the event. For example, if a customer moves from Boston to New York, SCD Type-2 ensures sales from before the move still show Boston, and sales after show New York. If you used Type-1, you'd rewrite history — all historical Boston sales would suddenly look like New York sales, making your regional analysis wrong.

### Design decision questions

**Q: A column is numeric. Does it automatically go in a fact table?**

> No. The test is not whether it is numeric — it is whether it is produced by a measurement event and meaningful to aggregate. RuntimeMinutes is numeric but it is intrinsic to the movie — it exists the moment the movie is catalogued, not because an event occurred. It belongs in the dimension. AverageRating is numeric and produced by the voting event — it belongs in the fact. The key question is: was this value generated by an event, or is it a property of the entity?

**Q: What is a bridge table and when do you need one?**

> A bridge table resolves a many-to-many relationship between a fact table and a dimension — or between two dimensions. You need one when one entity can have multiple values of another entity: one movie can have multiple genres, one person can have multiple professions, one order can ship to multiple addresses. Without a bridge, you'd store comma-separated values in a single column, which breaks joins, kills performance, and prevents proper aggregation.

**Q: What is a degenerate dimension?**

> A degenerate dimension is a natural key stored in the fact table with no associated dimension table. It is a key that has no additional attributes worth storing separately. Order numbers, invoice numbers, and ticket numbers are common examples — the number itself is the only useful value; there is no "dim_order" needed. It stays in the fact table for filtering and grouping without the overhead of a separate dimension join.

**Q: Why use surrogate keys instead of natural keys?**

> Three reasons. First, surrogate keys are integers — integer joins are significantly faster than string joins at scale. Second, they insulate the warehouse from source system changes — if the CRM changes its customer ID format, your warehouse surrogate keys remain valid. Third, SCD Type-2 requires multiple rows per entity (one per version), which is impossible if the natural key is the PK — surrogate keys give each version its own unique identifier.

### Advanced questions

**Q: What is the difference between a transaction fact, periodic snapshot, and accumulating snapshot?**

> A transaction fact records discrete events — one row per event, append-only, point in time. A periodic snapshot takes a regular measurement at fixed intervals — one row per entity per period, even if nothing changed, used for trend analysis. An accumulating snapshot tracks one entity through a pipeline lifecycle — one row per entity, updated in place as it moves through stages, with multiple date FKs for each milestone. The type you choose depends on the business process being modeled.

**Q: Can a fact table change over time (like SCD)?**

> Facts don't use SCD — they use snapshot patterns instead. For a slowly changing measure like a stock price or account balance, you use a periodic snapshot fact — insert a new row for each time period rather than updating existing rows. For truly historical fact tracking, you add a snapshot_date column and retain all versions. The difference from SCD is that facts are event-driven — the mechanism is inserting new rows (representing new measurements) rather than versioning existing rows (representing attribute changes on an entity).

**Q: What is a factless fact table?**

> A factless fact table contains only foreign keys with no measures. It records that an event occurred or captures a relationship between entities without any numeric measurement. Common use cases are coverage tables (which promotions apply to which products on which dates) and event occurrence tables (which students are enrolled in which courses — the row itself means enrollment happened, no quantity to measure).

---

## Quick Reference — Rules Summary

### Fact table rules
1. Declare grain first — before any columns
2. Measures must be additive, semi-additive, or non-additive (never meaningless to aggregate)
3. Measures are produced by events, not inherent to entities
4. Only FK references + measures + optional degenerate dimensions
5. Never store descriptive text directly — use FK to dimension
6. Tall (many rows) and narrow (few columns)

### Dimension table rules
1. Describes an entity — one row per entity
2. Wide — as many descriptive attributes as useful (denormalize)
3. Attributes used to filter, group, and label — not aggregate
4. Surrogate integer PK — never natural key as PK
5. Keep natural key as a regular attribute column
6. One canonical place for each attribute — no duplication across tables

### SCD selection rules
- Correcting wrong data → Type 1
- Change happened, history irrelevant → Type 1
- Change happened, historical fact accuracy required → Type 2
- Before/after comparison for one attribute → Type 3
- Never changes → Type 0

### Schema selection rules
- Single process, one fact table → Star schema
- Multiple processes, shared dimensions → Galaxy schema (Kimball Bus)
- Huge dimensions with complex hierarchies, storage constrained → Snowflake (selective)
- Single subject, no complex joins → OBT

---

*Reference document compiled from Kimball Group methodology, "The Data Warehouse Toolkit" (Kimball & Ross), and production data engineering practice.*
