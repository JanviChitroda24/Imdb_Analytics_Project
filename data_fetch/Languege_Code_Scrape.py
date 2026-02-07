"""
scrape_language_codes.py
========================
Scrapes ISO 639 language codes from Wikipedia and saves to CSV.

Output: language_codes.csv
Columns:
    LanguageCode        — 2-letter ISO 639-1 code (e.g. "en", "fr", "ja")
                          This is what IMDb uses in title.akas.language
    LanguageDescription — English name of the language (e.g. "English", "French")

Run:
    python scrape_language_codes.py

Requirements:
    pip install requests beautifulsoup4 pandas
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

# ── Step 1: Fetch the Wikipedia page ──────────────────────────────────────────
URL = "https://en.wikipedia.org/wiki/List_of_ISO_639_language_codes"

print(f"Fetching: {URL}")
response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
response.raise_for_status()
print(f"Status: {response.status_code} OK")

# ── Step 2: Parse the HTML ─────────────────────────────────────────────────────
soup = BeautifulSoup(response.text, "html.parser")

# The language table is the first wikitable on the page
# It has columns: ISO Language Names | Set 1 | Set 2 | Set 3 | Scope | Type | ...
table = soup.find("table", {"class": "wikitable"})
if not table:
    raise ValueError("Could not find the wikitable on the page. Wikipedia may have changed its layout.")

print("Found wikitable. Parsing rows...")

# ── Step 3: Extract rows ───────────────────────────────────────────────────────
rows = table.find_all("tr")
print(f"Total rows found (including header): {len(rows)}")

results = []
skipped = 0

for row in rows[1:]:  # skip the header row
    cells = row.find_all(["td", "th"])

    if len(cells) < 2:
        skipped += 1
        continue

    # Column 0: ISO Language Name(s) — may contain commas for alt names
    # e.g. "Catalan, Valencian" → we keep the full string as the description
    lang_name_raw = cells[0].get_text(separator=" ", strip=True)

    # Column 1: Set 1 (2-letter ISO 639-1 code)
    # Some rows have multiple codes or no Set 1 code at all (only Set 2/3)
    set1_raw = cells[1].get_text(strip=True) if len(cells) > 1 else ""

    # Clean the language name:
    # - Remove footnote references like [1], [2]
    # - Normalize whitespace
    lang_name = re.sub(r"\[\d+\]", "", lang_name_raw).strip()
    # Take only the first name if there are multiple (e.g. "Catalan, Valencian" → "Catalan")
    # We keep the full description for richness but store the primary name
    lang_description = lang_name  # full name including alternatives

    # Clean the code:
    # - Strip whitespace
    # - Remove footnote refs
    # - Must be exactly 2 lowercase letters to be a valid ISO 639-1 code
    code = re.sub(r"\[\d+\]", "", set1_raw).strip()

    # Skip rows where Set 1 code is missing (some languages only have 3-letter codes)
    if not code or not re.match(r"^[a-z]{2}$", code):
        skipped += 1
        continue

    # Skip if language name is empty
    if not lang_description:
        skipped += 1
        continue

    results.append({
        "LanguageCode": code,
        "LanguageDescription": lang_description
    })

print(f"Rows parsed successfully: {len(results)}")
print(f"Rows skipped (no valid Set 1 code): {skipped}")

# ── Step 4: Build DataFrame and validate ──────────────────────────────────────
df = pd.DataFrame(results)

# Deduplicate — shouldn't happen but Wikipedia tables can have oddities
before_dedup = len(df)
df = df.drop_duplicates(subset=["LanguageCode"])
after_dedup = len(df)
if before_dedup != after_dedup:
    print(f"Deduplication removed {before_dedup - after_dedup} duplicate codes")

# Sort by code for clean output
df = df.sort_values("LanguageCode").reset_index(drop=True)

# ── Step 5: Validate ──────────────────────────────────────────────────────────
print("\n── Validation ──────────────────────────────────────────────")

# Check expected count (Wikipedia lists ~184 ISO 639-1 codes)
print(f"Total rows: {len(df)}")
assert 150 <= len(df) <= 220, f"Unexpected row count: {len(df)}. Check the scraper."

# Check no null codes
null_codes = df["LanguageCode"].isnull().sum()
print(f"Null codes: {null_codes} (expected: 0)")
assert null_codes == 0

# Check no null descriptions
null_desc = df["LanguageDescription"].isnull().sum()
print(f"Null descriptions: {null_desc} (expected: 0)")
assert null_desc == 0

# Spot-check a few well-known codes
spot_checks = {
    "en": "English",
    "fr": "French",
    "de": "German",
    "ja": "Japanese",
    "zh": "Chinese",
    "es": "Spanish",
}
print("\nSpot checks:")
for code, expected_name in spot_checks.items():
    match = df[df["LanguageCode"] == code]
    if match.empty:
        print(f"  ❌ Code '{code}' NOT FOUND")
    else:
        actual = match.iloc[0]["LanguageDescription"]
        # Check that the expected name appears somewhere in the description
        found = expected_name.lower() in actual.lower()
        status = "✅" if found else "⚠️ "
        print(f"  {status} '{code}' → '{actual}'")

# ── Step 6: Save to CSV ───────────────────────────────────────────────────────
OUTPUT_PATH = "../data/reference/language_codes.csv"
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
print(f"\n✅ Saved to: {OUTPUT_PATH}")
print(f"   Rows: {len(df)}")
print(f"   Columns: {list(df.columns)}")

# ── Step 7: Preview ───────────────────────────────────────────────────────────
print("\nFirst 10 rows:")
print(df.head(10).to_string(index=False))

print("\nLast 5 rows:")
print(df.tail(5).to_string(index=False))