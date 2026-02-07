"""
scrape_region_codes.py
======================
Scrapes IMDb country/region codes from the IMDb Help Center and saves to CSV.

Source: https://help.imdb.com/article/contribution/other-submission-guides/country-codes/G99K4LFRMSC37DCN

Output: region_codes.csv
Columns:
    RegionCode        — 2-letter (or 4-letter for historical) code  e.g. "us", "gb", "cshh"
                        This is what IMDb uses in title.akas.region
    RegionDescription — Country name e.g. "United States", "United Kingdom (Great Britain)"
    IsHistorical      — 0 = current country, 1 = defunct/historical country

Key difference from language scraper:
    The IMDb page does NOT use an HTML table for the main list.
    Data is in <ul> bullet lists formatted as:  "code CountryName"
    e.g. "af Afghanistan", "us United States"
    The historical codes section IS in a small HTML table at the bottom.

Run:
    python scrape_region_codes.py

Requirements:
    pip install requests beautifulsoup4 pandas
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

# ── Step 1: Fetch the IMDb Help page ──────────────────────────────────────────
URL = "https://help.imdb.com/article/contribution/other-submission-guides/country-codes/G99K4LFRMSC37DCN#"

print(f"Fetching: {URL}")
response = requests.get(
    URL,
    headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    },
    timeout=30
)
response.raise_for_status()
print(f"Status: {response.status_code} OK")

# ── Step 2: Parse HTML ─────────────────────────────────────────────────────────
soup = BeautifulSoup(response.text, "html.parser")

# ── Step 3: Extract CURRENT country codes from bullet lists ───────────────────
# The page has 3 <ul> lists side by side, each containing <li> items
# Each <li> looks like:  "af Afghanistan"
# The code is always the FIRST word (2 lowercase letters)
# The description is everything after the code

results = []

all_li_items = soup.find_all("li")
print(f"Total <li> items found on page: {len(all_li_items)}")

for li in all_li_items:
    text = li.get_text(separator=" ", strip=True)

    # Clean up whitespace and non-breaking spaces
    text = re.sub(r"\s+", " ", text).strip()

    # Pattern: starts with a 2-letter lowercase code followed by a space and country name
    # e.g. "af Afghanistan" or "gb United Kingdom (Great Britain)"
    match = re.match(r"^([a-z]{2})\s+(.+)$", text)
    if not match:
        continue  # skip nav items, footer links, etc.

    code = match.group(1)
    description = match.group(2).strip()

    # Extra guard: description must be at least 3 chars (avoids matching random 2-letter items)
    if len(description) < 3:
        continue

    results.append({
        "RegionCode": code,
        "RegionDescription": description,
        "IsHistorical": 0
    })

print(f"Current country codes extracted: {len(results)}")

# ── Step 4: Extract HISTORICAL codes from the second table ────────────────────
# The historical table has 2 columns: code | country name
# Codes here are 4-letter (e.g. "cshh", "ddde", "suhh")

tables = soup.find_all("table")
print(f"Tables found on page: {len(tables)}")

historical_count = 0
for table in tables:
    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        code_raw = cells[0].get_text(strip=True)
        desc_raw = cells[1].get_text(strip=True)

        # Historical codes are 4 lowercase letters
        if re.match(r"^[a-z]{4}$", code_raw) and len(desc_raw) > 2:
            results.append({
                "RegionCode": code_raw,
                "RegionDescription": desc_raw,
                "IsHistorical": 1
            })
            historical_count += 1

print(f"Historical country codes extracted: {historical_count}")

# ── Step 5: Build DataFrame and clean ─────────────────────────────────────────
df = pd.DataFrame(results)

# Deduplicate — the page has "gb United Kingdom" listed twice (once in each column group)
before_dedup = len(df)
df = df.drop_duplicates(subset=["RegionCode"])
after_dedup = len(df)
if before_dedup != after_dedup:
    print(f"Deduplication removed {before_dedup - after_dedup} duplicate codes (expected: gb listed twice)")

# Sort: current countries first (alphabetical by code), then historical
df_current    = df[df["IsHistorical"] == 0].sort_values("RegionCode").reset_index(drop=True)
df_historical = df[df["IsHistorical"] == 1].sort_values("RegionCode").reset_index(drop=True)
df = pd.concat([df_current, df_historical], ignore_index=True)

# ── Step 6: Validate ──────────────────────────────────────────────────────────
print("\n── Validation ──────────────────────────────────────────────")
print(f"Total rows: {len(df)}")
print(f"  Current  countries: {len(df[df['IsHistorical'] == 0])}")
print(f"  Historical countries: {len(df[df['IsHistorical'] == 1])}")

# IMDb lists ~246 current codes + 5 historical = ~251 total
assert 200 <= len(df) <= 300, f"Unexpected total row count: {len(df)}"
assert len(df[df["IsHistorical"] == 1]) == 5, \
    f"Expected exactly 5 historical codes, got {len(df[df['IsHistorical'] == 1])}"

# No nulls
assert df["RegionCode"].isnull().sum() == 0, "Null codes found"
assert df["RegionDescription"].isnull().sum() == 0, "Null descriptions found"

# Spot-check well-known codes
spot_checks = {
    "us": "United States",
    "gb": "United Kingdom",
    "de": "Germany",
    "fr": "France",
    "jp": "Japan",
    "in": "India",
    "cn": "China",
    "au": "Australia",
    "cshh": "Czechoslovakia",   # historical
    "suhh": "Soviet Union",     # historical
}
print("\nSpot checks:")
for code, expected_name in spot_checks.items():
    match = df[df["RegionCode"] == code]
    if match.empty:
        print(f"  ❌ Code '{code}' NOT FOUND")
    else:
        actual = match.iloc[0]["RegionDescription"]
        found = expected_name.lower() in actual.lower()
        status = "✅" if found else "⚠️ "
        print(f"  {status} '{code}' → '{actual}'")

# ── Step 7: Save to CSV ───────────────────────────────────────────────────────
OUTPUT_PATH = "../data/reference/region_codes.csv"
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
print(f"\n✅ Saved to: {OUTPUT_PATH}")
print(f"   Rows: {len(df)}")
print(f"   Columns: {list(df.columns)}")

# ── Step 8: Preview ───────────────────────────────────────────────────────────
print("\nFirst 10 rows (current):")
print(df.head(10).to_string(index=False))

print("\nHistorical codes:")
print(df[df["IsHistorical"] == 1].to_string(index=False))