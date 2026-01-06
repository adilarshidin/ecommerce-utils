import csv
from collections import Counter

INPUT_FILE = "output/translated_catalog.csv"
OUTPUT_FILE = "output/duplicated_catalog.csv"

# Read all rows
with open(INPUT_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    fieldnames = reader.fieldnames

# Count occurrences of NOMBRE
nombre_counts = Counter(row["NOMBRE"] for row in rows)

# Filter rows where NOMBRE appears more than once
duplicate_rows = [
    row for row in rows if nombre_counts[row["NOMBRE"]] > 1
]

# Write duplicates to new file
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(duplicate_rows)

print(f"Found {len(duplicate_rows)} duplicated rows.")
print(f"Saved to {OUTPUT_FILE}")
