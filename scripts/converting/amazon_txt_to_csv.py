import pandas as pd
import csv  # <-- import Python's built-in csv module

# Input and output files
INPUT_TXT = "input/all_listings.txt"
OUTPUT_CSV = "output/all_listings.csv"

# Read the TXT file (tab-separated)
df = pd.read_csv(INPUT_TXT, sep="\t", dtype=str)

# Save as CSV with quoting to handle commas properly
df.to_csv(OUTPUT_CSV, index=False, quoting=csv.QUOTE_ALL)

print(f"Saved CSV with {len(df)} rows to {OUTPUT_CSV}")
