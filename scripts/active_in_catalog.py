import pandas as pd

# Input files
ACTIVE_CSV = "../output/active_items.csv"
CATALOG_CSV = "../output/asin_results.csv"

# Output file for missing ASINs
MISSING_CSV = "../output/missing_in_catalog.csv"

# Read files
active_df = pd.read_csv(ACTIVE_CSV, dtype=str)
catalog_df = pd.read_csv(CATALOG_CSV, dtype=str)

# Melt ASIN columns in active items to one row per ASIN
asin_cols = ["asin1", "asin2", "asin3"]
active_melted = active_df.melt(
    id_vars=[col for col in active_df.columns if col not in asin_cols],
    value_vars=asin_cols,
    var_name="ASIN_col",
    value_name="ASIN"
).dropna(subset=["ASIN"])

# Create sets for faster comparison
active_asins = set(active_melted["ASIN"])
catalog_asins = set(catalog_df["ASIN"])

# Calculate how many active products exist in the catalog
present_asins = active_asins & catalog_asins
missing_asins = active_asins - catalog_asins

print(f"Total active ASINs: {len(active_asins)}")
print(f"Active ASINs found in catalog: {len(present_asins)} ({len(present_asins)/len(active_asins)*100:.2f}%)")
print(f"Active ASINs missing in catalog: {len(missing_asins)} ({len(missing_asins)/len(active_asins)*100:.2f}%)")

# Optional: save missing ASINs with full original row info
missing_rows = active_melted[active_melted["ASIN"].isin(missing_asins)]
missing_rows.to_csv(MISSING_CSV, index=False)

print(f"Missing ASINs saved to {MISSING_CSV}")
