import pandas as pd

# Input files
ACTIVE_CSV = "output/active_items.csv"
CATALOG_CSV = "output/asin_results.csv"

# Output file
UNDERSOLD_CSV = "output/undersold_items.csv"

# Read files
active_df = pd.read_csv(ACTIVE_CSV, dtype=str)
catalog_df = pd.read_csv(CATALOG_CSV, dtype=str)

# Convert prices to numeric for comparison
active_df["price"] = pd.to_numeric(active_df["price"], errors="coerce")
catalog_df["price"] = pd.to_numeric(catalog_df["Final PVP €"], errors="coerce")

# Melt ASIN columns in active items to one row per ASIN
active_asin_cols = ["asin1", "asin2", "asin3"]
active_melted = active_df.melt(
    id_vars=[col for col in active_df.columns if col not in active_asin_cols],
    value_vars=active_asin_cols,
    var_name="ASIN_col",
    value_name="ASIN"
)

# Drop rows with empty ASIN
active_melted = active_melted.dropna(subset=["ASIN"])

# Merge catalog with active items by ASIN
merged = pd.merge(
    catalog_df,
    active_melted,
    on="ASIN",
    suffixes=("_catalog", "_amazon")
)

# Identify undersold products (Amazon price < catalog price)
undersold = merged[merged["price_amazon"] < merged["price_catalog"]].copy()

# Optional: keep only relevant columns
undersold = undersold[
    ["ASIN", "item-name", "price_catalog", "price_amazon"]
]

# Save result
undersold.to_csv(UNDERSOLD_CSV, index=False)
print(f"Found {len(undersold)} undersold items. Saved to {UNDERSOLD_CSV}")
