import pandas as pd

# Paths
INPUT_CSV = "output/catalog_es_us_filtered_by_asin.csv"
OUTPUT_CSV = "output/catalog_for_buyer.csv"

# Load input file
df = pd.read_csv(INPUT_CSV)

# Build Amazon ES URL from ASIN
df["url"] = "https://www.amazon.es/dp/" + df["ASIN"].astype(str)

# Keep only required columns
output_df = df[["NOMBRE_ES", "NOMBRE_EN", "url"]]

# Write output file
output_df.to_csv(OUTPUT_CSV, index=False)

print(f"File written successfully: {OUTPUT_CSV}")
