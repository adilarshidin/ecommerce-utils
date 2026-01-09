import pandas as pd

# Input files
CSV_EN = "output/translated_catalog.csv"
CSV_ES = "output/translated_catalog_es.csv"
OUTPUT = "output/catalog_ready.csv"

# Read CSVs
df_en = pd.read_csv(CSV_EN)
df_es = pd.read_csv(CSV_ES)

# Rename NOMBRE columns
df_en = df_en.rename(columns={"NOMBRE": "NOMBRE_EN"})
df_es = df_es.rename(columns={"NOMBRE": "NOMBRE_ES"})

# Drop Spanish NOMBRE from EN df and English NOMBRE from ES df
df_es = df_es[["NOMBRE_ES"]]

# Combine side-by-side
df_combined = pd.concat([df_en, df_es], axis=1)

# Save output
df_combined.to_csv(OUTPUT, index=False)

print(f"Combined CSV saved as {OUTPUT}")
