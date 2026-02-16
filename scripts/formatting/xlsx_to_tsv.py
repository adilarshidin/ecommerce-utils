import pandas as pd

# Input and output file paths
input_file = "output/Flat.File.PriceInventory.es.xlsx"
output_file = "output/Flat.File.PriceInventory.es.tsv"

# Read the Excel file
df = pd.read_excel(input_file)

# Save as TSV (tab-separated values)
df.to_csv(output_file, sep='\t', index=False)

print(f"Converted '{input_file}' to '{output_file}' successfully!")
