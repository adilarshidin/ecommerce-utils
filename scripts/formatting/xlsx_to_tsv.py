import pandas as pd

# Input and output file paths
input_file = "output/konus_amazon_ready.xlsx"  # replace with your .xlsx file path
output_file = "output/konus_amazon_ready.tsv"  # desired output .tsv file path

# Read the Excel file
df = pd.read_excel(input_file)

# Save as TSV (tab-separated values)
df.to_csv(output_file, sep='\t', index=False)

print(f"Converted '{input_file}' to '{output_file}' successfully!")
