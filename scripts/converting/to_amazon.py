import pandas as pd

# Load your CSV
input_csv = 'input/konus_catalog.csv'  # your source CSV
df = pd.read_csv(input_csv, delimiter=';', encoding='latin-1')

# Create a new DataFrame for Amazon flat file
amazon_df = pd.DataFrame()

# Mapping your CSV columns to Amazon required fields
amazon_df['SKU'] = df['Código']  # unique identifier
amazon_df['Product Type'] = df['Tipo']  # e.g., Prismático
amazon_df['Brand Name'] = df['Marca']  # e.g., Konus
amazon_df['Item Name'] = df['Título_producto']  # Amazon title
amazon_df['Product Description'] = df['Descripción_larga']  # Long description
amazon_df['Bullet Point1'] = df['Descripción_corta']  # Short description as bullet
amazon_df['MSRP'] = df['PVP FINAL'] # Amazon expects decimal
amazon_df['MSRP'] = (
    amazon_df['MSRP']
    .str.replace('€', '')      # normal euro
    .str.replace('', '')      # malformed euro
    .str.replace(',', '.')     # convert decimal comma to dot
    .str.strip()
)
amazon_df['Main Image URL'] = df['Imagen_grande']  # Image URL

# Optional: add default values for mandatory Amazon fields
amazon_df['Quantity'] = 1  # default stock
amazon_df['Condition Type'] = 'New'
amazon_df['Fulfillment Latency'] = 1  # default 1 day for FBA
amazon_df['Standard Price'] = amazon_df['MSRP']

# Fill other required fields if needed
amazon_df['Product ID Type'] = 'EAN'
amazon_df['Product ID'] = df['EAN']


# Save to TSV for Amazon upload
output_tsv = 'output/konus_amazon_ready.tsv'
amazon_df.to_csv(output_tsv, index=False, sep='\t')  # Amazon usually prefers tab-delimited
print(f"Amazon-ready TSV saved to {output_tsv}")
