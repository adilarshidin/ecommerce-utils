import pandas as pd
from datetime import datetime

# Load Shopify inventory and Amazon catalog
shopify_file = 'input/sellerboard_inventory.xlsx'
amazon_catalog_file = 'input/catalog.csv'
df_shopify = pd.read_excel(shopify_file)
df_amazon_catalog = pd.read_csv(amazon_catalog_file)

# Get unique PROVEEDOR values from existing catalog
unique_proveedores = set(df_amazon_catalog['PROVEEDOR'].dropna())
print("Distinct PROVEEDOR values:")
print(unique_proveedores)

# Function to match PROVEEDOR in product title
def find_proveedor(title):
    for proveedor in unique_proveedores:
        if proveedor.lower() in str(title).lower():  # case-insensitive match
            return proveedor
    return 'Shopify'  # fallback if no match

# Prepare catalog DataFrame
df_catalog = pd.DataFrame()
df_catalog['PROVEEDOR'] = df_shopify['Title'].apply(find_proveedor)
df_catalog['EAN'] = df_shopify['SKU']  # assuming SKU is the EAN
df_catalog['NOMBRE'] = df_shopify['Title']
df_catalog['COSTOS'] = df_shopify['Cost']
df_catalog['FECHA'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
df_catalog['Fijo €'] = 1.5
df_catalog['ASIN'] = df_shopify['ASIN']

# Fill the rest with empty strings
columns_to_add = ['Variable %', 'Variable €', 'Precio €', 'Beneficio €', 
                  'Beneficio %', 'PVP (Con Tax)', 'Comision Amazon %', 
                  'Comision Amazon €', 'Final PVP €']

for col in columns_to_add:
    df_catalog[col] = ''

# Save to CSV
df_catalog.to_csv('output/shopify_inventory_formatted.csv', index=False)
