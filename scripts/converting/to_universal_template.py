import json
from openpyxl import load_workbook
from openpyxl.worksheet.datavalidation import DataValidation

def flatten_categories(categories, parent_path=""):
    paths = []

    for cat in categories:
        current_path = (
            f"{parent_path} > {cat['name']}"
            if parent_path
            else cat["name"]
        )

        if "children" not in cat or not cat["children"]:
            paths.append(current_path)
        else:
            paths.extend(flatten_categories(cat["children"], current_path))

    return paths


# Load JSON
with open("templates/shopify_categories.json", "r", encoding="utf-8") as f:
    data = json.load(f)

category_paths = flatten_categories(data)

wb = load_workbook("templates/universal_template.xlsx")

product_sheet = wb.active  # or wb["Products"]

# Create / reuse hidden sheet
sheet_name = "Shopify_Categories"
if sheet_name in wb.sheetnames:
    cat_sheet = wb[sheet_name]
else:
    cat_sheet = wb.create_sheet(sheet_name)

# Write categories to column A
for i, path in enumerate(category_paths, start=1):
    cat_sheet.cell(row=i, column=1, value=path)

cat_sheet.sheet_state = "hidden"

# Create dropdown
max_row = len(category_paths)
dv = DataValidation(
    type="list",
    formula1=f"='{sheet_name}'!$A$1:$A${max_row}",
    allow_blank=True
)

product_sheet.add_data_validation(dv)

# Apply dropdown to G3 and below
dv.add("G3:G10000")

wb.save("products_with_categories.xlsx")
