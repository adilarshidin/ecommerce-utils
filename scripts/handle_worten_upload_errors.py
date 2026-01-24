import os
import json
import pandas as pd
import re
from dotenv import load_dotenv
from mistralai import Mistral
from openpyxl import load_workbook

# =========================
# Config
# =========================
load_dotenv()

MODEL = "mistral-large-latest"
client = Mistral(api_key=os.getenv("MISTRAL_API_TOKEN"))

ERRORS_FILE = "input/worten_errors_bricolaje_y_construccion.xlsx"
PRODUCTS_FILE = "output/worten/bricolaje_y_construccion.xlsx"
OUTPUT_FILE = "output/worten/bricolaje_y_construccion_full.xlsx"  # Save as Excel

# =========================
# Load Excel files
# =========================
errors_df = pd.read_excel(ERRORS_FILE)
products_df = pd.read_excel(PRODUCTS_FILE, sheet_name="Data", header=1)

# =========================
# Utils
# =========================
def extract_json(text: str) -> dict:
    text = text.strip()
    text = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return {}
    try:
        return json.loads(match.group())
    except json.JSONDecodeError:
        return {}

def get_image_urls(product_row: pd.Series) -> list[str]:
    image_cols = [c for c in product_row.index if c.startswith("image") and pd.notna(product_row[c])]
    return [product_row[c] for c in image_cols]

# =========================
# Process each error
# =========================
results = []

for idx, err_row in errors_df.iterrows():
    product_id = err_row["product_id"]
    missing_cols = [col.strip() for col in str(err_row["errors"]).split(",")]

    product_row = products_df[products_df["product_id"] == product_id]
    if product_row.empty:
        print(f"⚠ Product {product_id} not found in products file.")
        continue

    product_dict = product_row.iloc[0].to_dict()
    images = get_image_urls(product_row.iloc[0])
    product_dict["images"] = images

    instructions = (
        "You are completing missing product data for Worten.\n"
        f"Product data: {json.dumps(product_dict)}\n"
        f"Missing columns: {missing_cols}\n\n"

        "STRICT OUTPUT RULES:\n"
        "- Return ONLY a JSON object.\n"
        "- Keys MUST match the missing column names exactly.\n"
        "- Values MUST be plain strings or numbers (NO objects, NO arrays).\n"
        "- Do NOT include explanations, units outside the value, or extra text.\n"
        "- Fill ONLY the missing fields.\n\n"

        "COLUMN-SPECIFIC RULES (MANDATORY):\n"

        "- If 'product-dimensions' is requested:\n"
        "  - Return a SINGLE STRING in the exact format: LxWxH cm\n"
        "  - Example: \"100x50x50 cm\"\n"
        "  - Use lowercase 'x' and a space before 'cm'\n"
        "  - Do NOT return objects or labels\n\n"

        "- If 'blade-length-cm' is requested:\n"
        "  - Return an INTEGER NUMBER ONLY\n"
        "  - Do NOT include units\n"
        "  - Do NOT include decimals\n"
        "  - Example: 150\n\n"

        "- If 'safety-system_pt_PT' is requested:\n"
        "  - Return EXACTLY ONE of the following values (case-sensitive):\n"
        "    Sim\n"
        "    Não\n"
        "    Sí\n"
        "    Si\n"
        "    No\n"
        "    Não Aplicável\n"
        "    No aplicable\n"
        "    No Aplicable\n"
        "  - Do NOT return any other value\n"
        "  - Do NOT explain or translate\n"
    )


    message_content = [{"type": "text", "text": instructions}]
    for img_url in images:
        message_content.append({"type": "image_url", "image_url": img_url})

    try:
        response = client.chat.complete(
            model=MODEL,
            messages=[{"role": "user", "content": message_content}],
            temperature=0
        )

        raw = response.choices[0].message.content
        missing_values = extract_json(raw)
        print(raw)
        if not missing_values:
            print(f"⚠ No valid JSON returned for product {product_id}")
            continue

        output_row = {**product_dict, **missing_values}
        results.append(output_row)
        print(f"✅ Processed product {product_id}")

    except Exception as e:
        print(f"❌ Error processing product {product_id}: {e}")
        continue

# =========================
# Save results to Excel preserving formatting & sheets
# =========================
if not results:
    print("⚠ No results to save.")
    exit(0)

# Load template workbook
wb = load_workbook(PRODUCTS_FILE)
if "Data" not in wb.sheetnames:
    raise ValueError("Sheet 'Data' not found in template")

ws = wb["Data"]

# Build column index from header row (row 2, same as your other script)
col_index = {}
for col in range(1, ws.max_column + 1):
    header = ws.cell(row=2, column=col).value
    if header:
        col_index[header] = col

ANCHOR_COLUMN = "product_id"
if ANCHOR_COLUMN not in col_index:
    raise ValueError(f"Anchor column '{ANCHOR_COLUMN}' not found")

# Convert results to dict keyed by product_id for fast lookup
results_by_id = {
    str(r["product_id"]): r
    for r in results
    if "product_id" in r
}

updated = 0

# Iterate existing rows and fill only missing values
row = 3
while ws.cell(row=row, column=col_index[ANCHOR_COLUMN]).value:
    product_id = str(ws.cell(row=row, column=col_index[ANCHOR_COLUMN]).value)

    if product_id not in results_by_id:
        row += 1
        continue

    result = results_by_id[product_id]

    for field, value in result.items():
        if field not in col_index:
            continue

        cell = ws.cell(row=row, column=col_index[field])

        # Only write if empty in template
        if cell.value in (None, "", "nan"):
            cell.value = value

    updated += 1
    row += 1

# Save as new file
wb.save(OUTPUT_FILE)

print(f"✅ Completed! {updated} products enriched and saved to {OUTPUT_FILE}")
