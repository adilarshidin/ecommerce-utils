import os
import json
import pandas as pd
from dotenv import load_dotenv
from mistralai import Mistral

# =========================
# Config
# =========================
load_dotenv()

SHOPIFY_TEMPLATE_CSV = "templates/shopify_template.csv"
PRODUCTS_CSV = "input/konus_catalog.csv"
CATEGORY_JSON = "templates/shopify_categories.json"
OUTPUT_CSV = "output/konus_shopify.csv"
CHECKPOINT_FILE = "checkpoints/konus_checkpoint.txt"

MODEL = "mistral-large-latest"
client = Mistral(api_key=os.getenv("MISTRAL_API_KEY"))

# =========================
# Load CSVs and JSON
# =========================
shopify_df = pd.read_csv(SHOPIFY_TEMPLATE_CSV)
products_df = pd.read_csv(PRODUCTS_CSV, sep=";", encoding="latin1")
products_df = products_df[products_df["Código"] == "AR02084"].reset_index(drop=True)

shopify_columns = shopify_df.columns.tolist()
shopify_example = shopify_df.iloc[0].to_dict() if len(shopify_df) > 0 else None

with open(CATEGORY_JSON, "r", encoding="utf-8") as f:
    categories_tree = json.load(f)

# =========================
# Utils
# =========================
def extract_json(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(line for line in lines if not line.strip().startswith("```"))
    return text.strip()

def load_checkpoint() -> int:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return int(f.read().strip())
    return 0

def save_checkpoint(index: int):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(index))

def append_row(row: dict):
    df = pd.DataFrame([row], columns=shopify_columns)
    df.to_csv(
        OUTPUT_CSV,
        mode="a",
        header=not os.path.exists(OUTPUT_CSV),
        index=False
    )

# =========================
# Stepwise Category Assignment
# =========================
def choose_category_level(product: dict, category_options: list, level: int) -> tuple[str, bool, int]:
    """
    Prompt the model to choose one category from the current level.
    Returns:
        - chosen_category (str)
        - verified (bool): True if model picked valid category
        - failure_level (int): level of failure, 0 if verified
    """
    for attempt in range(3):
        prompt = f"""
You are classifying a Shopify product.

### Input product
{json.dumps(product, indent=2)}

### Category options at this level
{json.dumps(category_options, indent=2)}

### Instructions
- Choose only ONE category from the options.
- Return ONLY the exact category name, no explanations or extra text.
"""
        response = client.chat.complete(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )

        chosen = extract_json(response.choices[0].message.content).strip()

        # Check if chosen category is valid
        valid_choice = next((opt for opt in category_options if opt.lower() == chosen.lower()), None)
        if valid_choice:
            return valid_choice, True, 0  # Verified, no failure

        # Log invalid choice
        print(f"⚠ Model failed at level {level}!")
        print(f"   Attempt {attempt+1}/3")
        print(f"   Model output: '{chosen}'")
        print(f"   Valid options: {category_options}")

    # fallback after 3 attempts
    fallback = category_options[0]
    print(f"⚠ Using fallback at level {level}")
    print(f"   Valid options were: {category_options}")
    return fallback, False, level  # Not verified, failure at this level

def traverse_category_tree(product: dict, tree: dict) -> tuple[list[str], bool, int]:
    """
    Stepwise traversal of category tree.
    Returns:
        - full category path (list of strings)
        - verified (bool) if all levels were valid
        - failure_level (int): 0 if no failure, otherwise depth where model failed
    """
    path = []
    verified = True
    failure_level = 0
    current_level = tree
    level = 1

    while True:
        options = [child["name"] for child in current_level.get("children", [])] if "children" in current_level else []
        if not options:
            break  # No more nested categories

        chosen, level_verified, level_failure = choose_category_level(product, options, level)
        path.append(chosen)

        if not level_verified:
            verified = False
            failure_level = level_failure  # record the first failure

        # Move to chosen node
        current_level = next((c for c in current_level.get("children", []) if c["name"].lower() == chosen.lower()), {"children": []})
        level += 1

    return path, verified, failure_level

# =========================
# Shopify CSV Row Generation
# =========================
def build_shopify_prompt(product: dict, category_path: list) -> str:
    product_copy = product.copy()
    product_copy["Category Path"] = " > ".join(category_path)

    return f"""
You are transforming product data into a Shopify import CSV row.

### Shopify schema
Columns (must match EXACTLY):
{json.dumps(shopify_columns, indent=2)}

Example Shopify row:
{json.dumps(shopify_example, indent=2)}

### Input product
{json.dumps(product_copy, indent=2)}

### Instructions
- Output MUST be raw JSON
- DO NOT wrap in ``` or ```json
- DO NOT include explanations
- Set all column names in ENGLISH
- Keep original Spanish values where they exist
- All Boolean type columns must have EXACTLY the english value either "true" or "false"
- Columns "Title" and "SEO title" must have the same value
- Columns "Description" and "SEO description" must have the same value
- Return exactly one JSON object
"""

def format_product(product: dict, category_path: list, category_verified: bool, failure_level: int) -> dict:
    """
    Generates Shopify CSV row, adding:
    - Category Verified: Yes/No
    - Category Failure Level: 0 if verified, else depth of failure
    """
    product_copy = product.copy()
    product_copy["Category Path"] = " > ".join(category_path)
    product_copy["Category Verified"] = "Yes" if category_verified else "No"
    product_copy["Category Failure Level"] = failure_level

    prompt = f"""
You are transforming product data into a Shopify import CSV row.

### Shopify schema
Columns (must match EXACTLY):
{json.dumps(shopify_columns, indent=2)}

Example Shopify row:
{json.dumps(shopify_example, indent=2)}

### Input product
{json.dumps(product_copy, indent=2)}

### Instructions
- Output MUST be raw JSON
- DO NOT wrap in ``` or ```json
- DO NOT include explanations
- Set all column names in ENGLISH
- Keep original Spanish values where they exist
- Return exactly one JSON object
"""

    response = client.chat.complete(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    raw = response.choices[0].message.content
    cleaned = extract_json(raw)
    try:
        row = json.loads(cleaned)
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON from model:\n{raw}")

    # Ensure our new columns are present
    row["Category Verified"] = "Yes" if category_verified else "No"
    row["Category Failure Level"] = failure_level
    return {col: row.get(col, "") for col in shopify_columns + ["Category Verified", "Category Failure Level"]}

# =========================
# Process products (checkpointed)
# =========================
start_idx = load_checkpoint()
total = len(products_df)
print(f"▶ Resuming from product {start_idx + 1}/{total}")

for idx in range(start_idx, total):
    print(f"Processing product {idx + 1}/{total}")
    product_dict = products_df.iloc[idx].to_dict()

    try:
        # Stepwise category assignment
        top_level_tree = {"children": categories_tree}  # Wrap tree to match expected structure
        category_path, category_verified, failure_level = traverse_category_tree(product_dict, top_level_tree)

        # Shopify CSV row generation
        shopify_row = format_product(product_dict, category_path, category_verified, failure_level)
        append_row(shopify_row)
        save_checkpoint(idx + 1)

    except Exception as e:
        print(f"❌ Failed at row {idx + 1}: {e}")
        print("⏭ Skipping and continuing...")
        save_checkpoint(idx + 1)

print("✅ Shopify CSV generation completed")
