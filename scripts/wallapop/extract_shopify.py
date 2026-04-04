import csv
from pathlib import Path

INPUT_FILE = Path("input/shopify_catalog.csv")
OUTPUT_FILE = Path("output/category_type_pairs.csv")


def main():
    rows = []

    with INPUT_FILE.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for r in reader:
            cat = (r.get("Product Category") or "").strip()
            typ = (r.get("Type") or "").strip()
            sku = (r.get("Variant SKU") or "").strip()
            title = (r.get("Title") or "").strip()

            if not sku or not cat or not typ:
                continue

            rows.append((cat, typ, sku, title))

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Product Category", "Type", "Variant SKU", "Title"])

        for row in rows:
            writer.writerow(row)

    print(f"Extracted {len(rows)} SKUs")


if __name__ == "__main__":
    main()
