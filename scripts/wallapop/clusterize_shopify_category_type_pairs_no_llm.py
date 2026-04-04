import csv
import os
import numpy as np
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

INPUT_FILE = Path("output/category_type_pairs.csv")
OUTPUT_FILE = Path("output/category_type_pairs_with_clusters.csv")
TMP_FILE = Path("output/.tmp_with_clusters.csv")

CLUSTERS = [
    "backpacks",
    "boots",
    "bags",
    "camping",
    "apparel",
    "edc_gear",
    "electronics",
    "accessories",
]

# =========================
# RULES
# =========================

RULES = {
    "backpacks": ["backpack", "rucksack", "bag pack", "mochila"],
    "boots": ["boot", "botas", "combat boot", "military boot", "hiking boot"],
    "bags": ["duffel", "tote", "bag", "messenger", "shoulder bag"],
    "camping": ["tent", "sleeping bag", "camp", "camping", "hammock", "stove"],
    "apparel": ["jacket", "shirt", "pants", "hoodie", "clothing", "uniform"],
    "edc_gear": ["knife", "multitool", "edc", "flashlight", "torch"],
    "electronics": ["radio", "gps", "battery", "charger", "power bank", "device"],
    "accessories": ["belt", "gloves", "patch", "pouch", "wallet", "strap"],
}


def rule_based_cluster(text: str):
    t = text.lower()
    for cluster, keywords in RULES.items():
        for kw in keywords:
            if kw in t:
                return cluster
    return None


# =========================
# LOAD DATA
# =========================

def load_rows():
    rows = []

    with INPUT_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            cat = (r.get("Product Category") or "").strip()
            typ = (r.get("Type") or "").strip()
            sku = (r.get("Variant SKU") or "").strip()

            if not sku:
                continue
            if not cat or not typ:
                continue
            if cat.lower() in {"nan", "none"}:
                continue
            if typ.lower() in {"nan", "none"}:
                continue

            rows.append((cat, typ, sku))

    return rows


# =========================
# TEXT
# =========================

def build_texts(rows):
    return [f"{cat} {typ}".lower() for cat, typ, _ in rows]


# =========================
# MAIN
# =========================

def main():
    rows = load_rows()
    texts = build_texts(rows)

    print(f"Loaded: {len(rows)} rows")

    # rule labels
    initial_labels = [rule_based_cluster(t) for t in texts]

    vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=2)
    X = vectorizer.fit_transform(texts)

    # build centroids ONLY from rule-labeled data
    centroids = {}

    for cluster in CLUSTERS:
        idx = [i for i, l in enumerate(initial_labels) if l == cluster]
        if not idx:
            continue

        centroids[cluster] = X[idx].mean(axis=0).A1

    results = []
    kept_rows = []

    # =========================
    # CLASSIFICATION (NO "OTHER")
    # =========================

    for i, (cat, typ, sku) in enumerate(rows):
        rule_label = initial_labels[i]

        if rule_label:
            results.append(rule_label)
            kept_rows.append((cat, typ, sku))
            continue

        x = X[i].toarray().ravel()

        best_cluster = None
        best_score = 0.0

        for cluster, centroid in centroids.items():
            score = cosine_similarity([x], [centroid])[0][0]

            if score > best_score:
                best_score = score
                best_cluster = cluster

        # IMPORTANT: reject low-confidence matches instead of "other"
        if best_score < 0.08 or best_cluster is None:
            continue  # DROP ROW

        results.append(best_cluster)
        kept_rows.append((cat, typ, sku))

    # =========================
    # WRITE OUTPUT
    # =========================

    TMP_FILE.parent.mkdir(parents=True, exist_ok=True)

    with TMP_FILE.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Product Category", "Type", "Variant SKU", "Cluster"])

        for (cat, typ, sku), cluster in zip(kept_rows, results):
            writer.writerow([cat, typ, sku, cluster])

    os.replace(TMP_FILE, OUTPUT_FILE)

    print(f"Done → {OUTPUT_FILE}")
    print(f"Kept: {len(kept_rows)} / {len(rows)} (filtered low-confidence SKUs)")


if __name__ == "__main__":
    main()
