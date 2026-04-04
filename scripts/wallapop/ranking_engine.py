"""
ranking_engine.py
-----------------
Ranks every SKU in the catalog individually against the Wallapop demand
keywords scraped for each cluster.

Score formula (all factors in [0, 1] range, final score in [0, 1]):
    score = keyword_match × cluster_weight × price_factor

keyword_match  — cosine similarity between the SKU's text (category + type)
                 and the closest demand keyword for its cluster.
cluster_weight — editorial priority from config.CLUSTER_WEIGHTS.
price_factor   — Wallapop sellability by price band (see PRICE_BANDS below).
                 If your catalog CSV has no "Price" column this defaults to 1.0.
"""

import csv
import warnings
from pathlib import Path

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from config import CLUSTER_WEIGHTS
from demand_aggregator import load_demand

warnings.filterwarnings("ignore", category=UserWarning)  # sklearn sparse warnings

INPUT_FILE = Path("output/category_type_pairs.csv")
OUTPUT_FILE = Path("output/top_200_wallapop.csv")

# ---------------------------------------------------------------------------
# Price bands — tuned for Wallapop Spain outdoor/survival niche.
# Edit freely. If your CSV has no Price column these are not applied.
# ---------------------------------------------------------------------------
PRICE_BANDS = [
    (0,    15,   0.50),   # too cheap — low perceived value on Wallapop
    (15,   50,   0.85),
    (50,   120,  1.00),   # sweet spot
    (120,  200,  0.85),
    (200,  300,  0.65),
    (300,  float("inf"), 0.35),  # very hard to sell secondhand at 300 €+
]


def price_factor(price: float | None) -> float:
    if price is None:
        return 1.0
    for lo, hi, factor in PRICE_BANDS:
        if lo <= price < hi:
            return factor
    return 0.35


# ---------------------------------------------------------------------------
# Cluster assignment — same rule set as before, kept here so ranking_engine
# is self-contained. clusterize_shopify runs first and writes its own output;
# this is just a lightweight fallback for rows that weren't clustered.
# ---------------------------------------------------------------------------
_CLUSTER_RULES: dict[str, list[str]] = {
    "backpacks":   ["mochila", "backpack", "rucksack", "mochila táctica"],
    "boots":       ["botas", "boot", "bota"],
    "camping":     ["camping", "tienda", "saco dormir", "hamaca", "hornillo"],
    "edc_gear":    ["linterna", "navaja", "multiherramienta", "cuchillo", "hacha"],
    "bags":        ["bolsa", "duffel", "bag", "tote", "messenger"],
    "apparel":     ["chaqueta", "jacket", "pantalon", "camisa", "ropa", "hoodie"],
    "accessories": ["cinturon", "guantes", "parches", "brujula", "silbato"],
    "electronics": ["radio", "gps", "power bank", "bateria", "camara termica"],
}


def _rule_cluster(text: str) -> str | None:
    t = text.lower()
    for cluster, kws in _CLUSTER_RULES.items():
        if any(kw in t for kw in kws):
            return cluster
    return None


# ---------------------------------------------------------------------------
# Core: build per-cluster TF-IDF index over demand keywords, then score SKUs
# ---------------------------------------------------------------------------

def _build_keyword_index(
    demand_map: dict[str, list[str]],
) -> dict[str, tuple[TfidfVectorizer, np.ndarray]]:
    """
    For every cluster, fit a TF-IDF vectorizer on its demand keywords and
    return (vectorizer, keyword_matrix) so we can score any SKU text against it.
    """
    index: dict[str, tuple[TfidfVectorizer, np.ndarray]] = {}

    for cluster, keywords in demand_map.items():
        if not keywords:
            continue
        vec = TfidfVectorizer(ngram_range=(1, 2), analyzer="word")
        try:
            mat = vec.fit_transform(keywords)
            index[cluster] = (vec, mat)
        except ValueError:
            # Happens if all keywords are stop-words — just skip
            pass

    return index


def _sku_match_score(
    sku_text: str,
    cluster: str,
    index: dict[str, tuple[TfidfVectorizer, np.ndarray]],
) -> float:
    """
    Returns the max cosine similarity between sku_text and any demand
    keyword in the cluster's index.  Returns 0.0 if cluster not indexed.
    """
    if cluster not in index:
        return 0.0

    vec, mat = index[cluster]

    try:
        sku_vec = vec.transform([sku_text])
    except Exception:
        return 0.0

    sims = cosine_similarity(sku_vec, mat).ravel()
    return float(sims.max()) if sims.size else 0.0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    demand_map = load_demand()

    if not demand_map:
        print("⚠  No demand data found in output/wallapop_keywords.json")
        print("   Run scrape_suggestions.py first, then re-run this script.")
        return

    print(f"Loaded demand keywords for clusters: {sorted(demand_map.keys())}")

    keyword_index = _build_keyword_index(demand_map)

    rows: list[tuple[str, str, float, float, float, float]] = []
    skipped = 0

    with INPUT_FILE.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        catalog_rows = list(reader)

    # Try to read price — optional column
    has_price = "Price" in (catalog_rows[0].keys() if catalog_rows else [])

    for r in catalog_rows:
        cat = (r.get("Product Category") or "").strip()
        typ = (r.get("Type") or "").strip()
        sku = (r.get("Variant SKU") or "").strip()
        title = (r.get("Title") or "").strip()

        if not sku or not cat or not typ:
            skipped += 1
            continue

        cluster_text = f"{cat} {typ} {title}".lower()
        cluster = _rule_cluster(cluster_text)
        sku_text = f"{cat} {typ}".lower()

        if not cluster:
            skipped += 1
            continue

        # Keyword match score in [0, 1]
        kw_score = _sku_match_score(sku_text, cluster, keyword_index)

        # Cluster weight in [0, 1]
        weight = CLUSTER_WEIGHTS.get(cluster, 0.5)

        # Price factor in [0, 1]
        price: float | None = None
        if has_price:
            raw = (r.get("Price") or "").strip().replace(",", ".")
            try:
                price = float(raw)
            except ValueError:
                price = None
        pf = price_factor(price)

        final_score = kw_score * weight * pf

        rows.append((sku, cluster, round(final_score, 6),
                     round(kw_score, 4), round(weight, 4), round(pf, 4)))

    if not rows:
        print("⚠  No rows were scored. Check that category_type_pairs.csv is populated.")
        return

    rows.sort(key=lambda x: x[2], reverse=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "SKU", "Cluster", "Score",
            "KeywordMatch", "ClusterWeight", "PriceFactor",
        ])
        writer.writerows(rows[:200])

    top = rows[:5]
    print(f"\nTop 5 preview:")
    for r in top:
        print(f"  {r[0]:30s}  cluster={r[1]:12s}  score={r[2]:.4f}"
              f"  (kw={r[3]:.3f}, w={r[4]:.2f}, price={r[5]:.2f})")

    print(f"\nSkipped (no cluster match): {skipped}")
    print(f"Scored: {len(rows)}  →  Top 200 saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
