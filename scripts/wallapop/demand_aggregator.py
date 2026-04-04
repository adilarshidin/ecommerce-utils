import json
from collections import defaultdict
from pathlib import Path

INPUT_FILE = Path("output/wallapop_keywords.json")


def load_demand() -> dict[str, list[str]]:
    """
    Returns a dict mapping cluster -> list of unique demand keywords
    scraped from Wallapop autocomplete.

    Example:
        {
            "backpacks": ["mochila militar", "mochila táctica molle", ...],
            "boots":     ["botas militares", "botas trekking", ...],
        }
    """
    with INPUT_FILE.open(encoding="utf-8") as f:
        data = json.load(f)

    cluster_map: dict[str, set] = defaultdict(set)

    for entry in data:
        cluster = entry.get("cluster", "").strip()
        suggestions = entry.get("suggestions", [])

        if not cluster:
            continue

        for kw in suggestions:
            kw = kw.strip().lower()
            if kw:
                cluster_map[cluster].add(kw)

    # Convert sets back to sorted lists for deterministic behaviour
    return {cluster: sorted(kws) for cluster, kws in cluster_map.items()}
