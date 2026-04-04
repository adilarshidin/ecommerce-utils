# config.py

BASE_URL = "https://es.wallapop.com"

CONCURRENT_PAGES = 1

# ---------------------------------------------------------------------------
# Cluster weights — editorial priority for your shop's niche.
# Higher = more important category for Northvivor's Wallapop presence.
# All values should be in (0, 1].
# ---------------------------------------------------------------------------
CLUSTER_WEIGHTS = {
    "backpacks":   1.00,
    "boots":       0.95,
    "edc_gear":    0.90,
    "camping":     0.85,
    "bags":        0.80,
    "apparel":     0.75,
    "accessories": 0.70,
    "electronics": 0.60,
}

# ---------------------------------------------------------------------------
# Seed queries per cluster.
# These are typed into Wallapop's search bar to harvest autocomplete keywords.
# More seeds → richer demand signal → better SKU-level scoring.
# Add/remove seeds here without touching any other file.
# ---------------------------------------------------------------------------
SEED_MAP = {
    "backpacks": [
        "mochila militar",
        "mochila trekking",
        "mochila 50l",
        "mochila impermeable",
        "mochila táctica",
        "mochila assault",
        "mochila supervivencia",
        "mochila molle",
    ],
    "boots": [
        "botas militares",
        "botas montaña",
        "botas trekking",
        "botas tacticas",
        "botas impermeables",
        "botas gore-tex",
        "botas combate",
    ],
    "camping": [
        "material camping",
        "tienda campaña",
        "saco dormir",
        "equipo camping",
        "hamaca camping",
        "hornillo camping",
        "esterilla camping",
        "kit supervivencia",
    ],
    "edc_gear": [
        "linterna tactica",
        "multiherramienta",
        "navaja",
        "cuchillo supervivencia",
        "cuchillo bushcraft",
        "hacha camping",
        "linterna led",
    ],
    "bags": [
        "bolsa militar",
        "bolsa duffel",
        "bolsa molle",
        "bolsa táctica",
        "bolsa viaje",
    ],
    "apparel": [
        "chaqueta militar",
        "chaqueta táctica",
        "pantalon militar",
        "ropa táctica",
        "chaqueta outdoor",
        "ropa supervivencia",
    ],
    "accessories": [
        "cinturon tactico",
        "guantes tacticos",
        "parches militares",
        "brujula militar",
        "pouch molle",
        "silbato supervivencia",
    ],
    "electronics": [
        "radio portatil",
        "gps outdoor",
        "bateria externa",
        "camara termica",
        "walkie talkie",
    ],
}
