import requests
import json
from bs4 import BeautifulSoup

URL = "https://shopify.github.io/product-taxonomy/releases/unstable/"
OUTPUT_FILE = "templates/shopify_categories.json"

response = requests.get(URL)
soup = BeautifulSoup(response.content, "html.parser")

category_divs = {}
for div in soup.find_all("div", class_="category-level"):
    parent_id = div.get("data-parent-id")
    category_divs[parent_id] = div

def build_category_tree(parent_id):
    div = category_divs.get(parent_id)
    if not div:
        return []

    tree = []
    ul = div.find("ul", class_="category-level__list")
    if not ul:
        return tree

    for li in ul.find_all("li", class_="category-node", recursive=False):
        category_id = li.get("id")
        category_name = li.get_text(strip=True)
        children = build_category_tree(category_id)
        node = {"name": category_name}
        if children:
            node["children"] = children
        tree.append(node)

    return tree

category_tree = build_category_tree("root")

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(category_tree, f, indent=2, ensure_ascii=False)

print(f"Saved category hierarchy to {OUTPUT_FILE}")
