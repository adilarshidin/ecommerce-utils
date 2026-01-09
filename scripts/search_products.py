import pandas as pd
import random
import time
import os

from playwright.sync_api import sync_playwright, Page

input_catalog = "output/catalog_ready.csv"
output_catalog = "output/filtered_catalog.csv"
checkpoint_file = "input/filter_checkpoint.csv"

df = pd.read_csv(input_catalog)


def check_page(page: Page, asin: str) -> bool:
    page.goto(f"https://www.amazon.es/dp/{asin}")
    ppd_div = page.locator("#ppd")
    if ppd_div.count() > 0:
        check = True
        inner = ppd_div.inner_text().lower()
        if "no disponible" in inner:
            check = False
        elif "lo sentimos. la dirección web que has especificado no es una página activa de nuestro sitio." in inner:
            check = False

        if not check:
            page.goto(f"https://www.amazon.com/dp/{asin}")
            ppd_div = page.locator("#ppd")
            if ppd_div.count() > 0:
                inner = ppd_div.inner_text().lower()
                if "currently unavailable" in inner:
                    check = False
                elif "this item cannot be shipped to your selected delivery location. please choose a different delivery location" in inner:
                    check = False

        return check
    return False


with sync_playwright() as playwright:
    chromium = playwright.chromium
    instance = chromium.launch(headless=True)
    page = instance.new_page()

    processed_asins = []

    if os.path.exists(checkpoint_file):
        processed_asins = pd.read_csv(checkpoint_file)["ASIN"].to_list()

    if os.path.exists(output_catalog):
        saved_asins = pd.read_csv(output_catalog)["ASIN"].to_list()
    else:
        saved_asins = []

    for index, asin in df["ASIN"].items():
        if asin in processed_asins:
            continue
        print(f"Processing ASIN #{index}: {asin}")
        time.sleep(random.randrange(3, 7))
        passed = check_page(page, asin)
        print(f"ASIN {'passed' if passed else 'did not pass'}")
        pd.DataFrame([{"ASIN": asin}]).to_csv(
            checkpoint_file,
            mode="a",
            header=not os.path.exists(checkpoint_file),
            index=False
        )

        if passed and asin not in saved_asins:
            pd.DataFrame([{"ASIN": asin}]).to_csv(
                output_catalog,
                mode="a",
                header=not os.path.exists(output_catalog),
                index=False
            )
            saved_asins.append(asin)
