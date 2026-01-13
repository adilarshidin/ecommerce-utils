import pandas as pd
import random
import asyncio
import os
from playwright.async_api import async_playwright, Page

input_catalog = "output/catalog_ready.csv"
output_catalog = "output/filtered_catalog.csv"
checkpoint_file = "input/filter_checkpoint.csv"

df = pd.read_csv(input_catalog)
df["FECHA"] = pd.to_datetime(df["FECHA"])
df = df.drop_duplicates(subset="ASIN", keep="last")


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

LANG_HEADERS = [
    {"Accept-Language": "es-ES,es;q=0.9,en;q=0.8"},
    {"Accept-Language": "en-US,en;q=0.9"},
    {"Accept-Language": "es-ES,es;q=0.9,en;q=0.8"},
]


async def check_page(page: Page, asin: str, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            await page.goto(f"https://www.amazon.es/dp/{asin}")
            ppd_div = page.locator("#ppd")

            if await ppd_div.count() > 0:
                check = True
                inner = (await ppd_div.inner_text()).lower()

                if "lo sentimos. la dirección web que has especificado no es una página activa de nuestro sitio." in inner:
                    check = False
                elif "no disponible por el momento" in inner:
                    check = False
                elif "no disponible" in inner:
                    check = False

                if not check:
                    await asyncio.sleep(random.randrange(3, 7))
                    await page.goto(f"https://www.amazon.com/dp/{asin}")
                    ppd_div = page.locator("#ppd")

                    if await ppd_div.count() > 0:
                        inner = (await ppd_div.inner_text()).lower()
                        if "currently unavailable" in inner:
                            check = False
                        elif "this item cannot be shipped to your selected delivery location. please choose a different delivery location" in inner:
                            check = False
                        elif "no puede enviarse este producto al punto de entrega seleccionado. selecciona un punto de entrega diferente" in inner:
                            check = False

                return check

            return False

        except Exception as e:
            print(f"Retry {attempt + 1}/{retries} for ASIN {asin} due to error: {e}")
            await asyncio.sleep(random.randrange(2, 5))

    return False


async def main():
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)
        context = await browser.new_context()

        # 🔹 CREATE 3 PAGES WITH UNIQUE HEADERS
        pages = []
        for i in range(3):
            page = await context.new_page()
            await page.set_extra_http_headers({
                **LANG_HEADERS[i],
                "User-Agent": USER_AGENTS[i]
            })
            pages.append(page)

        semaphore = asyncio.Semaphore(3)

        processed_asins = []
        if os.path.exists(checkpoint_file):
            processed_asins = pd.read_csv(checkpoint_file)["ASIN"].to_list()

        saved_asins = set()
        if os.path.exists(output_catalog):
            saved_asins = set(pd.read_csv(output_catalog)["ASIN"].to_list())

        asins = df["ASIN"].tolist()

        async def process_asin(page: Page, asin: str, index: int):
            async with semaphore:
                if asin in processed_asins:
                    return

                print(f"Processing ASIN #{index}: {asin}")
                await asyncio.sleep(random.randrange(3, 7))

                passed = await check_page(page, asin)
                print(f"ASIN {'passed' if passed else 'did not pass'}")

                pd.DataFrame([{"ASIN": asin}]).to_csv(
                    checkpoint_file,
                    mode="a",
                    header=not os.path.exists(checkpoint_file),
                    index=False
                )
                processed_asins.append(asin)

                if passed and asin not in saved_asins:
                    row_df = df.loc[df["ASIN"] == asin]
                    row_df.to_csv(
                        output_catalog,
                        mode="a",
                        header=not os.path.exists(output_catalog),
                        index=False
                    )
                    saved_asins.add(asin)

        tasks = []
        for i, asin in enumerate(asins):
            page = pages[i % 3]
            tasks.append(process_asin(page, asin, i))

        await asyncio.gather(*tasks)
        await browser.close()


asyncio.run(main())
