import pandas as pd
import random
import asyncio
import os
from playwright.async_api import async_playwright, Page

# Input / Output files
input_catalog = "input/active_listing_amazon.csv"
output_catalog = "output/active_listing_amazon_corrected.csv"
checkpoint_file = "input/active_listing_amazon_checkpoint.csv"

# Load CSV and preprocess
df = pd.read_csv(input_catalog)
df["open-date"] = pd.to_datetime(df["open-date"], errors="coerce")
df = df.drop_duplicates(subset="asin1", keep="last")

# User agents and language headers
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

# ──────────────────────────────────────────────────────────────
# Scraping function
# ──────────────────────────────────────────────────────────────
async def check_page(page: Page, asin: str, retries: int = 3) -> float | None:
    replacements = str.maketrans({"\n": "", ".": "", ",": ""})

    for attempt in range(retries):
        try:
            await page.goto(f"https://www.amazon.es/dp/{asin}", timeout=30000)

            price_div = page.locator(
                "#corePriceDisplay_desktop_feature_div"
            )
            price_primary = price_div.locator("span.a-price-whole")
            price_fraction = price_div.locator("span.a-price-fraction")

            if await price_primary.count() == 0 or await price_fraction.count() == 0:
                return None

            whole = (
                await price_primary.first.inner_text()
            ).strip().translate(replacements)

            fraction = (
                await price_fraction.first.inner_text()
            ).strip().translate(replacements)

            return float(f"{whole}.{fraction}")

        except Exception as e:
            print(f"Retry {attempt + 1}/{retries} for ASIN {asin}: {e}")
            await asyncio.sleep(random.randint(2, 5))

    return None


# ──────────────────────────────────────────────────────────────
# Main async function
# ──────────────────────────────────────────────────────────────
async def main():
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)
        context = await browser.new_context()

        # Create pages with unique headers
        pages = []
        for i in range(3):
            page = await context.new_page()
            await page.set_extra_http_headers({
                **LANG_HEADERS[i],
                "User-Agent": USER_AGENTS[i]
            })
            pages.append(page)

        semaphore = asyncio.Semaphore(3)

        # 🔒 WRITE LOCK (STEP 1)
        write_lock = asyncio.Lock()

        # Load checkpoint
        processed_asins = set()
        if os.path.exists(checkpoint_file):
            processed_asins = set(
                pd.read_csv(checkpoint_file)["asin1"].tolist()
            )

        asins = df["asin1"].tolist()

        async def process_asin(page: Page, asin: str, index: int):
            async with semaphore:
                if asin in processed_asins:
                    return

                print(f"Processing ASIN #{index}: {asin}")
                await asyncio.sleep(random.randint(3, 7))

                price = await check_page(page, asin)
                print(f"ASIN {asin} price found: {price}")

                # Save checkpoint
                async with write_lock:
                    pd.DataFrame([{"asin1": asin}]).to_csv(
                        checkpoint_file,
                        mode="a",
                        header=not os.path.exists(checkpoint_file),
                        index=False
                    )
                processed_asins.add(asin)

                # ──────────────────────────────────────────
                # STEP 2 + 3 + 4: SAFE WRITE LOGIC
                # ──────────────────────────────────────────
                if price is not None:
                    row_df = df.loc[df["asin1"] == asin].copy()

                    old_price = row_df.iloc[0]["price"]
                    old_price = float(old_price) if pd.notna(old_price) else 0

                    if price > old_price:
                        row_df.at[row_df.index[0], "price"] = price

                        async with write_lock:
                            if os.path.exists(output_catalog):
                                out_df = pd.read_csv(output_catalog)

                                if asin in out_df["asin1"].values:
                                    out_df.loc[
                                        out_df["asin1"] == asin, "price"
                                    ] = price
                                else:
                                    out_df = pd.concat(
                                        [out_df, row_df],
                                        ignore_index=True
                                    )
                            else:
                                out_df = row_df

                            out_df.to_csv(output_catalog, index=False)

        # Schedule tasks
        tasks = []
        for i, asin in enumerate(asins):
            page = pages[i % 3]
            tasks.append(process_asin(page, asin, i))

        await asyncio.gather(*tasks)
        await browser.close()


# Run
asyncio.run(main())
