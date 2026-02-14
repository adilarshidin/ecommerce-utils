import pandas as pd
import random
import asyncio
import os
import json
from pathlib import Path
from playwright.async_api import async_playwright, Page

input_file = "output/matched_asin1.csv"
output_file = "output/asin_prices_es.csv"

df = pd.read_csv(input_file)
df = df.dropna(subset=["asin1"])
df = df.drop_duplicates(subset="asin1", keep="last")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

CONCURRENT_PAGES = 2  # Reduced concurrency for safer scraping

CHECKPOINT_DIR = Path("checkpoints")
CHECKPOINT_FILE = CHECKPOINT_DIR / "asin_progress.json"


# -----------------------------
# Price Scraper (No Offers)
# -----------------------------
async def scrape_price(page: Page, asin: str):
    try:
        url = f"https://amazon.es/dp/{asin}"

        # Pre-navigation delay (human-like thinking time)
        await asyncio.sleep(random.uniform(3, 6))

        print(f"[{asin}] Opening product page")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Post-load delay
        await asyncio.sleep(random.uniform(4, 8))

        selector = (
            "span.a-price.aok-align-center.reinventPricePriceToPayMargin.priceToPay"
        )

        try:
            await page.wait_for_selector(selector, timeout=15000)
        except:
            print(f"[{asin}] Price container not found")
            return None

        whole = await page.locator(
            f"{selector} span.a-price-whole"
        ).first.text_content()

        fraction = await page.locator(
            f"{selector} span.a-price-fraction"
        ).first.text_content()

        if not whole:
            print(f"[{asin}] Whole price missing")
            return None

        whole = whole.replace(".", "").replace(",", "").strip()
        fraction = fraction.strip() if fraction else "00"

        price = f"{whole},{fraction}"

        print(f"[{asin}] Price: {price}")

        return price

    except Exception as e:
        print(f"[{asin}] Error: {e}")
        return None


# -----------------------------
# Main
# -----------------------------
async def main():
    CHECKPOINT_DIR.mkdir(exist_ok=True)

    processed_asins = set()
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r") as f:
            processed_asins = set(json.load(f))
        print(f"Loaded checkpoint with {len(processed_asins)} processed ASINs")

    remaining_asins = [
        (row["asin1"], row["seller-sku"])
        for _, row in df.iterrows()
        if row["asin1"] not in processed_asins
    ]

    print(f"Remaining ASINs to process: {len(remaining_asins)}")

    queue = asyncio.Queue()
    for asin, sku in remaining_asins:
        await queue.put((asin, sku))

    write_lock = asyncio.Lock()

    async def atomic_append(result_row):
        async with write_lock:
            file_exists = os.path.exists(output_file)
            temp_file = output_file + ".tmp"

            if file_exists:
                existing_df = pd.read_csv(output_file)

                for col in result_row.keys():
                    if col not in existing_df.columns:
                        existing_df[col] = None

                updated_df = pd.concat(
                    [existing_df, pd.DataFrame([result_row])],
                    ignore_index=True
                )
            else:
                updated_df = pd.DataFrame([result_row])

            updated_df.to_csv(temp_file, index=False)
            os.replace(temp_file, output_file)

            processed_asins.add(result_row["asin1"])
            with open(CHECKPOINT_FILE, "w") as f:
                json.dump(list(processed_asins), f)

    async def worker(name: int, page: Page):
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break

            asin, sku = item
            print(f"[Worker {name}] Processing {asin}")

            price = await scrape_price(page, asin)

            row = {
                "asin1": asin,
                "sku": sku,
                "buybox_price": price
            }

            await atomic_append(row)

            # Inter-request cooldown (critical for bot protection)
            await asyncio.sleep(random.uniform(5, 10))

            queue.task_done()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)

        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="es-ES"
        )

        # Block heavy assets to reduce bandwidth fingerprinting
        await context.route(
            "**/*",
            lambda route: asyncio.create_task(
                route.abort()
                if route.request.resource_type in ["image", "font", "media"]
                else route.continue_()
            ),
        )

        pages = [await context.new_page() for _ in range(CONCURRENT_PAGES)]

        workers = [
            asyncio.create_task(worker(i + 1, pages[i]))
            for i in range(CONCURRENT_PAGES)
        ]

        await queue.join()

        for _ in range(CONCURRENT_PAGES):
            await queue.put(None)

        await asyncio.gather(*workers)
        await browser.close()

    print("\nScraping completed.")


asyncio.run(main())
