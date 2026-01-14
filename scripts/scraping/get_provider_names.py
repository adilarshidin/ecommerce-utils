import pandas as pd
import random
import asyncio
import os
from dotenv import load_dotenv
from mistralai import Mistral
from playwright.async_api import async_playwright, Page

load_dotenv()

# Files
input_catalog = "output/sellerboard_inventory_formatted.csv"
output_catalog = "output/sellerboard_products_with_providers.csv"
checkpoint_file = "checkpoints/sellerboard_products_checkpoint.csv"

# Load CSV
df = pd.read_csv(input_catalog)
df = df.drop_duplicates(subset="ASIN", keep="last")  # make sure ASIN matches CSV
df["PROVEEDOR"] = df["PROVEEDOR"].astype(str)  # <-- ensures strings

# User agents / language headers
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
# LLM-based provider cleaning
# ──────────────────────────────────────────────────────────────
def clean_provider_with_llm(provider_text: str, mistral: Mistral) -> str:
    """
    Use LLM to extract only the provider name from messy text.
    Example: "Marca: Mil-Tec - Visit the shop" -> "Mil-Tec"
    """
    prompt = f"""
You are given a string from an Amazon product page that may contain extra words, like 'Marca:', 'Visit the shop', or other non-brand text.

Return ONLY the clean provider/brand name. No extra text, no markdown, no explanations.

Input: "{provider_text}"
"""
    response = mistral.chat.complete(
        model="mistral-small-latest",
        messages=[{"role": "user", "content": prompt}],
        stream=False,
    )

    # LLM output should be the clean provider name
    clean_name = response.choices[0].message.content.strip()
    return clean_name

# ──────────────────────────────────────────────────────────────
# Scraping function: get provider/brand
# ──────────────────────────────────────────────────────────────
async def get_provider(page: Page, asin: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            await page.goto(f"https://www.amazon.es/dp/{asin}", timeout=30000)
            
            brand_locator = page.locator("#bylineInfo")
            if await brand_locator.count() > 0:
                return (await brand_locator.first.inner_text()).strip()
            
            brand_alt = page.locator("#brand")
            if await brand_alt.count() > 0:
                return (await brand_alt.first.inner_text()).strip()
            
            return None
        except Exception as e:
            print(f"Retry {attempt + 1}/{retries} for ASIN {asin}: {e}")
            await asyncio.sleep(random.randint(2, 5))
    return None

# ──────────────────────────────────────────────────────────────
# Main async function
# ──────────────────────────────────────────────────────────────
async def main():
    async with Mistral(api_key=os.getenv("MISTRAL_API_KEY", "")) as mistral:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=False)
            context = await browser.new_context()

            pages = []
            for i in range(3):
                page = await context.new_page()
                await page.set_extra_http_headers({
                    **LANG_HEADERS[i],
                    "User-Agent": USER_AGENTS[i]
                })
                pages.append(page)

            semaphore = asyncio.Semaphore(3)
            write_lock = asyncio.Lock()

            processed_asins = set()
            if os.path.exists(checkpoint_file):
                processed_asins = set(
                    pd.read_csv(checkpoint_file)["ASIN"].tolist()
                )

            asins = df["ASIN"].tolist()

            async def process_asin(page: Page, asin: str, index: int):
                async with semaphore:
                    if asin in processed_asins:
                        return

                    print(f"Processing ASIN #{index}: {asin}")
                    await asyncio.sleep(random.randint(3, 7))

                    raw_provider = await get_provider(page, asin)
                    if raw_provider:
                        provider = clean_provider_with_llm(raw_provider, mistral)
                    else:
                        provider = ""

                    print(f"ASIN {asin} provider cleaned: {provider}")

                    # Save checkpoint
                    async with write_lock:
                        pd.DataFrame([{"ASIN": asin}]).to_csv(
                            checkpoint_file,
                            mode="a",
                            header=not os.path.exists(checkpoint_file),
                            index=False
                        )
                    processed_asins.add(asin)

                    # Save full row with cleaned provider
                    row_df = df.loc[df["ASIN"] == asin].copy()
                    row_df.at[row_df.index[0], "PROVEEDOR"] = provider

                    async with write_lock:
                        if os.path.exists(output_catalog):
                            out_df = pd.read_csv(output_catalog)
                            if asin in out_df["ASIN"].values:
                                # safer update
                                out_df.loc[out_df["ASIN"] == asin, row_df.columns] = row_df.iloc[0]
                            else:
                                out_df = pd.concat([out_df, row_df], ignore_index=True)
                        else:
                            out_df = row_df

                        out_df.to_csv(output_catalog, index=False)

            tasks = []
            for i, asin in enumerate(asins):
                page = pages[i % 3]
                tasks.append(process_asin(page, asin, i))

            await asyncio.gather(*tasks)
            await browser.close()


# Run
asyncio.run(main())
