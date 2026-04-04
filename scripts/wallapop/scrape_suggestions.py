import asyncio
import random
import json
from pathlib import Path
from playwright.async_api import async_playwright
from config import BASE_URL, SEED_MAP, CONCURRENT_PAGES

OUTPUT_FILE = Path("output/wallapop_keywords.json")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

_COOKIE_DISMISSED = False


def save_output(data):
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


async def _dismiss_cookie_banner(page):
    """Dismiss the cookie/consent banner once per session."""
    global _COOKIE_DISMISSED
    if _COOKIE_DISMISSED:
        return
    try:
        await page.wait_for_selector("span#cmpbntnotxt", timeout=5000)
        reject_btn = page.locator("span#cmpbntnotxt").locator("xpath=ancestor::a")
        await reject_btn.click()
        await asyncio.sleep(1)
        _COOKIE_DISMISSED = True
    except Exception:
        pass


async def scrape_suggestions(page, seed):
    """
    Navigate to Wallapop search results for `seed` and return all listing
    titles from the first results page (~40 items).

    Listing titles are real buyer-facing descriptions like:
      "mochila helikon tex 25l verde oliva nueva"
      "mochila assault direct action coyote molle"
    This vocabulary is far more specific than autocomplete suggestions and
    allows the TF-IDF scorer to genuinely discriminate between individual SKUs.
    """
    search_url = (
        f"{BASE_URL}/search?keywords={seed.replace(' ', '%20')}"
        "&filters_source=search_box"
    )

    await page.goto(search_url, wait_until="domcontentloaded")
    await asyncio.sleep(random.uniform(2.5, 4.0))

    await _dismiss_cookie_banner(page)

    # Wait for listing cards to render
    try:
        await page.wait_for_selector(
            "[class*='ItemCard'], [class*='item-card'], a[href*='/item/']",
            timeout=12000,
        )
    except Exception:
        print(f"  No results found for seed: '{seed}'")
        return []

    await asyncio.sleep(random.uniform(0.5, 1.5))

    titles = []

    # Try selectors from most to least specific — Wallapop's class names
    # are stable but obfuscated; we try several patterns.
    for selector in [
        "[class*='ItemCard__title']",
        "[class*='item-card__title']",
        "[class*='ItemCard'] p",
        "a[href*='/item/'] p",
        "a[href*='/item/'] span",
    ]:
        els = page.locator(selector)
        count = await els.count()
        if count >= 3:
            titles = await els.all_text_contents()
            break

    # Last-resort fallback
    if not titles:
        titles = await page.locator("main p").all_text_contents()

    cleaned = list(set([
        t.strip().lower()
        for t in titles
        if t and 4 < len(t.strip()) < 120
    ]))

    print(f"  '{seed}' -> {len(cleaned)} listing titles")
    return cleaned


async def worker(queue, page, results):
    while True:
        item = await queue.get()
        if item is None:
            break

        cluster = item["cluster"]
        seed = item["seed"]

        titles = await scrape_suggestions(page, seed)

        results.append({
            "cluster": cluster,
            "seed": seed,
            "suggestions": titles,
        })

        queue.task_done()


async def main():
    global _COOKIE_DISMISSED
    _COOKIE_DISMISSED = False

    queue = asyncio.Queue()
    results = []

    total = sum(len(seeds) for seeds in SEED_MAP.values())
    print(f"Scraping {total} seeds across {len(SEED_MAP)} clusters...")

    for cluster, seeds in SEED_MAP.items():
        for seed in seeds:
            await queue.put({"cluster": cluster, "seed": seed})

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="es-ES",
        )

        pages = [await context.new_page() for _ in range(CONCURRENT_PAGES)]

        workers = [
            asyncio.create_task(worker(queue, pages[i], results))
            for i in range(CONCURRENT_PAGES)
        ]

        await queue.join()

        for _ in range(CONCURRENT_PAGES):
            await queue.put(None)

        await asyncio.gather(*workers)
        await browser.close()

    save_output(results)

    total_titles = sum(len(r["suggestions"]) for r in results)
    print(f"\nDone. {total_titles} listing titles saved -> {OUTPUT_FILE}")


asyncio.run(main())
