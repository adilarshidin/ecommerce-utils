import asyncio
import random
import json
from pathlib import Path
from playwright.async_api import async_playwright

# -----------------------------
# Config
# -----------------------------
BASE_URL = "https://es.wallapop.com"
SEEDS = [
    "mochila",
    "botas",
    "tactico",
    "camping",
    "supervivencia",
    "linterna",
    "cuchillo",
]

CONCURRENT_PAGES = 1
OUTPUT_FILE = Path("output/wallapop_keywords.json")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1)...",
]

# -----------------------------
# Save output
# -----------------------------
def save_output(data):
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


# -----------------------------
# Scraper
# -----------------------------
async def scrape_suggestions(page, seed):
    try:
        print(f"[{seed}] Opening Wallapop")

        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(2, 4))

        # --- Reject cookies ---
        try:
            await page.wait_for_selector("span#cmpbntnotxt", timeout=5000)
            reject_btn = page.locator("span#cmpbntnotxt").locator("xpath=ancestor::a")
            await reject_btn.click()
            print("[Cookies] Rejected")
            await asyncio.sleep(1)
        except:
            print("[Cookies] No popup or already handled")

        # --- Locate input ---
        search_input = page.get_by_placeholder("Buscar en Wallapop")
        print(search_input)
        await asyncio.sleep(2)
        await search_input.wait_for(state="visible", timeout=15000)

        await search_input.click(force=True)

        await asyncio.sleep(0.5)

        # --- Type seed (IMPORTANT: this was missing) ---
        await search_input.fill(seed)
        await asyncio.sleep(2)

        # --- Extract suggestions ---
        options = page.locator("[role='option']")

        if await options.count() > 0:
            suggestions = await options.all_text_contents()
        else:
            suggestions = await page.locator("ul li").all_text_contents()

        clean = list(set([
            s.strip().lower()
            for s in suggestions
            if s and len(s.strip()) > 2
        ]))

        print(f"[{seed}] Suggestions: {clean}")

        return clean

    except Exception as e:
        print(f"[{seed}] Error: {e}")
        return []


# -----------------------------
# Worker
# -----------------------------
async def worker(name, queue, page, results):
    while True:
        seed = await queue.get()
        if seed is None:
            break

        print(f"[Worker {name}] {seed}")

        suggestions = await scrape_suggestions(page, seed)

        results.append({
            "seed": seed,
            "suggestions": suggestions
        })

        await asyncio.sleep(random.uniform(3, 6))
        queue.task_done()


# -----------------------------
# Main
# -----------------------------
async def main():
    queue = asyncio.Queue()

    for seed in SEEDS:
        await queue.put(seed)

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="es-ES"
        )

        pages = [await context.new_page() for _ in range(CONCURRENT_PAGES)]

        workers = [
            asyncio.create_task(worker(i + 1, queue, pages[i], results))
            for i in range(CONCURRENT_PAGES)
        ]

        await queue.join()

        for _ in range(CONCURRENT_PAGES):
            await queue.put(None)

        await asyncio.gather(*workers)
        await browser.close()

    save_output(results)
    print("Done.")


asyncio.run(main())
