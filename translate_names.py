import pandas as pd
import asyncio
from langdetect import detect, DetectorFactory
from googletrans import Translator

DetectorFactory.seed = 0
translator = Translator()

CATALOG = "output/asin_results.csv"
CATALOG_TRANSLATED = "output/catalog_translated.csv"

def is_english(text):
    try:
        return detect(text) == 'en'
    except:
        return False

def fix_german(text):
    replacements = {
        'ae': 'ä', 'oe': 'ö', 'ue': 'ü', 'Ae': 'Ä', 'Oe': 'Ö', 'Ue': 'Ü', 'ss': 'ß'
    }
    for wrong, correct in replacements.items():
        text = text.replace(wrong, correct)
    return text

async def translate_name(text):
    if not text or is_english(text):
        return text
    text_fixed = fix_german(text)
    try:
        translated = await translator.translate(text_fixed, dest='en')
        return await translated.text
    except Exception as e:
        print(f"Translation failed for '{text_fixed}': {e}")
        return text_fixed

async def main():
    df = pd.read_csv(CATALOG, encoding="utf-8")
    # run translations concurrently
    tasks = [translate_name(name) for name in df['NOMBRE']]
    translated_names = await asyncio.gather(*tasks)
    df['nombre_en'] = translated_names
    df.to_csv(CATALOG_TRANSLATED, index=False)
    print("Translation complete! Saved to catalog_translated.csv")

asyncio.run(main())
