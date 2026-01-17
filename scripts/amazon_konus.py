from openpyxl import load_workbook
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os
import json
from mistralai import Mistral

# ---------------- CONFIG ----------------
excel_path = "templates/konus.xlsm"
sheet_name = "Plantilla"
csv_path = "input/konus_catalog.csv"
output_path = "output/amazon_konux.xlsm"
start_row = 6

DIRECT_HEADERS = set([
    "SKU",
    "SKU principal",
    "ID del producto",
    "Marca",
    "Nombre Modelo",
    "Nombre del producto",
    "Palabra clave genérica",
    "Descripción del producto",
    "Peso Artículo",
    "URL de la imagen principal",
])

# ---------------- ENV ----------------
load_dotenv()
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
if not MISTRAL_API_KEY:
    raise RuntimeError("MISTRAL_API_KEY not set")

mistral = Mistral(api_key=MISTRAL_API_KEY)

# ---------------- LOAD FILES ----------------
Path(output_path).parent.mkdir(parents=True, exist_ok=True)

wb = load_workbook(excel_path, keep_vba=True)
ws = wb[sheet_name]

df = pd.read_csv(
    csv_path,
    sep=";",
    encoding="latin-1",
    engine="python"
)

# ---------------- AMAZON HEADERS ----------------
amazon_headers = [cell.value for cell in ws[4]]

# ---------------- CLEAN ROW 6 ----------------
for cell in ws[start_row]:
    cell.value = None

# ---------------- LLM FUNCTION ----------------
def extract_json(text: str) -> dict:
    text = text.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"No JSON object found in LLM response:\n{text}")
    return json.loads(text[start:end + 1])

def direct_map(csv_row):
    weight = csv_row.get("PesoNeto")
    if isinstance(weight, str):
        weight = weight.lower().replace("gr.", "").replace("gr", "").strip()

    medidas_raw = csv_row.get("Medidas")
    medidas = None
    if isinstance(medidas_raw, str):
        # Remove "cm" or spaces, split by "x", join by ";"
        medidas_clean = medidas_raw.lower().replace("cm", "").replace(" ", "")
        medidas_parts = medidas_clean.split("x")
        medidas = "; ".join(medidas_parts)

    return {
        "SKU": csv_row.get("EAN"),
        "SKU principal": csv_row.get("EAN"),
        "ID del producto": csv_row.get("EAN"),
        "Marca": csv_row.get("Marca"),
        "Nombre Modelo": csv_row.get("Modelo"),
        "Nombre del producto": csv_row.get("Título_producto"),
        "Palabra clave genérica": csv_row.get("Descripción_corta"),
        "Descripción del producto": csv_row.get("Descripción_larga"),
        "Peso Artículo": weight,
        "URL de la imagen principal": csv_row.get("Imagen_grande"),
        "Tipo de identificador del producto": "EAN",
        "Estado del producto": "Nuevo",
        "Precio de venta recomendado (PVPR)": csv_row.get("PVP FINAL").strip("€").strip(),
        "Tamaño del anillo": medidas
    }

def map_with_llm(amazon_headers, csv_row, mistral):
    llm_headers = [h for h in amazon_headers if h not in DIRECT_HEADERS]

    llm_csv = {
        k: v for k, v in csv_row.items()
        if k not in {
            "EAN",
            "Marca",
            "Modelo",
            "Imagen_grande",
        }
        and pd.notna(v)
    }

    if not llm_headers or not llm_csv:
        return {}

    prompt = f"""
You are mapping supplier data to an Amazon Seller Central flat file.

CRITICAL RULES:
- Return ONLY valid JSON
- Return ONLY headers you can confidently populate
- Keys MUST be taken from the Amazon headers list
- Do NOT include headers with null or empty values
- Values must be plain strings
- Escape all special characters correctly
- Do NOT invent data
- No explanations, no markdown

Amazon headers you MAY use:
{llm_headers}

Supplier entry:
{llm_csv}
"""

    res = mistral.chat.complete(
        model="mistral-small-latest",
        messages=[{"role": "user", "content": prompt}],
        stream=False,
    )

    return extract_json(res.choices[0].message.content)

# ---------------- PROCESS ROWS ----------------
current_row = start_row

for _, csv_row in df.iterrows():
    csv_dict = csv_row.to_dict()

    mapped = {}
    mapped.update(direct_map(csv_dict))
    mapped.update(map_with_llm(amazon_headers, csv_dict, mistral))

    for col_idx, header in enumerate(amazon_headers, start=1):
        value = mapped.get(header)
        if value is not None:
            ws.cell(row=current_row, column=col_idx, value=value)

    current_row += 1
    break  # keep for testing; remove to process all rows

# ---------------- SAVE ----------------
wb.save(output_path)
print(f"Amazon XLSM generated: {output_path}")
