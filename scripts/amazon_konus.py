import re
import logging
import pandas as pd
import json
import os
from dotenv import load_dotenv
from mistralai import Mistral
from openpyxl import load_workbook
from pathlib import Path

# ---------------- CONFIG ----------------
excel_path = "templates/konus.xlsm"
sheet_name = "Plantilla"
csv_path = "input/konus_catalog.csv"
output_path = "output/amazon_konus.xlsm"
start_row = 6
log_path = "logs/amazon_konus.log"
checkpoint_dir = Path("checkpoints")
checkpoint_dir.mkdir(parents=True, exist_ok=True)
checkpoint_file = checkpoint_dir / "amazon_konus.json"

# ---------------- ENV ----------------
load_dotenv()

MISTRAL_API_TOKEN = os.getenv("MISTRAL_API_TOKEN")
if not MISTRAL_API_TOKEN:
    raise RuntimeError("MISTRAL_API_TOKEN not found in environment")

# ---------------- LOGGING ----------------
Path(log_path).parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Script started")

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

# ---------------- CLEAN TEMPLATE ----------------
max_row = ws.max_row
if max_row >= start_row:
    ws.delete_rows(start_row, max_row - start_row + 1)

# ---------------- HELPERS ----------------
def clean_price(price_str):
    if not price_str:
        return None
    return re.sub(r"[^\d.]", "", str(price_str))


def clean_json(text):
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        raise ValueError(f"Invalid JSON from LLM: {text}")
    return json.loads(match.group())

def safe_dim(dims, key, subkey):
    """
    Safely extract nested dimension values from LLM output.
    Returns None if the structure is missing or null.
    """
    val = dims.get(key)
    if isinstance(val, dict):
        return val.get(subkey)
    return None

# ---------------- DIRECT MAP ----------------
def direct_map(csv_row, enrichment):
    weight = csv_row.get("PesoNeto")
    if isinstance(weight, str):
        if 'kg' in weight:
            weight = weight.lower().replace("kg.", "").replace("kg", "").replace(",", ".").strip()
            weight = str(float(weight) * 1000)
        else:
            weight = weight.lower().replace("gr.", "").replace("gr", "").strip()

    medidas_raw = csv_row.get("Medidas")
    medidas = None
    if isinstance(medidas_raw, str):
        medidas_clean = medidas_raw.lower().replace("cm", "").replace(" ", "")
        medidas = "; ".join(medidas_clean.split("x"))

    dims = enrichment.get("dimensions", {})

    browse_node = None
    product_type = enrichment.get("product_type")
    match product_type:
        case "RANGEFINDER":
            browse_node = "Bricolaje y herramientas > Herramientas manuales y eléctricas > Herramientas de medición y diseño > Herramientas para medición láser y accesorios > Telémetros láser (3053092031)"
        case "CAMERA_TRIPOD":
            browse_node = "Electrónica > Comunicación móvil y accesorios > Accesorios > Accesorios de foto y vídeo > Trípodes (21529596031)"
        case "MICROSCOPES":
            browse_node = "Industria, empresas y ciencia > Artículos educativos > Recursos para planes de estudios > Ciencia > Microscopios > Microscopios monoculares (1443222031)"
        case "AIMING_SCOPE_SIGHT":
            browse_node = "Electrónica > Fotografía y videocámaras > Prismáticos, telescopios y óptica > Dispositivos de visión nocturna (930881031)"
        case "MAGNIFIER":
            browse_node = "Electrónica > Comunicación móvil y accesorios > Accesorios > Accesorios de juegos > Ampliadores y magnificadores de pantalla (21529576031)"
        case "TELESCOPE":
            browse_node = "Electrónica > Fotografía y videocámaras > Prismáticos, telescopios y óptica > Monoculares (930884031)"
        case "BINOCULAR":
            browse_node = "Juguetes y juegos > Aprendizaje y educación > Óptica > Prismáticos (14525749031)"
        case "FLASHLIGHT":
            browse_node = "Bricolaje y herramientas > Ferretería > Linternas y faroles de mano > Linternas (3053011031)"
        case "NAVIGATION_COMPASS":
            browse_node = "Deportes y aire libre > Electrónica y dispositivos > Brújulas (2928776031)"

    return {
        "SKU": csv_row.get("EAN"),
        "SKU principal": csv_row.get("EAN"),
        "ID del producto": csv_row.get("EAN"),
        "Marca": csv_row.get("Marca"),
        "Fabricante": "Konus",
        "Nombre Modelo": csv_row.get("Modelo"),
        "Nombre del producto": csv_row.get("Título_producto"),
        "Palabra clave genérica": csv_row.get("Descripción_corta"),
        "Descripción del producto": csv_row.get("Descripción_larga"),
        "Viñeta": enrichment.get("bullet"),
        "Nodos recomendados de búsqueda": browse_node,
        "Tipo de producto": enrichment.get("product_type"),

        "Precio de venta recomendado (PVPR)": clean_price(csv_row.get("PVP FINAL")),
        "Tu precio EUR (Vender en Amazon, ES)": clean_price(csv_row.get("PVP FINAL")),
        "Precio de venta. EUR (Vender en Amazon, ES)": clean_price(csv_row.get("PVP FINAL")),

        "Estado del producto": "Nuevo",
        "Tipo de identificador del producto": "EAN",
        "Grupo de la marina mercante (ES)": "Nueva plantilla Envios",
        "Cumplimiento de código de canal (ES)": "DEFAULT",
        "Cantidad (ES)": "1",
        "Número de Artículos": "1",
        "Número de cajas": "1",
        "Componentes Incluidos": "1 artículo",
        "Numero de pieza": csv_row.get("Título_producto"),

        "Peso Artículo": weight,
        "Unidad de peso del artículo": "Gramos",
        "Tamaño del anillo": medidas,

        "Grosor del artículo desde la parte delantera hasta la trasera": safe_dim(dims, "thickness", "value"),
        "Unidad de altura del artículo": safe_dim(dims, "height", "unit"),
        "Ancho del artículo de lado a lado": safe_dim(dims, "width", "value"),
        "Unidad del ancho del artículo": safe_dim(dims, "width", "unit"),

        "Aumento máximo": dims.get("max_magnification"),
        "Distancia focal mínima": safe_dim(dims, "min_focal_distance", "value"),

        "Longitud Paquete": safe_dim(dims, "package_length", "value"),
        "Unidad de longitud del paquete": safe_dim(dims, "package_length", "unit"),
        "Ancho Paquete": safe_dim(dims, "package_width", "value"),
        "Unidad de anchura del paquete": safe_dim(dims, "package_width", "unit"),
        "Altura Paquete": safe_dim(dims, "package_height", "value"),
        "Unidad de altura del paquete": safe_dim(dims, "package_height", "unit"),
        "Peso del paquete": safe_dim(dims, "package_weight", "value"),
        "Unidad del peso del paquete": safe_dim(dims, "package_weight", "unit"),

        "País de origen": enrichment.get("country_of_origin"),
        "Garantía de Producto": "2",
        "¿Se necesitan baterías?": "No",
        "Normativas sobre mercancías peligrosas": "No aplicable",
        "Riesgo del GDPR": "No hay información electrónica almacenada.",
        "URL de la imagen principal": csv_row.get("Imagen_grande"),
        "País de origen": "Italia",
        "Nodos recomendados de búsqueda": enrichment.get("search_modes")
    }


# ---------------- CHECKPOINT ----------------
if checkpoint_file.exists():
    with open(checkpoint_file, "r") as f:
        processed_skus = set(json.load(f))
else:
    processed_skus = set()

# ---------------- LLM ----------------
mistral = Mistral(api_key=MISTRAL_API_TOKEN)

ALLOWED_PRODUCT_TYPES = [
    "NAVIGATION_COMPASS",
    "FLASHLIGHT",
    "BINOCULAR",
    "TELESCOPE",
    "MAGNIFIER",
    "AIMING_SCOPE_SIGHT",
    "MICROSCOPES",
    "CAMERA_TRIPOD",
    "RANGEFINDER",
]


def classify_product_enrichment(csv_row, mistral):
    prompt = f"""
You are enriching Amazon product listings.

Rules:
- Choose EXACTLY ONE product type from the allowed list
- Generate EXACTLY ONE factual bullet point
- Infer values only if clearly implied, otherwise return null
- Country must be a real country name in Spanish
- Warranty must be concise (e.g. "2 años")
- Units must be metric
- Return ONLY valid JSON

Allowed product types:
{ALLOWED_PRODUCT_TYPES}

Product data:
- Title: {csv_row.get("Título_producto")}
- Short description: {csv_row.get("Descripción_corta")}
- Long description: {csv_row.get("Descripción_larga")}
- Family: {csv_row.get("Familia")}
- Model: {csv_row.get("Modelo")}

Return format:
{{
  "product_type": "ONE_OF_THE_ALLOWED_VALUES",
  "bullet": "Short factual bullet",
  "dimensions": {{
    "thickness": {{ "value": number|null, "unit": "cm"|null }},
    "height": {{ "value": number|null, "unit": "cm"|null }},
    "width": {{ "value": number|null, "unit": "cm"|null }},
    "max_magnification": number|null,
    "min_focal_distance": {{ "value": number|null, "unit": "cm"|null }},
    "package_length": {{ "value": number|null, "unit": "cm"|null }},
    "package_width": {{ "value": number|null, "unit": "cm"|null }},
    "package_height": {{ "value": number|null, "unit": "cm"|null }},
    "package_weight": {{ "value": number|null, "unit": "kg"|null }}
  }}
}}
"""

    res = mistral.chat.complete(
        model="mistral-small-latest",
        messages=[{"role": "user", "content": prompt}],
        stream=False,
    )

    data = clean_json(res.choices[0].message.content)

    if data.get("product_type") not in ALLOWED_PRODUCT_TYPES:
        raise ValueError(f"Invalid product type: {data.get('product_type')}")

    return data


# ---------------- PROCESS ----------------
current_row = start_row

for idx, csv_row in df.iterrows():
    csv_dict = csv_row.to_dict()
    sku = csv_dict.get("EAN")

    if sku in processed_skus:
        logging.info(f"Skipping SKU {sku}")
        current_row += 1
        continue

    logging.info(f"Processing SKU {sku}")

    try:
        enrichment = classify_product_enrichment(csv_dict, mistral)
    except Exception as e:
        logging.error(f"LLM failed for SKU {sku}: {e}")
        enrichment = {
            "product_type": None,
            "bullet": None,
            "country_of_origin": None,
            "warranty": None,
            "dimensions": {}
        }

    mapped = direct_map(csv_dict, enrichment)

    for col_idx, header in enumerate(amazon_headers, start=1):
        value = mapped.get(header)
        if value is not None:
            ws.cell(row=current_row, column=col_idx, value=value)

    processed_skus.add(sku)
    with open(checkpoint_file, "w") as f:
        json.dump(list(processed_skus), f)

    wb.save(output_path)
    current_row += 1

logging.info(f"Amazon XLSM generated: {output_path}")
print(f"Amazon XLSM generated: {output_path}")
