# run_hotels.py
"""
Orquesta el scraping de múltiples hoteles listados en un CSV.
Para cada hotel:
 - Crea una carpeta con el nombre del hotel (limpio)
 - Ejecuta el spider de Scrapy especificando la URL
 - Guarda en la carpeta un JSON Lines con la información
 - Descarga todos los PDFs encontrados en el JSON Lines
"""
import csv
import re
import subprocess
import sys
import requests
import json
from pathlib import Path

# Archivo CSV con columnas: name,url
CSV_PATH = 'hotels_list.csv'
# Carpeta raíz para resultados
OUTPUT_ROOT = 'hotels_data'

# Sanitizar nombres válidos para carpeta
def sanitize(name: str) -> str:
    safe = re.sub(r'[\\/*?"<>|:]', '', name)
    safe = safe.strip().replace(' ', '_')
    return safe or 'hotel'

# Descargar PDF
def download_pdf(url: str, dest_folder: Path):
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        filename = url.split('/')[-1]
        dest = dest_folder / filename
        with open(dest, 'wb') as f:
            f.write(resp.content)
        print(f"  ↳ PDF descargado: {filename}")
    except Exception as e:
        print(f"  ⚠️ Falló descargar {url}: {e}")


def main():
    Path(OUTPUT_ROOT).mkdir(exist_ok=True)

    with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            name = row.get('name', '').strip()
            url  = row.get('url', '').strip()
            if not url:
                continue

            folder_name = sanitize(name)
            hotel_dir = Path(OUTPUT_ROOT) / folder_name
            hotel_dir.mkdir(exist_ok=True)
            print(f"Procesando hotel: {name} -> {url}")

            # Definir salida JSON Lines
            output_jl = hotel_dir / 'data.jl'
            cmd = [
    "scrapy", "runspider", "hotel_scraper/spiders/hotel_spider.py",
    "-a", f"start_url={url}",
    "-o", str(output_jl),
    "-t", "jsonlines"
    ]
            result = subprocess.run(cmd, cwd='.', capture_output=True, text=True)
            if result.returncode != 0:
                print(f"⚠️ Error en Scrapy: {result.stderr}")
                continue
            print(f"  ✔️ Scrapy completado, datos en {output_jl}")

            # Leer JSON Lines y descargar PDFs
            pdf_folder = hotel_dir / 'pdfs'
            pdf_folder.mkdir(exist_ok=True)
            try:
                items = []
                with open(output_jl, encoding='utf-8') as f:
                    for line in f:
                        items.append(json.loads(line))
            except Exception as e:
                print(f"⚠️ Falló parsear JSONL de salida: {e}")
                continue

            all_pdfs = set()
            for item in items:
                for pdf in item.get('pdfs', []):
                    all_pdfs.add(pdf.get('url'))

            for pdf_url in sorted(all_pdfs):
                download_pdf(pdf_url, pdf_folder)

    print("\nProceso completado para todos los hoteles.")

if __name__ == '__main__':
    main()
