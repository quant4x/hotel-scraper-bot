import pandas as pd
import time
import requests
from urllib.parse import urlparse

API_KEY = "AIzaSyDpyU2fCvNTwaDJNx2uUd0v8QeKUJsfV_Y"
SEARCH_ENGINE_ID = "1302a3cde8de3429a"
INPUT_CSV = "hoteles_para_google.csv"
OUTPUT_CSV = "procesamiento_google.csv"

# Cargar datos
df = pd.read_csv(INPUT_CSV)

# Añadir columnas de URL si no existen
for i in range(1, 11):
    col = f"url_{i}"
    if col not in df.columns:
        df[col] = ""
    df[col] = df[col].astype(str) # Previene errores al guardar strings

def buscar_urls_google(query, max_urls=10):
    urls = []
    dominios = set()
    try:
        for start in range(1, 30, 10):  # Paginación (máximo 30 resultados: 3 páginas)
            url = (
                "https://www.googleapis.com/customsearch/v1?"
                f"key={API_KEY}&cx={SEARCH_ENGINE_ID}&q={query}&start={start}"
            )
            res = requests.get(url)
            if res.status_code != 200:
                print(f"❌ Error HTTP: {res.status_code}")
                break
            resultados = res.json().get("items", [])
            for r in resultados:
                enlace = r.get("link")
                if enlace:
                    dominio = urlparse(enlace).netloc
                    if dominio not in dominios:
                        dominios.add(dominio)
                        urls.append(enlace)
                if len(urls) >= max_urls:
                    break
            if len(urls) >= max_urls:
                break
    except Exception as e:
        print(f"❌ Error en búsqueda: {e}")
    return urls

# Iterar sobre hoteles
for idx, row in df.iterrows():
    if pd.isna(row["url_principal"]):
        query = f"{row['nombre']}, {row['ciudad']}, {row['pais']}"
        print(f"🔍 Buscando URLs con Google para: {query}")
        urls = buscar_urls_google(query)

        for i, url in enumerate(urls):
            df.at[idx, f"url_{i+1}"] = url
        if urls:
            print(f"✅ {len(urls)} URLs encontradas y guardadas.")
        else:
            print("⚠️ Ninguna URL encontrada.")
        time.sleep(5)  # Espera corta para evitar límite de cuota

    if idx % 10 == 0:
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"💾 Progreso guardado en fila {idx}")

# Guardado final
df.to_csv(OUTPUT_CSV, index=False)
print("🎉 Proceso completo.")
