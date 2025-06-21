import os
import pandas as pd
import time
from duckduckgo_search import DDGS
from urllib.parse import urlparse

print("🚀 Iniciando proceso de enriquecimiento con DuckDuckGo...")

# Cargar CSV original
csv_path = os.path.join(os.path.dirname(__file__), "hoteles_para_duck.csv")
df = pd.read_csv(csv_path)

# Filtrar solo hoteles sin URL principal
df = df[df['url_principal'].isna()]

# Añadir columnas de URL si no existen
for i in range(1, 11):
    col = f"url_{i}"
    if col not in df.columns:
        df[col] = ""
    df[col] = df[col].astype(str)
    print(f"🔍 Procesando fila {idx}: {row['nombre']} ({row['ciudad']}, {row['pais']})")

ddgs = DDGS()

def obtener_urls(query, max_urls=10):
    urls = []
    dominios = set()
    try:
        resultados = ddgs.text(query, max_results=20)
        for r in resultados:
            url = r.get("href")
            if url:
                dominio = urlparse(url).netloc
                if dominio not in dominios:
                    dominios.add(dominio)
                    urls.append(url)
                if len(urls) >= max_urls:
                    break
    except Exception as e:
        print(f"❌ Error: {e}")
    return urls

# Procesar por fila
for idx, row in df.iterrows():
    if pd.isna(row["url_principal"]):
        query = f"{row['nombre']}, {row['ciudad']}, {row['pais']}"
        print(f"🔍 Buscando URLs para: {query}")
        urls = obtener_urls(query)
        for i, url in enumerate(urls):
            df.at[idx, f"url_{i+1}"] = url
        time.sleep(5)

    # Guardar progreso cada 10 filas
    if idx % 10 == 0:
        df.to_csv("procesamiento_1_enriquecido.csv", index=False)
        print(f"💾 Progreso guardado en fila {idx}")

# Guardado final
df.to_csv("procesamiento_1_enriquecido.csv", index=False)
print("✅ Proceso completado y archivo guardado.")
