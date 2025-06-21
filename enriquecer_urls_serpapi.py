import pandas as pd
import requests
import time

API_KEY = "cddd4ca9e371d23ac53e319e78dafc85856e01ef7d8ee6cedbabf5e34a288b84"
SEARCH_URL = "https://serpapi.com/search"

def buscar_url(nombre, ciudad, pais):
    consulta = f"{nombre} {ciudad or ''} {pais or ''} sitio oficial"
    params = {
        "q": consulta,
        "engine": "google",
        "hl": "es",
        "gl": "cl",
        "api_key": API_KEY
    }
    try:
        res = requests.get(SEARCH_URL, params=params)
        res.raise_for_status()
        data = res.json()
        resultados = data.get("organic_results", [])
        for r in resultados:
            url = r.get("link")
            if url and not any(x in url for x in ["tripadvisor", "booking", "expedia"]):
                return url
    except Exception as e:
        print(f"⚠️ Error con '{consulta}': {e}")
    return None

# Leer el archivo original
df = pd.read_csv("hoteles10.csv")

# Buscar y actualizar
for i, row in df.iterrows():
    if pd.isna(row.url_principal):
        url = buscar_url(row.nombre, row.ciudad, row.pais)
        if url:
            df.at[i, "url_principal"] = url
            print(f"🔗 {row.nombre} → {url}")
        else:
            print(f"❌ No encontrado: {row.nombre}")
        time.sleep(2)  # evita rate-limit

# Guardar nuevo archivo enriquecido
df.to_csv("hoteles10_enriquecido.csv", index=False)
print("✅ URLs actualizadas guardadas en hoteles10_enriquecido.csv")
