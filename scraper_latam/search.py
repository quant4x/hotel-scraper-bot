import pandas as pd
import time
import threading
from serpapi import GoogleSearch
from duckduckgo_search import ddg

# === CONFIGURACIÓN GENERAL ===
SERPAPI_KEY = "TU_API_KEY"
TIEMPO_ESPERA = 1.8  # segundos entre búsquedas
NUM_RESULTADOS = 5

# === CARGA DE ARCHIVOS ===
lodgings = pd.read_csv("lodgings.csv")
lodging_address = pd.read_csv("lodging_address.csv")
lodging_urls = pd.read_csv("lodging_urls.csv")

# === MERGE PARA ARMAR LAS QUERIES ===
merged = lodgings.merge(lodging_address, on="id", how="left")

def build_query(row):
    fields = [row.get("name", ""), row.get("city", ""), row.get("state", ""), row.get("country", "")]
    return " ".join([str(f).strip() for f in fields if pd.notna(f) and f.strip() != ""])

merged["search_query"] = merged.apply(build_query, axis=1)

# === DOMINIOS A BUSCAR ===
DOMAINS = {
    "booking": "booking.com",
    "tripadvisor": "tripadvisor.com",
    "expedia": "expedia.com",
    "google": "google.com"
}
URL_COLUMNS = ["website", "url_2", "url_3", "booking", "tripadvisor", "expedia", "google"]

# === ACTUALIZADOR DE URLs ===
def completar_urls(row, urls, modo):
    if row["id"] not in lodging_urls["id"].values:
        return
    idx = lodging_urls[lodging_urls["id"] == row["id"]].index[0]
    current = lodging_urls.loc[idx]

    # Genéricas
    generales = [u for u in urls if all(d not in u for d in DOMAINS.values())]
    for col in ["website", "url_2", "url_3"]:
        if pd.isna(current[col]) and generales:
            lodging_urls.at[idx, col] = generales.pop(0)

    # Específicas
    for key, domain in DOMAINS.items():
        if pd.isna(current[key]):
            candidatos = [u for u in urls if domain in u]
            if candidatos:
                lodging_urls.at[idx, key] = candidatos[0]

    print(f"✅ ({modo}) Actualizado ID: {row['id']}")

# === BÚSQUEDA CON SERPAPI ===
def buscar_con_serpapi():
    for i, row in merged.iterrows():
        if row["id"] not in lodging_urls["id"].values:
            continue
        current = lodging_urls[lodging_urls["id"] == row["id"]].iloc[0]
        if all(pd.notna(current[c]) for c in URL_COLUMNS):
            continue

        try:
            search = GoogleSearch({
                "q": row["search_query"],
                "api_key": SERPAPI_KEY,
                "num": NUM_RESULTADOS
            })
            result = search.get_dict()
            urls = [r.get("link") for r in result.get("organic_results", []) if r.get("link")]
            completar_urls(row, urls, "SerpAPI")
            time.sleep(TIEMPO_ESPERA)
        except Exception as e:
            print(f"❌ (SerpAPI) Error en ID {row['id']}: {e}")

# === BÚSQUEDA CON DUCKDUCKGO ===
def buscar_con_duck():
    for i in reversed(range(len(merged))):
        row = merged.iloc[i]
        if row["id"] not in lodging_urls["id"].values:
            continue
        current = lodging_urls[lodging_urls["id"] == row["id"]].iloc[0]
        if all(pd.notna(current[c]) for c in URL_COLUMNS):
            continue

        try:
            results = ddg(row["search_query"], max_results=NUM_RESULTADOS)
            urls = [r["href"] for r in results if "href" in r]
            completar_urls(row, urls, "DuckDuckGo")
            time.sleep(TIEMPO_ESPERA)
        except Exception as e:
            print(f"❌ (DuckDuckGo) Error en ID {row['id']}: {e}")

# === LANZAR LOS DOS PROCESOS ===
t1 = threading.Thread(target=buscar_con_serpapi)
t2 = threading.Thread(target=buscar_con_duck)

t1.start()
t2.start()

t1.join()
t2.join()

# === GUARDAR RESULTADO ===
lodging_urls.to_csv("lodging_urls.csv", index=False)
print("💾 Archivo lodging_urls.csv actualizado.")