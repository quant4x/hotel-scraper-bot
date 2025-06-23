import requests
import pandas as pd
import json
import time
from urllib.parse import urlparse

# --- Excluir registros sin nombre de hotel ---
def tiene_nombre_valido(row):
    name = row.get('name', '')
    return pd.notna(name) and str(name).strip() != ''

# --- Configuración ---
API_KEY = '46e1661ae76b5da648b374f11f287e6d55fa4ff5'
API_URL = "https://google.serper.dev/search"
TIEMPO_ENTRE_CONSULTAS = 0.10  # segundos
START_FROM_ID = 609824781  # Cambia esto si quieres forzar a partir de un ID

# --- Cargar data ---
lodgings = pd.read_csv("lodgings.csv")
lodgings = lodgings[lodgings['tourism'] == 'hotel']  # <-- Solo hoteles
lodging_urls = pd.read_csv("lodging_urls.csv")
lodging_address = pd.read_csv("lodging_address.csv")

# --- Unir ciudad ---
lodgings = lodgings.merge(lodging_address[['id', 'city']], on='id', how='left')

# Asegurar columnas necesarias
for col in ['website', 'url_2', 'url_3', 'booking', 'tripadvisor', 'expedia', 'google']:
    if col not in lodging_urls.columns:
        lodging_urls[col] = None

# Recolectar info
info_rows = []

# Contadores
total_urls_insertadas = 0
total_info_registros = 0
procesadas_desde_ultimo_guardado = 0

start_processing = START_FROM_ID is None  # Si es None, empieza desde el principio

for _, row in lodgings.iterrows():
    hotel_id = row['id']

    # Si no hemos llegado al ID de inicio, seguimos buscando
    if not start_processing:
        if hotel_id == START_FROM_ID:
            start_processing = True
        else:
            continue  # Salta hasta llegar al ID

     # --- Salta si no hay nombre válido ---
    if not tiene_nombre_valido(row):
        continue

    # --- Procesamiento ---
    name = str(row['name'])
    city = str(row['city']) if pd.notna(row['city']) else ""
    country = str(row['country'])

    # ... el resto de tu código de procesamiento ...

    query = f"{name} {city} {country}".strip()
    print(f"\n🔍 Buscando: {query}")

    headers = {
        'X-API-KEY': API_KEY,
        'Content-Type': 'application/json'
    }
    payload = json.dumps({ "q": query })

    try:
        response = requests.post(API_URL, headers=headers, data=payload)
        if response.status_code != 200:
            print(f"⚠️ Error {response.status_code} en búsqueda para '{query}'")
            continue

        results = response.json().get("organic", [])
        urls_insertadas = 0

        for res in results:
            url = res.get("link", "")
            snippet = res.get("snippet", "")
            domain = urlparse(url).netloc.replace("www.", "")

            # Guardar en lodging_info.csv
            info_rows.append({
                "id_lodging": hotel_id,
                "domain": domain,
                "url": url,
                "info": snippet
            })
            total_info_registros += 1

            # Insertar en lodging_urls
            updated = False
            if "booking.com" in url:
                lodging_urls.loc[lodging_urls['id'] == hotel_id, 'booking'] = url
                updated = True
            elif "tripadvisor.com" in url:
                lodging_urls.loc[lodging_urls['id'] == hotel_id, 'tripadvisor'] = url
                updated = True
            elif "expedia.com" in url:
                lodging_urls.loc[lodging_urls['id'] == hotel_id, 'expedia'] = url
                updated = True
            elif "google.com" in url:
                lodging_urls.loc[lodging_urls['id'] == hotel_id, 'google'] = url
                updated = True
            else:
                urls_row = lodging_urls[lodging_urls['id'] == hotel_id].iloc[0]
                for field in ['website', 'url_2', 'url_3']:
                    if pd.isna(urls_row[field]) or urls_row[field] == "":
                        lodging_urls.loc[lodging_urls['id'] == hotel_id, field] = url
                        updated = True
                        break

            if updated:
                urls_insertadas += 1
                total_urls_insertadas += 1

        procesadas_desde_ultimo_guardado += 1
        print(f"✅ {len(results)} resultados procesados")
        print(f"   ➕ URLs añadidas en esta búsqueda: {urls_insertadas}")
        print(f"   📝 Registros añadidos a lodging_info: {len(results)}")
        print(f"📊 Totales acumulados: URLs={total_urls_insertadas} | INFO={total_info_registros}")

        # --- Guardado incremental cada 10 ---
        if procesadas_desde_ultimo_guardado >= 10:
            pd.DataFrame(info_rows).to_csv("lodging_info.csv", index=False)
            lodging_urls.to_csv("lodging_urls.csv", index=False)
            print("💾 Guardado intermedio completado.")
            procesadas_desde_ultimo_guardado = 0

        time.sleep(TIEMPO_ENTRE_CONSULTAS)

    except Exception as e:
        print(f"❌ Error procesando '{query}': {e}")
        continue

# --- Guardado final ---
pd.DataFrame(info_rows).to_csv("lodging_info.csv", index=False)
lodging_urls.to_csv("lodging_urls.csv", index=False)
print("\n📦 Guardado final completo.")
print(f"🔚 Proceso terminado. Total URLs insertadas = {total_urls_insertadas}, Total info registros = {total_info_registros}")