import pandas as pd
import time
import requests
import os
from dotenv import load_dotenv

# Cargar .env
load_dotenv()
MAPBOX_KEY = os.getenv("MAPBOX_KEY")
if not MAPBOX_KEY:
    raise ValueError("❌ MAPBOX_KEY no encontrada en .env")

# Cargar archivo
df = pd.read_csv("hoteles.csv")

# Columnas necesarias
for col in ["barrio", "suburbio", "ciudad_geopy", "estado", "region", "codigo_postal", "direccion_formateada", "geosource"]:
    if col not in df.columns:
        df[col] = ""

# Función Mapbox
def geocode_mapbox(lat, lon):
    try:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
        params = {
            "access_token": MAPBOX_KEY,
            "language": "es",
            "types": "place,locality,neighborhood,address,region,district,postcode"
        }
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            data = resp.json()
            components = {c['id'].split('.')[0]: c['text'] for c in data['features']}
            return components, data['features'][0]['place_name'], "mapbox"
    except Exception as e:
        print(f"[Mapbox] ❌ Error: {e}")
    return None, None, None

# Filtrar solo Colombia
df_colombia = df[(df["pais"].str.lower() == "colombia") & (df["ciudad_geopy"].isna())]

# Procesar de abajo hacia arriba
for idx in reversed(df_colombia.index):
    row = df.loc[idx]
    lat, lon = row["latitud"], row["longitud"]

    if pd.isna(lat) or pd.isna(lon):
        continue

    info, direccion, proveedor = geocode_mapbox(lat, lon)
    time.sleep(1.5)

    if info:
        df.at[idx, "barrio"] = info.get("neighborhood", "")
        df.at[idx, "suburbio"] = info.get("locality", "")
        df.at[idx, "ciudad_geopy"] = info.get("place", "")
        df.at[idx, "estado"] = info.get("region", "")
        df.at[idx, "region"] = info.get("district", "")
        df.at[idx, "codigo_postal"] = info.get("postcode", "")
        df.at[idx, "direccion_formateada"] = direccion
        df.at[idx, "geosource"] = proveedor
        print(f"🇨🇴 ✅ Fila {idx} actualizada con Mapbox (Colombia)")
    else:
        print(f"🇨🇴 ❌ Fila {idx} sin datos Mapbox")

    if idx % 10 == 0:
        df.to_csv("hoteles.csv", index=False)
        print(f"💾 Progreso guardado en fila {idx}")

df.to_csv("hoteles.csv", index=False)
print("🏁 Finalizado proceso Mapbox para Colombia")
