import pandas as pd
import time
from geopy.geocoders import Nominatim
from opencage.geocoder import OpenCageGeocode
import requests
import os
from dotenv import load_dotenv

load_dotenv()  # Cargar variables desde .env

# Claves API
OPEN_CAGE_KEY = os.getenv("OPEN_CAGE_KEY")
LOCATIONIQ_KEY = os.getenv("LOCATIONIQ_KEY")

# Inicializar geocoders
geolocator = Nominatim(user_agent="hotel_geocoder")
opencage = OpenCageGeocode(OPEN_CAGE_KEY)

# Leer archivo
df = pd.read_csv("hoteles.csv")

# Columnas nuevas si no existen
for col in ["barrio", "suburbio", "ciudad_geopy", "estado", "region", "codigo_postal", "direccion_formateada", "geosource"]:
    if col not in df.columns:
        df[col] = ""

# Funciones para cada proveedor
def geocode_geopy(lat, lon):
    try:
        location = geolocator.reverse((lat, lon), language="en", timeout=10)
        if location and location.raw.get("address"):
            return location.raw["address"], location.address, "geopy"
    except Exception as e:
        print(f"[geopy] ❌ Error: {e}")
    return None, None, None

def geocode_opencage(lat, lon):
    try:
        results = opencage.reverse_geocode(lat, lon)
        if results and len(results):
            comp = results[0]["components"]
            return comp, results[0]["formatted"], "opencage"
    except Exception as e:
        print(f"[opencage] ❌ Error: {e}")
    return None, None, None

def geocode_locationiq(lat, lon):
    try:
        url = f"https://us1.locationiq.com/v1/reverse.php?key={LOCATIONIQ_KEY}&lat={lat}&lon={lon}&format=json"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data.get("address", {}), data.get("display_name", ""), "locationiq"
    except Exception as e:
        print(f"[locationiq] ❌ Error: {e}")
    return None, None, None

# Ciclo de actualización
for idx, row in df.iterrows():
    if pd.isna(row["latitud"]) or pd.isna(row["longitud"]):
        continue
    if pd.notna(row["ciudad_geopy"]) and row["ciudad_geopy"] != "":
        continue  # Ya está poblado

    lat, lon = row["latitud"], row["longitud"]

    for fuente in [geocode_geopy, geocode_opencage, geocode_locationiq]:
        info, direccion, proveedor = fuente(lat, lon)
        time.sleep(3 if proveedor == "geopy" else 2.5 if proveedor == "opencage" else 2)

        if info:
            df.at[idx, "barrio"] = info.get("neighbourhood", "")
            df.at[idx, "suburbio"] = info.get("suburb", "")
            df.at[idx, "ciudad_geopy"] = info.get("city") or info.get("town") or info.get("village") or info.get("municipality", "")
            df.at[idx, "estado"] = info.get("state", "")
            df.at[idx, "region"] = info.get("region") or info.get("state_district", "")
            df.at[idx, "codigo_postal"] = info.get("postcode", "")
            df.at[idx, "direccion_formateada"] = direccion
            df.at[idx, "geosource"] = proveedor
            print(f"✅ Fila {idx} actualizada con datos de {proveedor}")
            break  # No seguir probando otros servicios
        else:
            print(f"❌ Fila {idx}: sin resultados en {proveedor}")

    # Guardar cada 10 filas
    if idx % 10 == 0:
        df.to_csv("hoteles.csv", index=False)
        print(f"💾 Progreso guardado en fila {idx}")

# Guardado final
df.to_csv("hoteles.csv", index=False)
print("🏁 Proceso completado.")
