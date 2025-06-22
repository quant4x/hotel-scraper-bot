import pandas as pd
import time
from opencage.geocoder import OpenCageGeocode
import os
import requests
from dotenv import load_dotenv

load_dotenv()

# Clave API
OPEN_CAGE_KEY = os.getenv("OPEN_CAGE_KEY")
opencage = OpenCageGeocode(OPEN_CAGE_KEY)

# Cargar CSV
df = pd.read_csv("hoteles.csv")

# Asegurar columnas nuevas
for col in ["barrio", "suburbio", "ciudad_geopy", "estado", "region", "codigo_postal", "direccion_formateada", "geosource"]:
    if col not in df.columns:
        df[col] = ""

# Procesar de abajo hacia arriba
for idx in reversed(df.index):
    row = df.loc[idx]

    if pd.isna(row["latitud"]) or pd.isna(row["longitud"]):
        continue
    if str(row["pais"]).strip().lower() != "méxico":
        continue
    if pd.notna(row["ciudad_geopy"]) and row["ciudad_geopy"] != "":
        continue

    lat, lon = row["latitud"], row["longitud"]

    try:
        results = opencage.reverse_geocode(lat, lon, language="es")
        if results and len(results):
            comp = results[0]["components"]
            direccion = results[0]["formatted"]

            df.at[idx, "barrio"] = comp.get("neighbourhood", "")
            df.at[idx, "suburbio"] = comp.get("suburb", "")
            df.at[idx, "ciudad_geopy"] = comp.get("city") or comp.get("town") or comp.get("village") or comp.get("municipality", "")
            df.at[idx, "estado"] = comp.get("state", "")
            df.at[idx, "region"] = comp.get("region") or comp.get("state_district", "")
            df.at[idx, "codigo_postal"] = comp.get("postcode", "")
            df.at[idx, "direccion_formateada"] = direccion
            df.at[idx, "geosource"] = "opencage"
            print(f"✅ Fila {idx} actualizada con datos de opencage")
        else:
            print(f"❌ Fila {idx}: sin resultados")
    except Exception as e:
        print(f"🛑 Error en fila {idx} con opencage: {e}")

    time.sleep(2.5)

    if idx % 10 == 0:
        df.to_csv("hoteles.csv", index=False)
        print(f"💾 Progreso guardado en fila {idx}")

# Guardado final
df.to_csv("hoteles.csv", index=False)
print("🏁 Proceso con OpenCage en México completado.")
