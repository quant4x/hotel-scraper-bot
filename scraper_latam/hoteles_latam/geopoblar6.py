import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

HERE_API_KEY = os.getenv("HERE_API_KEY")
assert HERE_API_KEY, "⚠️ La clave HERE_API_KEY no fue encontrada en el archivo .env"

# Cargar CSV
df = pd.read_csv("hoteles.csv")

# Columnas nuevas si no existen
for col in ["barrio", "suburbio", "ciudad_geopy", "estado", "region", "codigo_postal", "direccion_formateada", "geosource"]:
    if col not in df.columns:
        df[col] = ""

# Filtrar hoteles en Perú con campos vacíos
df_peru = df[(df["pais"].str.lower() == "perú") & (df["ciudad_geopy"].isna() | df["ciudad_geopy"].eq(""))]

# Script de geolocalización
for idx in df_peru.index:
    row = df.loc[idx]
    lat, lon = row["latitud"], row["longitud"]

    if pd.isna(lat) or pd.isna(lon):
        continue

    try:
        url = (
            f"https://revgeocode.search.hereapi.com/v1/revgeocode"
            f"?at={lat},{lon}&lang=es&apikey={HERE_API_KEY}"
        )
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            if items:
                address = items[0].get("address", {})
                df.at[idx, "barrio"] = address.get("district", "")
                df.at[idx, "suburbio"] = address.get("subdistrict", "")
                df.at[idx, "ciudad_geopy"] = address.get("city", "")
                df.at[idx, "estado"] = address.get("state", "")
                df.at[idx, "region"] = address.get("county", "")
                df.at[idx, "codigo_postal"] = address.get("postalCode", "")
                df.at[idx, "direccion_formateada"] = address.get("label", "")
                df.at[idx, "geosource"] = "here"
                print(f"✅ Fila {idx} actualizada con HERE")
            else:
                print(f"⚠️ Fila {idx}: sin resultados en HERE")
        else:
            print(f"❌ Error en HERE para fila {idx}: {response.status_code}")
    except Exception as e:
        print(f"💥 Excepción en fila {idx}: {e}")

    # Respetar límites (1 request/segundo)
    time.sleep(1.1)

    # Guardar progreso
    if idx % 10 == 0:
        df.to_csv("hoteles.csv", index=False)
        print(f"💾 Progreso guardado en fila {idx}")

# Guardado final
df.to_csv("hoteles.csv", index=False)
print("🏁 Proceso con HERE completado.")
