import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
assert TOMTOM_API_KEY, "⚠️ No se encontró la clave TOMTOM_API_KEY en el archivo .env"

# Cargar CSV
df = pd.read_csv("hoteles.csv")

# Asegurar columnas de geodatos
for col in ["barrio", "suburbio", "ciudad_geopy", "estado", "region", "codigo_postal", "direccion_formateada", "geosource"]:
    if col not in df.columns:
        df[col] = ""

# Filtrar hoteles en Argentina con ciudad_geopy vacía
df_arg = df[(df["pais"].str.lower() == "argentina") & (df["ciudad_geopy"].isna() | df["ciudad_geopy"].eq(""))]

# Iterar por filas
for idx in df_arg.index:
    row = df.loc[idx]
    lat, lon = row["latitud"], row["longitud"]

    if pd.isna(lat) or pd.isna(lon):
        continue

    try:
        url = (
            f"https://api.tomtom.com/search/2/reverseGeocode/{lat},{lon}.json"
            f"?key={TOMTOM_API_KEY}&language=es-ES"
        )
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            address = data.get("addresses", [{}])[0].get("address", {})

            df.at[idx, "barrio"] = address.get("municipalitySubdivision", "")
            df.at[idx, "suburbio"] = ""  # No hay campo específico en TomTom
            df.at[idx, "ciudad_geopy"] = address.get("municipality", "")
            df.at[idx, "estado"] = address.get("countrySubdivision", "")
            df.at[idx, "region"] = address.get("countrySecondarySubdivision", "")
            df.at[idx, "codigo_postal"] = address.get("postalCode", "")
            df.at[idx, "direccion_formateada"] = address.get("freeformAddress", "")
            df.at[idx, "geosource"] = "tomtom"
            print(f"✅ Fila {idx} actualizada con TomTom")
        else:
            print(f"❌ Error TomTom fila {idx}: {response.status_code}")
    except Exception as e:
        print(f"💥 Excepción en fila {idx}: {e}")

    # Esperar 1.1 segundos por política de rate limit
    time.sleep(1.1)

    if idx % 10 == 0:
        df.to_csv("hoteles.csv", index=False)
        print(f"💾 Progreso guardado en fila {idx}")

# Guardado final
df.to_csv("hoteles.csv", index=False)
print("🏁 Proceso con TomTom para Argentina finalizado.")
