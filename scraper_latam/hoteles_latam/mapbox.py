import pandas as pd
import requests
import json
import time
from dotenv import load_dotenv
import os
from tqdm import tqdm

# Cargar API key desde .env
load_dotenv()
MAPBOX_KEY = os.getenv("MAPBOX_KEY")

# Leer CSV
df = pd.read_csv("hoteles.csv")

# Filtrar solo Colombia
df_colombia = df[df["pais"].str.lower() == "colombia"].copy()
print(f"🔍 Total de hoteles en Colombia: {len(df_colombia)}")

# Cargar IDs ya procesados (si existe archivo)
output_file = "mapbox_output_colombia.jsonl"
existing_ids = set()
if os.path.exists(output_file):
    with open(output_file, "r") as f:
        for line in f:
            try:
                data = json.loads(line)
                existing_ids.add(data["id_hotel"])
            except:
                continue
    print(f"🧠 {len(existing_ids)} hoteles ya procesados previamente.")

# Procesar
with open(output_file, "a") as f:
    for _, row in tqdm(df_colombia.iterrows(), total=len(df_colombia), desc="📡 Procesando"):
        id_hotel = int(row["id_hotel"])
        if id_hotel in existing_ids:
            continue

        lat = row["latitud"]
        lon = row["longitud"]

        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json?access_token={MAPBOX_KEY}&language=es"
        try:
            response = requests.get(url)
            response.raise_for_status()
            result = response.json()
        except Exception as e:
            print(f"❌ Error con hotel {id_hotel}: {e}")
            continue

        output = {
            "id_hotel": id_hotel,
            "latitud": lat,
            "longitud": lon,
            "respuesta_mapbox": result
        }

        f.write(json.dumps(output, ensure_ascii=False) + "\n")
        time.sleep(0.3)  # evitar bloqueo de rate limit

print("✅ Extracción finalizada para Colombia.")
