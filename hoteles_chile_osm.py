import requests
import pandas as pd
import time

# Consulta Overpass API para hoteles en Chile
query = """
[out:json][timeout:60];
area["name"="Chile"]->.searchArea;
(
  node["tourism"="hotel"](area.searchArea);
  way["tourism"="hotel"](area.searchArea);
  relation["tourism"="hotel"](area.searchArea);
);
out center;
"""

# Hacer la petición a Overpass
print("🔄 Consultando Overpass API...")
url = "https://overpass-api.de/api/interpreter"
response = requests.post(url, data={'data': query})

if response.status_code != 200:
    print("❌ Error al consultar Overpass API:", response.status_code)
    exit()

data = response.json()

# Parsear los datos recibidos
hoteles = []
for element in data['elements']:
    tags = element.get('tags', {})
    nombre = tags.get('name')
    lat = element.get('lat') or element.get('center', {}).get('lat')
    lon = element.get('lon') or element.get('center', {}).get('lon')
    ciudad = tags.get('addr:city')
    url_web = tags.get('website')

    if nombre:
        hoteles.append({
            'id_hotel': element['id'],
            'nombre': nombre,
            'url_principal': url_web,
            'ciudad': ciudad,
            'pais': 'Chile',
            'latitud': lat,
            'longitud': lon
        })

# Exportar a CSV
df = pd.DataFrame(hoteles)
df.to_csv("hoteles_chile_osm.csv", index=False)

print(f"✅ {len(df)} hoteles guardados en hoteles_chile_osm.csv")
