import requests
import csv
import time

# Países a consultar
COUNTRIES = [
    "México", "Brasil", "Chile", "Perú", "Colombia", "Costa Rica", "Argentina"
]

# Tipos válidos (sin hotel ni museum)
ALLOWED_TYPES = [
    "guest_house", "hostel", "motel", "apartment", "chalet", "resort",
    "camp_site", "caravan_site", "alpine_hut", "wilderness_hut"
]

def build_query(country_name):
    regex = "|".join(ALLOWED_TYPES)
    return f"""
    [out:json][timeout:180];
    area["name"="{country_name}"][admin_level=2]->.searchArea;
    (
      node["tourism"~"^{regex}$"](area.searchArea);
      way["tourism"~"^{regex}$"](area.searchArea);
      relation["tourism"~"^{regex}$"](area.searchArea);
    );
    out center;
    """

def run_query(country):
    print(f"🔍 Consultando alojamientos en {country}...")
    query = build_query(country)
    url = "https://overpass-api.de/api/interpreter"
    response = requests.post(url, data={"data": query})

    if response.status_code == 200:
        data = response.json()
        print(f"✅ {len(data['elements'])} elementos encontrados en {country}.")
        return data["elements"]
    else:
        print(f"❌ Error {response.status_code} en {country}: {response.text}")
        return []

def extract_info(element, country):
    tags = element.get("tags", {})
    base = {
        "id": element.get("id"),
        "type": element.get("type"),
        "lat": element.get("lat") or element.get("center", {}).get("lat"),
        "lon": element.get("lon") or element.get("center", {}).get("lon"),
        "country": country
    }
    return {**base, **tags}

if __name__ == "__main__":
    all_rows = []
    all_keys = set(["id", "type", "lat", "lon", "country"])

    for country in COUNTRIES:
        elements = run_query(country)
        for el in elements:
            row = extract_info(el, country)
            all_rows.append(row)
            all_keys.update(row.keys())
        time.sleep(10)

    # Ordenar columnas con base primero
    ordered_keys = ["id", "type", "lat", "lon", "country"] + sorted(k for k in all_keys if k not in {"id", "type", "lat", "lon", "country"})

    # Guardar en CSV
    with open("alojamientos_turisticos_latam_completo.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ordered_keys)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"\n📦 Archivo generado con {len(all_rows)} registros y {len(ordered_keys)} columnas.")
