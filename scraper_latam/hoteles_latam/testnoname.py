import requests

def buscar_hotel_osm(lat, lon, radius=100):
    query = f"""
    [out:json];
    (
      node["tourism"="hotel"](around:{radius},{lat},{lon});
      way["tourism"="hotel"](around:{radius},{lat},{lon});
      relation["tourism"="hotel"](around:{radius},{lat},{lon});
    );
    out center;
    """
    url = "https://overpass-api.de/api/interpreter"
    response = requests.post(url, data={"data": query})

    if response.status_code == 200:
        data = response.json()
        if data["elements"]:
            for e in data["elements"]:
                tags = e.get("tags", {})
                print("✅ Hotel detectado:")
                print(f" - Nombre: {tags.get('name', 'Desconocido')}")
                print(f" - Tipo: {tags.get('tourism', 'Desconocido')}")
                print(f" - Amenidades: {tags}")
        else:
            print("❌ No se encontró ningún hotel en el área.")
    else:
        print(f"Error {response.status_code}: {response.text}")

# Coordenadas del hotel en Costa Rica
lat = 9.0750025
lon = -83.6541234

buscar_hotel_osm(lat, lon)
