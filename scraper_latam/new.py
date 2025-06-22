import requests
import csv
import time

COUNTRIES = ["México", "Brasil", "Chile", "Perú", "Colombia", "Costa Rica", "Argentina"]

TOURISM_TYPES = [
    "hotel", "guest_house", "hostel", "motel", "apartment", "chalet", "resort",
    "camp_site", "caravan_site", "alpine_hut", "wilderness_hut"
]

def build_query(country_name):
    regex = "|".join(TOURISM_TYPES)
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

# Inicialización de datasets
lodgings, contacts, addresses, services, urls = [], [], [], [], []

def normalize(element, country):
    tags = element.get("tags", {})
    base_id = f"{element['type']}:{element['id']}"
    lat = element.get("lat") or element.get("center", {}).get("lat")
    lon = element.get("lon") or element.get("center", {}).get("lon")

    # Tabla principal
    lodgings.append({
        "id": base_id,
        "osm_type": element["type"],
        "lat": lat,
        "lon": lon,
        "country": country,
        "name": tags.get("name"),
        "tourism": tags.get("tourism"),
        "operator": tags.get("operator"),
        "brand": tags.get("brand"),
        "stars": tags.get("stars"),
        "reservation": tags.get("reservation")
    })

    # Contacto
    contacts.append({
        "lodging_id": base_id,
        "phone": tags.get("phone") or tags.get("contact:phone"),
        "email": tags.get("email") or tags.get("contact:email"),
        "facebook": tags.get("contact:facebook"),
        "instagram": tags.get("contact:instagram"),
        "whatsapp": tags.get("contact:whatsapp")
    })

    # Dirección
    addresses.append({
        "lodging_id": base_id,
        "street": tags.get("addr:street"),
        "housenumber": tags.get("addr:housenumber"),
        "city": tags.get("addr:city") or tags.get("addr:municipality"),
        "state": tags.get("addr:state") or tags.get("addr:province"),
        "postcode": tags.get("addr:postcode"),
        "full_address": tags.get("addr:full")
    })

    # Servicios
    services.append({
        "lodging_id": base_id,
        "internet_access": tags.get("internet_access"),
        "air_conditioning": tags.get("air_conditioning"),
        "hot_water": tags.get("hot_water"),
        "swimming_pool": tags.get("swimming_pool"),
        "laundry": tags.get("laundry"),
        "kitchen": tags.get("kitchen"),
        "breakfast": tags.get("breakfast"),
        "toilets": tags.get("toilets"),
        "wheelchair": tags.get("wheelchair"),
        "check_in": tags.get("check_in"),
        "check_out": tags.get("check_out"),
        "rooms": tags.get("rooms"),
        "beds": tags.get("beds")
    })

    # URLs externas
    urls.append({
        "lodging_id": base_id,
        "website": tags.get("website") or tags.get("contact:website"),
        "booking": tags.get("website:booking"),
        "tripadvisor": tags.get("contact:tripadvisor"),
        "expedia": tags.get("website:expedia"),
        "google": tags.get("contact:google_maps") or tags.get("contact:google")
    })

def save_csv(filename, rows, headers):
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    for country in COUNTRIES:
        elements = run_query(country)
        for el in elements:
            normalize(el, country)
        time.sleep(10)

    print("\n💾 Guardando archivos...")

    save_csv("lodgings.csv", lodgings, lodgings[0].keys())
    save_csv("lodging_contact.csv", contacts, contacts[0].keys())
    save_csv("lodging_address.csv", addresses, addresses[0].keys())
    save_csv("lodging_services.csv", services, services[0].keys())
    save_csv("lodging_urls.csv", urls, urls[0].keys())

    print("📦 Archivos listos para importar a Supabase.")
