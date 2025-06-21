from crawl4ai import crawl_website

hotel = {
    "id": 252528804,
    "nombre": "Le Meridien Hotel",
    "url": "https://www.marriott.com/es/hotels/sclhp-le-meridien-santiago/overview/"
}

output_path = f"{hotel['id']}_{hotel['nombre'].replace(' ', '_')}.json"

print(f"🔍 Procesando: {hotel['nombre']} ({hotel['url']})")
result = crawl_website(hotel['url'], language="es", max_depth=2)

with open(output_path, "w", encoding="utf-8") as f:
    f.write(result.model_dump_json(indent=2, ensure_ascii=False))

print(f"✅ Guardado en {output_path}")
