import csv
import json

# Rutas de archivo
csv_path = 'lodging_address.csv'
json_path = 'mapbox_raw_chile.jsonl'

# Cargar datos JSON
with open(json_path, 'r', encoding='utf-8') as f:
    enriched_data = [json.loads(line) for line in f]

# Crear un diccionario de acceso rápido por id_hotel
mapbox_lookup = {entry["id_hotel"]: entry for entry in enriched_data}

# Leer CSV
with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    fieldnames = reader.fieldnames

# Campos a actualizar si están vacíos
update_fields = ['street', 'housenumber', 'city', 'state', 'region', 'postcode', 'full_address']

# Contador de registros actualizados
updated_count = 0

# Procesar filas
for row in rows:
    hotel_id = row.get('id')
    if hotel_id in mapbox_lookup:
        entry = mapbox_lookup[hotel_id]
        feature = entry.get('features', [{}])[0]
        context = {c['id'].split('.')[0]: c['text'] for c in feature.get('context', [])} if 'context' in feature else {}
        updated = False
        # Actualizar solo campos vacíos
        for field in update_fields:
            if (row.get(field, '') == '') and (field in feature.get('properties', {}) or field in context or field in feature):
                # Buscar el valor adecuado
                if field in feature.get('properties', {}):
                    value = feature['properties'][field]
                elif field in context:
                    value = context[field]
                elif field in feature:
                    value = feature[field]
                else:
                    value = ''
                if value:
                    row[field] = value
                    updated = True
        if updated:
            updated_count += 1

# Escribir el CSV actualizado (sobrescribiendo el original)
with open(csv_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print("✅ Archivo actualizado con datos de Mapbox")
print(f"Registros actualizados: {updated_count}")

