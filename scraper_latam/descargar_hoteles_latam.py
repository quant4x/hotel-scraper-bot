import requests
import pandas as pd
import time
from pathlib import Path

# Lista completa de países latinoamericanos
PAISES_LATAM = [
    "Argentina", "Bolivia", "Brasil", "Chile", "Colombia", "Costa Rica", "Cuba",
    "Ecuador", "El Salvador", "Guatemala", "Honduras", "México", "Nicaragua",
    "Panamá", "Paraguay", "Perú", "República Dominicana", "Uruguay", "Venezuela"
]

# Crear carpeta de salida
output_dir = Path("hoteles_latam")
output_dir.mkdir(exist_ok=True)

# Función para obtener hoteles desde Overpass
def obtener_hoteles_por_pais(pais):
    query = f"""
    [out:json][timeout:90];
    area["name"="{pais}"]->.searchArea;
    (
      node["tourism"="hotel"](area.searchArea);
    );
    out center;
    """
    url = "https://overpass-api.de/api/interpreter"
    try:
        response = requests.post(url, data={"data": query}, timeout=120)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error con {pais}: {e}")
        return None

# Procesar países
resumen = []

for pais in PAISES_LATAM:
    archivo_salida = output_dir / f"{pais}.csv"

    if archivo_salida.exists():
        print(f"⏭️  {pais} ya procesado. Saltando...")
        continue

    print(f"🌎 Procesando {pais}...")
    datos = obtener_hoteles_por_pais(pais)
    time.sleep(60)

    if not datos:
        resumen.append({
            "pais": pais,
            "total_hoteles": 0,
            "con_nombre": 0,
            "con_url": 0,
            "con_ciudad": 0
        })
        continue

    hoteles = []
    for e in datos.get('elements', []):
        tags = e.get('tags', {})
        lat = e.get('lat') or e.get('center', {}).get('lat')
        lon = e.get('lon') or e.get('center', {}).get('lon')
        hoteles.append({
            "id_hotel": e['id'],
            "nombre": tags.get("name"),
            "url_principal": tags.get("website"),
            "ciudad": tags.get("addr:city"),
            "pais": pais,
            "latitud": lat,
            "longitud": lon
        })

    df = pd.DataFrame(hoteles)
    df.to_csv(archivo_salida, index=False)

    if len(df) == 0 or 'nombre' not in df.columns:
        resumen.append({
            "pais": pais,
            "total_hoteles": 0,
            "con_nombre": 0,
            "con_url": 0,
            "con_ciudad": 0
        })
    else:
        resumen.append({
            "pais": pais,
            "total_hoteles": len(df),
            "con_nombre": df['nombre'].notna().sum(),
            "con_url": df['url_principal'].notna().sum(),
            "con_ciudad": df['ciudad'].notna().sum()
        })

# === Reporte final ===
df_resumen = pd.DataFrame(resumen)
df_resumen["porcentaje_nombre"] = (df_resumen["con_nombre"] / df_resumen["total_hoteles"]).round(2).fillna(0)
df_resumen["porcentaje_url"] = (df_resumen["con_url"] / df_resumen["total_hoteles"]).round(2).fillna(0)
df_resumen["porcentaje_ciudad"] = (df_resumen["con_ciudad"] / df_resumen["total_hoteles"]).round(2).fillna(0)

df_resumen.to_csv("reporte_completitud_latam.csv", index=False)

print("\n✅ Proceso completo. Revisa:")
print("📁 Carpeta: hoteles_latam/")
print("📄 Reporte: reporte_completitud_latam.csv")
