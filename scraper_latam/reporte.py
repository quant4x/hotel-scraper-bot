import pandas as pd
from pathlib import Path

input_dir = Path("hoteles_latam")
archivos = list(input_dir.glob("*.csv"))

resumen = []

for archivo in archivos:
    try:
        df = pd.read_csv(archivo)
    except pd.errors.EmptyDataError:
        print(f"⚠️ Archivo vacío: {archivo.name}. Saltando...")
        resumen.append({
            "pais": archivo.stem,
            "total_hoteles": 0,
            "con_nombre": 0,
            "con_url": 0,
            "con_ciudad": 0
        })
        continue

    pais = archivo.stem

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

df_resumen = pd.DataFrame(resumen)
df_resumen["porcentaje_nombre"] = (df_resumen["con_nombre"] / df_resumen["total_hoteles"]).round(2).fillna(0)
df_resumen["porcentaje_url"] = (df_resumen["con_url"] / df_resumen["total_hoteles"]).round(2).fillna(0)
df_resumen["porcentaje_ciudad"] = (df_resumen["con_ciudad"] / df_resumen["total_hoteles"]).round(2).fillna(0)

df_resumen.to_csv("reporte_completitud_latam.csv", index=False)

print("✅ Reporte generado correctamente: reporte_completitud_latam.csv")
