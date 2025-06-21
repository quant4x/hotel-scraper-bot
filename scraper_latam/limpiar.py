from pathlib import Path
import pandas as pd

carpeta = Path("hoteles_latam")
archivos = list(carpeta.glob("*.csv"))
eliminados = []

for archivo in archivos:
    try:
        df = pd.read_csv(archivo)
        if df.empty:
            archivo.unlink()
            eliminados.append(archivo.name)
    except pd.errors.EmptyDataError:
        archivo.unlink()
        eliminados.append(archivo.name)

if eliminados:
    print("🗑️ Archivos eliminados por estar vacíos:")
    for nombre in eliminados:
        print(f" - {nombre}")
else:
    print("✅ No se encontraron archivos vacíos.")
