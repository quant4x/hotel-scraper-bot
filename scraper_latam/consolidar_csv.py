import pandas as pd
import glob
import os

# Ruta a los archivos CSV
csv_files = glob.glob("hoteles_latam/*.csv")

dataframes = []

for archivo in csv_files:
    # Extraer el nombre del país desde el nombre del archivo, sin extensión
    pais = os.path.splitext(os.path.basename(archivo))[0]
    
    try:
        df = pd.read_csv(archivo)
        df["pais"] = pais  # Agregar columna país
        dataframes.append(df)
    except Exception as e:
        print(f"⚠️ Error leyendo {archivo}: {e}")

# Consolidar todos los dataframes
df_consolidado = pd.concat(dataframes, ignore_index=True)

# Guardar el resultado
df_consolidado.to_csv("hoteles_consolidado.csv", index=False)
print("✅ Consolidado guardado como 'hoteles_consolidado.csv'")
