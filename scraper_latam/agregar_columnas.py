import pandas as pd

# Ruta del archivo consolidado
archivo = 'hoteles_latam/hoteles_consolidado.csv'

# Cargar CSV
df = pd.read_csv(archivo)

# Crear columnas url_2 a url_10 si no existen
for i in range(2, 11):
    col = f"url_{i}"
    if col not in df.columns:
        df[col] = ""

# Guardar de nuevo el archivo
df.to_csv(archivo, index=False)
print("✅ Columnas url_2 a url_10 agregadas correctamente.")
