import pandas as pd
from urllib.parse import urlparse

# Cargar archivo maestro
df = pd.read_csv("hoteles.csv")

# Eliminar columnas dominio_x si ya existen
for i in range(1, 11):
    dominio_col = f"dominio_{i}"
    if dominio_col in df.columns:
        df.drop(columns=[dominio_col], inplace=True)

# Iterar sobre columnas url_1 a url_10 y agregar dominios
for i in range(1, 11):
    url_col = f"url_{i}"
    dominio_col = f"dominio_{i}"

    if url_col in df.columns:
        # Extraer dominio
        df[dominio_col] = df[url_col].apply(lambda x: urlparse(x).netloc if isinstance(x, str) and x.startswith("http") else "")

        # Mover dominio_col justo después de url_col
        url_idx = df.columns.get_loc(url_col)
        cols = df.columns.tolist()
        # Eliminar primero dominio_col (lo insertamos de nuevo)
        cols.remove(dominio_col)
        cols.insert(url_idx + 1, dominio_col)
        df = df[cols]

# Guardar archivo actualizado
df.to_csv("hoteles.csv", index=False)
print("✅ Dominios insertados correctamente al lado de cada URL.")
