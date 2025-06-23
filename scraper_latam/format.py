import pandas as pd
import re

df = pd.read_csv('lodgings.csv')

# Limpiar la columna 'name' dejando solo un espacio entre palabras
df['name'] = df['name'].astype(str).apply(lambda x: re.sub(r'\s+', ' ', x).strip())

df.to_csv('lodgings.csv', index=False)
print("Columna 'name' limpiada correctamente.")

print("\n⚠️ Si ejecutas este script mientras otro proceso está leyendo o escribiendo 'lodgings.csv', podrías causar errores o corrupción de datos. Es recomendable ejecutar este tipo de cambios solo cuando ningún otro proceso esté usando el archivo.")