import pandas as pd

df = pd.read_csv('lodging_urls.csv')

# Si la columna no existe, la crea como string vacía
if 'instagram' not in df.columns:
    df['instagram'] = ''

# Convertir a string y reemplazar 'nan' y NaN por cadena vacía
df['instagram'] = df['instagram'].astype(str).replace(['nan', 'NaN'], '')

df.to_csv('lodging_urls.csv', index=False)
print("Columna 'instagram' convertida a string y limpiada de valores nulos.")