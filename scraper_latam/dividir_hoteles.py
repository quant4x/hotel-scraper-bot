import pandas as pd

# Cargar consolidado
df = pd.read_csv('hoteles_latam/hoteles_consolidado.csv')

# Asegurar que columnas de URL existen
url_cols = ['url_principal'] + [f'url_{i}' for i in range(2, 11)]
for col in url_cols:
    if col not in df.columns:
        df[col] = None

# Filtrar hoteles sin ninguna URL
df_sin_urls = df[df[url_cols].isnull().all(axis=1)].copy()

# Dividir en dos mitades
mitad = len(df_sin_urls) // 2
df_duck = df_sin_urls.iloc[:mitad]
df_google = df_sin_urls.iloc[mitad:]

# Guardar resultados
df_duck.to_csv('hoteles_para_duck.csv', index=False)
df_google.to_csv('hoteles_para_google.csv', index=False)

print(f'✅ División completada: {len(df_duck)} hoteles para DuckDuckGo, {len(df_google)} para Google.')
