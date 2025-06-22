import pandas as pd

# Cargar el CSV
df = pd.read_csv("backup_hoteles_20250621_2107.csv")

# Columnas a analizar
columnas = ["nombre", "region", "estado", "ciudad_geopy", "ciudad", "barrio", "suburbio"]

# Agrupar por país
resultados = []

for pais, grupo in df.groupby("pais"):
    total = len(grupo)
    fila = {"pais": pais}
    for col in columnas:
        porcentaje = grupo[col].notnull().sum() / total * 100
        fila[col] = round(porcentaje, 2)
    resultados.append(fila)

# Crear DataFrame ordenado por país
df_resultado = pd.DataFrame(resultados).sort_values("pais")

# Mostrar resultado
print("📊 Porcentaje de campos no nulos por país:")
print(df_resultado.to_string(index=False))
