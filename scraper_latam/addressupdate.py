import pandas as pd

# Cargar los archivos
lodging_df = pd.read_csv('lodging_address.csv')
hoteles_df = pd.read_csv('hoteles.csv')

# Limpiar el id en lodging_df (remover 'node:') y asegurar que ambos ids sean string
lodging_df['hotel_id_clean'] = lodging_df['lodging_id'].astype(str).str.replace('node:', '', regex=False)
hoteles_df['id_hotel'] = hoteles_df['id_hotel'].astype(str)

# Realizar el merge usando el id limpio y los nombres de columna correctos
merged_df = lodging_df.merge(
    hoteles_df[['id_hotel', 'ciudad_geopy']],
    left_on='hotel_id_clean',
    right_on='id_hotel',
    how='left'
)

# Contar cuántas direcciones se actualizarán (donde 'ciudad_geopy' no es nulo)
actualizadas = merged_df['ciudad_geopy'].notna().sum()

# Actualizar la columna 'city' con el valor de 'ciudad_geopy'
merged_df['city'] = merged_df['ciudad_geopy']

# Eliminar columnas auxiliares
merged_df = merged_df.drop(columns=['hotel_id_clean', 'ciudad_geopy', 'id_hotel'])

# Sobrescribir el archivo original
merged_df.to_csv('lodging_address.csv', index=False)

print(f"Archivo 'lodging_address.csv' actualizado correctamente.")
print(f"Direcciones actualizadas: {actualizadas}")