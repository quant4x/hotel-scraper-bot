import pandas as pd

hoteles_df = pd.read_csv('hoteles.csv')
lodging_contact_df = pd.read_csv('lodging_urls.csv')

# Contar registros con datos en 'url_1'
con_url_1 = hoteles_df['url_1'].notna() & (hoteles_df['url_1'] != '')
print(f"Registros en 'hoteles.csv' con datos en 'url_1': {con_url_1.sum()}")

# Contar registros con datos en 'website'
con_website = lodging_contact_df['website'].notna() & (lodging_contact_df['website'] != '')
print(f"Registros en 'lodging_urls.csv' con datos en 'website': {con_website.sum()}")

import pandas as pd

# Cargar los archivos
urls_df = pd.read_csv('lodging_urls.csv')
hoteles_df = pd.read_csv('hoteles.csv')

# Asegurar que los IDs sean del mismo tipo
urls_df['id'] = urls_df['id'].astype(str)
hoteles_df['id_hotel'] = hoteles_df['id_hotel'].astype(str)

# Hacer merge para traer 'url_1' de hoteles
merged_df = urls_df.merge(
    hoteles_df[['id_hotel', 'url_1']],
    left_on='id',
    right_on='id_hotel',
    how='left'
)

# Solo actualizar 'website' si está vacío o nulo y 'url_1' tiene valor
mask = merged_df['website'].isna() | (merged_df['website'] == '')
actualizadas = mask & merged_df['url_1'].notna() & (merged_df['url_1'] != '')
merged_df.loc[actualizadas, 'website'] = merged_df.loc[actualizadas, 'url_1']

# Contar cuántos se actualizaron
count_actualizadas = actualizadas.sum()

# Eliminar columnas auxiliares
merged_df = merged_df.drop(columns=['id_hotel', 'url_1'])

# Sobrescribir el archivo original
merged_df.to_csv('lodging_urls.csv', index=False)

print(f"Archivo 'lodging_urls.csv' actualizado correctamente.")
print(f"Websites actualizados: {count_actualizadas}")