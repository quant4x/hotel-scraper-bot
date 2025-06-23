import pandas as pd

# Lista de archivos a consolidar
archivos = [
    'lodging_info.csv',
    'lodging_info_backup.csv',
    'lodging_info_backup2.csv',
    'lodging_info_backup3.csv',
    'lodging_info_backup4.csv',
    'lodging_info_backup5.csv',
    'lodging_info_backup6.csv',
    'lodging_info_7.csv'
]

# Leer y concatenar todos los archivos
dfs = [pd.read_csv(archivo) for archivo in archivos]
df_total = pd.concat(dfs, ignore_index=True)

# Contar duplicados exactos (todas las columnas)
duplicados = df_total.duplicated(keep='first').sum()

# Eliminar duplicados exactos, dejando solo la primera ocurrencia
df_total = df_total.drop_duplicates(keep='first')

# Guardar el archivo consolidado
df_total.to_csv('lodging_info_consolidado.csv', index=False)

print(f"Consolidación completa. Registros omitidos por duplicados exactos: {duplicados}")
print(f"Total de registros en el archivo consolidado: {len(df_total)}")