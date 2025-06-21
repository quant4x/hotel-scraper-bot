import pandas as pd
import os

# Archivos
archivo_consolidado = 'hoteles_latam/hoteles_consolidado.csv'
archivo_urls = 'hoteles_latam/hoteles_con_url_actualizado.csv'

# Validación de existencia de archivos
if not os.path.exists(archivo_consolidado) or not os.path.exists(archivo_urls):
    print("❌ Uno o ambos archivos no existen. Verifica las rutas.")
    exit()

# Cargar archivos
consolidado = pd.read_csv(archivo_consolidado)
actualizados = pd.read_csv(archivo_urls)

# Verificación de columnas necesarias
if 'id_hotel' not in actualizados.columns or 'url_principal' not in actualizados.columns:
    print("❌ El archivo de URLs no tiene las columnas esperadas ('id_hotel', 'url_principal').")
    exit()

# Contador de cambios
actualizados_count = 0

# Recorremos cada fila con URL encontrada
for _, row in actualizados.iterrows():
    id_hotel = row['id_hotel']
    nueva_url = row['url_principal']

    # Buscar la fila correspondiente en el consolidado
    match = consolidado[consolidado['id_hotel'] == id_hotel]

    if not match.empty:
        idx = match.index[0]
        if pd.isna(consolidado.at[idx, 'url_principal']) or consolidado.at[idx, 'url_principal'] == "":
            consolidado.at[idx, 'url_principal'] = nueva_url
            actualizados_count += 1
            print(f"✅ Actualizado hotel {id_hotel} con URL: {nueva_url}")

# Guardar cambios
consolidado.to_csv(archivo_consolidado, index=False)
print(f"\n🎉 Actualización completada: {actualizados_count} URLs incorporadas en 'url_principal'.")
