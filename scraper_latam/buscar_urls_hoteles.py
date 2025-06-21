from datetime import datetime
import pandas as pd
import time
from duckduckgo_search import DDGS

# Cargar el archivo consolidado de hoteles
archivo_consolidado = "hoteles_latam/hoteles_consolidado.csv"
df = pd.read_csv(archivo_consolidado)

# Filtrar solo los hoteles sin URL
df_sin_url = df[df["url_principal"].isna()].copy()
df_sin_url["url_principal"] = ""

# Reanudar desde el último hotel con URL completada si hay un archivo previo
archivo_resultado = "hoteles_latam/hoteles_con_url_actualizado.csv"
try:
    df_existente = pd.read_csv(archivo_resultado)
    hoteles_procesados = set(df_existente["id_hotel"])
    df_sin_url = df_sin_url[~df_sin_url["id_hotel"].isin(hoteles_procesados)]
except FileNotFoundError:
    df_existente = pd.DataFrame()

ddgs = DDGS()
resultados = []

print(f"🔎 Procesando {len(df_sin_url)} hoteles sin URL...")

for index, row in df_sin_url.iterrows():
    query = f"{row['nombre']}, {row['ciudad'] or ''}, {row['pais']}"
    print(f"🔍 Buscando: {query.strip()}")

    try:
        resultados_busqueda = ddgs.text(query.strip(), max_results=3)
        url_encontrada = None
        for r in resultados_busqueda:
            if any(dominio in r["href"] for dominio in ["hotel", "resort", "inn", "hostal", "lodging", "motel"]):
                url_encontrada = r["href"]
                break
        if url_encontrada:
            print(f"✅ Encontrado: {url_encontrada}")
            row["url_principal"] = url_encontrada
        else:
            print("❌ No encontrada")

    except Exception as e:
        print(f"⚠️ Error en {row['nombre']}: {e}")
        row["url_principal"] = ""

    resultados.append(row)
    time.sleep(5)  # Espera de 5 segundos para evitar bloqueos

    # Guardar backup progresivo cada 10 hoteles
    if len(resultados) % 10 == 0:
        df_parcial = pd.DataFrame(resultados)
        df_actualizado = pd.concat([df_existente, df_parcial], ignore_index=True)
        df_actualizado.to_csv(archivo_resultado, index=False)
        print(f"💾 Progreso guardado: {len(resultados)} hoteles actualizados")

# Guardado final
df_final = pd.DataFrame(resultados)
df_actualizado = pd.concat([df_existente, df_final], ignore_index=True)
df_actualizado.to_csv(archivo_resultado, index=False)
print("✅ Proceso completo. Archivo actualizado guardado.")
