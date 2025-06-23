import pandas as pd

df = pd.read_csv('lodging_info.csv')

# Filtrar solo los de dominio instagram.com
insta = df[df['domain'] == 'instagram.com']

# Contar repeticiones por id_lodging
repetidos = insta.groupby('id_lodging').size()
repetidos = repetidos[repetidos > 1]

print(f"Cantidad de id_lodging con más de una URL de instagram.com: {len(repetidos)}")