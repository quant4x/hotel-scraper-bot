import pandas as pd

df = pd.read_csv('lodging_urls.csv')

# If 'node' column does not exist, create it as empty string
if 'node' not in df.columns:
    df['node'] = ''

# Create the new column merging 'id' and 'node' with an underscore
df['id_node'] = df['id'].astype(str) + '_' + df['node'].astype(str)

df.to_csv('lodging_urls.csv', index=False)
print("Nueva columna 'id_node' creada en lodging_urls.csv.")