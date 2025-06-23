import pandas as pd
df = pd.read_csv('lodging_urls.csv')
df = df.drop_duplicates(subset=['id'], keep='first')
df.to_csv('lodging_urls.csv', index=False)
print("Duplicados eliminados de lodging_urls.csv")