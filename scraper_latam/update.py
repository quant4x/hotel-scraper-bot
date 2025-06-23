import pandas as pd

# Load data
info_df = pd.read_csv('lodging_info_consolidado.csv')
urls_df = pd.read_csv('lodging_urls.csv')

# Ensure IDs are strings for matching
info_df['id_lodging'] = info_df['id_lodging'].astype(str).str.strip()
urls_df['id'] = urls_df['id'].astype(str).str.strip()

# Domain to column mapping
domain_map = {
    'trivago.com': 'trivago',
    'hotels.com': 'hotels',
    'facebook.com': 'facebook',
    'expedia.mx': 'expedia',
    'expedia.com': 'expedia',
    'tripadvisor.com': 'tripadvisor',
    'tripadvisor.es': 'tripadvisor',
    'agoda.com': 'agoda',
    'airbnb.com': 'airbnb',
    'airbnb.es': 'airbnb'
}

# Set id as index for fast lookup
urls_df = urls_df.set_index('id')

# For each domain, insert only one url per id_lodging if empty in lodging_urls
inserted = 0
for domain, col in domain_map.items():
    # Filter info_df for this domain
    domain_rows = info_df[info_df['domain'] == domain]
    # Keep only the first URL per id_lodging
    domain_unique = domain_rows.drop_duplicates(subset=['id_lodging'], keep='first')[['id_lodging', 'url']]
    for _, row in domain_unique.iterrows():
        id_lodging = row['id_lodging']
        if id_lodging in urls_df.index:
            current_value = urls_df.at[id_lodging, col] if col in urls_df.columns else None
            if pd.isna(current_value) or current_value == '':
                urls_df.at[id_lodging, col] = row['url']
                inserted += 1

urls_df = urls_df.reset_index()
urls_df.to_csv('lodging_urls.csv', index=False)
print(f"Inserted {inserted} URLs from lodging_info_consolidado.csv into lodging_urls.csv according  to domain mapping.")