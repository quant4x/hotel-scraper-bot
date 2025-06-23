import pandas as pd

lodgings_df = pd.read_csv('lodgings.csv')
total_hotels = (lodgings_df['tourism'] == 'hotel').sum()
print(f"Total registros tourism = hotel: {total_hotels}")
