EXPECTED_NUM_COMMAS = 9
with open('lodging_urls.csv') as f:
    for i, line in enumerate(f, 1):
        if line.count(',') != EXPECTED_NUM_COMMAS:
            print(f"Línea {i} tiene {line.count(',')+1} columnas")