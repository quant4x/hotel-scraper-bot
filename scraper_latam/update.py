with open('lodging_urls.csv', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('"', '')

with open('lodging_urls.csv', 'w', encoding='utf-8') as f:
    f.write(content)

print('Todas las comillas dobles han sido eliminadas de lodging_urls.csv.')