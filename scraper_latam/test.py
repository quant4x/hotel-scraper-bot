import requests

url = "https://google.serper.dev/search"
headers = {
    "X-API-KEY": "46e1661ae76b5da648b374f11f287e6d55fa4ff5",
    "Content-Type": "application/json"
}
payload = {"q": "Hotel Sheraton Santiago Chile"}

response = requests.post(url, headers=headers, json=payload)
print(response.status_code)
print(response.text)