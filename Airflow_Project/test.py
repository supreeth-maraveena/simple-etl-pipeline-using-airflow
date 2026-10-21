import requests

response = requests.get("http://localhost:8000/fetch_data")
print(response.json())