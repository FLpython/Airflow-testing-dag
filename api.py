import requests

amount = 100
api = 'https://v6.exchangerate-api.com/v6/7dbce18c99cd7ea949727872/pair/USD/BYN/'
url = api + str(amount)
print([url])
responce = requests.get(url)
c = responce.json()
print(c['conversion_result'])