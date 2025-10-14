import requests
import time
from datetime import datetime, timedelta

url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries"

headers = {
    "X-Nickname": "Vlad",
    "X-Cohort": "41",
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
}

params = {
    "sort_field": "date",
    "sort_direction": "asc",
    "limit": 50,
    "offset": 0,
    "from": '2025-10-07 00:00:00',
    "to": '2025-10-14 00:00:00'
}

start_date = datetime.now().strftime("%Y-%m-%d") + ' 00:00:00'
end_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d") + ' 00:00:00'

# all_responses = []

# while True:
#     response = requests.get(url, headers=headers, params=params)
    
#     if response.status_code != 200:
#         print("Failed request")
#         break
    
#     response_json = response.json()
#     if len(response_json) == 0:
#         print("Finished loading")
#         break
    
#     print(f"Loaded {len(response_json)} records")
#     all_responses += response_json
#     params["offset"] += len(response_json)

# print(f"{len(all_responses)} records loaded")
# print(all_responses[:10])
