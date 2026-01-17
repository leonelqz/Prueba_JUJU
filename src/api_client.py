import json


## Simula response del API
def get_api_data() -> dict:
    with open("sample_data/api_orders.json") as f:
        api_orders = json.load(f)
    return api_orders