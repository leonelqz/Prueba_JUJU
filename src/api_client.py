import json, os, sys, pendulum
from src.retries_config import backoff


## Obtengo estado actual del job
def get_current_state():
    if "state.json" in os.listdir():    # En este caso se evalua la existencia de state.json en el repo (en prod en un bucket)
        with open("state.json", encoding="utf-8") as f:
            current_state =json.load(f)
            return current_state


## Simulo response del API
@backoff()
def get_api_data(since=None) -> list:
    with open("sample_data/api_orders.json") as f:
        api_orders = json.load(f)

    ## Procesar desde fecha indicada como input
    if since:
        try:
            since_parse = pendulum.from_format(since, "YYYY-MM-DDTHH:mm:ss")
        except:
            print("El input no es valido. Ingresa la fecha en formato ISO8601")
            sys.exit()
        tmp = []
        for k in api_orders:
            if k["created_at"] and pendulum.parse(k["created_at"]) >= since_parse:
                tmp.append(k)
        api_orders = tmp

    ## Evaluo estado para evitar procesar nuevamente
    current_state = get_current_state()
    if current_state:
        for i in api_orders:
            if i["created_at"]==current_state["created_at"] and i["order_id"]==current_state["order_id"]:
                print("El estado actual del job indica que los registros de la api ya han sido procesados. No se procesa nuevamente")
                sys.exit() 

    return api_orders