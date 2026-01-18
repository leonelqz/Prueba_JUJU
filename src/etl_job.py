from src.logs_config import setup_logging
# setup_logging(store_logs=True)     # Descomentar para enviar prints a logs.log 
from src.api_client import get_api_data, get_current_state
from src.db import read_db
from src.alerts import send_mail
from src.transforms import *
import pendulum, json, click


def get_inputs(since=None):
    print("Obteniendo data...")
    api_orders = get_api_data(since)
    products = read_db("products")
    users = read_db("users")
    print("Data cargada en memoria")
    return api_orders, products, users


def apply_transforms(api_orders, products, users):
    print("Aplicando transformaciones...")  
    api_orders_dedup = dedup_orders(api_orders)
    dim_order = get_dim_order(api_orders_dedup)
    dim_user = get_dim_user(api_orders_dedup, users)
    dim_product = get_dim_product(api_orders_dedup, products)
    fact_sales = get_fact_sales(api_orders_dedup)
    print("Tranformaciones aplicadas")
    return dim_order, dim_user, dim_product, fact_sales


def store_outputs(dim_order, dim_user, dim_product, fact_sales, api_orders):
    print("Exportando data...")
    # Raw
    with open(f"output/raw/api_orders_raw_{pendulum.today().date()}.json", "w", encoding="utf-8") as file:
        json.dump(api_orders, file) 
    print("Capa raw exportada")

    # Curated
    dim_user.write_parquet("output/curated/dim_user.parquet")
    dim_product.write_parquet("output/curated/dim_product.parquet")
    dim_order.write_parquet("output/curated/dim_order", partition_by="order_date")
    fact_sales.write_parquet("output/curated/fact_sales",  partition_by="order_date")
    print("Capa curated exportada")


### Defino flags para controlar la ejecucion del script
@click.command()
@click.option("--last-processed", is_flag=True)
@click.option("--since")
def run_job(last_processed, since):
    print("Inicia ejecucion")
    if last_processed:
        current_state = get_current_state()
        if current_state:
            print(f"El ultimo registro procesado, que define el estado del job es:\n{current_state}")
        else:
            print("El job NO tiene estado almacenado")
        return
    elif since:
        api_orders, products, users = get_inputs(since)
        print(f"Ejecutando job para fechas >= {since}")
    else:
        api_orders, products, users = get_inputs()
        print("Ejecutando job para todos los registros disponibles en el api")

    dim_order, dim_user, dim_product, fact_sales = apply_transforms(api_orders, products, users)
    store_outputs(dim_order, dim_user, dim_product, fact_sales, api_orders)
    store_last_state(fact_sales)  # Guardo el ultimo registro con la fecha mas reciente en el json de estado
    print("Termina ejecucion")


try:
    run_job()
except Exception as e:
    send_mail(f"Problema con la ejecucion del ETL. La excepcion es:\n\n{e}")
    raise e
