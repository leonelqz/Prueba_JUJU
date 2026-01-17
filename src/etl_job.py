from src.api_client import get_api_data
from src.db import read_db
from src.transforms import *
import pendulum, json
print("Inicia ejecucion")


## Inputs
print("Data cargada en memoria")
api_orders = get_api_data()
products = read_db("products")
users = read_db("users")


## Transformaciones
api_orders_dedup = dedup_orders(api_orders)
fact_sales = get_fact_sales(api_orders_dedup)
dim_order = get_dim_order(api_orders_dedup)
dim_user = get_dim_user(api_orders_dedup, users)
dim_product = get_dim_product(api_orders_dedup, products)


## Outputs
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

print("Termina ejecucion")