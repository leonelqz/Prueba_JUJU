import polars as pl
import logging


## Configuro logs
logging.basicConfig(filename='logs.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
print = logger.info
print("")


## Aplano json y deduplico ordenes ya que pueden venir duplicadas del api
def dedup_orders(api_orders:dict) -> pl.DataFrame:
    df_api = pl.json_normalize(api_orders)
    df_dedup = df_api.unique("order_id")
    print("Registros de la api deduplicados por order_id")
    return df_dedup


## Obtengo fact_sales
def get_fact_sales(df_api:pl.DataFrame) -> pl.DataFrame:
    print("Obteniendo fact_sales...")
    return (
        df_api.explode("items").unnest("items").select(
            "order_id", "user_id", "sku", "created_at", "price", "qty"
        ).rename(
            {"price": "unit_price", "qty": "quantity"}
        ).with_columns(
            pl.col("created_at").str.to_datetime()
        ).with_columns(
            order_date = pl.col("created_at").dt.date()
        )
    )


## Obtengo dim_order
def get_dim_order(df_api:pl.DataFrame) -> pl.DataFrame:
    print("Obteniendo dim_order...")
    return (
        df_api.select(
            "order_id", "created_at", "amount", "currency", "metadata.source", "metadata.promo"
        ).rename(
            {"metadata.source": "source", "metadata.promo":"promo", "amount":"total_amount"}
        ).with_columns(
            pl.col("created_at").str.to_datetime()
        ).with_columns(
            order_date = pl.col("created_at").dt.date()
        )
    )


## Obtengo dim_user
def get_dim_user(df_api:pl.DataFrame, df_users:pl.DataFrame) -> pl.DataFrame:
    print("Obteniendo dim_user...")
    return (
        df_api.select("user_id").unique("user_id").join(df_users, on="user_id")
    ) 


## Obtengo dim_product
def get_dim_product(df_api:pl.DataFrame, df_products:pl.DataFrame) -> pl.DataFrame:
    print("Obteniendo dim_product...")
    return (
        df_api.explode("items").unnest("items").select("sku").unique("sku").join(df_products, on="sku")
    )

