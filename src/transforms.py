import polars as pl


## Aplano json y deduplico ordenes ya que pueden venir duplicadas del api
def dedup_orders(api_orders:list) -> pl.DataFrame:
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
            order_date = pl.col("created_at").dt.date(),
            is_valid = pl.col("created_at").is_not_null()
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


## Defino estado de ejecucion (idempotencia e incrementalidad) guardando el registro con la fecha mas reciente en un json de estado
def store_last_state(fact_sales):
    last_processed = fact_sales.filter( 
        (pl.col("created_at")==pl.col("created_at").max())
        & (pl.col("is_valid")==True)   ## Con esta col is_valid excluyo posibles ordenes que NO tengan created_at

    ).with_columns(  ## Debo extraer enteros puesto que en el input dado el order_id esta compuesto por string + entero
        int_order_id=pl.col("order_id").str.extract(r"(\d+)").cast(pl.Int64),
        created_at=pl.col("created_at").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
    ).filter(   ## Con este segundo filtro gestiono los casos borde en que un mismo timestamp comparte varias ordenes validas
        pl.col("int_order_id") == pl.col("int_order_id").max()
    )

    last_processed.select("order_id", "created_at").write_ndjson("state.json")   ## En prod este archivo de estado se almacena en un "backend remoto", es decir un bucket, analogo a lo que hace terraform
    print("Estado del job actualizado en state.json")


