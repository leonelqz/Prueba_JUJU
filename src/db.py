import polars as pl


## Simula lectura a una BD
def read_db(table_name:str) -> pl.DataFrame:
    return pl.read_csv(f"sample_data/{table_name}.csv")



