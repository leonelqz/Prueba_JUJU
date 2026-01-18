import polars as pl
from src.transforms import dedup_orders, get_fact_sales


def test_dedup_orders():
    duplicate_items = [ { 
            "order_id": "o_1001", 
            "user_id": "u_1", 
            "amount": 125.50, 
            "currency": "USD", 
            "created_at": "2025-08-20T15:23:10Z", 
            "items": [ 
            {"sku": "p_1", "qty": 2, "price": 60.0} 
            ], 
            "metadata": {"source": "api", "promo": None} 
        },
        { 
            "order_id": "o_1001", 
            "user_id": "u_1", 
            "amount": 125.50, 
            "currency": "USD", 
            "created_at": "2025-08-20T15:23:10Z", 
            "items": [ 
            {"sku": "p_1", "qty": 2, "price": 60.0} 
            ], 
            "metadata": {"source": "api", "promo": None} 
        }
    ]

    ref_df = pl.DataFrame(
        { 
            "order_id": "o_1001", 
            "user_id": "u_1", 
            "amount": 125.50, 
            "currency": "USD", 
            "created_at": "2025-08-20T15:23:10Z", 
            "items": [[ 
            {"sku": "p_1", "qty": 2, "price": 60.0} 
            ]], 
            "metadata.source": "api",
            "metadata.promo": None 
        }
    )
    
    result = dedup_orders(duplicate_items)
    assert result.equals(ref_df) 


def test_get_fact_sales():
    df_inp = pl.DataFrame(
        { 
            "order_id": "o_1001", 
            "user_id": "u_1", 
            "amount": 125.50, 
            "currency": "USD", 
            "created_at": "2025-08-20T15:23:10Z", 
            "items": [[ 
            {"sku": "p_1", "qty": 2, "price": 60.0} 
            ]], 
            "metadata.source": "api",
            "metadata.promo": None 
        }
    )

    ref_df = pl.DataFrame(
        {
            "order_id": "o_1001",
            "user_id": "u_1",
            "sku": "p_1",
            "created_at": "2025-08-20T15:23:10",
            "unit_price": 60.0,
            "quantity": 2,
            "order_date": "2025-08-20",
            "is_valid": True
        },
        schema_overrides={ "order_date": pl.Date }
    ).with_columns(pl.col("created_at").str.to_datetime(time_zone="UTC"))

    result = get_fact_sales(df_inp)
    assert result.equals(ref_df) 