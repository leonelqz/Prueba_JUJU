CREATE OR REPLACE TABLE fact_sales (
order_id string,
user_id string,
sku string,
created_at timestamp,
order_date date,
unit_price float,
quantity int,
is_valid bool
);

CREATE OR REPLACE TABLE dim_user (
user_id string,
email string,
country string,
created_at date
);

CREATE OR REPLACE TABLE dim_product (
sku string,
name string,
category string,
price float
);

CREATE OR REPLACE TABLE dim_order (
order_id string,
created_at timestamp,
total_amount float,
currency string,
source string,
promo string,
order_date date,
);