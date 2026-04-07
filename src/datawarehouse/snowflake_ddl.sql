-- DDL FOR SNOWFLAKE DWH

CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE TABLE staging.catalog (
    product_id INT PRIMARY KEY,
    product_model STRING,
    product_type STRING,
    product_price NUMBER(10,2)
);

CREATE OR REPLACE TABLE staging.customers (
    customer_id INT PRIMARY KEY,
    first_name STRING NOT NULL,
    last_name STRING NOT NULL,
    email STRING NOT NULL,
    segment STRING NOT NULL
);

CREATE OR REPLACE TABLE staging.sales_data (
    id NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    quantity INT NOT NULL,
    price NUMBER(10,2) NOT NULL,
    time_stamp TIMESTAMP_NTZ NOT NULL,
    channel STRING NOT NULL,
    promo_code STRING NOT NULL,
    discount_percent NUMBER(5,2) NOT NULL
);