-- DDL For MYSQL
CREATE TABLE sales_data (
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL,
    time_stamp DATETIME NOT NULL,
    channel VARCHAR(20) NOT NULL,
    promo_code VARCHAR(20) NOT NULL,
    discount_percent DECIMAL(5,2) NOT NULL
);
-- Indexing for faster queries output , to avoid full table scan
CREATE INDEX idx_sales_ts
ON sales_data (time_stamp);

CREATE TABLE IF NOT EXISTS softcart_sales.customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    segment VARCHAR(20) NOT NULL
);

--DDL FOR Postgres

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.catalog (
    _id VARCHAR(255) PRIMARY KEY,
    product_type VARCHAR(100),
    product_model VARCHAR(100),
    product_price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    segment VARCHAR(20) NOT NULL
);
  
CREATE INDEX IF NOT EXISTS idx_catalog_product_type 
    ON staging.catalog(product_type);

CREATE INDEX IF NOT EXISTS idx_catalog_product_model 
    ON staging.catalog(product_model);


CREATE TABLE IF NOT EXISTS staging.sales_data (
    -- Surrogate key (internal Postgres operations)
    id BIGSERIAL PRIMARY KEY,
    
    -- Source columns (MySQL schema replica)
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL,
    time_stamp TIMESTAMP NOT NULL,
    channel VARCHAR(20) NOT NULL,
    promo_code VARCHAR(20) NOT NULL,
    discount_percent DECIMAL(5,2) NOT NULL
);


-- DDL for analytics layer (Postgres)
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_id INT PRIMARY KEY,
    product_model VARCHAR(100) NOT NULL,
    product_type VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    segment VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_id INT PRIMARY KEY,
    date DATE NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    calendar_month INT NOT NULL,
    calendar_year INT NOT NULL,
    weekday_indicator VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_channel (
    channel_id SERIAL PRIMARY KEY,
    channel_name VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_promotion (
    promo_id SERIAL PRIMARY KEY,
    promo_code VARCHAR(20) NOT NULL,
    discount_percent DECIMAL(5,2) NOT NULL
);


CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    sales_id BIGSERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES analytics.dim_product(product_id),
    customer_id INT NOT NULL REFERENCES analytics.dim_customer(customer_id),
    date_id INT NOT NULL REFERENCES analytics.dim_date(date_id),
    channel_id INT NOT NULL REFERENCES analytics.dim_channel(channel_id),
    promo_id INT NOT NULL REFERENCES analytics.dim_promotion(promo_id),
    product_quantity INT NOT NULL,
    product_price DECIMAL(10,2) NOT NULL
);

-- Moving data from staging to analytics with transformations
INSERT INTO analytics.dim_product (product_id, product_model, product_type)
SELECT DISTINCT product_id, product_model, product_type
FROM staging.catalog;

INSERT INTO analytics.dim_customer (customer_id, first_name, last_name, email, segment)
SELECT DISTINCT customer_id, first_name, last_name, email, segment
FROM staging.customers;

INSERT INTO analytics.dim_date (date_id, date, day_of_week, calendar_month, calendar_year, weekday_indicator)
SELECT DISTINCT
    EXTRACT(YEAR FROM time_stamp) * 10000 + EXTRACT(MONTH FROM time_stamp) * 100 + EXTRACT(DAY FROM time_stamp) AS date_id,
    time_stamp::DATE AS date,
    TO_CHAR(time_stamp, 'Day') AS day_of_week,
    EXTRACT(MONTH FROM time_stamp) AS calendar_month,
    EXTRACT(YEAR FROM time_stamp) AS calendar_year,
    CASE WHEN EXTRACT(DOW FROM time_stamp) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS weekday_indicator
FROM staging.sales_data;

INSERT INTO analytics.fact_sales (product_id, customer_id, date_id, product_quantity, product_price, promo_code, discount_percent)
SELECT
    product_id,
    customer_id,
    EXTRACT(YEAR FROM time_stamp) * 10000 + EXTRACT(MONTH FROM time_stamp) * 100 + EXTRACT(DAY FROM time_stamp) AS date_id,
    quantity AS product_quantity,
    price AS product_price,
    promo_code,
    discount_percent

FROM staging.sales_data;



