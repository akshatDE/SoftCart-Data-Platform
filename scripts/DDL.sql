-- DDL For MYSQL
CREATE TABLE sales_data (
  product_id   INT NOT NULL,
  customer_id  INT NOT NULL,
  price        DECIMAL(10,2) NOT NULL,
  quantity     INT NOT NULL,
  time_stamp  DATETIME NOT NULL
);
-- Indexing for faster queries output , to avoid full table scan
CREATE INDEX idx_sales_ts
ON sales_data (time_stamp);

--DDL FOR Postgres

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.catalog (
    _id VARCHAR(255) PRIMARY KEY,
    product_type VARCHAR(100),
    model VARCHAR(100),
    screen_size DOUBLE PRECISION,
    raw_json JSONB NOT NULL
);
  

CREATE INDEX IF NOT EXISTS idx_catalog_product_type 
    ON staging.catalog(product_type);

CREATE INDEX IF NOT EXISTS idx_catalog_model 
    ON staging.catalog(model);


CREATE TABLE IF NOT EXISTS staging.sales_data (
    -- Surrogate key (internal Postgres operations)
    id BIGSERIAL PRIMARY KEY,
    
    -- Source columns (MySQL schema replica)
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL,
    time_stamp TIMESTAMP NOT NULL);