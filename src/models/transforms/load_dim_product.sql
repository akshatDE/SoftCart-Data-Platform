INSERT INTO analytics.dim_product (product_id, product_model, product_type)
SELECT DISTINCT product_id, product_model, product_type
FROM staging.catalog;