INSERT INTO analytics.dim_customer (customer_id, first_name, last_name, email, segment)
SELECT DISTINCT customer_id, first_name, last_name, email, segment
FROM STAGING.customers;