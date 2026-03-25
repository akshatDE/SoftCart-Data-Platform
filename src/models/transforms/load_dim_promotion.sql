INSERT INTO analytics.dim_promotion (promo_code, discount_percent)
SELECT DISTINCT promo_code, discount_percent
FROM staging.sales_data;