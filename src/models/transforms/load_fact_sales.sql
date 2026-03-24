INSERT INTO analytics.fact_sales
    (product_id, customer_id, date_id, channel_id, promo_id, product_quantity, product_price)
SELECT
    sd.product_id,
    sd.customer_id,
    CAST(EXTRACT(YEAR FROM sd.time_stamp) * 10000 + EXTRACT(MONTH FROM sd.time_stamp) * 100 + EXTRACT(DAY FROM sd.time_stamp) AS INT),
    dc.channel_id,
    dp.promo_id,
    sd.quantity,
    sd.price
FROM staging.sales_data sd
JOIN analytics.dim_channel dc
    ON sd.channel = dc.channel_name
JOIN analytics.dim_promotion dp
    ON sd.promo_code = dp.promo_code
    AND sd.discount_percent = dp.discount_percent;

