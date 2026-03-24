INSERT INTO analytics.dim_channel (channel_name)
SELECT DISTINCT channel
FROM staging.sales_data;