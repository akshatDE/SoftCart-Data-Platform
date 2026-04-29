INSERT INTO analytics.dim_channel (channel_name)
SELECT DISTINCT channel
FROM STAGING.sales_data;