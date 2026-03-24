-- Schema : analytics
-- Object : dim_modeling_info
-- Purpose: Row count audit view across all dimensional modeling tables in analytics schema

CREATE VIEW analytics.dim_modeling_info AS
    (
SELECT 'dim_channel' AS table_name, COUNT(*) AS row_count FROM analytics.dim_channel
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM analytics.dim_customer
UNION ALL
SELECT 'dim_date', COUNT(*) FROM analytics.dim_date
UNION ALL
SELECT 'dim_product', COUNT(*) FROM analytics.dim_product
UNION ALL
SELECT 'dim_promotion', COUNT(*) FROM analytics.dim_promotion
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM analytics.fact_sales
);

SELECT * FROM analytics.dim_modeling_info;



