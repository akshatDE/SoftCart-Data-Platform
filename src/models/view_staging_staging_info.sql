-- Schema : staging
-- Object : staging_info
-- Purpose: Row count audit view across all staging tables


CREATE VIEW staging.staging_info AS
(
SELECT 'sales_data' as table_name,COUNT(*) as row_count FROM staging.sales_data
UNION ALL
SELECT 'catalog' as table_name ,COUNT(*) as row_count FROM staging.catalog
UNION ALL
SELECT 'customers' as table_name,COUNT(*) as row_count FROM staging.customers
);
SELECT * FROM staging.staging_info;

