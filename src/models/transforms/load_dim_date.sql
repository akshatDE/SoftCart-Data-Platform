INSERT INTO analytics.dim_date (date_id, date, day_of_week, calendar_month, calendar_year, weekday_indicator)
SELECT DISTINCT
    EXTRACT(YEAR FROM time_stamp) * 10000 + EXTRACT(MONTH FROM time_stamp) * 100 + EXTRACT(DAY FROM time_stamp) AS date_id,
    time_stamp::DATE AS date,
    TO_CHAR(time_stamp, 'Day') AS day_of_week,
    EXTRACT(MONTH FROM time_stamp) AS calendar_month,
    EXTRACT(YEAR FROM time_stamp) AS calendar_year,
    CASE WHEN EXTRACT(DOW FROM time_stamp) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS weekday_indicator
FROM staging.sales_data;