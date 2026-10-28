{{ config(materialized='table') }}

/*
  Dimension Date Table
  Contains calendar attributes, month, quarter, year, and day metadata.
*/
WITH date_spine AS (
    SELECT
        datum::DATE AS date_actual
    FROM GENERATE_SERIES(
        '2024-01-01'::DATE,
        '2027-12-31'::DATE,
        '1 day'::INTERVAL
    ) AS datum
)
SELECT
    TO_CHAR(date_actual, 'YYYYMMDD')::INT AS date_key,
    date_actual AS date,
    EXTRACT(DAY FROM date_actual)::INT AS day_of_month,
    EXTRACT(MONTH FROM date_actual)::INT AS month,
    TO_CHAR(date_actual, 'Month') AS month_name,
    EXTRACT(QUARTER FROM date_actual)::INT AS quarter,
    EXTRACT(YEAR FROM date_actual)::INT AS year,
    EXTRACT(ISODOW FROM date_actual)::INT AS day_of_week,
    TO_CHAR(date_actual, 'Day') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM date_actual) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine
