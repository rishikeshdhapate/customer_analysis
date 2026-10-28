WITH source AS (
    SELECT * FROM {{ source('raw_feed', 'stg_raw_customers') }}
),
deduped AS (
    SELECT
        customer_id,
        TRIM(name) AS name,
        LOWER(TRIM(email)) AS email,
        TRIM(city) AS city,
        TRIM(state) AS state,
        COALESCE(country, 'USA') AS country,
        signup_date::DATE AS signup_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC NULLS LAST) AS rn
    FROM source
    WHERE customer_id IS NOT NULL
)
SELECT
    customer_id,
    name,
    email,
    city,
    state,
    country,
    signup_date
FROM deduped
WHERE rn = 1
