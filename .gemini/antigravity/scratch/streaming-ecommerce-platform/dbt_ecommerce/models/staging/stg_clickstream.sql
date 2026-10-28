WITH source AS (
    SELECT * FROM {{ source('raw_feed', 'stg_raw_clickstream') }}
),
deduped AS (
    SELECT
        event_id,
        session_id,
        customer_id,
        product_id,
        LOWER(TRIM(event_type)) AS event_type,
        page_url,
        COALESCE(referrer, 'direct') AS referrer,
        COALESCE(device, 'unknown') AS device,
        event_timestamp::TIMESTAMPTZ AS event_timestamp,
        event_date::DATE AS event_date,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn
    FROM source
    WHERE event_id IS NOT NULL
      AND session_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND LOWER(TRIM(event_type)) IN ('view', 'add_to_cart', 'checkout')
)
SELECT
    event_id,
    session_id,
    customer_id,
    product_id,
    event_type,
    page_url,
    referrer,
    device,
    event_timestamp,
    event_date
FROM deduped
WHERE rn = 1
