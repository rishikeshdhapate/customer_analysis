WITH source AS (
    SELECT * FROM {{ source('raw_feed', 'stg_raw_orders') }}
),
deduped AS (
    SELECT
        order_id,
        session_id,
        customer_id,
        product_id,
        quantity::INT AS quantity,
        unit_price::NUMERIC(10, 2) AS unit_price,
        total_amount::NUMERIC(10, 2) AS total_amount,
        COALESCE(order_status, 'COMPLETED') AS order_status,
        order_timestamp::TIMESTAMPTZ AS order_timestamp,
        order_date::DATE AS order_date,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) AS rn
    FROM source
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND product_id IS NOT NULL
      AND quantity > 0
      AND total_amount > 0
)
SELECT
    order_id,
    session_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    order_status,
    order_timestamp,
    order_date
FROM deduped
WHERE rn = 1
