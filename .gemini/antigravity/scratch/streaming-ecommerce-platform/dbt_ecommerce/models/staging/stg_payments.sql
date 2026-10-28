WITH source AS (
    SELECT * FROM {{ source('raw_feed', 'stg_raw_payments') }}
),
deduped AS (
    SELECT
        payment_id,
        order_id,
        customer_id,
        payment_method,
        payment_status,
        amount::NUMERIC(10, 2) AS amount,
        payment_timestamp::TIMESTAMPTZ AS payment_timestamp,
        payment_date::DATE AS payment_date,
        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY ingested_at DESC) AS rn
    FROM source
    WHERE payment_id IS NOT NULL
      AND order_id IS NOT NULL
      AND amount > 0
)
SELECT
    payment_id,
    order_id,
    customer_id,
    payment_method,
    payment_status,
    amount,
    payment_timestamp,
    payment_date
FROM deduped
WHERE rn = 1
