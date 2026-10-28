WITH source AS (
    SELECT * FROM {{ source('raw_feed', 'stg_raw_products') }}
),
deduped AS (
    SELECT
        product_id,
        TRIM(name) AS name,
        TRIM(category) AS category,
        price::NUMERIC(10, 2) AS price,
        COALESCE(stock, 0)::INT AS stock,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price DESC) AS rn
    FROM source
    WHERE product_id IS NOT NULL
      AND price > 0
)
SELECT
    product_id,
    name,
    category,
    price,
    stock
FROM deduped
WHERE rn = 1
