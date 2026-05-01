WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_products') }}

),

cleaned AS (

    SELECT
        CAST(product_id AS INTEGER) AS product_id,
        TRIM(name) AS product_name,
        CAST(price AS FLOAT) AS price

    FROM source

    WHERE product_id IS NOT NULL
      AND price IS NOT NULL
      AND price >= 0

)

SELECT * FROM cleaned