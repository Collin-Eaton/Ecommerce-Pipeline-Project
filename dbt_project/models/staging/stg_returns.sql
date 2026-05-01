WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_returns') }}

),

cleaned AS (

    SELECT
        CAST(return_id AS INTEGER) AS return_id,
        CAST(order_id AS INTEGER) AS order_id,
        LOWER(TRIM(reason)) AS return_reason

    FROM source

    WHERE return_id IS NOT NULL
      AND order_id IS NOT NULL

)

SELECT * FROM cleaned