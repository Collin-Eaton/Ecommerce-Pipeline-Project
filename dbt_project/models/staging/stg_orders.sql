WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_orders') }}

),

cleaned AS (

    SELECT
        CAST(order_id AS INTEGER) AS order_id,
        CAST(customer_id AS INTEGER) AS customer_id,
        CAST(order_date AS TIMESTAMP) AS order_date,
        CAST(total_amount AS FLOAT) AS total_amount

    FROM source

    WHERE order_id IS NOT NULL

)

SELECT * FROM cleaned