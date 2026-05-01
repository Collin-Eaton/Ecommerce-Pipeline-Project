WITH orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

returns AS (

    SELECT * FROM {{ ref('stg_returns') }}

)

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,

    -- return flag
    CASE 
        WHEN r.order_id IS NOT NULL THEN 1
        ELSE 0
    END AS is_returned,

    -- optional: return date
    r.return_date

FROM orders o

LEFT JOIN returns r
    ON o.order_id = r.order_id