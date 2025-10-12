INSERT INTO dds.fct_product_sales
(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
WITH order_info AS (
    SELECT 
        json_array_elements(((object_value::json)->>'order_items')::json)->>'id' AS product_id,
        object_id AS order_id,
        json_array_elements(((object_value::json)->>'order_items')::json)->>'quantity' AS "count",
        json_array_elements(((object_value::json)->>'order_items')::json)->>'price' AS price
    FROM stg.ordersystem_orders
), bonus_info AS (
    SELECT
        (event_value::json)->>'order_id' AS order_id,
        json_array_elements(((event_value::json)->>'product_payments')::json)->>'product_id' AS product_id,
        json_array_elements(((event_value::json)->>'product_payments')::json)->>'bonus_payment' AS bonus_payment,
        json_array_elements(((event_value::json)->>'product_payments')::json)->>'bonus_grant' AS bonus_grant
    FROM stg.bonussystem_events
)
SELECT
    dp.id AS product_id,
    dord.id AS order_id,
    count::int AS count,
    price::numeric(19,5) AS price,
    (count::int * price::numeric(19,5))::numeric(19,5) AS total_sum,
    COALESCE(bonus_payment::numeric(19,5), 0) AS bonus_payment,
    COALESCE(bonus_grant::numeric(19,5), 0) AS bonus_grant
FROM order_info oi
JOIN bonus_info bi USING (order_id, product_id)
JOIN dds.dm_products dp USING (product_id)
JOIN dds.dm_orders dord ON oi.order_id = dord.order_key
WHERE dp.active_to = '2099-12-31'
ON CONFLICT (product_id, order_id) DO UPDATE
SET
    count = EXCLUDED.count,
    price = EXCLUDED.price,
    total_sum = EXCLUDED.total_sum,
    bonus_payment = EXCLUDED.bonus_payment,
    bonus_grant = EXCLUDED.bonus_grant;
