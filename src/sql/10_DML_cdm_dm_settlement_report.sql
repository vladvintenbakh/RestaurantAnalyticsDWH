INSERT INTO cdm.dm_settlement_report
(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, 
orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
SELECT
    dr.restaurant_id,
    dr.restaurant_name,
    dt.date,
    COUNT(DISTINCT order_id) AS orders_count,
    SUM(total_sum) AS orders_total_sum,
    SUM(bonus_payment) AS orders_bonus_payment_sum,
    SUM(bonus_grant) AS orders_bonus_granted_sum,
    SUM(total_sum) * 0.25 AS order_processing_fee,
    SUM(total_sum) * 0.75 - SUM(bonus_payment) AS restaurant_reward_sum
FROM dds.fct_product_sales fps
JOIN dds.dm_orders dord ON fps.order_id = dord.id
JOIN dds.dm_restaurants dr ON dord.restaurant_id = dr.id
JOIN dds.dm_timestamps dt ON dord.timestamp_id = dt.id
GROUP BY dr.restaurant_id, dr.restaurant_name, dt.date
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
SET
    restaurant_name = EXCLUDED.restaurant_name,
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
