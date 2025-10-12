INSERT INTO dds.dm_orders
(user_id, restaurant_id, timestamp_id, order_key, order_status)
WITH tmp AS (
    SELECT
        object_id,
        (object_value::json)->>'update_ts' AS update_ts,
        (object_value::json)->>'final_status' AS final_status,
        (((object_value::json)->>'restaurant')::json)->>'id' AS restaurant_id,
        (((object_value::json)->>'user')::json)->>'id' AS user_id
    FROM stg.ordersystem_orders
)
SELECT
    du.id AS user_id,
    dr.id AS restaurant_id,
    dt.id AS timestamp_id,
    object_id AS order_key,
    final_status AS order_status
FROM tmp t
JOIN dds.dm_users du USING (user_id)
JOIN dds.dm_restaurants dr USING (restaurant_id)
JOIN dds.dm_timestamps dt ON t.update_ts::timestamp = dt.ts
WHERE dr.active_to = '2099-12-31'
ON CONFLICT (order_key) DO UPDATE
SET
    user_id = EXCLUDED.user_id,
    restaurant_id = EXCLUDED.restaurant_id,
    timestamp_id = EXCLUDED.timestamp_id,
    order_status = EXCLUDED.order_status;
