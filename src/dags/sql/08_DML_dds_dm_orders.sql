INSERT INTO dds.dm_orders
(user_id, restaurant_id, timestamp_id, courier_id, order_key, order_status)
WITH tmp_orders AS (
    SELECT
        object_id,
        (object_value::json)->>'update_ts' AS update_ts,
        (object_value::json)->>'final_status' AS final_status,
        (((object_value::json)->>'restaurant')::json)->>'id' AS restaurant_id,
        (((object_value::json)->>'user')::json)->>'id' AS user_id
    FROM stg.ordersystem_orders
),
tmp_deliveries AS (
    SELECT
		(object_value::json)->>'order_id' as order_id,
		(object_value::json)->>'order_ts' as order_ts,
		(object_value::json)->>'courier_id' as courier_id
	FROM stg.deliverysystem_deliveries
)
SELECT
    du.id AS user_id,
    dr.id AS restaurant_id,
    dt.id AS timestamp_id,
    dc.id AS courier_id,
    object_id AS order_key,
    final_status AS order_status
FROM tmp_orders tord
JOIN dds.dm_users du USING (user_id)
JOIN dds.dm_restaurants dr USING (restaurant_id)
JOIN dds.dm_timestamps dt ON tord.update_ts::timestamp = dt.ts
JOIN tmp_deliveries td ON tord.object_id = td.order_id
JOIN dds.dm_couriers dc USING (courier_id)
WHERE dr.active_to = '2099-12-31'
ON CONFLICT (order_key) DO UPDATE
SET
    user_id = EXCLUDED.user_id,
    restaurant_id = EXCLUDED.restaurant_id,
    timestamp_id = EXCLUDED.timestamp_id,
    courier_id = EXCLUDED.courier_id,
    order_status = EXCLUDED.order_status;
