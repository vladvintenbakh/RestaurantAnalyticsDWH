INSERT INTO dds.fct_deliveries
(order_id, courier_id, rate, tip_sum)
SELECT
	dord.id as order_id,
	dc.id as courier_id,
	((object_value::json)->>'rate')::int as rate,
	((object_value::json)->>'tip_sum')::numeric as tip_sum
FROM stg.deliverysystem_deliveries del
JOIN dds.dm_orders dord ON (object_value::json)->>'order_id' = dord.order_key
JOIN dds.dm_couriers dc ON (object_value::json)->>'courier_id' = dc.courier_id
ON CONFLICT (order_id, courier_id) DO UPDATE
SET
	rate = EXCLUDED.rate,
	tip_sum = EXCLUDED.tip_sum;
