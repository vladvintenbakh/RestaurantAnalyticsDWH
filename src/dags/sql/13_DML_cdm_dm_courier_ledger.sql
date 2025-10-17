INSERT INTO cdm.dm_courier_ledger
(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, 
rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
WITH tmp as (
	SELECT
		dc.courier_id AS courier_id,
		dc.courier_name AS courier_name,
		dt."year" AS settlement_year,
		dt."month" AS settlement_month,
		COUNT(fd.order_id) AS orders_count,
		SUM(order_sum) AS orders_total_sum,
		AVG(rate) AS rate_avg,
		SUM(order_sum) * 0.25 AS order_processing_fee,
		CASE
			WHEN AVG(rate) < 4 THEN GREATEST(SUM(order_sum) * 0.05, 100)
			WHEN (AVG(rate) >= 4 AND AVG(rate) < 4.5) THEN GREATEST(SUM(order_sum) * 0.07, 150)
			WHEN (AVG(rate) >= 4.5 AND AVG(rate) < 4.9) THEN GREATEST(SUM(order_sum) * 0.08, 175)
			WHEN AVG(rate) >= 5 THEN GREATEST(SUM(order_sum) * 0.1, 200)
		END AS courier_order_sum,
		SUM(tip_sum) AS courier_tips_sum
	FROM dds.fct_deliveries fd
	JOIN dds.dm_couriers dc ON fd.courier_id = dc.id
	JOIN dds.dm_orders dord ON fd.order_id = dord.id
	JOIN dds.dm_timestamps dt ON dord.timestamp_id = dt.id
	GROUP BY 1, 2, 3, 4
)
SELECT 
	*, 
	courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
FROM tmp
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
SET
	courier_name = EXCLUDED.courier_name, 
	orders_count = EXCLUDED.orders_count, 
	orders_total_sum = EXCLUDED.orders_total_sum, 
	rate_avg = EXCLUDED.rate_avg, 
	order_processing_fee = EXCLUDED.order_processing_fee, 
	courier_order_sum = EXCLUDED.courier_order_sum, 
	courier_tips_sum = EXCLUDED.courier_tips_sum, 
	courier_reward_sum = EXCLUDED.courier_reward_sum;
