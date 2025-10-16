INSERT INTO dds.dm_couriers (courier_id, courier_name)
SELECT 
	id AS courier_id, 
	courier_name
FROM stg.deliverysystem_couriers
ON CONFLICT (courier_id) DO UPDATE
SET
	courier_name = EXCLUDED.courier_name;
