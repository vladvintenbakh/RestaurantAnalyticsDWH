BEGIN;

DROP TABLE IF EXISTS updated_restaurants;

CREATE TEMP TABLE updated_restaurants AS
SELECT
    object_id AS restaurant_id,
    (object_value::json)->>'name' AS restaurant_name,
    ((object_value::json)->>'update_ts')::timestamp AS active_from,
    '2099-12-31'::timestamp AS active_to
FROM stg.ordersystem_restaurants
WHERE (object_id, (object_value::json)->>'name') NOT IN (
    SELECT DISTINCT restaurant_id, restaurant_name
    FROM dds.dm_restaurants
);

UPDATE dds.dm_restaurants
SET active_to = ur.active_from
FROM updated_restaurants ur
WHERE dds.dm_restaurants.restaurant_id = ur.restaurant_id
  AND dds.dm_restaurants.active_to = '2099-12-31';

INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
SELECT restaurant_id, restaurant_name, active_from, active_to
FROM updated_restaurants;

COMMIT;
