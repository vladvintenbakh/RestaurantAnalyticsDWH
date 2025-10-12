BEGIN;

DROP TABLE IF EXISTS updated_products;

CREATE TEMP TABLE updated_products AS
WITH tmp AS (
    SELECT object_id, update_ts, json_array_elements(((object_value::json)->>'menu')::json) AS menu_item
    FROM stg.ordersystem_restaurants
)
SELECT
    (menu_item::json)->>'_id' AS product_id,
    dr.id AS restaurant_id,
    (menu_item::json)->>'name' AS product_name,
    ((menu_item::json)->>'price')::numeric(14,2) AS product_price,
    update_ts AS active_from,
    '2099-12-31'::timestamp AS active_to
FROM tmp t
JOIN dds.dm_restaurants dr ON t.object_id = dr.restaurant_id
WHERE (dr.id, (menu_item::json)->>'name', ((menu_item::json)->>'price')::numeric(14,2)) NOT IN (
    SELECT DISTINCT restaurant_id, product_name, product_price
    FROM dds.dm_products
);

UPDATE dds.dm_products
SET active_to = up.active_from
FROM updated_products up
WHERE dds.dm_products.product_id = up.product_id
  AND dds.dm_products.active_to = '2099-12-31';

INSERT INTO dds.dm_products
(product_id, restaurant_id, product_name, product_price, active_from, active_to)
SELECT product_id, restaurant_id, product_name, product_price, active_from, active_to
FROM updated_products;

COMMIT;
