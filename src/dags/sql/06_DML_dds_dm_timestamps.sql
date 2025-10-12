INSERT INTO dds.dm_timestamps
(ts, "year", "month", "day", "time", "date")
WITH tmp AS (
    SELECT ((object_value::json)->>'date')::timestamp AS ts
    FROM stg.ordersystem_orders
    WHERE (object_value::json)->>'final_status' = 'CLOSED'
       OR (object_value::json)->>'final_status' = 'CANCELLED'
)
SELECT
    ts,
    EXTRACT(YEAR FROM ts)::int AS "year",
    EXTRACT(MONTH FROM ts)::int AS "month",
    EXTRACT(DAY FROM ts)::int AS "day",
    ts::time AS "time",
    ts::date AS "date"
FROM tmp
ON CONFLICT (ts) DO UPDATE
SET
	"year" = EXCLUDED."year",
    "month" = EXCLUDED."month",
    "day" = EXCLUDED."day",
    "time" = EXCLUDED."time",
    "date" = EXCLUDED."date";
