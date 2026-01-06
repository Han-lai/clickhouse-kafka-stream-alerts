CREATE MATERIALIZED VIEW  IF NOT EXISTS `{CLICKHOUSE_DATABASE}`.`{STATUS_CHANGE_MVIEW}`
TO `{CLICKHOUSE_DATABASE}`.`{KAFKA_STATUS_TABLE}` AS
SELECT * FROM(
SELECT
    source_kafka_timestamp,
    unique_id,
    region,
    product_line,
    deviceid,
    device_timestamp,
    arrayMap(x -> toString(x), groupArray(`device_status`) OVER w2) AS status_flags,
    errcode,
    toDateTime64(now(), 6) AS porcess_detected_time
FROM `{CLICKHOUSE_DATABASE}`.`{MERGETREE_STORAGE_TABLE}`
WINDOW
    w2 AS (PARTITION BY region, product_line, deviceid ORDER BY device_timestamp ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
    )
WHERE LENGTH(status_flags) = 2
AND status_flags[1] != status_flags[2] AND status_flags[2]= '5000' ;






