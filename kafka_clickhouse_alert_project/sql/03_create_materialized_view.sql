
CREATE MATERIALIZED VIEW  IF NOT EXISTS `{CLICKHOUSE_DATABASE}`.`{MATERIALIZED_VIEW}`
TO `{CLICKHOUSE_DATABASE}`.`{MERGETREE_STORAGE_TABLE}` AS
SELECT
    unique_id ,  -- 新增的欄位，用來存儲唯一的 ID
    region ,
    product_line ,
    message_name ,
    deviceid ,
    device_status ,
    errcode ,
    temp ,
    pressure ,
    device_timestamp ,
    toDateTime64(_timestamp, 6) AS source_kafka_timestamp---virtural column 在mview裡拉取
FROM `{CLICKHOUSE_DATABASE}`.`{KAFKA_SOURCE_TABLE}`


