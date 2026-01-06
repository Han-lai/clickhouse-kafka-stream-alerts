CREATE TABLE IF NOT EXISTS `{CLICKHOUSE_DATABASE}`.`{MERGETREE_STORAGE_TABLE}` (
    unique_id String,  -- 新增的欄位，用來存儲唯一的 ID
    region String,
    product_line String,
    message_name String,
    deviceid String,
    device_status Int32,
    errcode String,
    temp Float32,
    pressure Float32,
    device_timestamp DateTime64(6),
    source_kafka_timestamp DateTime64(6)
) ENGINE = MergeTree()
ORDER BY (region, product_line,deviceid,device_timestamp)