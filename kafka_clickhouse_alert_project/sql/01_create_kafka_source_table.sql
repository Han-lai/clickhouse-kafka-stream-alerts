CREATE TABLE IF NOT EXISTS `{CLICKHOUSE_DATABASE}`.`{KAFKA_SOURCE_TABLE}` (
    unique_id String,  -- 新增的欄位，用來存儲唯一的 ID
    region String,
    product_line String,
    message_name String,
    deviceid String,
    device_status Int32,
    errcode String,
    temp Float32,
    pressure Float32,
    device_timestamp DateTime64(6)
) ENGINE = Kafka
SETTINGS kafka_broker_list = '{KAFKA_BROKER}',
         kafka_topic_list = '{KAFKA_SOURCE_TOPIC}',
         kafka_group_name = '{KAFKA_GROUP_NAME}', -- 添加這一行
         kafka_format = 'JSONEachRow',
         kafka_flush_interval_ms = 5000;
        --  kafka_max_block_size = 1000,
        -- kafka_flush_interval_ms = 100; -- 設定 Kafka flush 間隔 -- 設定每次消費的行數;
