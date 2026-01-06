CREATE TABLE  IF NOT EXISTS  `{CLICKHOUSE_DATABASE}`.`{KAFKA_STATUS_TABLE}` (
    source_kafka_timestamp  DateTime64(6),
    region String,
    product_line String,
    deviceid String,
    device_timestamp DateTime64(6),
    status_flags Array(Int32),
    errcode String,
    porcess_detected_time DateTime64(6),
    unique_id String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '{KAFKA_BROKER}',  -- 替換為你的 Kafka 服務地址
    kafka_topic_list = '{KAFKA_STATUS_TOPIC}',     -- 替換為你的目標 Kafka Topic
    kafka_group_name = '{KAFKA_STATUS_GROUP_NAME}', -- 添加這一行
    kafka_format = 'JSONEachRow'                  -- 以 JSON 格式發送數據
    -- kafka_flush_interval_ms = 1000;




    
