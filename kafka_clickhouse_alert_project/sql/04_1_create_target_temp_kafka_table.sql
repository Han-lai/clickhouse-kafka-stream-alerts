CREATE TABLE  IF NOT EXISTS  `{CLICKHOUSE_DATABASE}`.`{KAFKA_TEMP_TARGET_TABLE}` (
    unique_id String,
    source_kafka_timestamp  DateTime64(6),
    region String,
    product_line String,
    deviceid String,
    device_timestamp DateTime64(6),
    status_flags  Array(Int32),
    temp  Float32,
    weekly_avg_temp  Float64,
    temp_alert String,
    errcode String,
    porcess_detected_time DateTime64(6)

) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '{KAFKA_BROKER}',  -- 替換為你的 Kafka 服務地址
    kafka_topic_list = '{KAFKA_TEMP_TOPIC}',     -- 替換為你的目標 Kafka Topic
    kafka_group_name = '{KAFKA_TEMP_GROUP_NAME}', -- 添加這一行
    kafka_format = 'JSONEachRow'                  -- 以 JSON 格式發送數據
    -- kafka_flush_interval_ms = 1000                -- 每秒刷新一次數據

