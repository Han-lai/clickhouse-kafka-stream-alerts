# config.py
import os

# Kafka 配置
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')  # Kafka Broker 地址
# 來源 Kafka topic，存放來自設備的原始狀態數據
KAFKA_SOURCE_TOPIC = os.getenv('KAFKA_SOURCE_TOPIC', 'device_status_events')
# ---------------------------------------------------
KAFKA_GROUP_NAME = 'kafka_ch_consumer'
KAFKA_STATUS_GROUP_NAME = 'kafka_ch_producer_status'
KAFKA_TEMP_GROUP_NAME = 'kafka_ch_producer_temp'
# Kafka topic，存放計算完成的設備狀態變更數據
KAFKA_STATUS_TOPIC = os.getenv('KAFKA_STATUS_TOPIC', 'device_status_alerts')
KAFKA_TEMP_TOPIC = os.getenv('KAFKA_TEMP_TOPIC', 'device_temp_alerts')

#-----------------------------------------------------------------------------------------------------
# ClickHouse 配置
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')  # ClickHouse 服務的主機
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT', '9000')  # ClickHouse 端口
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')  # 用戶名
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'your_password')  # 密碼
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'device_monitoring')  # 預設資料庫名稱
# ----------------------------------------------------------------
'''順序是kafka source 流進clickhouse kafka engine ,mview把資料流到mergetree確認資料'''
#-----------------------------------------------------------------
# Kafka 來源資料進入 ClickHouse 的 Kafka Engine 表，再透過 Materialized View 流入 MergeTree 表
KAFKA_SOURCE_TABLE = "device_source_kafka_engine"  # Kafka Engine 表
MERGETREE_STORAGE_TABLE = "device_source_mergetree"  # MergeTree 存儲表
MATERIALIZED_VIEW = "mv_source_kafka_2_source_mergetree"  # Materialized View 負責將 Kafka 資料轉存至 MergeTree

# 從 MergeTree 取得資料，在 Materialized View 中計算狀態變更，將變動資料傳送到 Kafka 另一個 topic
STATUS_CHANGE_MVIEW = "mv_process_status_change_window_2_kafka_target"  # Materialized View 計算狀態變更
KAFKA_STATUS_TABLE = "device_status_change_target_kafka_engine"  # Kafka Engine 目標表，存放計算後的狀態變更
# MERGETREE_TARGET_TABLE = "process_device_status_mergetree"  # MergeTree 存儲表，存放最終前後處理的時間差數據

# -------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------
'''順序是從mergetree計算指標 ,mview把資料流到kafka_engine'''
#-----------------------------------------------------------------

# 從 MergeTree 取得資料，在 Materialized View 中計算狀態變更，將變動資料傳送到 Kafka 另一個 topic
TEMP_ALERT_MVIEW = "mv_process_temp_alert_window_2_kafka_target"  # Materialized View 計算狀態變更
KAFKA_TEMP_TARGET_TABLE = "device_temp_alert_target_kafka_engine"  # Kafka Engine 目標表，存放計算後的狀態變更



# ---------
# KAFKA_TARGET_consumer_TABLE = "mes_status_change_target_consumer_kafka_engine"  # Kafka Engine 目標表，存放計算後的狀態變更