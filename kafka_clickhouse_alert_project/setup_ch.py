import os
import logging
from clickhouse_driver import Client
from src.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    KAFKA_BROKER, KAFKA_SOURCE_TOPIC, KAFKA_GROUP_NAME, KAFKA_SOURCE_TABLE,
    MERGETREE_STORAGE_TABLE, MATERIALIZED_VIEW, STATUS_CHANGE_MVIEW,
    KAFKA_STATUS_TABLE, TEMP_ALERT_MVIEW, KAFKA_TEMP_TARGET_TABLE, KAFKA_STATUS_TOPIC, KAFKA_TEMP_TOPIC,
    KAFKA_TEMP_GROUP_NAME, KAFKA_STATUS_GROUP_NAME
)

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("setup_clickhouse.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def execute_sql_file(filename):
    """讀取 SQL 檔案並執行"""
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            sql = file.read()
            # 替換 SQL 內的變數
            sql = sql.format(
                CLICKHOUSE_DATABASE=CLICKHOUSE_DATABASE,
                KAFKA_SOURCE_TABLE=KAFKA_SOURCE_TABLE,
                MERGETREE_STORAGE_TABLE=MERGETREE_STORAGE_TABLE,
                MATERIALIZED_VIEW=MATERIALIZED_VIEW,
                KAFKA_BROKER=KAFKA_BROKER,
                KAFKA_SOURCE_TOPIC=KAFKA_SOURCE_TOPIC,
                KAFKA_GROUP_NAME=KAFKA_GROUP_NAME,
                STATUS_CHANGE_MVIEW=STATUS_CHANGE_MVIEW,
                KAFKA_TEMP_GROUP_NAME=KAFKA_TEMP_GROUP_NAME,
                KAFKA_STATUS_GROUP_NAME=KAFKA_STATUS_GROUP_NAME,
                KAFKA_STATUS_TOPIC=KAFKA_STATUS_TOPIC,
                KAFKA_STATUS_TABLE=KAFKA_STATUS_TABLE,
                KAFKA_TEMP_TOPIC=KAFKA_TEMP_TOPIC,
                TEMP_ALERT_MVIEW=TEMP_ALERT_MVIEW,
                KAFKA_TEMP_TARGET_TABLE=KAFKA_TEMP_TARGET_TABLE
            )
        logging.info(f"Executing SQL file: {filename}")
        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
        client.execute(sql)
        logging.info(f"SQL file `{filename}` executed successfully.")
    except Exception as e:
        logging.error(f"Error executing SQL file `{filename}`: {e}")

def main():
    """主函數"""
    sql_files = [
        "./sql/01_create_kafka_source_table.sql",
        "./sql/02_create_mergetree_storage_table.sql",
        "./sql/03_create_materialized_view.sql",
        "./sql/04_create_target_kafka_table.sql",
        "./sql/05_compute_status_change_window.sql",
        "./sql/04_1_create_target_temp_kafka_table.sql",
        "./sql/05_1_compute_status_change_window_temp.sql"
    ]

    for idx, sql_file in enumerate(sql_files, start=1):
        logging.info(f"Executing step {idx}: {sql_file}")
        execute_sql_file(sql_file)

if __name__ == "__main__":
    main()