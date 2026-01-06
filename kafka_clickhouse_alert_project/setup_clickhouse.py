

import os
from clickhouse_driver import Client
from src.config import CLICKHOUSE_HOST, CLICKHOUSE_PORT,CLICKHOUSE_USER,  CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,\
    KAFKA_BROKER, KAFKA_SOURCE_TOPIC,KAFKA_GROUP_NAME,KAFKA_SOURCE_TABLE\
        ,MERGETREE_STORAGE_TABLE,MATERIALIZED_VIEW,STATUS_CHANGE_MVIEW,\
        KAFKA_STATUS_TABLE,TEMP_ALERT_MVIEW,KAFKA_TEMP_TARGET_TABLE,KAFKA_STATUS_TOPIC,KAFKA_TEMP_TOPIC,\
                KAFKA_TEMP_GROUP_NAME,KAFKA_STATUS_GROUP_NAME



def execute_sql_file(filename):
    """ 讀取 SQL 檔案並執行 """
    with open(filename, 'r', encoding='utf-8') as file:
        sql = file.read()
        # 替換 SQL 內的 {KAFKA_BROKER} 和 {KAFKA_TOPIC}
        sql = sql.format(
            CLICKHOUSE_DATABASE=CLICKHOUSE_DATABASE,
            KAFKA_SOURCE_TABLE=KAFKA_SOURCE_TABLE,
            MERGETREE_STORAGE_TABLE=MERGETREE_STORAGE_TABLE,
            MATERIALIZED_VIEW=MATERIALIZED_VIEW,
            KAFKA_BROKER=KAFKA_BROKER, 
            KAFKA_SOURCE_TOPIC=KAFKA_SOURCE_TOPIC,
            KAFKA_GROUP_NAME=KAFKA_GROUP_NAME,
            STATUS_CHANGE_MVIEW =STATUS_CHANGE_MVIEW,
            KAFKA_TEMP_GROUP_NAME =KAFKA_TEMP_GROUP_NAME,
            KAFKA_STATUS_GROUP_NAME=KAFKA_STATUS_GROUP_NAME,
            KAFKA_STATUS_TOPIC=KAFKA_STATUS_TOPIC,
            KAFKA_STATUS_TABLE= KAFKA_STATUS_TABLE,
            KAFKA_TEMP_TOPIC =KAFKA_TEMP_TOPIC,
            TEMP_ALERT_MVIEW =TEMP_ALERT_MVIEW,
            KAFKA_TEMP_TARGET_TABLE=KAFKA_TEMP_TARGET_TABLE
        )
    print(sql)
    client = Client(host=CLICKHOUSE_HOST,port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    print(client)
    client.execute(sql)
    # print(f"✅ SQL 文件 `{filename}` 已執行完成")

if __name__ == "__main__":
    print(os.getcwd())
    # print(KAFKA_GROUP_NAME)
    execute_sql_file("./sql/01_create_kafka_source_table.sql")
    print('scuess 1')
    execute_sql_file("./sql/02_create_mergetree_storage_table.sql")
    print('scuess 2 ') 
    execute_sql_file("./sql/03_create_materialized_view.sql")
    print('scuess 3')
    execute_sql_file("./sql/04_create_target_kafka_table.sql")
    print('scuess 4')
    execute_sql_file("./sql/05_compute_status_change_window.sql")
    print('scuess 5')
    execute_sql_file("./sql/04_1_create_target_temp_kafka_table.sql")
    print('scuess 6')
    execute_sql_file("./sql/05_1_compute_status_change_window_temp.sql")
    print('scuess 7')


