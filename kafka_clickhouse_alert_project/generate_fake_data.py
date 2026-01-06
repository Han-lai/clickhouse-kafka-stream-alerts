import json
import random
import time
import uuid  # 引入 uuid 模組來生成唯一 ID
from datetime import datetime,timedelta
from confluent_kafka import Producer
from src import config   # 引入 config.py


# 使用 config.py 中的 Kafka 設定
KAFKA_BROKER = config.KAFKA_BROKER
DEVICE_TOPIC = config.KAFKA_SOURCE_TOPIC
print(KAFKA_BROKER, DEVICE_TOPIC)

conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(conf)

# 設備狀態與錯誤碼對應 (共16個狀態)
device_status_map = {
    1000: "PRD",                            # 生產狀態
    # 1100: "PRD\\Regular work",              # 正常工作狀態
    # 1200: "PRD\\Idle",                      # 正常工作但閒置狀態
    # 1300: "PRD\\Waiting for material",      # 正常工作但等待材料
    2000: "SBY",                            # 待機狀態
    # 2100: "SBY\\No operator",               # 待機且無操作員
    # 2200: "SBY\\Material shortage",         # 待機且材料短缺
    # 2201: "SBY\\No product\\Blocked",       # 待機且無產品、被阻塞
    3000: "ENG",                            # 設備在工程狀態（如調整、設定）
    # 3100: "ENG\\Calibration",               # 工程狀態，進行校準
    # 3200: "ENG\\Testing",                   # 工程狀態，進行測試
    4000: "SDT",                            # 停機狀態
    # 4300: "SDT\\Preventive maintenance",    # 停機且進行預防性維護
    # 4400: "SDT\\Emergency repair",          # 停機且進行緊急維修
    5000: "USD",                            # 非生產狀態
    # 5300: "USD\\Repair",                    # 設備在修理中
    # 5400: "USD\\Out-of-spec input material" # 輸入材料不合規格
}


# 模擬設備數據
def generate_fake_data(device_id):
    status_code = random.choice(list(device_status_map.keys()))
    errcode = f"ERR_{random.randint(100, 105)}" if status_code >= 5000 else None
    # 生成 ISO 格式的時間戳
    # iso_timestamp = datetime.utcnow().isoformat()
    # clickhouse_timestamp = datetime.fromisoformat(iso_timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
    now = datetime.utcnow()
    one_week_ago = now - timedelta(days=7)
    random_seconds = random.randint(0, 7 * 24 * 60 * 60)  # 過去一周的秒數
    random_time = one_week_ago + timedelta(seconds=random_seconds)

    iso_timestamp = random_time.isoformat()
    clickhouse_timestamp = random_time.strftime('%Y-%m-%d %H:%M:%S.%f')
    print(clickhouse_timestamp)
    message = {
        
        "unique_id": str(uuid.uuid4()),  # 為每一筆資料生成唯一的 UUID,
        "region": random.choice(["North", "South", "East", "West"]),
        "product_line": random.choice(["Line_A", "Line_B", "Line_C"]),
        "message_name": "device_status_update",
        "deviceid": device_id,
        "device_status": status_code,
        "errcode": errcode,
        "temp": round(random.uniform(40, 100), 2),
        "pressure": round(random.uniform(800, 1000), 2),
        "device_timestamp": clickhouse_timestamp

    }
    return message

# 每個機台生成 10 筆資料
num_devices = 10# 機台數量
for device_id in range(1, num_devices + 1):
    for _ in range(100):  # 每個機台生成 5 筆資料
        data = generate_fake_data(f"device_{device_id}")
        data_bytes = json.dumps(data, indent=2)

        # print(f"Sending: {data_bytes}")
        print(f"Generated JSON: {data_bytes}")  # 打印生成的 JSON 消息
        producer.produce(DEVICE_TOPIC, value=data_bytes)
        producer.flush()
        time.sleep(random.uniform(1, 2))  # 隨機等待 1-2 秒
