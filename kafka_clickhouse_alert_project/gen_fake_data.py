import json
import random
import time
import uuid
import logging
from datetime import datetime, timedelta
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from src import config
import argparse

# Kafka 配置
KAFKA_BROKER = config.KAFKA_BROKER
DEVICE_TOPIC = config.KAFKA_SOURCE_TOPIC
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all',
    'enable.idempotence': True,
    'retries': 5,
}
producer = Producer(conf)

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("fake_data.log"),
        logging.StreamHandler()
    ]
)

# 設備狀態與錯誤碼對應
device_status_map = {
    1000: "PRD",
    2000: "SBY",
    3000: "ENG",
    4000: "SDT",
    5000: "USD",
}

def generate_fake_data(device_id):
    """生成單筆模擬設備數據"""
    status_code = random.choice(list(device_status_map.keys()))
    errcode = f"ERR_{random.randint(100, 105)}" if status_code >= 5000 else None
    now = datetime.utcnow()
    one_week_ago = now - timedelta(days=7)
    random_seconds = random.randint(0, 7 * 24 * 60 * 60)
    random_time = one_week_ago + timedelta(seconds=random_seconds)

    clickhouse_timestamp = random_time.strftime('%Y-%m-%d %H:%M:%S.%f')
    message = {
        "unique_id": str(uuid.uuid4()),
        "region": random.choice(["North", "South", "East", "West"]),
        "product_line": random.choice(["Line_A", "Line_B", "Line_C"]),
        "message_name": "device_status_update",
        "deviceid": device_id,
        "device_status": status_code,
        "errcode": errcode,
        "temp": round(random.uniform(40, 100), 2),
        "pressure": round(random.uniform(800, 1000), 2),
        "device_timestamp": clickhouse_timestamp,
    }
    return message

def check_kafka_topic():
    """檢查 Kafka Topic 是否存在"""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    metadata = admin_client.list_topics(timeout=10)
    if DEVICE_TOPIC not in metadata.topics:
        raise Exception(f"Topic {DEVICE_TOPIC} does not exist.")
    print(f"Topic {DEVICE_TOPIC} exists with {len(metadata.topics[DEVICE_TOPIC].partitions)} partitions.")

def produce_fake_data(num_devices=10, messages_per_device=100):
    """生成並發送模擬數據到 Kafka"""
    for device_id in range(1, num_devices + 1):
        for _ in range(messages_per_device):
            try:
                data = generate_fake_data(f"device_{device_id}")
                data_bytes = json.dumps(data, indent=2)
                logging.info(f"Generated JSON: {data_bytes}")
                producer.produce(DEVICE_TOPIC, value=data_bytes)
                producer.flush()
            except Exception as e:
                logging.error(f"Error producing message: {e}")
            time.sleep(random.uniform(1, 2))

def main():
    """主函數"""
    parser = argparse.ArgumentParser(description="Generate and send fake data to Kafka.")
    parser.add_argument("--num_devices", type=int, default=10, help="Number of devices to simulate.")
    parser.add_argument("--messages_per_device", type=int, default=100, help="Number of messages per device.")
    args = parser.parse_args()

    check_kafka_topic()
    print("Starting fake data generation...")
    produce_fake_data(num_devices=args.num_devices, messages_per_device=args.messages_per_device)
    print("Fake data generation completed.")

if __name__ == "__main__":
    main()