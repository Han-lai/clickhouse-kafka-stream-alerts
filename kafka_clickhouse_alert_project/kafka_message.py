from confluent_kafka import Consumer, KafkaException
from datetime import datetime

def get_kafka_metadata(bootstrap_servers, topic):
    from confluent_kafka.admin import AdminClient
    
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    
    # 获取 topic metadata
    metadata = admin_client.list_topics(timeout=10)
    topic_metadata = metadata.topics.get(topic)
    
    if topic_metadata is None:
        print(f"Topic {topic} not found.")
        return None
    
    print(f"Topic: {topic}, Partitions: {len(topic_metadata.partitions)}")
    for partition_id, partition_metadata in topic_metadata.partitions.items():
        print(f"Partition {partition_id}: Leader {partition_metadata.leader}, Replicas {partition_metadata.replicas}")

def consume_messages(bootstrap_servers, group_id, topic):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    
    consumer.subscribe([topic])
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            timestamp_type, timestamp = msg.timestamp()
            # 提取 Kafka Metadata Timestamp
            kafka_timestamp = timestamp
            
            # 获取消息的内容，并解析出 `source_kafka_timestamp`
            message_value = msg.value().decode('utf-8')
            print(f"Message Value: {message_value}")
            # 假设 message_value 是 JSON 格式
            import json
            message_data = json.loads(message_value)
            source_kafka_timestamp_str = message_data.get("source_kafka_timestamp", None)
            
            if source_kafka_timestamp_str:
                # 将 source_kafka_timestamp 字符串转换为时间戳
                source_kafka_timestamp = datetime.strptime(source_kafka_timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                source_kafka_timestamp = int(source_kafka_timestamp.timestamp() * 1000)  # 转换为毫秒时间戳
                
                # 计算 Kafka Metadata Timestamp 与 source_kafka_timestamp 之间的时间差
                time_diff_ms = kafka_timestamp - source_kafka_timestamp
                print(f"Kafka Metadata Timestamp: {kafka_timestamp}")
                print(f"Source Kafka Timestamp: {source_kafka_timestamp}")
                print(f"Time Difference (ms): {time_diff_ms}")
            else:
                print("source_kafka_timestamp not found in message.")
                
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "localhost:9092"  # Replace with your Kafka broker address
    TOPIC = "device_status_change_alerts"  # Replace with your topic name
    GROUP_ID = "kafka_py_consumer"
    
    print("Fetching Kafka metadata...")
    get_kafka_metadata(BOOTSTRAP_SERVERS, TOPIC)
    
    print("\nStarting Kafka consumer...")
    consume_messages(BOOTSTRAP_SERVERS, GROUP_ID, TOPIC)
