from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'data',
    bootstrap_servers=['192.168.80.128:9092'],
    auto_offset_reset='earliest',  # 从最早的消息开始消费
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(f"分区: {message.partition}, 偏移量: {message.offset}")
    print(f"键: {message.key}, 值: {message.value}")
    # 按 Ctrl+C 退出循环