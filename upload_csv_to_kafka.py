from kafka import KafkaProducer
from kafka.errors import KafkaError
import csv
import json
import time

# Kafka 生产者配置
producer = KafkaProducer(
    bootstrap_servers=['192.168.80.128:9092'],
    # 确保 JSON 序列化时不转义非 ASCII 字符
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    retries=3,
    linger_ms=5,
    batch_size=16384,
    max_request_size=10485760,
    request_timeout_ms=30000
)

topic_name = 'data'
csv_file_path = '/home/hadoop/大数据处理实训/science1.csv'

try:
    try:
        # 验证文件编码
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            # 读取前 1024 字节检查是否有编码错误
            sample = file.read(1024)
            print("文件编码验证成功，前 100 个字符:", sample[:100])

            file.seek(0)  # 重置文件指针
            reader = csv.DictReader(file)

            # 处理表头中的 None 值
            headers = [col for col in next(reader) if col is not None]
            print("CSV 表头: {}".format(headers))

            file.seek(0)  # 再次重置，确保从第一行开始
            reader = csv.DictReader(file, fieldnames=headers)  # 使用处理后的表头

            line_count = 0
            start_time = time.time()

            for row in reader:
                if row is None or all(v is None or v == '' for v in row.values()):
                    continue  # 跳过空行

                # 打印前 5 条记录，验证中文处理
                if line_count < 5:
                    print("准备发送第 {} 条记录: {}".format(line_count + 1, row))

                future = producer.send(topic_name, value=row)

                # 可选：同步等待确认（会降低性能）
                # try:
                #     record_metadata = future.get(timeout=10)
                # except KafkaError as e:
                #     print("发送失败: {}".format(e))

                line_count += 1
                if line_count % 1000 == 0:
                    elapsed_time = time.time() - start_time
                    print("已发送 {} 条记录，速度: {:.2f} 条/秒".format(line_count, line_count / elapsed_time))

    except FileNotFoundError:
        raise Exception("找不到文件: {}".format(csv_file_path))
    except UnicodeDecodeError as e:
        raise Exception("文件编码错误，尝试使用其他编码打开: {}".format(e))

    producer.flush()
    print("数据已成功从 {} 上传到 Kafka 主题 {}".format(csv_file_path, topic_name))
    print("总共发送了 {} 条记录".format(line_count))

except Exception as e:
    print("上传数据时出现错误: {}".format(str(e)))
finally:
    if producer:
        producer.close()