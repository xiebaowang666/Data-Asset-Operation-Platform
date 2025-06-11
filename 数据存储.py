from kafka import KafkaConsumer
import json
import pymysql
from pymysql.connections import Connection
from pymysql.cursors import DictCursor

# 配置 Kafka 消费者（假设 Kafka 在同一虚拟机或可通过 localhost 访问）
consumer = KafkaConsumer(
    'data',
    bootstrap_servers=['192.168.80.128:9092'],  # 修改为虚拟机本地 Kafka
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='mysql_consumer_group'
)


# 配置 MySQL 连接（修改为虚拟机本地 MySQL）
def get_mysql_connection() -> Connection:
    return pymysql.connect(
        host='localhost',  # 连接本地 MySQL
        user='root',  # MySQL 用户名
        password='08110707Qxy!',  # MySQL 密码
        database='qxy',  # 数据库名
        charset='utf8mb4',  # 字符集
        cursorclass=DictCursor
    )


# 创建表（如果不存在）
with get_mysql_connection() as conn:
    with conn.cursor() as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS patent_data (
                `ID` INT PRIMARY KEY AUTO_INCREMENT,
                `年份` VARCHAR(20),
                `地区` VARCHAR(50),
                `行政区划代码` VARCHAR(20),
                `所属省份` VARCHAR(50),
                `所属地域` VARCHAR(20),
                `长江经济带` TINYINT,
                `专利申请总量` INT,
                `专利申请_发明专利` INT,
                `专利申请_实用新型` INT,
                `专利申请_外观设计` INT,
                `专利授权总量` INT,
                `专利授权_发明专利` INT,
                `专利授权_实用新型` INT,
                `专利授权_外观设计` INT,
                `Unnamed: 15` VARCHAR(100)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')

# 批量消费并写入 MySQL
batch_size = 1000
records = []

try:
    for message in consumer:
        records.append(message.value)

        if len(records) >= batch_size:
            with get_mysql_connection() as conn:
                with conn.cursor() as cursor:
                    # 构建批量插入语句
                    columns = ['年份', '地区', '行政区划代码', '所属省份', '所属地域', '长江经济带',
                               '专利申请总量', '专利申请_发明专利', '专利申请_实用新型', '专利申请_外观设计',
                               '专利授权总量', '专利授权_发明专利', '专利授权_实用新型', '专利授权_外观设计',
                               'Unnamed: 15']
                    placeholders = ', '.join(['%s'] * len(columns))
                    column_names = ', '.join([f'`{col}`' for col in columns])

                    query = f'''
                        INSERT INTO patent_data ({column_names})
                        VALUES ({placeholders})
                    '''

                    # 批量执行
                    for record in records:
                        # 确保记录中的字段顺序与表结构一致
                        values = [record.get(col) for col in columns]
                        cursor.execute(query, values)

                    conn.commit()
                    print(f"已批量插入 {len(records)} 条记录到 MySQL")

            records = []  # 清空批次

finally:
    consumer.close()