from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 创建 SparkSession 时指定 JAR 包路径，补充新增的两个 jar 包路径
spark = SparkSession.builder \
    .appName("KafkaConsumerBatchExample") \
    .config("spark.jars", "/home/hadoop/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka-clients-3.4.1.jar,/home/hadoop/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/commons-pool2-2.11.1.jar,/home/hadoop/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka_2.12-3.6.1.jar") \
    .getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("WARN")

# 定义 Kafka 连接参数
kafka_params = {
    "kafka.bootstrap.servers": "192.168.80.128:9092",  # Kafka 服务器地址
    "subscribe": "data",  # 订阅的主题
    "startingOffsets": "earliest",  # 从最早的消息开始消费
    "failOnDataLoss": "false"  # 忽略数据丢失错误
}

# 从 Kafka 读取数据（批处理模式，使用 read 而不是 readStream）
df = spark.read \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# 打印原始数据结构
print("原始数据结构:")
df.printSchema()

# 定义新的 JSON 消息结构
json_schema = StructType([
    StructField("ID", StringType()),
    StructField("年份", StringType()),
    StructField("地区", StringType()),
    StructField("行政区划代码", StringType()),
    StructField("所属省份", StringType()),
    StructField("所属地域", StringType()),
    StructField("长江经济带", StringType()),
    StructField("专利申请总量", StringType()),
    StructField("专利申请_发明专利", StringType()),
    StructField("专利申请_实用新型", StringType()),
    StructField("专利申请_外观设计", StringType()),
    StructField("专利授权总量", StringType()),
    StructField("专利授权_发明专利", StringType()),
    StructField("专利授权_实用新型", StringType()),
    StructField("专利授权_外观设计", StringType()),
    StructField("Unnamed: 15", StringType())
])

# 解析 Kafka 消息中的 value 字段
parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("parsed_value", from_json(col("value"), json_schema)) \
    .select("key", "parsed_value.*")

# 打印解析后的数据结构
print("解析后的数据结构:")
parsed_df.printSchema()

# 输出到控制台（批处理模式直接展示数据，可根据需要调整输出方式，比如写入文件等）
print("解析后的数据内容:")
parsed_df.show(truncate=False)