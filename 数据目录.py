from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

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

# ========== 输出数据目录部分 ==========
# 提取字段名并生成数据目录DataFrame
data_catalog = spark.createDataFrame(
    [(field.name, field.dataType.simpleString()) for field in parsed_df.schema.fields],
    ["字段名称", "数据类型"]
)

# 保存数据目录到本地
data_catalog_output = "file:///home/hadoop/大数据处理实训/数据目录相关结果"
data_catalog.write.csv(
    data_catalog_output,
    mode="overwrite",
    header=True
)
print(f"数据目录已保存至：{data_catalog_output}")
# ========================================

# 数据清洗：处理可能的空值（可选）
# 将字符串类型的数值列转换为整数类型
parsed_df = parsed_df.withColumn("专利申请总量", col("专利申请总量").cast(IntegerType()))
parsed_df = parsed_df.withColumn("专利授权总量", col("专利授权总量").cast(IntegerType()))
cleaned_df = parsed_df.na.drop(subset=[
    "专利申请总量", "专利授权总量"
])

# 数据转换：添加地域分区列（示例操作）
processed_df = cleaned_df.withColumn(
    "地域分区",
    F.when(F.col("所属地域") == "东部", "东部地区")
    .when(F.col("所属地域") == "中部", "中部地区")
    .when(F.col("所属地域") == "西部", "西部地区")
    .otherwise("未知地区")
)

# 数据统计：按年份和所属省份统计专利申请总量
year_province_stats = processed_df.groupBy(
    "年份", "所属省份"
).agg(
    F.sum("专利申请总量").alias("总申请量"),
    F.avg("专利授权总量").alias("平均授权量")
).orderBy("年份", ascending=False)

# 数据输出：将结果保存到本地指定目录
output_path = "file:///home/hadoop/大数据处理实训/output/science1_results"

year_province_stats.write.csv(
    output_path,
    mode="overwrite",
    header=True
)

# 打印前50条结果（调试用）
print("处理结果前50条：")
year_province_stats.show(50, truncate=False)

# 将 Spark DataFrame 转换为 Pandas DataFrame 以进行可视化
pandas_df = year_province_stats.toPandas()

# 添加中文字体路径
font_path = "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"
try:
    font = fm.FontProperties(fname=font_path)
    fm.fontManager.addfont(font_path)
    plt.rcParams["font.family"] = font.get_name()
except Exception as e:
    print(f"字体加载失败: {e}, 使用默认字体")
    plt.rcParams["font.family"] = "DejaVu Sans"

plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["figure.dpi"] = 300

# 可视化：按年份统计总申请量
yearly_total_applications = pandas_df.groupby('年份')['总申请量'].sum()

plt.figure(figsize=(10, 6))
yearly_total_applications.plot(kind='bar')
plt.title('按年份统计的专利总申请量')
plt.xlabel('年份')
plt.ylabel('总申请量')
plt.xticks(rotation=45)
plt.tight_layout()

# 保存图形为文件
image_path = "/home/hadoop/大数据处理实训/output/yearly_total_applications.png"
plt.savefig(image_path)
print(f"图形已保存至：{image_path}")

# 停止SparkSession
spark.stop()