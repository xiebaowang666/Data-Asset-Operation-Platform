from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField
import matplotlib.pyplot as plt
from pyspark.sql.functions import from_json, col
import matplotlib.font_manager as fm
import os

# 创建 SparkSession 时指定 JAR 包路径
spark = SparkSession.builder \
    .appName("DataSecurityFromKafka") \
    .config("spark.jars", "/home/hadoop/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka-clients-3.4.1.jar,/home/hadoop/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/commons-pool2-2.11.1.jar,/home/hadoop/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka_2.12-3.6.1.jar") \
    .getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("WARN")

# 定义 Kafka 连接参数
kafka_params = {
    "kafka.bootstrap.servers": "192.168.80.128:9092",
    "subscribe": "data",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

# 从 Kafka 读取数据
df = spark.read.format("kafka").options(**kafka_params).load()

# 解析 JSON 数据
json_schema = StructType([
    StructField("ID", StringType()),
    StructField("年份", StringType()),
    StructField("地区", StringType()),
    StructField("行政区划代码", StringType()),
    StructField("所属省份", StringType()),
    StructField("所属地域", StringType()),  # 确认该字段存在且数据正确
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

parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("parsed_value", from_json(col("value"), json_schema)) \
    .select("key", "parsed_value.*")

# 数据安全操作
def desensitize_province(province):
    if province:
        return province[0] + '*' * (len(province) - 1)
    else:
        return None

desensitize_udf = F.udf(desensitize_province, StringType())
masked_df = parsed_df.withColumn("所属省份_脱敏", desensitize_udf(F.col("所属省份")))
filtered_df = masked_df.filter(F.col("长江经济带") != "1")

# 安全写入本地
try:
    filtered_df.write.partitionBy("所属地域") \
        .csv("file:///home/hadoop/大数据处理实训/safe_output", mode="overwrite", header=True)
    print("数据安全处理并写入成功")
except Exception as e:
    print(f"数据写入失败: {str(e)}")
    spark.stop()
    exit(1)

# 数据可视化（关键修复部分）
font_path = "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"
try:
    font = fm.FontProperties(fname=font_path)
    fm.fontManager.addfont(font_path)
    plt.rcParams["font.family"] = font.get_name()
except Exception as e:
    print(f"字体加载失败: {e}, 使用默认字体")
    plt.rcParams["font.family"] = "DejaVu Sans"

plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题
plt.rcParams["figure.dpi"] = 300

# 筛选出只包含中部、西部、东部的数据
selected_areas = ["中部", "西部", "东部"]
selected_df = filtered_df.filter(F.col("所属地域").isin(selected_areas))

# 修复数据分组逻辑（确保所属地域字段正确分组）
grouped_df = selected_df.groupBy("所属地域") \
    .agg(F.count("*").alias("count")).toPandas()

# 绘制柱状图（调整x轴标签显示）
plt.figure(figsize=(10, 6))
bars = plt.bar(grouped_df["所属地域"], grouped_df["count"])

# 添加数值标签
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             str(int(height)),
             ha='center', va='bottom')

plt.xlabel("所属地域")
plt.ylabel("记录数")
plt.title("按所属地域分组的记录数分布")
plt.xticks(rotation=45, ha='right')  # 优化标签旋转角度和对齐
plt.tight_layout()

# 保存图片
output_dir = "/home/hadoop/大数据处理实训/数据安全结果/"
os.makedirs(output_dir, exist_ok=True)
image_path = os.path.join(output_dir, "地域记录数分布.png")
plt.savefig(image_path)
print(f"可视化图片已保存到: {image_path}")

spark.stop()