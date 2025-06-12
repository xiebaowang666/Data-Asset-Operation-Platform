from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull, trim, length, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.font_manager as fm
import os

# 创建 SparkSession 时指定 JAR 包路径
spark = SparkSession.builder \
    .appName("DataQualityCheck") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.80.128:9000") \
    .config("spark.jars",
            "/home/hadoop/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka-clients-3.4.1.jar,/home/hadoop/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/commons-pool2-2.11.1.jar,/home/hadoop/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka_2.12-3.6.1.jar") \
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

# 从 Kafka 读取数据
try:
    df = spark.read \
        .format("kafka") \
        .options(**kafka_params) \
        .load()

    print("Kafka数据读取成功！")
    print("原始数据结构:")
    df.printSchema()
    print(f"总行数: {df.count()}")

    # 定义 JSON 消息结构
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
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("parsed_value", from_json(col("value"), json_schema)) \
        .select("key", "parsed_value.*")

    print("解析后的数据结构:")
    df.printSchema()

except Exception as e:
    print(f"错误: {str(e)}")
    print("请检查Kafka连接或消息格式")

try:
    # 规则1：完整性校验（统计各列空值数量）
    null_counts = df.select(
        *[count(when(isnull(col(c)), 1)).alias(c) for c in df.columns]
    ).collect()[0].asDict()

    # 规则2：唯一性校验（统计重复记录）
    total_records = df.count()
    distinct_records = df.distinct().count()
    duplicate_records = total_records - distinct_records

    # 规则3：数据类型校验（提取各列数据类型）
    data_types = dict(df.dtypes)

    print("数据校验完成！正在生成报告...")
    print("-" * 50)

except Exception as e:
    print(f"错误：数据校验失败 - {str(e)}")

try:
    null_counts = df.select(
        *[count(when(isnull(col(c)), 1)).alias(c) for c in df.columns]
    ).collect()[0].asDict()

    total_records = df.count()
    distinct_records = df.distinct().count()
    duplicate_records = total_records - distinct_records
    data_types = dict(df.dtypes)

    string_cols = [c for c, t in data_types.items() if t in ['string', 'varchar', 'char']]

    if string_cols:
        # 为中文列名添加反引号（`）包裹
        invalid_string_conditions = [
            f"TRIM(`{c}`) = '' OR `{c}` IS NULL" for c in string_cols
        ]
        invalid_value_count = df.filter(" OR ".join(invalid_string_conditions)).count()
    else:
        invalid_value_count = 0

    print("数据质量报告")
    print("=" * 50)

    print("\n1. 数据基本信息")
    print(f"- 总记录数: {total_records}")
    print(f"- 去重后记录数: {distinct_records}")
    print(f"- 数据类型: {data_types}")

    print("\n2. 完整性校验结果")
    print(f"- 空值总数: {sum(null_counts.values())}")
    print("- 各列空值详情:")
    for col_name, null_count in null_counts.items():
        print(f"  - {col_name}: {null_count}")

    print("\n3. 唯一性校验结果")
    print(f"- 重复记录数: {duplicate_records}")
    print(f"- 唯一性通过率: {(distinct_records / total_records * 100):.2f}%")

    print("\n4. 逻辑校验结果")
    if string_cols:
        print(f"- 无效字符串记录数（空字符串或仅含空格）: {invalid_value_count}")
    else:
        print("警告：数据集中无字符串列，跳过字符串有效性校验")

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

    # 可视化部分
    # 各列空值数量柱状图
    columns = list(null_counts.keys())
    null_values = list(null_counts.values())

    plt.figure(figsize=(10, 6))
    plt.bar(columns, null_values)
    plt.xlabel('Columns')
    plt.ylabel('Null Values Count')
    plt.title('Null Values Count per Column')
    plt.xticks(rotation=45)

    # 保存柱状图
    save_path = "/home/hadoop/大数据处理实训/数据质量结果/"
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    bar_chart_path = os.path.join(save_path, "null_values_count_per_column.png")
    plt.savefig(bar_chart_path)
    plt.show()

    # 重复记录占比饼图
    labels = ['Distinct Records', 'Duplicate Records']
    sizes = [distinct_records, duplicate_records]
    colors = ['lightgreen', 'lightcoral']
    explode = (0, 0.1)

    plt.figure(figsize=(6, 6))
    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.2f%%', startangle=140)
    plt.axis('equal')
    plt.title('Duplicate Records Percentage')

    # 保存饼图
    pie_chart_path = os.path.join(save_path, "duplicate_records_percentage.png")
    plt.savefig(pie_chart_path)
    plt.show()

except Exception as e:
    print(f"错误：报告生成失败 - {str(e)}")