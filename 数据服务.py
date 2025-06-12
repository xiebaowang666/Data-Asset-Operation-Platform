import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import matplotlib

matplotlib.use('Agg')  # 设置非交互后端
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
import seaborn as sns

# 创建SparkSession
spark = SparkSession.builder \
    .appName("ScienceDataServiceEnhanced") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.80.128:9000") \
    .config("spark.jars",
            "/home/hadoop/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka-clients-3.4.1.jar,/home/hadoop/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/commons-pool2-2.11.1.jar,/home/hadoop/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka_2.12-3.6.1.jar") \
    .getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("WARN")

# 忽略 Kafka 配置未使用的警告
spark.conf.set("spark.sql.streaming.metricsEnabled", "false")

# 定义Kafka连接参数 - 移除不必要的配置
kafka_params = {
    "kafka.bootstrap.servers": "192.168.80.128:9092",
    "subscribe": "data"
}

# 从Kafka读取数据
df = spark.read \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# 打印原始数据结构
print("原始数据结构:")
df.printSchema()

# 定义JSON消息结构
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

# 解析Kafka消息中的value字段
parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("parsed_value", F.from_json(F.col("value"), json_schema)) \
    .select("key", "parsed_value.*")

# 转换所有数值类型为Double
numeric_columns = [
    "专利申请总量", "专利申请_发明专利", "专利申请_实用新型", "专利申请_外观设计",
    "专利授权总量", "专利授权_发明专利", "专利授权_实用新型", "专利授权_外观设计"
]

for col_name in numeric_columns:
    parsed_df = parsed_df.withColumn(col_name, F.col(col_name).cast("double"))

# 数据过滤：移除关键列中的NULL值
filtered_df = parsed_df.filter(
    F.col("地区").isNotNull() &
    F.col("所属地域").isNotNull() &
    F.col("专利申请总量").isNotNull() &
    F.col("专利授权总量").isNotNull()
)

# 新增计算列：专利授权率
df_with_metrics = filtered_df.withColumn(
    "专利授权率",
    F.when(F.col("专利申请总量") == 0, 0.0)
    .otherwise(F.round(F.col("专利授权总量") / F.col("专利申请总量"), 4))
)

# 统计各省份专利申请Top3地区
window_spec = Window.partitionBy("所属省份").orderBy(F.col("专利申请总量").desc())
top_regions = df_with_metrics.withColumn(
    "rank", F.rank().over(window_spec)
).filter(F.col("rank") <= 3)

# 按所属地域统计专利申请总量占比
total_applications = df_with_metrics.agg(F.sum("专利申请总量").alias("total")).collect()[0]["total"]

regional_summary = df_with_metrics.groupBy("所属地域") \
    .agg(F.sum("专利申请总量").alias("total_applications")) \
    .withColumn("占比", F.round(F.col("total_applications") / total_applications, 4)) \
    .filter(F.col("所属地域").isNotNull() & (F.col("total_applications") > 0)) \
    .orderBy(F.desc("total_applications"))


# ====================== 可视化功能（修复中文显示） ======================
def plot_regional_distribution(data):
    # 确保目标文件夹存在
    target_dir = "/home/hadoop/大数据处理实训/数据服务可视化图"
    os.makedirs(target_dir, exist_ok=True)

    # 指定中文字体路径（系统已验证存在）
    font_path = "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"
    fm.fontManager.addfont(font_path)  # 加载字体到FontManager

    # 强制设置字体（解决标签方框问题）
    plt.rcParams["font.family"] = "WenQuanYi Micro Hei"
    plt.rcParams["axes.unicode_minus"] = False  # 修复负号显示
    plt.rcParams["figure.dpi"] = 300  # 提高分辨率

    # 设置图表风格
    sns.set(style="whitegrid")

    # 转换为Pandas DataFrame
    pandas_df = data.toPandas()

    # 处理空数据情况
    if pandas_df.empty:
        print("警告：用于可视化的数据为空")
        return

    # 绘制饼图（明确设置中文文本字体）
    plt.figure(figsize=(10, 6))
    plt.pie(
        pandas_df["total_applications"],
        labels=pandas_df["所属地域"],
        autopct='%1.1f%%',
        startangle=90,
        colors=sns.color_palette('pastel'),
        textprops={"fontfamily": "WenQuanYi Micro Hei"}  # 强制标签字体
    )
    plt.axis('equal')  # 保持饼图圆形
    plt.title("各区域专利申请总量占比", fontsize=14, fontfamily="WenQuanYi Micro Hei")  # 强制标题字体
    plt.tight_layout()

    # 保存图片（自动覆盖旧文件）
    image_path = os.path.join(target_dir, "regional_patent_distribution.png")
    plt.savefig(image_path, bbox_inches="tight")  # 确保标签不被截断
    print(f"可视化图片已保存至：{image_path}")
    plt.close()


# ====================== 功能调用 ======================
# 数据预览（原始数据）
print("===== 原始数据预览 =====")
parsed_df.show(5, truncate=False)

# 各地区专利申请总量总和
print("\n===== 各地区专利申请总量总和 =====")
df_with_metrics.groupBy("地区") \
    .agg(F.sum("专利申请总量").alias("total_applications")) \
    .orderBy(F.desc("total_applications")) \
    .show(5, truncate=False)

# 专利授权率最高的前10条记录
print("\n===== 专利授权率最高的前10条记录 =====")
df_with_metrics.orderBy(F.desc("专利授权率")) \
    .select("地区", "年份", "专利申请总量", "专利授权总量", "专利授权率") \
    .show(10, truncate=False)

# 各省份Top3地区
print("\n===== 各省份专利申请Top3地区 =====")
top_regions.select("所属省份", "地区", "专利申请总量", "rank") \
    .show(truncate=False)

# 区域占比统计
print("\n===== 所属地域专利申请总量占比 =====")
regional_summary.show(truncate=False)

# 执行可视化（捕获潜在异常）
try:
    plot_regional_distribution(regional_summary)
except Exception as e:
    print(f"可视化执行异常: {str(e)}")
    plt.close('all')

# 停止SparkSession
spark.stop()