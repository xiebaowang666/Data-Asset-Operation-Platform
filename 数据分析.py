from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import matplotlib.pyplot as plt
import os
import matplotlib.font_manager as fm
import numpy as np
import pandas as pd

# 创建 SparkSession 时指定 JAR 包路径
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

# 从 Kafka 读取数据
df = spark.read \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# 打印原始数据结构
print("原始数据结构:")
df.printSchema()

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
parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("parsed_value", from_json(col("value"), json_schema)) \
    .select("key", "parsed_value.*")

# 打印解析后的数据结构
print("解析后的数据结构:")
parsed_df.printSchema()

# 输出到控制台
print("解析后的数据内容:")
parsed_df.show(truncate=False)

# ----------------------- 处理NaN及类型转换 -----------------------
# 先转为Double类型（保留小数处理能力），并处理NaN（默认转为NULL）
numeric_cols = [
    "专利申请总量", "专利申请_发明专利", "专利申请_实用新型", "专利申请_外观设计",
    "专利授权总量", "专利授权_发明专利", "专利授权_实用新型", "专利授权_外观设计"
]

for col_name in numeric_cols:
    parsed_df = parsed_df.withColumn(col_name, col(col_name).cast(DoubleType()))

# 使用when条件将NULL值转换为0（保留原始数据类型为Double）
for col_name in numeric_cols:
    parsed_df = parsed_df.withColumn(
        col_name,
        when(col(col_name).isNull(), 0.0).otherwise(col(col_name))
    )

# 指定中文字体路径
font_path = "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"
fm.fontManager.addfont(font_path)
plt.rcParams["font.family"] = "WenQuanYi Micro Hei"
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["figure.dpi"] = 300

# 确保保存图片的目录存在
save_dir = "/home/hadoop/大数据处理实训/数据分析可视化图"
os.makedirs(save_dir, exist_ok=True)

try:
    # ---------------------- 1. 数据可视化 - 趋势分析 ----------------------
    # 计算每年专利申请总量和授权总量
    annual_totals = parsed_df.groupBy("年份") \
        .agg(
        _sum("专利申请总量").alias("申请总量"),
        _sum("专利授权总量").alias("授权总量")
    ) \
        .orderBy("年份")

    annual_totals_pd = annual_totals.toPandas()

    # 绘制趋势折线图
    plt.figure(figsize=(12, 6))
    plt.plot(
        annual_totals_pd["年份"],
        annual_totals_pd["申请总量"],
        "o-",
        label="专利申请总量",
        color="#1f77b4",
        linewidth=2
    )
    plt.plot(
        annual_totals_pd["年份"],
        annual_totals_pd["授权总量"],
        "s-",
        label="专利授权总量",
        color="#d62728",
        linewidth=2
    )

    # 添加数据标签
    for i in range(len(annual_totals_pd)):
        x = annual_totals_pd["年份"].iloc[i]
        y1 = annual_totals_pd["申请总量"].iloc[i]
        y2 = annual_totals_pd["授权总量"].iloc[i]
        plt.annotate(f"{int(y1)}", (x, y1), xytext=(0, 10),
                     textcoords="offset points", ha="center", fontsize=9)
        plt.annotate(f"{int(y2)}", (x, y2), xytext=(0, -15),
                     textcoords="offset points", ha="center", fontsize=9)

    plt.title("历年专利申请与授权总量趋势")
    plt.xlabel("年份")
    plt.ylabel("数量")
    plt.xticks(rotation=45, ha="right")
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, "annual_patent_trend.png"))
    plt.close()

    # ---------------------- 2. 数据可视化 - 类型占比分析 ----------------------
    # 计算所有年份的专利类型总量
    total_type = parsed_df.agg(
        _sum("专利申请_发明专利").alias("发明专利"),
        _sum("专利申请_实用新型").alias("实用新型"),
        _sum("专利申请_外观设计").alias("外观设计")
    ).toPandas()

    # 绘制总年专利类型占比饼图
    values = [total_type["发明专利"][0], total_type["实用新型"][0], total_type["外观设计"][0]]
    total = sum(values)

    plt.figure(figsize=(10, 8))
    plt.pie(
        values,
        labels=["发明专利", "实用新型", "外观设计"],
        autopct="%1.1f%%",
        startangle=90,
        explode=(0.05, 0, 0),
        shadow=True,
        colors=["#3498db", "#2ecc71", "#f39c12"]
    )
    plt.axis("equal")
    plt.title(f"专利申请类型总年占比分布\n(总量: {int(total)})")
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, "total_patent_type_pie.png"))
    plt.close()

    # ---------------------- 3. 机器学习建模 - 专利申请预测 ----------------------
    # 按年份和省份分组，准备特征数据
    grouped_data = parsed_df.groupBy("年份", "所属省份") \
        .agg(
        _sum("专利申请总量").alias("专利申请总量"),
        _sum("专利申请_发明专利").alias("发明专利"),
        _sum("专利申请_实用新型").alias("实用新型"),
        _sum("专利申请_外观设计").alias("外观设计")
    ) \
        .orderBy("年份")

    # 特征工程
    feature_cols = ["发明专利", "实用新型", "外观设计"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    labeled_data = assembler.transform(grouped_data)

    # 划分训练集和测试集
    train_data, test_data = labeled_data.randomSplit([0.8, 0.2], seed=42)

    # 定义随机森林回归模型
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="专利申请总量",
        numTrees=100,
        maxDepth=5,
        seed=42
    )

    # 模型参数调优
    param_grid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [50, 100, 150]) \
        .addGrid(rf.maxDepth, [3, 5, 7]) \
        .build()

    cv = CrossValidator(
        estimator=rf,
        estimatorParamMaps=param_grid,
        evaluator=RegressionEvaluator(
            labelCol="专利申请总量",
            predictionCol="prediction",
            metricName="rmse"
        ),
        numFolds=3
    )

    # 训练模型
    model = cv.fit(train_data)
    best_rf = model.bestModel

    # 在测试集上预测
    predictions = best_rf.transform(test_data)

    # 评估模型性能
    evaluator = RegressionEvaluator(
        labelCol="专利申请总量",
        predictionCol="prediction",
        metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    mae = evaluator.setMetricName("mae").evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)

    # 打印评估结果
    print(f"模型评估结果:")
    print(f"均方根误差 (RMSE): {rmse:.2f}")
    print(f"平均绝对误差 (MAE): {mae:.2f}")
    print(f"决定系数 (R²): {r2:.4f}")

    # ---------------------- 4. 模型结果可视化 ----------------------
    # 提取测试集预测结果
    pred_pd = predictions.select("专利申请总量", "prediction").toPandas()

    # 绘制实际值与预测值对比图
    plt.figure(figsize=(10, 6))
    plt.scatter(pred_pd["专利申请总量"], pred_pd["prediction"],
                color="#3498db", alpha=0.7)
    plt.plot([pred_pd["专利申请总量"].min(), pred_pd["专利申请总量"].max()],
             [pred_pd["专利申请总量"].min(), pred_pd["专利申请总量"].max()],
             "r--", linewidth=2)

    plt.title("专利申请总量实际值 vs 预测值")
    plt.xlabel("实际专利申请总量")
    plt.ylabel("预测专利申请总量")
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, "prediction_vs_actual.png"))
    plt.close()

    # 特征重要性分析
    feature_importance = best_rf.featureImportances
    importance_array = feature_importance.toArray()
    importance_pd = pd.DataFrame({
        "特征": feature_cols,
        "重要性": importance_array
    })
    importance_pd = importance_pd.sort_values("重要性", ascending=False)

    # 绘制特征重要性柱状图
    plt.figure(figsize=(8, 6))
    plt.bar(
        importance_pd["特征"],
        importance_pd["重要性"],
        color=["#3498db", "#2ecc71", "#f39c12"],
        zorder=3
    )

    for i, v in enumerate(importance_pd["重要性"]):
        plt.text(i, v + 0.01, f"{v:.4f}", ha="center", fontsize=9)

    plt.title("专利类型对申请总量的特征重要性")
    plt.ylabel("重要性得分")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, "feature_importance.png"))
    plt.close()

    print(f"图表已保存至: {save_dir}")
    print(f"- 趋势图: annual_patent_trend.png")
    print(f"- 总年类型占比饼图: total_patent_type_pie.png")
    print(f"- 预测对比图: prediction_vs_actual.png")
    print(f"- 特征重要性图: feature_importance.png")

except Exception as e:
    print(f"执行过程中出现错误: {e}")
finally:
    # 关闭SparkSession
    spark.stop()