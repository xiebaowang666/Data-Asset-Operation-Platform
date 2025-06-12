#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import datetime
import logging
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import argparse
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
import csv

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kafka_monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Kafka_Monitor")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="Kafka数据监控脚本")
    parser.add_argument("--master", type=str, default="local[*]", help="Spark master URL")
    parser.add_argument("--retries", type=int, default=3, help="读取失败重试次数")
    parser.add_argument("--interval", type=int, default=0, help="监控间隔(秒)，0表示执行一次后退出")
    parser.add_argument("--threshold", type=float, default=0.1, help="数据变化阈值(百分比)")
    return parser.parse_args()


def retry_operation(operation, max_retries=3, wait_time=2):
    """通用重试装饰器"""

    def wrapper(*args, **kwargs):
        for attempt in range(max_retries):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                logger.warning(f"操作失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time * (attempt + 1))  # 指数退避
                else:
                    logger.error(f"操作失败，已达最大重试次数: {str(e)}")
                    raise

    return wrapper


@retry_operation
def read_kafka_data(spark):
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

    return parsed_df


def monitor_data(df, previous_stats=None, change_threshold=0.1):
    """监控数据并生成报告"""
    try:
        # 获取基础信息
        data_count = df.count()

        # 生成监控报告
        monitor_info = {
            "监控时间": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "数据行数": data_count,
            "数据状态": "正常" if data_count > 0 else "空数据"
        }

        # 检查数据变化
        if previous_stats and data_count > 0:
            prev_count = previous_stats.get("数据行数", 0)
            if prev_count > 0:
                change_ratio = abs(data_count - prev_count) / prev_count
                if change_ratio > change_threshold:
                    monitor_info["数据变化"] = f"变化率: {change_ratio:.2%}"
                    logger.warning(f"数据变化超过阈值: {change_ratio:.2%} (前: {prev_count}, 现: {data_count})")

        return monitor_info
    except Exception as e:
        logger.error(f"监控数据时发生错误: {str(e)}")
        return {
            "错误时间": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "错误类型": type(e).__name__,
            "错误信息": str(e)
        }


def plot_data(monitor_history):
    """绘制数据行数随时间变化的折线图"""
    timestamps = [entry["监控时间"] for entry in monitor_history if "监控时间" in entry]
    data_counts = [entry["数据行数"] for entry in monitor_history if "数据行数" in entry]

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

    plt.figure(figsize=(10, 6))
    plt.plot(timestamps, data_counts, marker='o')
    plt.title('数据行数随时间变化')
    plt.xlabel('监控时间')
    plt.ylabel('数据行数')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    # 保存可视化结果
    save_path = "/home/hadoop/大数据处理实训/数据监控结果/data_change_plot.png"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.savefig(save_path)
    plt.close()


def save_to_csv(monitor_result, csv_path):
    """将监控结果保存为CSV文件"""
    # 确保目录存在
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # 确定CSV文件的表头
    if "错误信息" in monitor_result:
        headers = ["错误时间", "错误类型", "错误信息"]
    else:
        headers = ["监控时间", "数据行数", "数据状态", "数据变化" if "数据变化" in monitor_result else ""]

    # 写入CSV文件
    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)

        # 如果文件为空，写入表头
        if os.path.getsize(csv_path) == 0:
            writer.writeheader()

        # 写入数据
        writer.writerow(monitor_result)


def main():
    """主函数"""
    args = parse_arguments()

    # 初始化Spark配置
    conf = SparkConf().setAppName("Kafka_Data_Monitoring")
    conf = conf.setMaster(args.master)
    conf = conf.set("dfs.client.socket-timeout", "60000")  # 增加HDFS客户端超时时间
    conf = conf.set("spark.network.timeout", "120s")  # 增加网络超时时间

    # 创建 SparkSession 时指定 JAR 包路径，补充新增的两个 jar 包路径
    spark = SparkSession.builder \
        .appName("KafkaConsumerBatchExample") \
        .config("spark.jars",
                "/home/hadoop/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka-clients-3.4.1.jar,/home/hadoop/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/commons-pool2-2.11.1.jar,/home/hadoop/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.5.jar,/home/hadoop/spark/jars/kafka_2.12-3.6.1.jar") \
        .config(conf=conf) \
        .getOrCreate()

    # 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    previous_stats = None
    monitor_history = []

    try:
        # 循环监控（如果设置了间隔）
        while True:
            logger.info("开始监控 Kafka 数据")

            # 读取 Kafka 数据
            try:
                parsed_df = read_kafka_data(spark)
            except Exception as e:
                monitor_result = {
                    "错误时间": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "错误类型": type(e).__name__,
                    "错误信息": str(e)
                }
            else:
                # 执行监控
                monitor_result = monitor_data(parsed_df, previous_stats, args.threshold)
                previous_stats = monitor_result
                monitor_history.append(monitor_result)

            # 格式化打印监控报告
            print("=" * 50)
            if "错误信息" in monitor_result:
                print("【错误监控报告】")
            else:
                print("【数据监控报告】")
            print("=" * 50)
            for key, value in monitor_result.items():
                print("{0}: {1}".format(key, value))
            print("=" * 50)

            # 数据预览（非空数据）
            if "数据行数" in monitor_result and monitor_result["数据行数"] > 0:
                print("\n数据预览（前5行）:")
                parsed_df.limit(5).show(truncate=False)

            # 结构化展示（DataFrame）
            if "错误信息" not in monitor_result:
                monitor_df = spark.createDataFrame([monitor_result])
                print("\n结构化监控数据:")
                monitor_df.show(truncate=False)

            # 保存监控结果为CSV文件
            csv_path = "/home/hadoop/大数据处理实训/数据监控结果/monitor_result.csv"
            save_to_csv(monitor_result, csv_path)

            # 如果没有设置间隔，则退出循环
            if args.interval <= 0:
                break

            # 等待下一次监控
            logger.info(f"等待 {args.interval} 秒后进行下一次监控...")
            time.sleep(args.interval)

        # 绘制可视化图表
        plot_data(monitor_history)

    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        print("=" * 50)
        print("【程序异常】")
        print("=" * 50)
        print("错误时间: {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        print("错误类型: {0}".format(type(e).__name__))
        print("错误信息: {0}".format(str(e)))
        print("=" * 50)
        spark.stop()
    finally:
        # 释放资源
        if 'spark' in locals():
            spark.stop()
        logger.info("SparkSession已关闭")


if __name__ == "__main__":
    main()