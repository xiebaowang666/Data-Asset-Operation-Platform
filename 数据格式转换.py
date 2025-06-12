import pandas as pd


def excel_to_csv(excel_file_path, csv_file_path):
    try:
        # 读取 Excel 文件
        excel_file = pd.ExcelFile(excel_file_path)

        # 获取所有表名
        sheet_names = excel_file.sheet_names
        sheet_names
        all_data = []
        for sheet_name in sheet_names:
            # 获取指定工作表中的数据
            df = excel_file.parse(sheet_name)
            all_data.append(df)

        # 合并所有工作表的数据
        combined_df = pd.concat(all_data, ignore_index=True)

        # 将合并后的数据转换为 CSV 文件并保存
        combined_df.to_csv(csv_file_path, index=False)
        print(f"文件已成功转换并保存为 {csv_file_path}")
    except FileNotFoundError:
        print(f"未找到文件 {excel_file_path}")
    except Exception as e:
        print(f"转换过程中出现错误: {e}")


# 输入 Excel 文件路径，根据实际情况修改
excel_file_path = 'D:/爬虫/pycharm文件/大数据处理实训/science1.xlsx'
# 输出 CSV 文件路径，根据实际情况修改
csv_file_path = 'D:/爬虫/pycharm文件/大数据处理实训/science1.csv'

# 调用函数进行转换
excel_to_csv(excel_file_path, csv_file_path)