import pandas as pd
import glob
import os

# 文件夹路径
input_folder = '../etl_pipeline/data'
output_folder = './data'

# 读取所有csv
all_files = glob.glob(os.path.join(input_folder, "*_daily.csv"))

# 存放所有股票数据的列表
df_list = []

for file in all_files:
    # 读取csv
    df = pd.read_csv(file)
    df_list.append(df)

# 合并所有 DataFrame
all_data = pd.concat(df_list, ignore_index=True)

# 保存为一个合并文件
all_data.to_csv(os.path.join(output_folder, 'all_stocks_daily.csv'), index=False)
