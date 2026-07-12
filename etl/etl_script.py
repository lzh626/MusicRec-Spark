import pandas as pd
import os

# ---------------------------------------------------------
# 1. 路径配置 (解决找不到文件的问题)
# ---------------------------------------------------------
# 获取当前脚本所在的文件夹路径
current_dir = os.path.dirname(os.path.abspath(__file__))

# 拼接输入和输出文件的绝对路径
input_path = os.path.join(current_dir, "../data/raw/spotify_millsongdata.csv") 
# 注意：假设你的原始文件放在 data/raw 下，如果不是，请修改上面的路径
# 如果就在同级目录下，直接写文件名即可： os.path.join(current_dir, "spotify_millsongdata.csv")

output_path = os.path.join(current_dir, "cleaned_data.csv")

print(f"正在读取文件: {input_path}")

# ---------------------------------------------------------
# 2. 读取数据
# ---------------------------------------------------------
try:
    df = pd.read_csv(input_path)
    print(f"原始数据量: {len(df)} 行")
except FileNotFoundError:
    print("错误：找不到输入文件，请检查路径！")
    exit()

# ---------------------------------------------------------
# 3. 数据清洗 (针对 Spark 和 MySQL 的优化)
# ---------------------------------------------------------

# (1) 重命名列：去除空格，转小写 (方便数据库建表和Spark读取)
# 假设原始列名可能是 "artist", "song", "link", "text" 等
# 打印一下原始列名看看
print("原始列名:", df.columns.tolist())

# 建议的重命名映射 (根据你的实际CSV列名修改)
# 示例：假设 CSV 里是 'artist', 'song', 'link'
df.rename(columns={
    'artist': 'artist_name',
    'song': 'song_title',
    'link': 'song_url',
    'text': 'lyrics'  # 如果有歌词列
}, inplace=True)

# (2) 去除空值：删除关键信息缺失的行
# subset 参数指定：只有 'song_title' 或 'artist_name' 为空时才删
df = df.dropna(subset=['song_title', 'artist_name'])

# (3) 去重：防止完全重复的数据
df = df.drop_duplicates()

# (4) 添加 ID 列 (如果原始数据没有 ID)
# 很多 CSV 只有歌名没有 ID，我们需要生成一个数字 ID 方便 Spark 计算
# 这里的逻辑是：给每一行生成一个唯一的数字索引，从 1 开始
if 'song_id' not in df.columns:
    df['song_id'] = range(1, len(df) + 1)
    print("已自动生成 song_id 列")

# (5) 字符串清洗 (可选)
# 去除歌名两端的空格，防止 "Hello " 和 "Hello" 被当成两首歌
if 'song_title' in df.columns:
    df['song_title'] = df['song_title'].str.strip()

# ---------------------------------------------------------
# 4. 保存数据
# ---------------------------------------------------------

# index=False 很重要，否则会多保存一列无用的索引
df.to_csv(output_path, index=False, encoding='utf-8')

print("-" * 30)
print("ETL 清洗完成！")
print(f"清洗后数据量: {len(df)} 行")
print(f"文件已保存至: {output_path}")
print("-" * 30)
