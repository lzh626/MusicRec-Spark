import pandas as pd
import os

# ================= 配置 =================
# 输入路径：指向 etl_spotify.py 生成的清洗后的文件
INPUT_INTER_PATH = "processed/spotify/interactions.csv" 
INPUT_META_PATH = "processed/spotify/tracks_meta.csv"

# 输出路径
OUT_DIR = "mini_data"
# =======================================

os.makedirs(OUT_DIR, exist_ok=True)

print("1. 正在读取 Spotify 元数据 (Metadata)...")
# 读取前 12,000 行
df_meta = pd.read_csv(INPUT_META_PATH, nrows=12000)

# 【关键检测】确认主键 song_id
if 'song_id' in df_meta.columns:
    id_col = 'song_id'
    print("   -> ✅ 检测到主键: song_id")
elif 'track_id' in df_meta.columns:
    id_col = 'track_id'
    print("   -> ⚠️ 检测到旧主键: track_id (建议检查 ETL 流程)")
else:
    print(f"❌ 错误：找不到 song_id 列。现有列: {df_meta.columns.tolist()}")
    exit(1)

# 【关键检测】确认播放键 real_id
if 'real_id' in df_meta.columns:
    print("   -> ✅ 检测到播放键: real_id (前端播放功能将正常工作)")
else:
    print("   -> ⚠️ 警告: 未检测到 real_id，生成的迷你数据可能无法跳转 Spotify 播放")

print(f"   -> 截取了 {len(df_meta)} 首歌")
valid_ids = set(df_meta[id_col])

print("2. 正在读取 Spotify 交互数据 (Interactions)...")
# 读取前 200,000 行
df_inter = pd.read_csv(INPUT_INTER_PATH, nrows=200000)

# 确认交互表的主键
inter_id_col = id_col if id_col in df_inter.columns else 'track_id'
if inter_id_col not in df_inter.columns:
    print(f"❌ 错误：交互表中找不到 {inter_id_col}。现有列: {df_inter.columns.tolist()}")
    exit(1)

# 过滤：只保留那些出现在元数据里的歌 (保证数据一致性)
df_inter_filtered = df_inter[df_inter[inter_id_col].isin(valid_ids)]
print(f"   -> 截取并过滤后剩余 {len(df_inter_filtered)} 条交互")

print("3. 保存迷你数据集...")
# 保持列名不变，因为 ETL 阶段已经处理好了
df_meta.to_csv(f"{OUT_DIR}/spotify_tracks_mini.csv", index=False)
df_inter_filtered.to_csv(f"{OUT_DIR}/spotify_interactions_mini.csv", index=False)

print("-" * 30)
print("✅ 瘦身完成！请执行以下命令上传到 HDFS：")
print(f"hdfs dfs -rm /input/spotify/*.csv")
print(f"hdfs dfs -put {OUT_DIR}/spotify_tracks_mini.csv /input/spotify/tracks_meta.csv")
print(f"hdfs dfs -put {OUT_DIR}/spotify_interactions_mini.csv /input/spotify/interactions.csv")
print("-" * 30)
