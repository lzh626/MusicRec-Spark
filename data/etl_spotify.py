import pandas as pd
import os

# ================= 配置区域 =================
# 请确保你的原始 Spotify 文件放在 data/raw/ 目录下
# 并且文件名要对应 (这里假设是你之前提供的文件名)
RAW_FILE_META = "raw/Spotify_track_features.csv" 
RAW_FILE_ACTS = "raw/Spotify_user_plays.csv"

# 输出目录
OUT_DIR = "processed/spotify"
# ===========================================

print("🚀 开始处理 Spotify 数据 (对齐网易云格式)...")

# 1. 检查文件
if not os.path.exists(RAW_FILE_META) or not os.path.exists(RAW_FILE_ACTS):
    print(f"❌ 错误：找不到原始文件。请确保 {RAW_FILE_META} 和 {RAW_FILE_ACTS} 存在。")
    exit()

# 2. 处理元数据 (Metadata)
print("   -> 读取元数据...")
meta_df = pd.read_csv(RAW_FILE_META)

# 【核心步骤 1】检测并提取真实的 Spotify ID (用于播放)
# 原始 CSV 里通常叫 'spotify_id'，我们把它改名为 'real_id'
if 'spotify_id' in meta_df.columns:
    meta_df.rename(columns={'spotify_id': 'real_id'}, inplace=True)
    print("      ✅ 成功提取 spotify_id 为 real_id (用于播放)")
else:
    # 如果没有 spotify_id，就只能用 track_id 顶替(虽然可能无法播放)
    meta_df['real_id'] = meta_df['track_id']
    print("      ⚠️ 警告：未找到 spotify_id，播放链接可能失效")

# 【核心步骤 2】列名对齐
# 将 track_id 改名为 song_id (用于内部关联)
# 将 name 改名为 title
meta_df = meta_df.rename(columns={
    'track_id': 'song_id',
    'name': 'title'
})

# 【核心步骤 3】补充缺失的标签 (对齐网易云)
meta_df['source'] = 'spotify'       # 来源标签
meta_df['image_url'] = ''           # 空图片 (Spotify不提供直接图片链接)
meta_df['playlist_id'] = 0          # 占位
meta_df['page_crawled'] = 0.0       # 占位

# 填充其他空值
meta_df = meta_df.fillna({
    'title': 'Unknown Track',
    'artist': 'Unknown Artist',
    'real_id': ''
})

# 选择最终输出的列 (顺序尽量美观，不影响 Spark 读取)
# 注意：我们特意输出了 real_id，Spark 读取时要用到
final_cols = ['song_id', 'title', 'artist', 'image_url', 'source', 'real_id']
meta_df_final = meta_df[final_cols]

print(f"   -> 元数据清洗完毕，共 {len(meta_df_final)} 首")

# 3. 处理交互数据 (Interactions)
print("   -> 读取用户行为数据...")
play_df = pd.read_csv(RAW_FILE_ACTS)

# 重命名以匹配 song_id
# 假设原始列是: user_id, track_id, playcount
if 'track_id' in play_df.columns:
    play_df = play_df.rename(columns={'track_id': 'song_id'})

# 4. 数据过滤 (Join)
# 只保留那些在元数据里存在的歌 (防止孤儿数据)
print("   -> 正在对齐数据 (Filtering)...")
valid_ids = set(meta_df_final['song_id'])
merged_df = play_df[play_df['song_id'].isin(valid_ids)]

print(f"   -> 过滤后剩余交互: {len(merged_df)} 条")

# 5. 保存结果
os.makedirs(OUT_DIR, exist_ok=True)

# 保存交互数据
merged_df[['user_id', 'song_id', 'playcount']].to_csv(
    os.path.join(OUT_DIR, "interactions.csv"), index=False
)

# 保存元数据 (去重)
meta_df_final = meta_df_final.drop_duplicates(subset=['song_id'])
meta_df_final.to_csv(
    os.path.join(OUT_DIR, "tracks_meta.csv"), index=False
)

print("-" * 30)
print("✅ Spotify ETL 完成！")
print("📂 输出文件已生成:")
print(f"   1. {os.path.join(OUT_DIR, 'tracks_meta.csv')} (包含 source 和 real_id)")
print(f"   2. {os.path.join(OUT_DIR, 'interactions.csv')}")
print("-" * 30)
