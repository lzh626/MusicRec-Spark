import math
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, BucketedRandomProjectionLSH
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, log1p, lit, explode, row_number, udf, rand
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from pyspark.ml.linalg import Vectors, VectorUDT

# ================= 配置 =================
MYSQL_URL = "jdbc:mysql://localhost:3306/music_rec_sys?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8&rewriteBatchedStatements=true"
MYSQL_USER = "hadoop_master" 
MYSQL_PWD = "hadoop"

# LSH 分批数量 (5批足够)
LSH_BATCH_COUNT = 5
# =======================================

spark = SparkSession.builder \
    .appName("MusicRecTurbo") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "5") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.local.dir", "/home/hadoop/spark_tmp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(">>> 任务开始 (列名适配版) <<<")

# ==========================================================
# Step 1: 读取与映射 (根据你提供的 CSV 结构)
# ==========================================================
print("Step 1: 读取数据...")

# --- A. 网易云数据 ---
# CSV结构: song_id, title, artist, image_url, playlist_id, page_crawled, source
df_ne_acts = spark.read.csv("hdfs://localhost:9000/input/netease/interactions.csv", header=True, inferSchema=True) \
    .select(col("user_id").cast("string"), col("song_id").cast("string"), col("playcount").cast("int"))

df_ne_meta = spark.read.csv("hdfs://localhost:9000/input/netease/tracks_meta.csv", header=True, inferSchema=True)

# 映射逻辑：
# 1. 内部ID (song_id) = CSV中的 song_id
# 2. 播放ID (original_id) = CSV中的 song_id (网易云两者一致)
# 3. 来源 (source) = 强制设为 "netease" (防止CSV里是空的)
df_ne_meta = df_ne_meta.select(
    col("song_id").cast("string"),
    col("song_id").cast("string").alias("original_id"), 
    col("title"), 
    col("artist"), 
    col("image_url"), 
    lit("netease").alias("source") 
)

# --- B. Spotify 数据 ---
# CSV结构: song_id, title, artist, image_url, source, real_id
df_sp_acts = spark.read.csv("hdfs://localhost:9000/input/spotify/interactions.csv", header=True, inferSchema=True) \
    .select(col("user_id").cast("string"), col("song_id").cast("string"), col("playcount").cast("int"))

df_sp_meta = spark.read.csv("hdfs://localhost:9000/input/spotify/tracks_meta.csv", header=True, inferSchema=True)

# 映射逻辑：
# 1. 内部ID (song_id) = CSV中的 song_id (即原来的 track_id)
# 2. 播放ID (original_id) = CSV中的 real_id (即 spotify_id)
# 3. 来源 (source) = CSV中的 source
df_sp_meta = df_sp_meta.select(
    col("song_id").cast("string"),
    col("real_id").alias("original_id"), # 【关键】使用 real_id 作为播放ID
    col("title"), 
    col("artist"), 
    col("image_url"), 
    col("source")
)

# ==========================================================
# Step 2: 融合
# ==========================================================
print("Step 2: 融合中英文数据...")
# 此时两边的 DataFrame 列顺序完全一致：song_id, original_id, title, artist, image_url, source
ratings_raw = df_ne_acts.union(df_sp_acts)
tracks_raw = df_ne_meta.union(df_sp_meta)

# 级联过滤 (只保留有交互的歌曲)
tracks_raw = tracks_raw.join(ratings_raw, on="song_id", how="left_semi")

# ==========================================================
# Step 3: 索引化
# ==========================================================
print("Step 3: 构建索引...")
ratings_raw = ratings_raw.withColumn("user_id_str", col("user_id").cast("string"))
user_indexer = StringIndexer(inputCol="user_id_str", outputCol="userId_int").setHandleInvalid("skip")
user_model = user_indexer.fit(ratings_raw)
ratings_indexed = user_model.transform(ratings_raw)

# 这里对 song_id (内部ID) 进行索引
song_indexer = StringIndexer(inputCol="song_id", outputCol="songId_int").setHandleInvalid("skip")
song_model = song_indexer.fit(tracks_raw)

ratings_final = song_model.transform(ratings_indexed).withColumn("rating", log1p(col("playcount")))
tracks_final = song_model.transform(tracks_raw)

# ==========================================================
# Step 4: 写入 Songs
# ==========================================================
print("Step 4: 写入 Songs 表...")
# 注意：original_id 已经包含了正确的 网易云ID 和 Spotify ID
songs_to_db = tracks_final.select(
    col("songId_int").cast(IntegerType()).alias("song_id"),
    col("original_id").alias("original_track_id"),
    col("title"), col("artist"), col("image_url"), col("source"),
    lit("Pop").alias("genre"), lit(0.0).alias("energy"), lit("").alias("tags")
).dropDuplicates(["song_id"])

prop = {"user": MYSQL_USER, "password": MYSQL_PWD, "driver": "com.mysql.cj.jdbc.Driver", "batchsize": "10000"}
songs_to_db.write.jdbc(url=MYSQL_URL, table="songs", mode="overwrite", properties=prop)

# ==========================================================
# Step 5: ALS
# ==========================================================
print("Step 5: 训练 ALS...")
als = ALS(maxIter=5, regParam=0.1, rank=5, userCol="userId_int", itemCol="songId_int", ratingCol="rating", implicitPrefs=True, coldStartStrategy="drop")
model = als.fit(ratings_final)

print("   -> 生成推荐并写入...")
user_recs = model.recommendForAllUsers(100) 
recs_to_db = user_recs.select(col("userId_int").alias("user_id"), explode("recommendations").alias("rec")).select(
    col("user_id"), col("rec.songId_int").alias("song_id"), col("rec.rating").alias("rank_score")
)
recs_to_db.write.jdbc(url=MYSQL_URL, table="recommendations", mode="overwrite", properties=prop)

# ==========================================================
# Step 6: LSH (简化版)
# ==========================================================
print(f"Step 6: 计算相似度 (批次: {LSH_BATCH_COUNT})...")

item_factors = model.itemFactors
list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
item_factors_vec = item_factors.withColumn("features_vec", list_to_vector_udf(col("features")))

# 降低哈希桶数，加快速度
brp = BucketedRandomProjectionLSH(inputCol="features_vec", outputCol="hashes", bucketLength=4.0, numHashTables=2)
lsh_model = brp.fit(item_factors_vec)

# 清空表
try:
    spark.createDataFrame([], schema="song_id int, related_song_id int, similarity_score float").write.jdbc(url=MYSQL_URL, table="related_songs", mode="overwrite", properties=prop)
except: pass

weights = [1.0] * LSH_BATCH_COUNT
batches = item_factors_vec.randomSplit(weights, seed=42)

for i, batch_df in enumerate(batches):
    print(f"   -> LSH 批次 {i+1}/{LSH_BATCH_COUNT}...")
    similarity_df = lsh_model.approxSimilarityJoin(batch_df, item_factors_vec, 1.0, distCol="EuclideanDistance")
    
    raw_recs = similarity_df.filter(col("datasetA.id") != col("datasetB.id")).select(
        col("datasetA.id").alias("song_id"),
        col("datasetB.id").alias("related_song_id"),
        (lit(1.0) / (lit(1.0) + col("EuclideanDistance"))).alias("similarity_score")
    )
    
    windowSpec = Window.partitionBy("song_id").orderBy(col("similarity_score").desc())
    top5_recs = raw_recs.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") <= 5).drop("rank")
    
    try:
        top5_recs.write.jdbc(url=MYSQL_URL, table="related_songs", mode="append", properties=prop)
    except: pass

# ==========================================================
# Step 7: 补漏
# ==========================================================
print("Step 7: 快速补漏...")
existing_df = spark.read.format("jdbc").option("url", MYSQL_URL).option("dbtable", "related_songs").option("user", MYSQL_USER).option("password", MYSQL_PWD).load().select("song_id").distinct()
missing_songs = item_factors_vec.join(existing_df, item_factors_vec.id == existing_df.song_id, "left_anti").select(col("id").alias("song_id"))

if missing_songs.count() > 0:
    hot_pool = [row.song_id for row in songs_to_db.limit(50).collect()]
    missing_ids = [row.song_id for row in missing_songs.collect()]
    fill_data = []
    import random
    for sid in missing_ids:
        recs = random.sample(hot_pool, min(5, len(hot_pool)))
        for rid in recs:
            if sid != rid:
                fill_data.append({'song_id': sid, 'related_song_id': rid, 'similarity_score': 0.01})
    
    if fill_data:
        spark.createDataFrame(fill_data).write.jdbc(url=MYSQL_URL, table="related_songs", mode="append", properties=prop)

print("🎉 极速完成！")
spark.stop()
