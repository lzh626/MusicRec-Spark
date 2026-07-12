from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, BucketedRandomProjectionLSH
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, log1p, lit, explode, row_number, udf
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from pyspark.ml.linalg import Vectors, VectorUDT

# ================= 配置 =================
# 开启 rewriteBatchedStatements 以获得 10倍 写入速度
MYSQL_URL = "jdbc:mysql://localhost:3306/music_rec_sys?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8&rewriteBatchedStatements=true"
MYSQL_USER = "hadoop_master" 
MYSQL_PWD = "hadoop"

# LSH 分批数量：将数据切成 5 份逐个计算，防止内存/磁盘爆炸
LSH_BATCH_COUNT = 5 
# =======================================

# 添加了网络绑定配置，防止报错 Cannot assign requested address
spark = SparkSession.builder \
    .appName("MusicRecFusionOptimized") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# Step 1: 读取并处理数据
# ==========================================================
print("Step 1.1: 读取网易云数据...")
df_ne_acts = spark.read.csv("hdfs://localhost:9000/input/netease/interactions.csv", header=True, inferSchema=True)
df_ne_meta = spark.read.csv("hdfs://localhost:9000/input/netease/tracks_meta.csv", header=True, inferSchema=True)

df_ne_acts = df_ne_acts.select(col("user_id").cast("string"), col("song_id").cast("string"), col("playcount").cast("int"))
df_ne_meta = df_ne_meta.select(col("song_id").cast("string").alias("original_id"), col("title"), col("artist"), col("image_url"), lit("netease").alias("source"))

print("Step 1.2: 读取 Spotify 数据...")
df_sp_acts = spark.read.csv("hdfs://localhost:9000/input/spotify/interactions.csv", header=True, inferSchema=True)
df_sp_meta = spark.read.csv("hdfs://localhost:9000/input/spotify/tracks_meta.csv", header=True, inferSchema=True)

print("   -> 对 Spotify 数据进行 10% 采样...")
df_sp_acts = df_sp_acts.sample(fraction=0.1, seed=42)

df_sp_acts = df_sp_acts.select(col("user_id").cast("string"), col("song_id").cast("string"), col("playcount").cast("int"))
df_sp_meta = df_sp_meta.select(col("song_id").alias("original_id"), col("title").alias("title"), col("artist"), lit(None).cast("string").alias("image_url"), lit("spotify").alias("source"))

# ==========================================================
# Step 2: 融合与级联过滤
# ==========================================================
print("Step 2: 融合中英文数据...")
ratings_raw = df_ne_acts.union(df_sp_acts).repartition(100)
tracks_raw = df_ne_meta.union(df_sp_meta).repartition(100)

# 改名对齐
ratings_raw = ratings_raw.withColumnRenamed("song_id", "original_id")

print("   -> 正在剔除无人听的僵尸歌曲...")
tracks_raw = tracks_raw.join(ratings_raw, on="original_id", how="left_semi")

# ==========================================================
# Step 3: 构建全局 ID 映射
# ==========================================================
print("Step 3: 构建全局 ID 映射...")
ratings_raw = ratings_raw.withColumn("user_id_str", col("user_id").cast("string"))
user_indexer = StringIndexer(inputCol="user_id_str", outputCol="userId_int").setHandleInvalid("skip")
user_model = user_indexer.fit(ratings_raw)
ratings_indexed = user_model.transform(ratings_raw)

song_indexer = StringIndexer(inputCol="original_id", outputCol="songId_int").setHandleInvalid("skip")
song_model = song_indexer.fit(tracks_raw)

ratings_final = song_model.transform(ratings_indexed)
tracks_final = song_model.transform(tracks_raw)
ratings_final = ratings_final.withColumn("rating", log1p(col("playcount")))

# ==========================================================
# Step 4: 写入 MySQL (Songs 表)
# ==========================================================
print("Step 4: 写入歌曲元数据...")
songs_to_db = tracks_final.select(
    col("songId_int").cast(IntegerType()).alias("song_id"),
    col("original_id").alias("original_track_id"),
    col("title"),
    col("artist"),
    col("image_url"),
    col("source"),
    lit("Pop").alias("genre"),
    lit(0.0).alias("danceability"),
    lit(0.0).alias("energy"),
    lit("").alias("tags")
).dropDuplicates(["song_id"])

prop = {"user": MYSQL_USER, "password": MYSQL_PWD, "driver": "com.mysql.cj.jdbc.Driver", "batchsize": "5000"}
songs_to_db.write.jdbc(url=MYSQL_URL, table="songs", mode="overwrite", properties=prop)

# ==========================================================
# Step 5: 训练 ALS 模型
# ==========================================================
print("Step 5: 训练 ALS 混合推荐模型...")
als = ALS(maxIter=10, regParam=0.1, rank=10, userCol="userId_int", itemCol="songId_int", ratingCol="rating", implicitPrefs=True, coldStartStrategy="drop")
model = als.fit(ratings_final)

# 【核心修改】：改为 100，满足前端“换一批”功能
print("   -> 生成推荐结果 (Top 100)...")
user_recs = model.recommendForAllUsers(100)

recs_to_db = user_recs.select(
    col("userId_int").alias("user_id"),
    explode("recommendations").alias("rec")
).select(
    col("user_id"),
    col("rec.songId_int").alias("song_id"),
    col("rec.rating").alias("rank_score")
)

recs_to_db.write.jdbc(url=MYSQL_URL, table="recommendations", mode="overwrite", properties=prop)

# ==========================================================
# Step 6: 分批计算相似歌曲 (LSH 优化版)
# ==========================================================
print("Step 6: 计算相似歌曲 (分批次优化，防止崩盘)...")

item_factors = model.itemFactors
# 转换 Array -> Vector
list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
item_factors_vec = item_factors.withColumn("features_vec", list_to_vector_udf(col("features")))

# 缓存全量向量，因为后面每个批次都要用到
item_factors_vec.cache()

brp = BucketedRandomProjectionLSH(inputCol="features_vec", outputCol="hashes", bucketLength=2.0, numHashTables=3)
lsh_model = brp.fit(item_factors_vec)

# 使用 randomSplit 将数据切分成 5 份
weights = [1.0] * LSH_BATCH_COUNT
batches = item_factors_vec.randomSplit(weights, seed=42)

# 先清空 related_songs 表 (因为后面是 append 模式)
try:
    # 这里用一个小技巧：写入一个空 DataFrame 覆盖表，相当于清空
    empty_df = spark.createDataFrame([], schema="song_id int, related_song_id int, similarity_score float")
    empty_df.write.jdbc(url=MYSQL_URL, table="related_songs", mode="overwrite", properties=prop)
    print("   -> 已清空 related_songs 表，开始分批写入...")
except:
    print("   -> related_songs 表不存在，将自动创建...")

# 循环处理每个批次
for i, batch_df in enumerate(batches):
    print(f"\n   --- 处理 LSH 批次 {i+1}/{LSH_BATCH_COUNT} ---")
    
    # 【核心优化】：Batch vs All (只算这一小批数据与全量的相似度)
    # Threshold 设为 1.2 (不要太宽)
    similarity_df = lsh_model.approxSimilarityJoin(batch_df, item_factors_vec, 1.2, distCol="EuclideanDistance")

    raw_similar = similarity_df.filter(col("datasetA.id") != col("datasetB.id")).select(
        col("datasetA.id").alias("song_id"),
        col("datasetB.id").alias("related_song_id"),
        (lit(1.0) / (lit(1.0) + col("EuclideanDistance"))).alias("similarity_score")
    )

    # Top-5 截断
    windowSpec = Window.partitionBy("song_id").orderBy(col("similarity_score").desc())
    top_similar_songs = raw_similar.withColumn("rank", row_number().over(windowSpec)) \
        .filter(col("rank") <= 5) \
        .drop("rank")

    # 使用 Append 模式追加写入
    print(f"   -> 写入批次 {i+1} 到 MySQL...")
    try:
        top_similar_songs.write.jdbc(url=MYSQL_URL, table="related_songs", mode="append", properties=prop)
    except Exception as e:
        print(f"   ⚠️ 写入批次 {i+1} 时出现警告 (可能是空数据): {e}")

print("🎉 任务全部完成！")
spark.stop()
