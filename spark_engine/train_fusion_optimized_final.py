from pyspark.sql import SparkSession
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, log1p, lit, explode, row_number, sum as spark_sum, rand, hash, abs, udf
from pyspark.sql.window import Window
from pyspark.ml.linalg import Vectors, VectorUDT

# ================= 配置 =================
MYSQL_URL = "jdbc:mysql://localhost:3306/music_rec_sys?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8&rewriteBatchedStatements=true"
MYSQL_USER = "hadoop_master"
MYSQL_PWD = "hadoop"
LSH_BATCH_COUNT = 5
# =======================================

spark = SparkSession.builder \
    .appName("MusicRec_Stage2_Clean") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.local.dir", "/tmp/spark_tmp") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
prop = {"user": MYSQL_USER, "password": MYSQL_PWD, "driver": "com.mysql.cj.jdbc.Driver", "batchsize": "1000"}

print("============== 阶段一至三：离线基线与增量闭环训练 ==============")

# ==========================================================
# Step 1: 读取 HDFS 历史数据
# ==========================================================
print("Step 1: 读取HDFS历史数据...")
df_ne_acts = spark.read.csv("hdfs://localhost:9000/input/netease/interactions_final.csv", header=True, inferSchema=True) \
    .select(col("user_id").cast("string"), col("song_id").cast("string"), col("playcount").cast("int"))

df_ne_meta = spark.read.csv("hdfs://localhost:9000/input/netease/tracks_meta_final.csv", header=True, inferSchema=True) \
    .select(col("song_id").cast("string"), col("song_id").cast("string").alias("original_id"),
            col("title"), col("artist"), col("image_url"), lit("netease").alias("source"))

df_sp_acts = spark.read.csv("hdfs://localhost:9000/input/spotify/spotify_interactions_mini.csv", header=True, inferSchema=True) \
    .select(col("user_id").cast("string"), col("song_id").cast("string"), col("playcount").cast("int"))

df_sp_meta = spark.read.csv("hdfs://localhost:9000/input/spotify/spotify_tracks_mini.csv", header=True, inferSchema=True) \
    .select(col("song_id").cast("string"), col("real_id").alias("original_id"),
            col("title"), col("artist"), col("image_url"), col("source"))

hdfs_acts = df_ne_acts.union(df_sp_acts)
tracks_raw = df_ne_meta.union(df_sp_meta)

# 生成统一数值型稳定 ID (10位数，严格在32位安全范围)
hdfs_acts_converted = hdfs_acts \
    .withColumn("stable_uid_hash", abs(hash(col("user_id"))).cast("long")) \
    .withColumn("stable_song_hash", abs(hash(col("song_id"))).cast("long")) \
    .select("stable_uid_hash", "stable_song_hash", "playcount")

tracks_final = tracks_raw \
    .withColumn("stable_song_hash", abs(hash(col("song_id"))).cast("long"))

# ==========================================================
# Step 2: 读取 MySQL 增量用户行为
# ==========================================================
print("Step 2: 读取MySQL增量用户行为...")
mysql_query = """
(SELECT ua.stable_uid_hash, ua.song_id AS stable_song_hash, ua.weight AS playcount
 FROM user_actions ua
 WHERE ua.stable_uid_hash IS NOT NULL AND ua.song_id IS NOT NULL) AS new_actions
"""
try:
    mysql_acts = spark.read.jdbc(url=MYSQL_URL, table=mysql_query, properties=prop) \
        .select(col("stable_uid_hash").cast("long"), col("stable_song_hash").cast("long"), col("playcount").cast("int"))
    cnt = mysql_acts.count()
    print(f"   -> 成功提取到 {cnt} 条线上真实增量行为！")
except Exception as e:
    print(f"   -> ⚠️ 未提取到用户行为或发生错误，使用空数据集代替。")
    mysql_acts = spark.createDataFrame([], schema="stable_uid_hash bigint, stable_song_hash bigint, playcount int")

# ==========================================================
# Step 3: 全量数据融合与去重聚合
# ==========================================================
print("Step 3: 融合并聚合所有行为矩阵...")
all_acts = hdfs_acts_converted.union(mysql_acts)
ratings_raw = all_acts.groupBy("stable_uid_hash", "stable_song_hash") \
    .agg(spark_sum("playcount").alias("playcount"))

ratings_final = ratings_raw.join(tracks_final.select("stable_song_hash").distinct(), on="stable_song_hash", how="inner") \
                           .withColumn("rating", log1p(col("playcount")))

# ==========================================================
# Step 4: 写入歌曲元数据 (全量覆盖或安全写入)
# ==========================================================
print("Step 4: 写入歌曲元数据 (Songs)...")
songs_to_db = tracks_final.select(
    col("stable_song_hash").alias("song_id"),
    col("original_id").alias("original_track_id"),
    col("title"), col("artist"), col("image_url"), col("source"),
    lit("Pop").alias("genre"), lit(0.0).alias("energy"), lit("").alias("tags")
).dropDuplicates(["song_id"])

# 覆盖写入，确保 songs 表是唯一的正确基线
songs_to_db.write.jdbc(url=MYSQL_URL, table="songs", mode="overwrite", properties=prop)

# ==========================================================
# Step 5: ALS 协同过滤训练
# ==========================================================
print("Step 5: 训练ALS模型...")
als = ALS(
    maxIter=5, regParam=0.1, rank=5,
    userCol="stable_uid_hash",
    itemCol="stable_song_hash",
    ratingCol="rating",
    implicitPrefs=True,
    coldStartStrategy="drop"
)
model = als.fit(ratings_final)

# 【新增此行】：固化持久化歌曲特征向量，供阶段2近线快速增量使用
print("   -> 持久化物品隐向量 (Item Factors) 到 HDFS...")
model.itemFactors.write.mode("overwrite").parquet("hdfs://localhost:9000/models/latest_item_factors")

print("   -> 生成个性化推荐 (Top 100)...")
user_recs = model.recommendForAllUsers(100)
recs_to_db = user_recs.select(
    col("stable_uid_hash"),
    explode("recommendations").alias("rec")
).select(
    col("stable_uid_hash"),
    col("rec.stable_song_hash").alias("song_id"),
    col("rec.rating").alias("rank_score")
)

recs_to_db.write.jdbc(url=MYSQL_URL, table="recommendations", mode="overwrite", properties=prop)

# ==========================================================
# Step 6: 分批计算 LSH 相似歌曲
# ==========================================================
print(f"Step 6: 计算相似歌曲 (批次: {LSH_BATCH_COUNT})...")
item_factors = model.itemFactors
list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
item_factors_vec = item_factors.withColumn("features_vec", list_to_vector_udf(col("features")))

brp = BucketedRandomProjectionLSH(
    inputCol="features_vec", outputCol="hashes",
    bucketLength=4.0, numHashTables=2
)
lsh_model = brp.fit(item_factors_vec)


try:
    spark.createDataFrame([], schema="song_id bigint, related_song_id bigint, similarity_score double") \
        .write.jdbc(url=MYSQL_URL, table="related_songs", mode="overwrite", properties=prop)
except Exception:
    pass

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
    except Exception as e:
        print("写入相似歌曲分批失败:", e)




# ==========================================================
# Step 7: 冷启动补漏
# ==========================================================
print("Step 7: 冷启动歌曲补漏...")
existing_df = spark.read.jdbc(url=MYSQL_URL, table="related_songs", properties=prop).select("song_id").distinct()
missing_songs = songs_to_db.select("song_id").join(existing_df, on="song_id", how="left_anti")

missing_cnt = missing_songs.count()
if missing_cnt > 0:
    print(f"   -> 发现 {missing_cnt} 首冷启动缺失歌曲，执行兜底...")
    hot_pool_rows = songs_to_db.limit(50).select("song_id").collect()
    hot_pool_ids = [row.song_id for row in hot_pool_rows]
    
    if hot_pool_ids:
        hot_pool_df = spark.createDataFrame([(sid,) for sid in hot_pool_ids], ["related_song_id"])
        fill_window = Window.partitionBy("song_id").orderBy(rand())
        fill_data = missing_songs.crossJoin(hot_pool_df) \
            .filter(col("song_id") != col("related_song_id")) \
            .withColumn("rn", row_number().over(fill_window)) \
            .filter(col("rn") <= 5) \
            .select(col("song_id"), col("related_song_id"), lit(0.025).alias("similarity_score"))
        
        fill_data.write.jdbc(url=MYSQL_URL, table="related_songs", mode="append", properties=prop)

print("🎉 阶段一至三训练完成！")
spark.stop()