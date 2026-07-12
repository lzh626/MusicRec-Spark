from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, BucketedRandomProjectionLSH
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, log1p, lit, explode, row_number, udf
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
# 【新增】引入向量相关的库
from pyspark.ml.linalg import Vectors, VectorUDT

# ================= 配置 =================
# 请确保这里填的是你的真实密码
MYSQL_URL = "jdbc:mysql://localhost:3306/music_rec_sys?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8&rewriteBatchedStatements=true"
MYSQL_USER = "hadoop_master" 
MYSQL_PWD = "hadoop" # <--- 【修改这里】
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
# Step 1: 读取数据
# ==========================================================
print("Step 1: 读取数据...")
ratings_raw = spark.read.csv("hdfs://localhost:9000/input/interactions.csv", header=True, inferSchema=True)
tracks_raw = spark.read.csv("hdfs://localhost:9000/input/tracks_meta.csv", header=True, inferSchema=True)

# ratings_raw = ratings_raw.sample(fraction=0.2, seed=42) # 调试时可开启

# ==========================================================
# Step 2: 构建 ID 索引
# ==========================================================
print("Step 2: 构建 ID 映射 (Raw ID -> Spark Int ID)...")

ratings_raw = ratings_raw.withColumn("user_id_str", col("user_id").cast("string"))
user_indexer = StringIndexer(inputCol="user_id_str", outputCol="userId_int").setHandleInvalid("skip")
user_model = user_indexer.fit(ratings_raw)
ratings_indexed = user_model.transform(ratings_raw)

tracks_raw = tracks_raw.withColumn("song_id_str", col("song_id").cast("string"))
ratings_indexed = ratings_indexed.withColumn("song_id_str", col("song_id").cast("string"))

song_indexer = StringIndexer(inputCol="song_id_str", outputCol="songId_int").setHandleInvalid("skip")
song_model = song_indexer.fit(tracks_raw)

ratings_final = song_model.transform(ratings_indexed)
tracks_final = song_model.transform(tracks_raw)

ratings_final = ratings_final.withColumn("rating", log1p(col("playcount")))

# ==========================================================
# Step 3: 数据入库 (歌曲元数据)
# ==========================================================
print("Step 3: 同步歌曲元数据到 MySQL...")
songs_to_db = tracks_final.select(
    col("songId_int").cast(IntegerType()).alias("song_id"),
    col("song_id").cast("string").alias("original_track_id"),
    col("title"),
    col("artist"),
    col("image_url"),
    lit("Pop").alias("genre"),
    lit(0.0).alias("danceability"),
    lit(0.0).alias("energy"),
    lit("").alias("tags")
).dropDuplicates(["song_id"])

prop = {"user": MYSQL_USER, "password": MYSQL_PWD, "driver": "com.mysql.cj.jdbc.Driver", "batchsize": "10000"}
songs_to_db.write.jdbc(url=MYSQL_URL, table="songs", mode="overwrite", properties=prop)

# ==========================================================
# Step 4: 训练 ALS 协同过滤模型
# ==========================================================
print("Step 4: 训练 ALS 模型...")
als = ALS(
    maxIter=10, 
    regParam=0.1, 
    rank=10,
    userCol="userId_int", 
    itemCol="songId_int", 
    ratingCol="rating",
    implicitPrefs=True, 
    coldStartStrategy="drop"
)
model = als.fit(ratings_final)

print("   -> 生成 User-Item 推荐结果...")
user_recs = model.recommendForAllUsers(10)

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
# Step 5: 计算相似歌曲 (基于 ALS 隐向量)
# ==========================================================
print("Step 5: 计算相似歌曲 (基于 ALS 隐向量)...")

item_factors = model.itemFactors

# 【核心修复】：定义一个 UDF，将 Array<Float> 转换为 VectorUDT
# 这样 LSH 才能识别它
list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())

# 应用转换：把 features 列从数组转为向量
item_factors_vec = item_factors.withColumn("features_vec", list_to_vector_udf(col("features")))

# 使用 LSH 对 物品向量 进行聚类搜索
# 注意：inputCol 改为我们转换后的 'features_vec'
brp = BucketedRandomProjectionLSH(
    inputCol="features_vec", outputCol="hashes", bucketLength=2.0, numHashTables=3
)
lsh_model = brp.fit(item_factors_vec)

# 计算相似度 (Self Join)
# Threshold 设为 2.0
similarity_df = lsh_model.approxSimilarityJoin(item_factors_vec, item_factors_vec, 2.0, distCol="EuclideanDistance")

# 过滤并格式化
raw_similar = similarity_df.filter(col("datasetA.id") != col("datasetB.id")).select(
    col("datasetA.id").alias("song_id"),
    col("datasetB.id").alias("related_song_id"),
    (lit(1.0) / (lit(1.0) + col("EuclideanDistance"))).alias("similarity_score")
)

print("   -> 执行 Top-5 截断 (防止磁盘爆炸)...")
windowSpec = Window.partitionBy("song_id").orderBy(col("similarity_score").desc())

top_similar_songs = raw_similar.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") <= 5) \
    .drop("rank")

print(f"   -> 正在写入 MySQL related_songs 表...")
top_similar_songs.write.jdbc(url=MYSQL_URL, table="related_songs", mode="overwrite", properties=prop)

print("🎉 所有任务完成！真实中文数据已部署！")
spark.stop()
