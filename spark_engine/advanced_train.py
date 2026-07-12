from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, BucketedRandomProjectionLSH
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, log1p, lit, expr
from pyspark.sql.types import IntegerType

# ================= 配置 =================
# 在末尾加上 &rewriteBatchedStatements=true,,开启高速写入的总开关
MYSQL_URL = "jdbc:mysql://localhost:3306/music_rec_sys?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8&rewriteBatchedStatements=true"
MYSQL_USER = "hadoop_master"      # <--- 建议改回 root，除非你确定 hadoop_master 建好了
MYSQL_PWD = "hadoop"    # <--- 【记得在这里填你的密码】
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

print("Step 1: 读取数据...")
# 读取交互数据
ratings_raw = spark.read.csv("hdfs://localhost:9000/input/interactions.csv", header=True, inferSchema=True)
# 读取元数据
tracks_raw = spark.read.csv("hdfs://localhost:9000/input/tracks_meta.csv", header=True, inferSchema=True)


# 【新增】只取 15% 的数据进行训练，防止虚拟机爆炸
#ratings_raw = ratings_raw.sample(fraction=0.15, seed=42) 
print(f"采样后数据量: {ratings_raw.count()}")

# ==========================================================
# 核心难点解决：String ID -> Integer ID 映射
# ==========================================================
print("Step 2: 构建 ID 索引 (String -> Int)...")

# 1. 处理 User ID
user_indexer = StringIndexer(inputCol="user_id", outputCol="userId_int").setHandleInvalid("skip")
user_model = user_indexer.fit(ratings_raw)
ratings_indexed = user_model.transform(ratings_raw)

# 2. 处理 Track ID (必须对 ratings 和 tracks 都做变换)
# 先fit一下所有的 track_id
track_indexer = StringIndexer(inputCol="track_id", outputCol="songId_int").setHandleInvalid("skip")
track_model = track_indexer.fit(tracks_raw)

ratings_final = track_model.transform(ratings_indexed)
tracks_final = track_model.transform(tracks_raw)

# 转换 playcount 为隐式评分 (log转换平滑数据)
# rating = log(playcount + 1)
ratings_final = ratings_final.withColumn("rating", log1p(col("playcount")))

# 缓存数据，因为后面要用多次
tracks_final.cache()
ratings_final.cache()

# ==========================================================
# 模块 A: 数据入库 (把带数字ID的歌曲信息存入 MySQL)
# ==========================================================
print("Step 3: 同步歌曲元数据到 MySQL...")
songs_to_db = tracks_final.select(
    col("songId_int").cast(IntegerType()).alias("song_id"),
    col("track_id").alias("original_track_id"),
    col("name").alias("title"),
    col("artist"),
    col("spotify_preview_url").alias("image_url"), # 暂时借用 URL 字段
    col("genre"),
    col("danceability"),
    col("energy"),
    col("tags")
).dropDuplicates(["song_id"])

prop = {
	"user": MYSQL_USER, 
	"password": MYSQL_PWD, 
	"driver": "com.mysql.cj.jdbc.Driver",
	"batchsize": "10000"   # <--- 新增这行，一次打包写入 1万条
}

songs_to_db.write.jdbc(url=MYSQL_URL, table="songs", mode="overwrite", properties=prop)

# ==========================================================
# 模块 B: 算法一 ALS 协同过滤 (User -> Item)
# ==========================================================
print("Step 4: 训练 ALS 模型...")
als = ALS(
    maxIter=10, 
    regParam=0.1, 
    userCol="userId_int", 
    itemCol="songId_int", 
    ratingCol="rating",
    implicitPrefs=True, # 开启隐式反馈模式 (重要!)
    coldStartStrategy="drop"
)
model = als.fit(ratings_final)

print("生成 ALS 推荐结果...")
user_recs = model.recommendForAllUsers(10)

# 展开并写入 MySQL
# 注意：这里我们得到的是 Int ID，MySQL 里存的也是 Int ID，完美对应
from pyspark.sql.functions import explode
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
# 模块 C: 算法二 基于内容推荐 (Item -> Item)
# 使用 LSH (局部敏感哈希) 计算相似歌曲
# ==========================================================
print("Step 5: 计算基于内容的相似歌曲 (Content-Based)...")

# 1. 特征向量化：把 danceability, energy, valence 拼成一个向量
assembler = VectorAssembler(
    inputCols=["danceability", "energy", "valence", "tempo"], # 还可以加 genre
    outputCol="features",
    handleInvalid="skip"
)
tracks_features = assembler.transform(tracks_final)

# 2. 使用 LSH 进行近似最近邻搜索 (比暴力计算快得多)
brp = BucketedRandomProjectionLSH(
    inputCol="features", outputCol="hashes", bucketLength=2.0, numHashTables=3
)
lsh_model = brp.fit(tracks_features)

# 3. 计算自连接相似度
print("正在进行 LSH 相似度计算...")
similarity_df = lsh_model.approxSimilarityJoin(
    tracks_features, tracks_features, 1.5, distCol="EuclideanDistance" # 阈值可以适当调小，比如 1.5 或 1.0
)

# 4. 过滤自己匹配自己
raw_similar_songs = similarity_df.filter(
    col("datasetA.songId_int") != col("datasetB.songId_int")
).select(
    col("datasetA.songId_int").alias("song_id"),
    col("datasetB.songId_int").alias("related_song_id"),
    (lit(1.0) / (lit(1.0) + col("EuclideanDistance"))).alias("similarity_score")
)

# ========================================================
# 【关键修改】：引入 Window 函数，强制只取 Top 5
# ========================================================
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

print("正在执行 Top-N 截断 (防止数据爆炸)...")

# 定义窗口：按 song_id 分组，按相似度降序排列
windowSpec = Window.partitionBy("song_id").orderBy(col("similarity_score").desc())

# 增加一列 rank，并过滤 rank <= 5
top_similar_songs = raw_similar_songs.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") <= 5) \
    .drop("rank")

# 5. 写入 MySQL
print(f"正在写入相似歌曲表 (预计数据量: {songs_to_db.count() * 5} 行)...")
top_similar_songs.write.jdbc(url=MYSQL_URL, table="related_songs", mode="overwrite", properties=prop)

print("🎉 所有任务完成！双算法模型已部署！")
spark.stop()
