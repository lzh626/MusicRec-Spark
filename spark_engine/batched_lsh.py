import math
import random
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, BucketedRandomProjectionLSH, Normalizer
from pyspark.sql.functions import col, lit, row_number, concat_ws, rand
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# ================= 配置 =================
MYSQL_URL = "jdbc:mysql://localhost:3306/music_rec_sys?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8&rewriteBatchedStatements=true"
MYSQL_USER = "hadoop_master" 
MYSQL_PWD = "hadoop"

# 批次数量：将 3万首歌切成 10 份，每份约 3000 首
NUM_PARTITIONS = 10
# =======================================

spark = SparkSession.builder \
    .appName("BatchedSimilarityFast") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate() # shuffle分区调小点，因为数据量不大

spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# 1. 准备数据
# ==========================================================
print("Step 1: 读取所有歌曲元数据...")
songs_df = spark.read.format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("dbtable", "songs") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PWD) \
    .load() \
    .select("song_id", "title", "artist")

# 准备热门歌曲池 (用于兜底填充)
hot_pool_ids = [row.song_id for row in songs_df.orderBy(rand()).limit(50).collect()]

# ==========================================================
# 2. 特征工程 (轻量化优化)
# ==========================================================
print("Step 2: 构建文本特征向量 (优化版)...")
songs_df = songs_df.withColumn("text", concat_ws(" ", col("title"), col("artist")))

tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordsData = tokenizer.transform(songs_df)

# 【优化1】：维度从 1024 降到 128
# 对于歌名+歌手这种短文本，128 维足够区分了，计算速度快 8 倍
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=128)
featurizedData = hashingTF.transform(wordsData)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=2.0)
dataset = normalizer.transform(rescaledData).select("song_id", "normFeatures")

# 缓存全量数据
dataset.cache()
print(f"   -> 数据准备完毕，共 {dataset.count()} 首歌")

# 训练 LSH 模型
print("Step 3: 训练 LSH 模型...")
# bucketLength 适当调大可以减少哈希桶数量，加快搜索
brp = BucketedRandomProjectionLSH(inputCol="normFeatures", outputCol="hashes", bucketLength=4.0, numHashTables=3)
model = brp.fit(dataset)

# ==========================================================
# 3. 分批次计算 (使用 randomSplit 优化)
# ==========================================================
print(f"Step 4: 开始分批次计算 (共 {NUM_PARTITIONS} 批)...")

# 【优化3】：直接将 DataFrame 切分成 10 份，比用 ID 过滤更高效
# weights=[1.0, 1.0, ...] 表示均分
weights = [1.0] * NUM_PARTITIONS
batches = dataset.randomSplit(weights, seed=42)

for i, batch_df in enumerate(batches):
    print(f"\n--- 处理第 {i+1}/{NUM_PARTITIONS} 批 ---")
    
    # 【优化2】：阈值从 1.8 降到 1.2
    # 只计算欧氏距离小于 1.2 的，太不相似的直接在底层被 Spark 丢弃，极大减少计算量
    similarity = model.approxSimilarityJoin(batch_df, dataset, 1.2, distCol="EuclideanDistance")
    
    # 过滤自己 & 取 Top 5
    raw_recs = similarity.filter(col("datasetA.song_id") != col("datasetB.song_id")) \
        .select(
            col("datasetA.song_id").alias("song_id"),
            col("datasetB.song_id").alias("related_song_id"),
            (lit(1.0) / (lit(1.0) + col("EuclideanDistance"))).alias("similarity_score")
        )
    
    windowSpec = Window.partitionBy("song_id").orderBy(col("similarity_score").desc())
    top5_recs = raw_recs.withColumn("rank", row_number().over(windowSpec)) \
        .filter(col("rank") <= 5) \
        .drop("rank")
    
    # 写入 MySQL
    print(f"   -> 写入批次 {i+1} 到 MySQL...")
    prop = {"user": MYSQL_USER, "password": MYSQL_PWD, "driver": "com.mysql.cj.jdbc.Driver", "batchsize": "10000"}
    
    try:
        top5_recs.write.jdbc(url=MYSQL_URL, table="related_songs", mode="append", properties=prop)
    except Exception as e:
        print(f"   ⚠️ 写入警告 (可能是重复数据): {e}")

# ==========================================================
# 4. 最终补漏 (填充那些实在找不到相似的歌)
# ==========================================================
print("\nStep 5: 最终补漏检查...")

# 找出数据库里已经算好的 ID
existing_df = spark.read.format("jdbc").option("url", MYSQL_URL) \
    .option("dbtable", "related_songs").option("user", MYSQL_USER).option("password", MYSQL_PWD) \
    .load().select("song_id").distinct()

# 找出漏网之鱼
missing_songs = dataset.join(existing_df, "song_id", "left_anti").select("song_id")
missing_count = missing_songs.count()

if missing_count > 0:
    print(f"   -> 正在为 {missing_count} 首孤儿歌曲填充随机推荐...")
    missing_ids = [r.song_id for r in missing_songs.collect()]
    
    fill_data = []
    for sid in missing_ids:
        # 随机从热门池里挑 5 个
        recs = random.sample(hot_pool_ids, 5)
        for rid in recs:
            if sid != rid:
                fill_data.append({'song_id': sid, 'related_song_id': rid, 'similarity_score': 0.05})
    
    if fill_data:
        spark.createDataFrame(fill_data).write.jdbc(url=MYSQL_URL, table="related_songs", mode="append", properties=prop)

print("🎉 极速版计算完成！")
spark.stop()
