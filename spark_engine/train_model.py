from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, explode, rand, row_number, lit  # <--- 补上了 lit
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.window import Window

# =============================================================
# 全局配置
# =============================================================
# HDFS文件路径 (确保这个文件存在)
HDFS_DATA_PATH = "hdfs://localhost:9000/input/cleaned_data.csv"

# MySQL 配置
MYSQL_URL = "jdbc:mysql://localhost:3306/music_rec_sys?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8"
MYSQL_USER = "hadoop_master"      # <--- 建议改回 root，除非你确定 hadoop_master 建好了
MYSQL_PWD = "hadoop"    # <--- 【记得在这里填你的密码】

# 算法参数
TOTAL_USERS = 100
SAMPLE_FRACTION = 0.1
RANDOM_SEED = 42

# =============================================================
# 1. 初始化 Spark
# =============================================================
def init_spark():
    # 使用括号包裹的方式，避免反斜杠缩进错误
    spark = (SparkSession.builder
        .appName("MusicRecommender")
        .master("local[*]")
        # 注意：这里去掉了 .config("spark.driver.extraClassPath"...)
        # 因为我们在 spark-submit 命令行里已经指定了 jar 包，代码里再写容易冲突或找不到文件
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

spark = init_spark()

# =============================================================
# 2. 读取并处理歌曲数据
# =============================================================
print("Step 1: 正在读取 HDFS 数据...")
try:
    songs_df = spark.read.csv(
        HDFS_DATA_PATH,
        header=True,
        inferSchema=True,
        nullValue="",
        nanValue="NaN",
        emptyValue=""
    )
except Exception as e:
    print(f"❌ 读取 HDFS 失败: {e}")
    spark.stop()
    exit(1)

# 方案A：尝试使用原始 song_id
song_id_valid = False
if "song_id" in songs_df.columns:
    cleaned_songs = (songs_df
        .na.drop(subset=["song_id"])
        .withColumn("song_id", col("song_id").cast(IntegerType()))
        .na.drop(subset=["song_id"])
    )
    if cleaned_songs.count() > 0:
        songs_df = cleaned_songs
        song_id_valid = True
        print("✅ 使用原始有效 song_id")

# 方案B：自动切换为行号生成 song_id
if not song_id_valid:
    print("⚠️  原始 song_id 无效，自动生成行号作为 song_id")
    window = Window.orderBy(col("_c0") if "_c0" in songs_df.columns else rand(RANDOM_SEED))
    songs_df = (songs_df
        .na.drop(how="all")
        .withColumn("song_id", row_number().over(window).cast(IntegerType()))
    )

song_count = songs_df.count()
if song_count == 0:
    print("❌ 错误：歌曲数据为空！")
    spark.stop()
    exit(1)
    
print(f"✅ 有效歌曲数：{song_count}")

# =============================================================
# 3. 生成模拟评分数据
# =============================================================
print("Step 2: 正在模拟生成用户评分数据...")
users_df = spark.range(1, TOTAL_USERS + 1).withColumnRenamed("id", "userId")

sampled_songs = songs_df.sample(fraction=SAMPLE_FRACTION, seed=RANDOM_SEED)
if sampled_songs.count() == 0:
    sampled_songs = songs_df.limit(50)

# 生成评分数据
training_data = (users_df
    .crossJoin(sampled_songs.select("song_id"))
    .withColumn("random_flag", rand(seed=RANDOM_SEED))
    .filter(col("random_flag") < 0.1)
    .withColumn("rating", (rand(seed=RANDOM_SEED) * 4 + 1).cast(FloatType()))
    .select(col("userId"), col("song_id").alias("songId"), col("rating"))
    .na.drop(subset=["userId", "songId", "rating"])
)

# 兜底数据生成
if training_data.count() == 0:
    print("⚠️ 采样数据不足，生成强制兜底数据...")
    training_data = (users_df
        .crossJoin(songs_df.select("song_id").limit(10))
        .withColumn("rating", lit(3.0).cast(FloatType()))  # 这里用到了 lit
        .select(col("userId"), col("song_id").alias("songId"), col("rating"))
    )

print(f"✅ 模拟评分数据量：{training_data.count()}")

# =============================================================
# 4. 训练 ALS 模型
# =============================================================
print("Step 3: 开始训练 ALS 推荐模型...")
als = ALS(
    maxIter=10,
    regParam=0.05,
    rank=10,
    userCol="userId",
    itemCol="songId",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True,
    seed=RANDOM_SEED
)

model = als.fit(training_data)

# =============================================================
# 5. 生成推荐结果
# =============================================================
print("Step 4: 为所有用户生成推荐列表...")
user_recs = model.recommendForAllUsers(10)

exploded_recs = (user_recs
    .select(
        col("userId").cast(IntegerType()).alias("user_id"),
        explode("recommendations").alias("rec")
    )
    .select(
        col("user_id"),
        col("rec.songId").cast(IntegerType()).alias("song_id"),
        col("rec.rating").cast(FloatType()).alias("rank_score")
    )
)

print(f"✅ 生成推荐结果数：{exploded_recs.count()}")

# =============================================================
# 6. 写入 MySQL
# =============================================================
print("Step 5: 正在写入 MySQL...")
mysql_props = {
    "user": MYSQL_USER,
    "password": MYSQL_PWD,
    "driver": "com.mysql.cj.jdbc.Driver",
    "batchsize": "1000",
    "rewriteBatchedStatements": "true"
}

try:
    exploded_recs.write.jdbc(
        url=MYSQL_URL,
        table="recommendations",
        mode="overwrite",
        properties=mysql_props
    )
    print(f"✅ 成功！推荐结果已存入 MySQL")

except Exception as e:
    print(f"❌ 写入 MySQL 失败: {str(e)}")

# =============================================================
# 7. 资源清理
# =============================================================
spark.stop()
print("🎉 程序执行完成")
