from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, rand
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# ================= 配置区域 =================
# 必须和 train_model.py 保持一致
HDFS_DATA_PATH = "hdfs://localhost:9000/input/cleaned_data.csv"
MYSQL_URL = "jdbc:mysql://localhost:3306/music_rec_sys?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8"
MYSQL_USER = "hadoop_master"
MYSQL_PWD = "hadoop"  # <--- 【再次提醒：填你的真实密码】
# ===========================================

spark = SparkSession.builder \
    .appName("LoadSongsToMySQL") \
    .master("local[*]") \
    .getOrCreate()

print("Step 1: 读取歌曲元数据...")
df = spark.read.csv(HDFS_DATA_PATH, header=True, inferSchema=True)

# -------------------------------------------------------
# 这里的逻辑必须和 train_model.py 里的 ID 生成逻辑一致
# 否则推荐出来的 ID=3 指向的歌可能不对
# -------------------------------------------------------
if "song_id" not in df.columns:
    print("⚠️ 自动生成 song_id (与训练逻辑保持一致)...")
    window = Window.orderBy(col("_c0") if "_c0" in df.columns else rand(42))
    df = df.withColumn("song_id", row_number().over(window).cast(IntegerType()))
else:
    df = df.withColumn("song_id", col("song_id").cast(IntegerType()))

print("Step 2: 整理列名以匹配 MySQL 表结构...")
# 假设 CSV 里有 song_title, artist_name, song_url (根据你的 cleaned_data.csv 实际情况)
# 如果你的列名不一样，请修改下面的 col("...") 里的名字
# 【关键修复】：给 publish_time 强制指定为 string 类型，解决 void 报错
songs_to_db = df.select(
    col("song_id"),
    col("song_title").alias("title"),       
    col("artist_name").alias("artist"),     
    col("song_url").alias("image_url"),     
    lit(None).cast("string").alias("publish_time")  # <--- 修复了这里：加了 .cast("string")
).drop_duplicates(["song_id"])

print(f"准备写入 {songs_to_db.count()} 首歌曲信息...")

print("Step 3: 写入 MySQL songs 表...")
prop = {
    "user": MYSQL_USER,
    "password": MYSQL_PWD,
    "driver": "com.mysql.cj.jdbc.Driver"
}

try:
    songs_to_db.write.jdbc(
        url=MYSQL_URL,
        table="songs",
        mode="overwrite",  # 覆盖模式，确保数据整洁
        properties=prop
    )
    print("✅ 成功！歌曲信息已导入 MySQL。")
except Exception as e:
    print(f"❌ 失败: {e}")

spark.stop()
