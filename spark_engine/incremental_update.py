"""
文件名：spark_engine/incremental_update.py
作用：近线增量调度脚本（每 10 分钟或每小时运行一次）
逻辑：只读取最近 1 小时内有新行为的用户，更新其 recommendations 结果
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, log1p, desc, row_number
from pyspark.sql.window import Window
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import BucketedRandomProjectionLSH
import numpy as np

MYSQL_URL = "jdbc:mysql://localhost:3306/music_rec_sys?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8&rewriteBatchedStatements=true"
MYSQL_USER = "hadoop_master"
MYSQL_PWD = "hadoop"

spark = SparkSession.builder \
    .appName("MusicRec_Stage2_Nearline_Incremental") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
prop = {"user": MYSQL_USER, "password": MYSQL_PWD, "driver": "com.mysql.cj.jdbc.Driver", "batchsize": "1000"}

print("== 启动阶段2：近线轻量级增量更新 ==")

# 1. 只读取最近1小时有行为的增量活跃用户
query_recent = """
(SELECT DISTINCT stable_uid_hash, song_id, weight 
 FROM user_actions 
 WHERE created_at >= NOW() - INTERVAL 1 HOUR) AS recent_acts
"""
recent_df = spark.read.jdbc(url=MYSQL_URL, table=query_recent, properties=prop)
active_users = [row.stable_uid_hash for row in recent_df.select("stable_uid_hash").distinct().collect()]

if not active_users:
    print("当前时间窗口内无新行为用户，跳过本次更新。")
    spark.stop()
    exit(0)

print(f"检测到 {len(active_users)} 位活跃用户需要增量更新推荐列表: {active_users}")

# 2. 读取离线基线沉淀的 Item 隐向量
item_factors = spark.read.parquet("hdfs://localhost:9000/models/latest_item_factors")
# item_factors schema: id (song_id), features (array<float>)

# 3. 将近期行为聚合为隐式评分
user_ratings = recent_df.groupBy("stable_uid_hash", "song_id") \
    .agg(spark_sum("weight").alias("w")) \
    .withColumn("rating", log1p(col("w")))

# 4. 针对这批增量用户，利用简单的加权平均快速生成实时偏好向量（工业界冷/增量更新折中方案）
# 用户向量 = 该用户交互过的歌曲向量按评分加权求和
user_profiles = user_ratings.join(item_factors, user_ratings.song_id == item_factors.id, "inner") \
    .rdd.map(lambda r: (r.stable_uid_hash, np.array(r.features) * float(r.rating))) \
    .reduceByKey(lambda a, b: a + b) \
    .mapValues(lambda v: (v / (np.linalg.norm(v) + 1e-6)).tolist())

user_profiles_df = spark.createDataFrame(user_profiles, ["stable_uid_hash", "user_features"])

# 5. 计算增量用户与全量候选库的点乘得分 (Top 100)
# 广播物品向量到内存，毫秒级得出点乘结果
items_local = item_factors.collect()
all_item_ids = [r.id for r in items_local]
all_item_mat = np.array([r.features for r in items_local]) # 维度: (N, Rank)

updated_recs = []
for row in user_profiles_df.collect():
    u_hash = row.stable_uid_hash
    u_vec = np.array(row.user_features)
    scores = np.dot(all_item_mat, u_vec)
    top_indices = np.argsort(-scores)[:100]
    
    for idx in top_indices:
        updated_recs.append({
            "stable_uid_hash": int(u_hash),
            "song_id": int(all_item_ids[idx]),
            "rank_score": float(scores[idx])
        })

# 6. 将增量用户的推荐覆盖到 recommendations 表中
if updated_recs:
    new_recs_df = spark.createDataFrame(updated_recs)
    # 先删除这几位老用户的旧推荐
    user_list_str = ",".join([str(u) for u in active_users])
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", user=MYSQL_USER, password=MYSQL_PWD, database="music_rec_sys")
    with conn.cursor() as cursor:
        cursor.execute(f"DELETE FROM recommendations WHERE stable_uid_hash IN ({user_list_str})")
    conn.commit()
    conn.close()

    # 写入该用户的新推荐
    new_recs_df.write.jdbc(url=MYSQL_URL, table="recommendations", mode="append", properties=prop)
    print(f"增量更新完成，已成功刷新 {len(active_users)} 个用户的推荐列表。")

spark.stop()