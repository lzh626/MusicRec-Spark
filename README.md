# 🎧 MusicRec-Spark: 基于Spark ALS与内容相似度融合音乐推荐系统






## 📖 项目简介
本项目是课程级**离线大数据音乐推荐全栈系统**，基于Ubuntu伪分布式Hadoop+Spark搭建数据处理流水线。
1. 数据源：网易云爬虫中文歌单数据集 + Last.fm/Spotify英文公开数据集；
2. 双推荐算法：
   - ALS协同过滤：基于用户播放隐式反馈生成个性化推荐；
   - ALS物品隐向量LSH相似度计算：实现“相似歌曲”内容推荐；
3. 完整链路：Scrapy采集→Pandas ETL清洗→HDFS存储→Spark分布式训练→MySQL存储推荐结果→Node.js网页可视化展示；
4. 解决冷启动、数据笛卡尔积爆炸、JDBC写入慢、跨域播放等工程问题。

## ✨ 核心功能
1. 数据采集：定向爬虫抓取网易歌曲ID、封面、歌手元数据；
2. 离线ETL：统一字段、去重、空值清洗，输出标准化CSV上传HDFS；
3. 分布式推荐计算：Spark MLlib ALS训练用户偏好向量，LSH计算歌曲相似度；
4. 数据持久化：歌曲库、个性化推荐、相似歌曲三张业务表入库MySQL；
5. Web前端：用户专属推荐页、歌曲详情相似推荐、网易内嵌播放器展示封面。

## 🛠️ 技术栈 & 参考文档
- 分布式计算：Apache Spark 3.5 PySpark MLlib [官方ALS文档](https://spark.apache.org/docs/latest/ml-collaborative-filtering.html)
- 存储：Hadoop HDFS（伪分布式）、MySQL8.0
- 数据处理：Python Pandas、Scrapy爬虫
- Web后端：Node.js Express [Express路由文档](https://expressjs.com/en/guide/routing.html)
- 前端：EJS模板、原生HTML/CSS、网易iframe播放器

## 📂 项目目录结构
```text
MusicRecSystem/
├── data/
│   ├── raw/              # 原始下载/爬虫CSV数据
│   ├── processed/        # ETL清洗后待上传HDFS文件
│   ├── spider_music163.py# 网易云歌曲爬虫
│   └── sql/init.sql     # MySQL建表脚本
├── etl/
│   └── etl_script.py    # Pandas数据清洗脚本
├── spark_engine/
│   ├── mysql-connector.jar # MySQL JDBC驱动
│   ├── train_fusion_optimized.py # 核心融合训练脚本(ALS+LSH)
│   └── train_model.py    # 简易单数据集测试脚本
├── web_app/
│   ├── app.js            # Express后端服务
│   ├── package.json      # Node依赖配置
│   └── views/            # EJS前端页面
│       ├── index.ejs     # 用户推荐主页
│       └── detail.ejs    # 歌曲相似推荐页
├── docs/                 # 截图、报告素材
├── docker/               # 【待补充】Docker配置空目录
└── README.md
```

## ⚙️ 前置环境依赖
JDK8、Hadoop伪分布式、Spark3.5、MySQL8.0、Python3、Node.js16+

## 🐳 Docker部署（预留配置区）
> 此处Dockerfile、docker-compose.yml待完善，后续补充镜像构建、容器一键启动脚本

## 🚀 完整复现步骤
### 1. 初始化MySQL数据库
```bash
mysql -u root -p
source /home/hadoop/Desktop/MusicRecSystem/data/sql/init.sql;
```

### 2. 数据采集与ETL清洗
```bash
# 1. 运行爬虫获取中文歌曲数据
cd data
python3 spider_music163.py
# 2. Pandas清洗原始数据
cd etl
python3 etl_script.py
# 3. 上传清洗文件至HDFS
hdfs dfs -rm -r /input
hdfs dfs -mkdir /input
hdfs dfs -put ../data/processed/*.csv /input
# 查看HDFS文件
hdfs dfs -ls /input
```

### 3. Hadoop/HDFS启停指令
```bash
# 启动HDFS
/usr/local/hadoop/sbin/start-dfs.sh
# 关闭HDFS
/usr/local/hadoop/sbin/stop-dfs.sh
# 退出HDFS安全模式
hdfs dfsadmin -safemode leave
# 清理HDFS回收站释放磁盘
hdfs dfs -rm -r -skipTrash /user/hadoop/.Trash
```

### 4. Spark模型训练（核心）
```bash
cd spark_engine
# 完整融合训练脚本（4G内存分配）
spark-submit \
--driver-class-path mysql-connector.jar \
--jars mysql-connector.jar \
--driver-memory 4g \
--executor-memory 4g \
train_fusion_optimized.py
```

数据集
spotify:
https://www.kaggle.com/datasets/undefinenull/million-song-dataset-spotify-lastfm



###  使用docker 快速部署
### 5. 启动Node前端网页服务
```bash
cd web_app
# 安装依赖
npm install
# 启动服务，访问http://localhost:3000
node app.js
```

## 📚 公开数据集下载地址
1. Last.fm / Spotify英文交互数据集：https://www.kaggle.com/datasets/undefinenull/million-song-dataset-spotify-lastfm
2. 网易云中文歌单数据：爬虫脚本自行抓取（本项目spider_music163.py）

## ⚠️ 常见问题优化说明
1. LSH相似度计算产生千万级数据：使用Window函数TOP5截断，限制单首歌曲仅保留5个相似结果；
2. MySQL写入缓慢：JDBC链接添加rewriteBatchedStatements=true，批量写入；
3. Spark找不到JDBC：spark-submit携带--jars指定jar包路径；
4. 磁盘空间不足：定期清空HDFS回收站、MySQL TRUNCATE无用大表。
