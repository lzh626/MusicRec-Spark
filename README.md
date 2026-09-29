
#### 实现 三阶段系统落地改造
按照工业界标准的「离线基线 + 近线增量 + 在线实时重排」三层架构，给出逐步改造实现方案。
```
阶段 1：离线基线批处理 (已打通核心，需固化持久化)
  │  └── 产出：全局稳定ID、全量Songs基线、ALS用户/物品向量、全局Top100推荐、LSH相似矩阵
  ▼
阶段 2：近线轻量级增量更新 (新增脚本，分钟级更新)
  │  └── 产出：仅针对 user_actions 中有新行为的用户做向量投影更新，重刷其个人推荐
  ▼
阶段 3：在线实时混合召回与重排 (Node.js 后端改造，秒级响应)
     └── 产出：用户操作后即刻通过 related_songs 触发 I2I 实时召回，与离线Top100动态混排

```



#### 快速启动指南
	Step 1: 拉取 GitHub 源码
	   ↓
	Step 2: 修改 docker-compose.yml（复现用 image: ， 初次用 build:）
	   ↓
	Step 3: 拉取镜像
	   ↓
	Step 4: 启动容器
	   ↓
	Step 5: 恢复 MySQL 和 HDFS 数据
	并验证container环境正常


#### ## 容器启动后操作
1. 进入容器：`docker exec -it music-rec-container bash`
2. 安装依赖：`cd /app/web_app && npm install`
3. 启动应用：`node app.js`


```bash
#运行 Spark 任务前，需要下载 MySQL JDBC 驱动：
cd spark_engine
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar -O mysql-connector.jar

#自行训练指令
#基线训练
spark-submit --driver-class-path mysql-connector.jar --jars mysql-connector.jar --driver-memory 4g train_fusion_optimized_final.py

#近线增量训练
spark-submit --driver-class-path mysql-connector.jar --jars mysql-connector.jar incremental_update.py

```




#### 附：docker linux config
- /usr/local/hadoop/etc/hadoop/ 
core-site.xml:
```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
~                            ```                                                                                     

hdfs-site.xml:
```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:/usr/local/hadoop/data/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/usr/local/hadoop/data/dfs/data</value>
    </property>
</configuration>
```
- ~/.bashrc:

```
# ================= Docker Env =================
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib

export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Spark 
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
# root 
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

```

---
#### DictionaryTree
MusicRecSystem
├─ 📁.vscode
├─ 📁data
│  ├─ 📁etl
│  │  └─ 📄etl_spotify.py
│  ├─ 📁processed
│  ├─ 📁processed_mini
│  │  ├─ 📄spotify_interactions_mini.csv
│  │  └─ 📄spotify_tracks_mini.csv
│  ├─ 📁raw
│  ├─ 📁sql
│  │  ├─ 📄update_1.sql
│  │  └─ 📄update_2.sql
│  ├─ 📄shrink_data.py
│  ├─ 📄spider_music163_1.py
│  ├─ 📄spider_music163_2.py
│  └─ 📄spider_state.txt
├─ 📁docker
│  ├─ 📄Dockerfile
│  └─ 📄init.sh
├─ 📁docker_linux_backup
├─ 📁docs
├─ 📁host_backup
│  ├─ 📁netease
│  │  ├─ 📄interactions_final.csv
│  │  └─ 📄tracks_meta_final.csv
│  ├─ 📁spotify
│  │  ├─ 📄spotify_interactions_mini.csv
│  │  └─ 📄spotify_tracks_mini.csv
│  └─ 📄music_db_backup.sql
├─ 📁spark_engine
│  ├─ 📄incremental_update.py
│  ├─ 📄previous_version_backup.py
│  ├─ 📄train_fusion_optimized_final.py
│  └─ 📄train_fusion_optimized_mini.py
├─ 📁web_app
│  ├─ 📁node_modules
│  ├─ 📁views
│  │  ├─ 📁.vscode
│  │  ├─ 📄home.ejs
│  │  ├─ 📄index.ejs
│  │  ├─ 📄login.ejs
│  │  ├─ 📄player.ejs
│  │  ├─ 📄playlist.ejs
│  │  ├─ 📄profile.ejs
│  │  ├─ 📄recommend.ejs
│  │  ├─ 📄register.ejs
│  │  ├─ 📄similar.ejs
│  │  └─ 📄temp.md
│  ├─ 📄app.js
│  ├─ 📄package-lock.json
│  └─ 📄package.json
├─ 📄.dockerignore
├─ 📄.gitignore
├─ 📄docker-compose.yml
└─ 📄README.md
```