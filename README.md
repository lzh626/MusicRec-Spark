### 补充一些正常运行的配置
#### docker linux config
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



#### MusicRecSystem
>   - 📁.vscode
>   - 📁data
>   - 📁docker
>     - 📄Dockerfile
>     - 📄init.sh
>   - 📁docker_linux_backup
>   - 📁docs
>   - 📁host_backup
>     - 📁netease
>     - 📁spotify
>     - 📄music_db_backup.sql
>     - 📄music_db_bk_20260919.sql
>   - 📁spark_engine
>     - 📄incremental_update.py
>     - 📄mysql-connector.jar
>     - 📄previous_version_backup.py
>     - 📄train_fusion_optimized_final.py
>     - 📄train_fusion_optimized_mini.py
>   - 📁web_app
>     - 📁node_modules
>     - 📁views
>       - 📁.vscode
>       - 📄home.ejs
>       - 📄index.ejs
>       - 📄login.ejs
>       - 📄player.ejs
>       - 📄playlist.ejs
>       - 📄profile.ejs
>       - 📄recommend.ejs
>       - 📄register.ejs
>       - 📄similar.ejs
>       - 📄temp.md
>     - 📄app.js
>     - 📄package-lock.json
>     - 📄package.json
>   - 📄.dockerignore
>   - 📄.gitignore
>   - 📄docker-compose.yml
>   - 📄README.md
