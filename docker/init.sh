#!/bin/bash
# 1. 格式化HDFS (仅首次)
if [ ! -d "/usr/local/hadoop/data/dfs/name" ]; then
    echo "Formatting HDFS..."
    hdfs namenode -format -force
fi

# 2. 启动服务
service ssh start
start-dfs.sh
service mysql start

# 3. 初始化 HDFS 目录 (适配你的双源架构)
hdfs dfs -mkdir -p /input/netease
hdfs dfs -mkdir -p /input/spotify

# 4. 初始化 MySQL 权限与数据库
mysql -u root -proot -e "CREATE DATABASE IF NOT EXISTS music_rec_sys DEFAULT CHARSET utf8mb4;"
mysql -u root -proot -e "CREATE USER IF NOT EXISTS 'hadoop_master'@'%' IDENTIFIED BY 'hadoop';"
mysql -u root -proot -e "GRANT ALL PRIVILEGES ON music_rec_sys.* TO 'hadoop_master'@'%'; FLUSH PRIVILEGES;"

# 导入建表SQL (如果存在)
if [ -f "/app/data/sql/update.sql" ]; then
    mysql -u root -proot music_rec_sys < /app/data/sql/update.sql
fi

echo "✅ 大数据环境初始化完成：HDFS + MySQL 就绪"
# 保持容器运行
tail -f /dev/null