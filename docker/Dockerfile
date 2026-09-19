FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# 新增：替换清华Ubuntu源，解决外网超时
RUN sed -i 's/archive.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list \
    && sed -i 's/security.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list

    # python pip换源
RUN pip3 config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# 1. 安装基础依赖 & Java 8
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk python3 python3-pip wget curl unzip vim openssh-server mysql-server tzdata \
    && ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && dpkg-reconfigure --frontend noninteractive tzdata

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# 2. 安装 Node.js 18+ (修复 EJS 和 ES Module 报错)
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs

# 3. 安装 Hadoop 3.3.6
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -zxf hadoop-3.3.6.tar.gz -C /usr/local && rm hadoop-3.3.6.tar.gz && \
    mv /usr/local/hadoop-3.3.6 /usr/local/hadoop
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
# 配置 SSH 免密
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# 4. 安装 Spark 3.5.0
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -zxf spark-3.5.0-bin-hadoop3.tgz -C /usr/local && rm spark-3.5.0-bin-hadoop3.tgz && \
    mv /usr/local/spark-3.5.0-bin-hadoop3 /usr/local/spark
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# 5. 配置 MySQL 允许远程访问 & 设 root 密码
RUN sed -i 's/127.0.0.1/0.0.0.0/g' /etc/mysql/mysql.conf.d/mysqld.cnf && \
    usermod -d /var/lib/mysql/ mysql && \
    service mysql start && mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root'; FLUSH PRIVILEGES;"

# 6. 安装 Python 依赖
RUN pip3 install pandas bs4 requests py4j

# 7. 配置启动脚本
COPY ./init.sh /usr/init.sh
RUN chmod +x /usr/init.sh

EXPOSE 9000 9870 8080 3000 3306
CMD ["/usr/init.sh"]