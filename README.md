


# 🎧 MusicRec-Spark: 基于大数据与多源融合的音乐推荐系统

![Spark](https://img.shields.io/badge/Spark-3.5.0-orange.svg) ![Node.js](https://img.shields.io/badge/Node.js-Express-green.svg) ![MySQL](https://img.shields.io/badge/MySQL-8.0-blue.svg) ![License](https://img.shields.io/badge/License-MIT-brightgreen.svg)

## 📖 项目简介
本项目是一个**端到端的大数据音乐推荐平台**，旨在解决海量音乐数据下的“信息过载”与单一数据源导致的“推荐茧房”问题。

项目采用经典的 **Lambda 架构变体**，创新性地实现了 **Spotify (全球版权) + 网易云音乐 (华语版权)** 的双源异构数据融合。通过 Apache Spark 分布式计算引擎，结合 **ALS (交替最小二乘法)** 与 **LSH (局部敏感哈希)** 双塔算法，为用户提供“千人千面”的个性化推荐与沉浸式的 Web 播放体验。

### ✨ 目前已实现的核心效果
1. **多源数据融合与自动对齐**：成功打通中英双语库，级联过滤清洗僵尸数据，提纯出近万首高质量曲库与数百万条交互记录。
2. **双塔推荐引擎落地**：
   * **精准日推 (ALS)**：基于隐式反馈（播放次数）降维提取用户偏好，生成个性化推荐。
   * **高维相似度计算 (LSH)**：提取物品隐向量进行 ANN 检索，实现了精准的“猜你喜欢”。
3. **全栈业务闭环与极致 UI**：
   * 采用深色磨砂玻璃拟态风格，实现 3D 矩阵布局的响应式界面。
   * 智能切换跨平台外链播放器，并通过 **iTunes API 代理** 实现了缺失专辑封面的 100% 自动补全。
   * 搭建完整的用户体系（注册、登录、收藏、评论），并通过 **Shadow ID 映射机制** 优雅解决了新用户的冷启动问题。

---


##  项目目录结构与功能说明

基于你提供的文件列表，以下是当前项目的完整结构及核心文件功能说明：

| 目录层级 | 文件/文件夹名 | 类型 | 功能描述 |
| :--- | :--- | :--- | :--- |
| **root/** | | | |
| **├── data/** | **数据采集与预处理** | | |
| | `spider_music163_2.py` | Python脚本 | **网易云爬虫**。逆向API接口，爬取热门歌单及歌曲元数据，模拟用户交互数据。 |
| | `shrink_data.py` | Python脚本 | **数据瘦身工具**。对下载的百万级Spotify原始数据进行采样、清洗和格式化，防止内存溢出。 |
| | `processed/` | 文件夹 | 存放爬虫和瘦身脚本生成的最终CSV文件，准备上传至HDFS。 |
| | `mini_data/` | 文件夹 | 存放Spotify的小规模测试数据。 |
| **├── spark_engine/** | **核心计算引擎** | | |
| | `train_fusion_optimized.py` | Python脚本 | **主训练脚本**。读取HDFS双源数据，进行融合、清洗；训练ALS模型生成个性化推荐；分批计算LSH生成相似歌曲；将结果写入MySQL。 |
| | `train_fusion_optimized_mini.py`| Python脚本 | **测试训练脚本**。逻辑同上，但参数和数据量较小，用于快速调试代码逻辑。 |
| | `mysql-connector.jar` | 依赖包 | Spark 连接 MySQL 必须的 JDBC 驱动。 |
| **├── web_app/** | **Web应用服务** | | |
| | `app.js` | JS脚本 | **后端入口**。基于Express框架，处理路由、用户Session登录、数据库查询、API接口（如图片代理、点赞删除）。 |
| | **└── views/** | **前端页面** | |
| | `home.ejs` | 模板 | **首页**。展示3D热歌矩阵、云朵标签导航、全球趋势轮播。 |
| | `player.ejs` | 模板 | **播放页**。全屏沉浸式播放，包含自动补图脚本、侧边栏相似推荐、评论区。 |
| | `profile.ejs` | 模板 | **个人中心**。展示播放历史、我的喜欢、我的评论及管理功能。 |
| | `recommend.ejs` | 模板 | **每日推荐页**。展示基于ALS算法计算的个性化推荐列表。 |
| | `login.ejs` / `register.ejs` | 模板 | 用户登录与注册页面。 |
| | `playlist.ejs` | 模板 | 歌单详情页面。 |



---


## ⚙️ 核心技术难点与工程优化 (Troubleshooting)

本项目在开发过程中克服了多个工业级场景下的典型痛点：

1. **分布式计算的 OOM (内存溢出) 危机**：
   * *挑战*：在计算 LSH 歌曲相似度时，全量自连接（Self Join）导致产生超 3000万条笛卡尔积数据，撑爆本地虚拟机磁盘与内存。
   * *优化*：引入 **Batch Processing (分批计算)** 机制，将特征向量切分为 5 份并行计算；同时引入 Spark SQL 的 **Window 窗口函数** 进行 Top-5 强制截断，将结果集从 3000万行压缩至 5万行内，彻底解决空间爆炸问题。
2. **跨域限制与视觉残缺**：
   * *挑战*：Spotify 数据集缺失封面 URL，且由于 GFW 与 CORS 限制，前端无法直接请求图片。
   * *优化*：在 Node.js 中搭建 `/api/cover_search` 代理层，引入无需鉴权的 **iTunes Search API**。前端 JS 并发检测缺失封面的 DOM 节点，通过“歌名+歌手”精准匹配，实现动态无缝补全。
3. **数据库并发写入瓶颈**：
   * *挑战*：Spark 计算产出的百万级结果写入 MySQL 耗时过长。
   * *优化*：配置 JDBC `rewriteBatchedStatements=true` 与 `batchsize` 参数，实现了写入性能 10 倍以上的提升。

---

## 🛠️ 核心技术栈与学习资源

* **大数据计算**：Apache Spark (PySpark), MLlib 
  * *回顾学习*：[Spark MLlib Collaborative Filtering 官方文档](https://spark.apache.org/docs/latest/ml-collaborative-filtering.html)
* **数据存储**：Hadoop HDFS (数据湖), MySQL 8.0 (业务库)
* **数据工程**：Python (Requests, BeautifulSoup, Pandas)
* **后端服务**：Node.js (Express framework), JDBC
  * *回顾学习*：[Express.js 路由与中间件指南](https://expressjs.com/en/guide/routing.html)
* **前端视图**：EJS 模板引擎, CSS3 (3D Transforms, Flex/Grid), Vanilla JS

---





## 🚀 快速启动

**环境依赖**：Java 8+, Hadoop 3.x, Spark 3.x, MySQL 8.x, Node.js v18+

1. **数据库初始化**：执行 `data/sql/update.sql` 创建业务表。
2. **数据就绪**：运行 `data/` 下的 Python 脚本生成 CSV，并上传至 HDFS 的 `/input/netease` 与 `/input/spotify` 目录下。
3. **模型训练**：
   ```bash
   cd spark_engine
   spark-submit --driver-class-path mysql-connector.jar --jars mysql-connector.jar --driver-memory 4g train_fusion_optimized.py
   ```
4. **启动服务**：
   ```bash
   cd web_app
   npm install
   node app.js
   ```
5. **访问**：打开浏览器访问 `http://localhost:3000`

---


##  项目复现流程总结

假设复现者已配置好 Linux/Windows 环境（Java, Hadoop, Spark, MySQL, Node.js）：

 **流程概述**：
- 获取元数据：
  - spotify：获取原始数据 CSV: 根据自身配置，调整并运行瘦身shrink_data.py
  - netease: 运行 `data/spider_2` 爬虫获取；

- 接着将生成的 CSV 文件上传至 HDFS 的对应目录（`/input/netease` 和 `/input/spotify`）；

- 然后执行 SQL 脚本初始化 MySQL 业务表结构；

- 随后提交 Spark 任务 `train_fusion_optimized_mini.py` 进行离线计算，Spark 会自动清洗数据并将推荐结果和歌曲元数据写入 MySQL；

- 最后进入 `web_app/` 目录安装依赖并启动 `node app.js`，即可在浏览器访问完整的推荐系统。

---



## 🔮 规划与未来拓展 (Future Work)

本系统目前已完成从 0 到 1 的基础建设，未来计划沿以下三个高阶方向进行重构与演进，进一步对标业界一线大厂架构：


---

### 🌟 核心痛点解决：如何让系统“活”起来？(闭环训练)

你目前觉得推荐是“写死”的，是因为你的 Spark 脚本只读取了 HDFS 上的静态历史数据。其实你已经建好了 `user_actions` 表，只要稍微改动 Spark 脚本，就能完成数据的**T+1 闭环**。

**操作思路（在 `train_fusion_optimized_final.py` 中实现）：**

1.  **读取增量数据**：在 Spark 脚本中，通过 JDBC 读取 MySQL 的 `user_actions` 表。
2.  **权重转换**：将 `play`(1分), `like`(3分), `comment`(5分) 转化为 `playcount` 的等效值。
3.  **数据合并**：将从 MySQL 读到的新数据，与 HDFS 读到的老数据进行 `Union` 合并。
4.  **重新训练**：拿着合并后的数据去训练 ALS 模型。


*   **自动化运行**：在 Linux 系统中，配置一个 `crontab` 定时任务，例如每天凌晨 3 点自动执行 `spark-submit train_fusion_optimized_final.py`。第二天用户登录时，推荐结果就自动更新了。

---

### 🚀 三大未来拓展详细指导 (基于当前架构)

#### （一）实时流式计算 (秒级推荐反馈)
*   **当前瓶颈**：必须等每天凌晨跑完 Spark 批处理，用户今天点的红心，明天才能体现在推荐里。
*   **改造路线**：
    1.  **引入消息队列 (Kafka)**：修改 Node.js 后端，当接收到 `/api/log_action` 时，除了写入 MySQL，同时向 Kafka 的 `user_behavior_topic` 发送一条 JSON 消息。
    2.  **实时计算 (Spark Streaming)**：编写一个新的 Spark Streaming 脚本，7x24 小时监听 Kafka。
    3.  **在线推荐策略 (Item-based)**：ALS 模型无法做到秒级更新，工业界的做法是：**离线算 ALS，在线算关联**。
        *   *逻辑*：Spark Streaming 发现用户刚刚“喜欢”了《七里香》(ID: 100)。它立刻去 MySQL 的 `related_songs` 表里查出与 100 最相似的 5 首歌，直接插入到一个叫做 `realtime_recommendations` 的高优缓存表（或 Redis）中。
    4.  **前端展示**：Node.js 获取推荐时，优先读取实时表。

#### （二）深度学习 Graph Embedding (让模型懂“语境”)
*   **当前瓶颈**：ALS 只能统计“共现次数”，无法理解“听歌顺序”。（先听 A 再听 B，和先听 B 再听 A，在 ALS 看来是一样的）。
*   **改造路线**：
    1.  **构建 Session**：将每个用户的 `user_actions` 按时间戳排序，把听过的 song_id 连成一个字符串句子。例如 User1: `["song_A", "song_B", "song_C"]`。
    2.  **应用 Word2Vec (PySpark MLlib 自带)**：将这些“句子”喂给 `Word2Vec` 模型。
    3.  **提取 Embedding**：模型会为每首歌生成一个密集的向量 (Embedding)。经常在一起被听的歌，向量距离就会很近。
    4.  **替换 LSH 输入**：把你现在 LSH 算法里用的 `itemFactors`，替换成 Word2Vec 算出来的 Embedding 向量，相似推荐的准确度会产生质的飞跃！

#### （三）容器化云原生部署 (解决环境依赖与空间占用)
*   **当前瓶颈**：虚拟机占用几十个 G 空间，换台电脑没法跑。
*   **改造路线 (Docker化)**：
    1.  在项目根目录编写 `docker-compose.yml`。
    2.  定义三个容器：`mysql:8.0` (数据库), `node:18` (Web后端), `bitnami/spark` (计算引擎)。
    3.  把数据目录挂载到容器外部。
*   **效果**：只需要在任意电脑上执行 `docker-compose up -d`，整个系统（包括数据库和计算引擎）就会自动拉起，无需配置任何环境变量。


