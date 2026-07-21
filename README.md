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

## 🛠️ 核心技术栈与学习资源

* **大数据计算**：Apache Spark (PySpark), MLlib 
  * *回顾学习*：[Spark MLlib Collaborative Filtering 官方文档](https://spark.apache.org/docs/latest/ml-collaborative-filtering.html)
* **数据存储**：Hadoop HDFS (数据湖), MySQL 8.0 (业务库)
* **数据工程**：Python (Requests, BeautifulSoup, Pandas)
* **后端服务**：Node.js (Express framework), JDBC
  * *回顾学习*：[Express.js 路由与中间件指南](https://expressjs.com/en/guide/routing.html)
* **前端视图**：EJS 模板引擎, CSS3 (3D Transforms, Flex/Grid), Vanilla JS

---

## 📂 项目结构规划

```text
MusicRecSystem/
├── data/                         # 数据采集与预处理层
│   ├── spider_music163_2.py      # 网易云 API 逆向爬虫 (带随机UA与动态延时防封)
│   ├── shrink_data.py            # Spotify 百万数据集下采样与清洗工具
│   ├── processed/                # 融合清洗后的 CSV 数据，待上传 HDFS
│   └── sql/update.sql            # MySQL 业务表结构初始化脚本
│
├── spark_engine/                 # 核心计算引擎层
│   ├── train_fusion_optimized.py # 【核心】双源融合、ALS训练、LSH批处理计算主脚本
│   ├── train_fusion_optimized_mini.py # 降采样测试脚本 (用于快速验证逻辑)
│   └── mysql-connector.jar       # Spark JDBC 依赖
│
└── web_app/                      # 表现层与业务服务层
    ├── app.js                    # Node.js 后端入口 (处理路由、API代理、Session鉴权)
    └── views/                    # EJS 前端页面
        ├── home.ejs              # 首页 (3D 视觉热歌矩阵、云朵浮动标签)
        ├── player.ejs            # 全屏沉浸式播放页 (封面自动补全、评论/收藏交互)
        ├── profile.ejs           # 个人中心 (播放历史记录、管理数据)
        ├── recommend.ejs         # 专属每日推荐页 (前端分页与随机刷新)
        ├── playlist.ejs          # 场景化歌单展示页
        └── login.ejs / register.ejs # 用户认证体系
```

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