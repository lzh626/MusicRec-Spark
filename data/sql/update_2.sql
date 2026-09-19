/* 
 * 数据库初始化与升级脚本 (阶段二：双稳定ID闭环架构)
 * 数据库名: music_rec_sys
 * 适用版本: Spark离线训练 + Node.js Web服务 + 增量反馈闭环
 */

CREATE DATABASE IF NOT EXISTS `music_rec_sys` DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `music_rec_sys`;

-- ========================================================
-- 第一部分：业务交互表 (Web App 读写维护)
-- ========================================================

-- 1. 用户表 (Users)
-- 维护基础账号信息，包含阶段二新增的全局永久映射字段 stable_uid_hash
DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `uid` int NOT NULL AUTO_INCREMENT,
  `username` varchar(50) NOT NULL,
  `password` varchar(100) NOT NULL,
  `avatar` varchar(200) DEFAULT 'default.png',
  `shadow_id` int DEFAULT NULL COMMENT '用于冷启动的替身ID',
  `public_playlists` tinyint NOT NULL DEFAULT '0',
  `stable_uid_hash` bigint DEFAULT NULL COMMENT '与算法对齐的永久稳定用户哈希',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`uid`),
  UNIQUE KEY `uniq_username` (`username`),
  KEY `idx_stable_uid_hash` (`stable_uid_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. 用户行为日志表 (User Actions)
-- 增量训练的核心源头：记录播放、点赞、取消喜欢、评论等行为
-- 关键变更：song_id 升级为 BIGINT，action_type 采用 varchar 兼容各类动作
DROP TABLE IF EXISTS `user_actions`;
CREATE TABLE `user_actions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `uid` int DEFAULT NULL,
  `stable_uid_hash` bigint DEFAULT NULL COMMENT '用户稳定哈希（供Spark直接读取）',
  `song_id` bigint DEFAULT NULL COMMENT '歌曲稳定哈希',
  `action_type` varchar(20) NOT NULL COMMENT '行为类型: play, like, unlike, comment',
  `weight` int DEFAULT '1' COMMENT '协同过滤隐式评分权重',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_uid` (`uid`),
  KEY `idx_stable_uid` (`stable_uid_hash`),
  KEY `idx_song_id` (`song_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. 临时喜欢表 (Ephemeral Likes)
-- 记录用户收藏的点赞记录（支持 TTL 过期逻辑）
-- 关键变更：song_id 升级为 BIGINT 保持全库一致
DROP TABLE IF EXISTS `ephemeral_likes`;
CREATE TABLE `ephemeral_likes` (
  `uid` int NOT NULL,
  `song_id` bigint NOT NULL COMMENT '对应 songs.song_id (BIGINT)',
  `expire_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`uid`, `song_id`),
  KEY `idx_song_id` (`song_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. 评论表 (Comments)
-- 存储歌曲评论，供播放页呈现
-- 关键变更：song_id 升级为 BIGINT 保持全库一致
DROP TABLE IF EXISTS `comments`;
CREATE TABLE `comments` (
  `id` int NOT NULL AUTO_INCREMENT,
  `uid` int DEFAULT NULL,
  `song_id` bigint DEFAULT NULL COMMENT '对应 songs.song_id (BIGINT)',
  `content` text,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_song_uid` (`song_id`, `uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. 原始评分备份表 (Ratings)
-- 归档外部导入的历史基础评分记录
DROP TABLE IF EXISTS `ratings`;
CREATE TABLE `ratings` (
  `user_id` int DEFAULT NULL,
  `song_id` bigint DEFAULT NULL,
  `score` float DEFAULT NULL,
  `timestamp` bigint DEFAULT NULL,
  KEY `idx_user_song` (`user_id`, `song_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ========================================================
-- 第二部分：推荐系统核心表 (Spark 算法引擎计算产出)
-- 提前建立好表结构及索引，防止 Spark overwrite 时丢掉索引导致查询变慢
-- ========================================================

-- 6. 歌曲元数据表 (Songs)
-- 存储双源（网易云 + Spotify）的歌曲信息
-- 关键说明：song_id 为经过哈希转换的全局持久唯一 BIGINT 主键
DROP TABLE IF EXISTS `songs`;
CREATE TABLE `songs` (
  `song_id` bigint NOT NULL,
  `original_track_id` text COMMENT '原始平台ID或播放标识',
  `title` text,
  `artist` text,
  `image_url` text,
  `source` varchar(30) DEFAULT NULL COMMENT 'netease 或 spotify',
  `genre` varchar(50) DEFAULT 'Pop',
  `energy` double DEFAULT '0',
  `tags` text,
  PRIMARY KEY (`song_id`),
  KEY `idx_source` (`source`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7. 个性化推荐表 (Recommendations)
-- ALS 矩阵分解离线训练直接输出的 Top N 推荐清单
-- 关键说明：使用 stable_uid_hash 关联用户，无需每次重训练都更新 Web 用户表
DROP TABLE IF EXISTS `recommendations`;
CREATE TABLE `recommendations` (
  `stable_uid_hash` bigint NOT NULL,
  `song_id` bigint NOT NULL,
  `rank_score` float DEFAULT NULL COMMENT 'ALS 预测评分',
  PRIMARY KEY (`stable_uid_hash`, `song_id`),
  KEY `idx_stable_uid` (`stable_uid_hash`),
  KEY `idx_song` (`song_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 8. 相似歌曲关联表 (Related Songs)
-- LSH (局部敏感哈希) 基于物品特征向量近邻召回的相似歌曲对
-- 关键说明：供播放页“猜你喜欢”与冷启动兜底毫秒级查询
DROP TABLE IF EXISTS `related_songs`;
CREATE TABLE `related_songs` (
  `song_id` bigint NOT NULL,
  `related_song_id` bigint NOT NULL,
  `similarity_score` double DEFAULT NULL COMMENT '余弦/欧氏空间相似度',
  PRIMARY KEY (`song_id`, `related_song_id`),
  KEY `idx_song` (`song_id`),
  KEY `idx_related` (`related_song_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ========================================================
-- 初始化基础测试账号 (可选)
-- 密码明文: 111, stable_uid_hash 预设为测试值 96321
-- ========================================================
INSERT INTO `users` (`uid`, `username`, `password`, `stable_uid_hash`) 
VALUES (1, 'aaa', '111', 96321)
ON DUPLICATE KEY UPDATE `stable_uid_hash`=VALUES(`stable_uid_hash`);