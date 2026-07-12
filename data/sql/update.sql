/* 
 * 数据库初始化脚本
 * 数据库名: music_rec_sys
 */

CREATE DATABASE IF NOT EXISTS music_rec_sys DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE music_rec_sys;

-- ========================================================
-- 第一部分：手动创建的业务表 (Business Tables)
-- 这些表用于存储用户信息、交互日志、评论等实时数据
-- ========================================================

-- 1. 用户表 (Users)
-- 存储账户信息及冷启动映射ID
DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `uid` int NOT NULL AUTO_INCREMENT,
  `username` varchar(50) NOT NULL,
  `password` varchar(100) NOT NULL,
  `avatar` varchar(200) DEFAULT 'default.png',
  `shadow_id` int DEFAULT NULL COMMENT '用于冷启动的替身ID',
  `public_playlists` tinyint NOT NULL DEFAULT '0',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`uid`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. 用户行为日志表 (User Actions)
-- 记录点击、播放等行为，用于后续增量训练
-- 注意：这里song_id只做索引，不设强制外键，防止Spark重写songs表时报错
DROP TABLE IF EXISTS `user_actions`;
CREATE TABLE `user_actions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `uid` int DEFAULT NULL,
  `song_id` int DEFAULT NULL,
  `action_type` enum('play','like','comment') DEFAULT NULL,
  `weight` int DEFAULT '1' COMMENT '行为权重',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_uid` (`uid`),
  KEY `idx_song_id` (`song_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. 短期收藏表 (Ephemeral Likes)
-- 存储用户点赞/喜欢的数据，支持过期逻辑
DROP TABLE IF EXISTS `ephemeral_likes`;
CREATE TABLE `ephemeral_likes` (
  `uid` int NOT NULL,
  `song_id` int NOT NULL,
  `expire_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`uid`,`song_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. 评论表 (Comments)
DROP TABLE IF EXISTS `comments`;
CREATE TABLE `comments` (
  `id` int NOT NULL AUTO_INCREMENT,
  `uid` int DEFAULT NULL,
  `song_id` int DEFAULT NULL,
  `content` text,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_song_uid` (`song_id`, `uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. 原始评分备份表 (Ratings)
-- 通常用于存档原始CSV导入的数据
DROP TABLE IF EXISTS `ratings`;
CREATE TABLE `ratings` (
  `user_id` int DEFAULT NULL,
  `song_id` int DEFAULT NULL,
  `score` float DEFAULT NULL,
  `timestamp` bigint DEFAULT NULL,
  KEY `idx_user_song` (`user_id`, `song_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ========================================================
-- 第二部分：Spark 自动创建/覆盖的表 (Spark Managed Tables)
-- 下列表结构仅供参考，实际运行 train_fusion_optimized.py 时
-- Spark 会使用 mode="overwrite" 自动创建或覆盖这些表。
-- 无需手动执行建表语句，但需了解其结构。
-- ========================================================

/*
-- [自动生成] 歌曲元数据表
CREATE TABLE `songs` (
  `song_id` int DEFAULT NULL,
  `original_track_id` longtext,
  `title` longtext,
  `artist` longtext,
  `image_url` longtext,
  `source` longtext,
  `genre` longtext,
  `energy` double,
  `tags` longtext
);

-- [自动生成] ALS 推荐结果表
CREATE TABLE `recommendations` (
  `user_id` int NOT NULL,
  `song_id` int DEFAULT NULL,
  `rank_score` float DEFAULT NULL
);

-- [自动生成] LSH 相似歌曲表
CREATE TABLE `related_songs` (
  `song_id` int DEFAULT NULL,
  `related_song_id` int DEFAULT NULL,
  `similarity_score` float DEFAULT NULL
);
*/