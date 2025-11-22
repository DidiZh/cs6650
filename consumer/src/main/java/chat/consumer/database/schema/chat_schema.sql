-- CS6650 Assignment 3 - Chat System Database Schema
-- Database: chat_system
-- Engine: InnoDB (支持事务)
-- Character Set: utf8mb4 (支持emoji)

DROP DATABASE IF EXISTS chat_system;
CREATE DATABASE chat_system CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE chat_system;

-- ============================================
-- 1. 主消息表 (messages)
-- 存储所有聊天消息
-- ============================================
CREATE TABLE messages (
    -- 主键：使用message_id作为唯一标识（来自RabbitMQ消息）
                          message_id VARCHAR(100) PRIMARY KEY,

    -- 消息基本信息
                          room_id VARCHAR(50) NOT NULL,
                          user_id VARCHAR(20) NOT NULL,
                          username VARCHAR(50) NOT NULL,
                          message TEXT NOT NULL,
                          message_type ENUM('TEXT', 'JOIN', 'LEAVE') NOT NULL DEFAULT 'TEXT',

    -- 时间戳 (从消息中的timestamp字段解析)
                          timestamp DATETIME(3) NOT NULL,

    -- 服务器信息
                          server_id VARCHAR(50),
                          client_ip VARCHAR(50),

    -- 元数据
                          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 索引设计
                          INDEX idx_room_time (room_id, timestamp),           -- 核心查询1: 按房间查消息
                          INDEX idx_user_time (user_id, timestamp),           -- 核心查询2: 用户消息历史
                          INDEX idx_timestamp (timestamp),                     -- 核心查询3: 时间范围统计
                          INDEX idx_user_room (user_id, room_id, timestamp)   -- 核心查询4: 用户参与的房间
) ENGINE=InnoDB;

-- ============================================
-- 2. 用户活动统计表 (user_statistics)
-- 预聚合的用户统计数据，加速Analytics查询
-- ============================================
CREATE TABLE user_statistics (
                                 user_id VARCHAR(20) PRIMARY KEY,
                                 username VARCHAR(50),
                                 total_messages INT DEFAULT 0,
                                 last_activity DATETIME(3),
                                 rooms_participated TEXT,  -- 逗号分隔的room_id列表
                                 updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                                 INDEX idx_total_messages (total_messages DESC),    -- 用于"最活跃用户"查询
                                 INDEX idx_last_activity (last_activity)
) ENGINE=InnoDB;

-- ============================================
-- 3. 房间活动统计表 (room_statistics)
-- 预聚合的房间统计数据
-- ============================================
CREATE TABLE room_statistics (
                                 room_id VARCHAR(50) PRIMARY KEY,
                                 total_messages INT DEFAULT 0,
                                 unique_users INT DEFAULT 0,
                                 last_activity DATETIME(3),
                                 updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                                 INDEX idx_total_messages (total_messages DESC),    -- 用于"最活跃房间"查询
                                 INDEX idx_last_activity (last_activity)
) ENGINE=InnoDB;

-- ============================================
-- 4. 时间窗口统计表 (time_window_stats)
-- 按分钟聚合的消息统计，用于快速计算messages/second
-- ============================================
CREATE TABLE time_window_stats (
                                   stat_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                   time_bucket DATETIME NOT NULL,     -- 精确到分钟
                                   room_id VARCHAR(50),
                                   message_count INT DEFAULT 0,
                                   unique_users INT DEFAULT 0,

                                   UNIQUE KEY uk_time_room (time_bucket, room_id),
                                   INDEX idx_time_bucket (time_bucket)
) ENGINE=InnoDB;