package chat.consumer.dao;

import chat.consumer.model.ChatMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Data Access Object for chat messages
 * Handles all database operations with batch processing support
 */
public class MessageDao {
    private static final Logger log = LoggerFactory.getLogger(MessageDao.class);

    private final DataSource dataSource;

    // Prepared statement SQL
    private static final String INSERT_MESSAGE =
            "INSERT INTO messages (message_id, room_id, user_id, username, message, " +
                    "message_type, timestamp, server_id, client_ip) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE message_id=message_id";

    private static final String UPDATE_USER_STATS =
            "INSERT INTO user_statistics (user_id, username, total_messages, last_activity, rooms_participated) " +
                    "VALUES (?, ?, 1, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "total_messages = total_messages + 1, " +
                    "last_activity = VALUES(last_activity), " +
                    "rooms_participated = CONCAT_WS(',', rooms_participated, ?)";

    private static final String UPDATE_ROOM_STATS =
            "INSERT INTO room_statistics (room_id, total_messages, unique_users, last_activity) " +
                    "VALUES (?, 1, 1, ?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "total_messages = total_messages + 1, " +
                    "last_activity = VALUES(last_activity)";

    private static final String UPDATE_TIME_WINDOW_STATS =
            "INSERT INTO time_window_stats (time_bucket, room_id, message_count, unique_users) " +
                    "VALUES (?, ?, 1, 1) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "message_count = message_count + 1";

    public MessageDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 生成消息ID（如果原始ID为空）
     */
    private String ensureMessageId(String originalId) {
        if (originalId != null && !originalId.trim().isEmpty()) {
            return originalId;
        }
        return UUID.randomUUID().toString();
    }

    /**
     * 批量插入消息（核心方法）
     * @param messages 消息列表
     * @return 成功插入的消息数量
     */
    public int batchInsertMessages(List<ChatMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return 0;
        }

        long startTime = System.currentTimeMillis();
        int successCount = 0;

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);

            // 1. 批量插入消息
            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_MESSAGE)) {
                for (ChatMessage msg : messages) {
                    // 修复：确保 messageId 不为空
                    String messageId = ensureMessageId(msg.messageId);

                    pstmt.setString(1, messageId);
                    pstmt.setString(2, msg.roomId);
                    pstmt.setString(3, msg.userId);
                    pstmt.setString(4, msg.username);
                    pstmt.setString(5, msg.message);
                    pstmt.setString(6, msg.messageType != null ? msg.messageType : "CHAT");
                    pstmt.setTimestamp(7, parseTimestamp(msg.timestamp));
                    pstmt.setString(8, msg.serverId);
                    pstmt.setString(9, msg.clientIp);
                    pstmt.addBatch();
                }

                int[] results = pstmt.executeBatch();
                successCount = results.length;
            }

            // 2. 批量更新用户统计
            try (PreparedStatement pstmt = conn.prepareStatement(UPDATE_USER_STATS)) {
                for (ChatMessage msg : messages) {
                    pstmt.setString(1, msg.userId);
                    pstmt.setString(2, msg.username);
                    pstmt.setTimestamp(3, parseTimestamp(msg.timestamp));
                    pstmt.setString(4, msg.roomId);
                    pstmt.setString(5, msg.roomId);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            // 3. 批量更新房间统计
            try (PreparedStatement pstmt = conn.prepareStatement(UPDATE_ROOM_STATS)) {
                for (ChatMessage msg : messages) {
                    pstmt.setString(1, msg.roomId);
                    pstmt.setTimestamp(2, parseTimestamp(msg.timestamp));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            // 4. 批量更新时间窗口统计
            try (PreparedStatement pstmt = conn.prepareStatement(UPDATE_TIME_WINDOW_STATS)) {
                for (ChatMessage msg : messages) {
                    Timestamp ts = parseTimestamp(msg.timestamp);
                    Timestamp bucket = getMinuteBucket(ts);
                    pstmt.setTimestamp(1, bucket);
                    pstmt.setString(2, msg.roomId);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            conn.commit();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Batch inserted {} messages in {}ms ({} msg/s)",
                    successCount, duration, (successCount * 1000 / Math.max(duration, 1)));

        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                    log.error("Transaction rolled back due to error", e);
                } catch (SQLException ex) {
                    log.error("Failed to rollback transaction", ex);
                }
            }
            log.error("Batch insert failed", e);
            return 0;
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    log.warn("Failed to close connection", e);
                }
            }
        }

        return successCount;
    }

    /**
     * 单条插入消息（用于测试）
     */
    public boolean insertMessage(ChatMessage msg) {
        return batchInsertMessages(List.of(msg)) > 0;
    }

    /**
     * 解析ISO-8601时间戳
     */
    private Timestamp parseTimestamp(String isoTimestamp) {
        try {
            LocalDateTime ldt = LocalDateTime.parse(isoTimestamp,
                    DateTimeFormatter.ISO_DATE_TIME);
            return Timestamp.valueOf(ldt);
        } catch (Exception e) {
            log.warn("Failed to parse timestamp: {}, using current time", isoTimestamp);
            return new Timestamp(System.currentTimeMillis());
        }
    }

    /**
     * 获取分钟级时间桶（用于时间窗口统计）
     */
    private Timestamp getMinuteBucket(Timestamp ts) {
        LocalDateTime ldt = ts.toLocalDateTime();
        LocalDateTime bucket = ldt.withSecond(0).withNano(0);
        return Timestamp.valueOf(bucket);
    }

    /**
     * 测试数据库连接
     */
    public boolean testConnection() {
        try (Connection conn = dataSource.getConnection()) {
            return conn.isValid(5);
        } catch (SQLException e) {
            log.error("Database connection test failed", e);
            return false;
        }
    }
}