package chat.consumer.analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Analytics service for querying chat statistics
 */
public class AnalyticsService {
    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);

    private final DataSource dataSource;

    public AnalyticsService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 获取所有metrics（核心查询 + Analytics查询）
     */
    public MetricsResponse getAllMetrics() {
        MetricsResponse response = new MetricsResponse();

        try {
            // Core Queries
            response.coreQueries = new CoreQueries();
            response.coreQueries.sampleRoomMessages = getMessagesForRoom("1", null, null, 100);
            response.coreQueries.sampleUserHistory = getUserMessageHistory("1", 50);
            response.coreQueries.activeUsersLast24h = countActiveUsers(24);
            response.coreQueries.userRooms = getUserRooms("1");

            // Analytics Queries
            response.analytics = new Analytics();
            response.analytics.totalMessages = getTotalMessages();
            response.analytics.messagesPerMinute = getMessagesPerMinute();
            response.analytics.topActiveUsers = getTopActiveUsers(10);
            response.analytics.topActiveRooms = getTopActiveRooms(10);
            response.analytics.userParticipationSummary = getUserParticipationSummary();

            // Database stats
            response.databaseStats = getDatabaseStats();

            response.success = true;
            response.timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

        } catch (Exception e) {
            log.error("Error gathering metrics", e);
            response.success = false;
            response.error = e.getMessage();
        }

        return response;
    }

    /**
     * Core Query 1: Get messages for a room in time range
     */
    public List<MessageSummary> getMessagesForRoom(String roomId, String startTime, String endTime, int limit) {
        List<MessageSummary> messages = new ArrayList<>();

        String sql = "SELECT message_id, user_id, username, message, message_type, timestamp " +
                "FROM messages WHERE room_id = ? ";

        if (startTime != null) sql += "AND timestamp >= ? ";
        if (endTime != null) sql += "AND timestamp <= ? ";
        sql += "ORDER BY timestamp DESC LIMIT ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            int paramIndex = 1;
            pstmt.setString(paramIndex++, roomId);
            if (startTime != null) pstmt.setString(paramIndex++, startTime);
            if (endTime != null) pstmt.setString(paramIndex++, endTime);
            pstmt.setInt(paramIndex, limit);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                MessageSummary msg = new MessageSummary();
                msg.messageId = rs.getString("message_id");
                msg.userId = rs.getString("user_id");
                msg.username = rs.getString("username");
                msg.message = rs.getString("message");
                msg.messageType = rs.getString("message_type");
                msg.timestamp = rs.getString("timestamp");
                messages.add(msg);
            }

        } catch (SQLException e) {
            log.error("Error getting room messages", e);
        }

        return messages;
    }

    /**
     * Core Query 2: Get user's message history
     */
    public List<MessageSummary> getUserMessageHistory(String userId, int limit) {
        List<MessageSummary> messages = new ArrayList<>();

        String sql = "SELECT message_id, room_id, username, message, message_type, timestamp " +
                "FROM messages WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, userId);
            pstmt.setInt(2, limit);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                MessageSummary msg = new MessageSummary();
                msg.messageId = rs.getString("message_id");
                msg.roomId = rs.getString("room_id");
                msg.username = rs.getString("username");
                msg.message = rs.getString("message");
                msg.messageType = rs.getString("message_type");
                msg.timestamp = rs.getString("timestamp");
                messages.add(msg);
            }

        } catch (SQLException e) {
            log.error("Error getting user history", e);
        }

        return messages;
    }

    /**
     * Core Query 3: Count active users in time window
     */
    public int countActiveUsers(int hours) {
        String sql = "SELECT COUNT(DISTINCT user_id) as count FROM messages " +
                "WHERE timestamp >= DATE_SUB(NOW(), INTERVAL ? HOUR)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, hours);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("count");
            }

        } catch (SQLException e) {
            log.error("Error counting active users", e);
        }

        return 0;
    }

    /**
     * Core Query 4: Get rooms user has participated in
     */
    public List<RoomParticipation> getUserRooms(String userId) {
        List<RoomParticipation> rooms = new ArrayList<>();

        String sql = "SELECT room_id, COUNT(*) as message_count, MAX(timestamp) as last_activity " +
                "FROM messages WHERE user_id = ? GROUP BY room_id ORDER BY last_activity DESC";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, userId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                RoomParticipation room = new RoomParticipation();
                room.roomId = rs.getString("room_id");
                room.messageCount = rs.getInt("message_count");
                room.lastActivity = rs.getString("last_activity");
                rooms.add(room);
            }

        } catch (SQLException e) {
            log.error("Error getting user rooms", e);
        }

        return rooms;
    }

    /**
     * Analytics: Total messages count
     */
    public long getTotalMessages() {
        String sql = "SELECT COUNT(*) as count FROM messages";

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return rs.getLong("count");
            }

        } catch (SQLException e) {
            log.error("Error getting total messages", e);
        }

        return 0;
    }

    /**
     * Analytics: Messages per minute
     */
    public double getMessagesPerMinute() {
        String sql = "SELECT " +
                "COUNT(*) as total, " +
                "TIMESTAMPDIFF(MINUTE, MIN(timestamp), MAX(timestamp)) as minutes " +
                "FROM messages";

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                long total = rs.getLong("total");
                long minutes = rs.getLong("minutes");
                if (minutes > 0) {
                    return (double) total / minutes;
                }
            }

        } catch (SQLException e) {
            log.error("Error calculating messages per minute", e);
        }

        return 0.0;
    }

    /**
     * Analytics: Top N active users
     */
    public List<UserStats> getTopActiveUsers(int limit) {
        List<UserStats> users = new ArrayList<>();

        String sql = "SELECT user_id, username, COUNT(*) as message_count " +
                "FROM messages GROUP BY user_id, username " +
                "ORDER BY message_count DESC LIMIT ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, limit);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                UserStats user = new UserStats();
                user.userId = rs.getString("user_id");
                user.username = rs.getString("username");
                user.messageCount = rs.getInt("message_count");
                users.add(user);
            }

        } catch (SQLException e) {
            log.error("Error getting top users", e);
        }

        return users;
    }

    /**
     * Analytics: Top N active rooms
     */
    public List<RoomStats> getTopActiveRooms(int limit) {
        List<RoomStats> rooms = new ArrayList<>();

        String sql = "SELECT room_id, COUNT(*) as message_count, " +
                "COUNT(DISTINCT user_id) as unique_users " +
                "FROM messages GROUP BY room_id " +
                "ORDER BY message_count DESC LIMIT ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, limit);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                RoomStats room = new RoomStats();
                room.roomId = rs.getString("room_id");
                room.messageCount = rs.getInt("message_count");
                room.uniqueUsers = rs.getInt("unique_users");
                rooms.add(room);
            }

        } catch (SQLException e) {
            log.error("Error getting top rooms", e);
        }

        return rooms;
    }

    /**
     * Analytics: User participation summary
     */
    public Map<String, Object> getUserParticipationSummary() {
        Map<String, Object> summary = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            // Total unique users
            ResultSet rs1 = stmt.executeQuery("SELECT COUNT(DISTINCT user_id) as count FROM messages");
            if (rs1.next()) {
                summary.put("totalUniqueUsers", rs1.getInt("count"));
            }

            // Average messages per user
            ResultSet rs2 = stmt.executeQuery(
                    "SELECT AVG(msg_count) as avg FROM " +
                            "(SELECT COUNT(*) as msg_count FROM messages GROUP BY user_id) as user_counts"
            );
            if (rs2.next()) {
                summary.put("avgMessagesPerUser", rs2.getDouble("avg"));
            }

        } catch (SQLException e) {
            log.error("Error getting participation summary", e);
        }

        return summary;
    }

    /**
     * Database stats
     */
    public Map<String, Object> getDatabaseStats() {
        Map<String, Object> stats = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(
                    "SELECT " +
                            "(SELECT COUNT(*) FROM messages) as msg_count, " +
                            "(SELECT COUNT(*) FROM user_statistics) as user_count, " +
                            "(SELECT COUNT(*) FROM room_statistics) as room_count"
            );

            if (rs.next()) {
                stats.put("totalMessagesStored", rs.getLong("msg_count"));
                stats.put("totalUsersTracked", rs.getLong("user_count"));
                stats.put("totalRoomsTracked", rs.getLong("room_count"));
            }

        } catch (SQLException e) {
            log.error("Error getting database stats", e);
        }

        return stats;
    }

    // ==================== Response Classes ====================

    public static class MetricsResponse {
        public boolean success;
        public String timestamp;
        public String error;
        public CoreQueries coreQueries;
        public Analytics analytics;
        public Map<String, Object> databaseStats;
    }

    public static class CoreQueries {
        public List<MessageSummary> sampleRoomMessages;
        public List<MessageSummary> sampleUserHistory;
        public int activeUsersLast24h;
        public List<RoomParticipation> userRooms;
    }

    public static class Analytics {
        public long totalMessages;
        public double messagesPerMinute;
        public List<UserStats> topActiveUsers;
        public List<RoomStats> topActiveRooms;
        public Map<String, Object> userParticipationSummary;
    }

    public static class MessageSummary {
        public String messageId;
        public String roomId;
        public String userId;
        public String username;
        public String message;
        public String messageType;
        public String timestamp;
    }

    public static class RoomParticipation {
        public String roomId;
        public int messageCount;
        public String lastActivity;
    }

    public static class UserStats {
        public String userId;
        public String username;
        public int messageCount;
    }

    public static class RoomStats {
        public String roomId;
        public int messageCount;
        public int uniqueUsers;
    }
}