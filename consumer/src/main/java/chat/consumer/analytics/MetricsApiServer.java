package chat.consumer.analytics;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Metrics API Server - REST endpoint for analytics queries
 * Listens on port 9090 by default
 */
public class MetricsApiServer {
    private static final Logger log = LoggerFactory.getLogger(MetricsApiServer.class);

    private final AnalyticsService analyticsService;
    private final int port;
    private Javalin app;
    private final Gson gson;

    public MetricsApiServer(DataSource dataSource, int port) {
        this.analyticsService = new AnalyticsService(dataSource);
        this.port = port;
        this.gson = new GsonBuilder().setPrettyPrinting().create();
    }

    /**
     * 启动API服务器
     */
    public void start() {
        app = Javalin.create(config -> {
            config.showJavalinBanner = false;
            config.http.defaultContentType = "application/json";
        }).start(port);

        // Health check endpoint
        app.get("/health", ctx -> {
            ctx.json(new HealthResponse("ok", System.currentTimeMillis()));
        });

        // Main metrics endpoint
        app.get("/metrics", this::handleMetrics);

        // Individual core query endpoints (optional, for debugging)
        app.get("/metrics/room/{roomId}", this::handleRoomMessages);
        app.get("/metrics/user/{userId}", this::handleUserHistory);
        app.get("/metrics/active-users", this::handleActiveUsers);
        app.get("/metrics/top-users", this::handleTopUsers);
        app.get("/metrics/top-rooms", this::handleTopRooms);

        log.info("✅ Metrics API Server started on port {}", port);
        log.info("   Access metrics at: http://localhost:{}/metrics", port);
    }

    /**
     * Main metrics endpoint - returns all analytics data
     */
    private void handleMetrics(Context ctx) {
        long startTime = System.currentTimeMillis();

        try {
            AnalyticsService.MetricsResponse metrics = analyticsService.getAllMetrics();
            long duration = System.currentTimeMillis() - startTime;

            log.info("Metrics request processed in {}ms", duration);

            ctx.json(metrics);

        } catch (Exception e) {
            log.error("Error handling metrics request", e);
            ctx.status(500).json(new ErrorResponse("Internal server error: " + e.getMessage()));
        }
    }

    /**
     * Get messages for a specific room
     */
    private void handleRoomMessages(Context ctx) {
        String roomId = ctx.pathParam("roomId");
        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(100);

        var messages = analyticsService.getMessagesForRoom(roomId, null, null, limit);
        ctx.json(messages);
    }

    /**
     * Get user message history
     */
    private void handleUserHistory(Context ctx) {
        String userId = ctx.pathParam("userId");
        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(50);

        var messages = analyticsService.getUserMessageHistory(userId, limit);
        ctx.json(messages);
    }

    /**
     * Get active users count
     */
    private void handleActiveUsers(Context ctx) {
        int hours = ctx.queryParamAsClass("hours", Integer.class).getOrDefault(24);
        int count = analyticsService.countActiveUsers(hours);

        ctx.json(new ActiveUsersResponse(count, hours));
    }

    /**
     * Get top active users
     */
    private void handleTopUsers(Context ctx) {
        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(10);
        var users = analyticsService.getTopActiveUsers(limit);
        ctx.json(users);
    }

    /**
     * Get top active rooms
     */
    private void handleTopRooms(Context ctx) {
        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(10);
        var rooms = analyticsService.getTopActiveRooms(limit);
        ctx.json(rooms);
    }

    /**
     * 停止API服务器
     */
    public void stop() {
        if (app != null) {
            app.stop();
            log.info("Metrics API Server stopped");
        }
    }

    // Response classes
    private static class HealthResponse {
        public final String status;
        public final long timestamp;

        HealthResponse(String status, long timestamp) {
            this.status = status;
            this.timestamp = timestamp;
        }
    }

    private static class ActiveUsersResponse {
        public final int count;
        public final int hoursWindow;

        ActiveUsersResponse(int count, int hours) {
            this.count = count;
            this.hoursWindow = hours;
        }
    }

    private static class ErrorResponse {
        public final String error;

        ErrorResponse(String error) {
            this.error = error;
        }
    }
}