package chat.consumer.supervisor;

import chat.consumer.broadcast.Broadcaster;
import chat.consumer.manager.RoomManager;
import chat.consumer.queue.QueueClient;
import chat.consumer.worker.MessageConsumer;
import chat.consumer.config.DatabaseConfig;
import chat.consumer.dao.MessageDao;
import chat.consumer.persistence.DatabaseWriter;
import chat.consumer.analytics.MetricsApiServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages consumer thread pool lifecycle and optional auto-scaling.
 * Broadcaster is injected externally (HTTP, gRPC, Noop, etc.).
 */
public class ConsumerSupervisor {
    private static final Logger log = LoggerFactory.getLogger(ConsumerSupervisor.class);

    private final QueueClient queue;
    private final RoomManager rooms;
    private final int initialThreads;
    private final boolean autoScale;
    private final int minThreads;
    private final int maxThreads;
    private final int prefetch;

    // Cached roomIds for restart
    private List<String> roomIds;

    // 数据库相关 - 新增这三行
    private final DatabaseConfig dbConfig;
    private final MessageDao messageDao;
    private final DatabaseWriter databaseWriter;
    private final MetricsApiServer metricsApiServer;

    private MessageConsumer consumer;
    private ScheduledExecutorService scaler;
    private volatile int currentThreads;

    public ConsumerSupervisor(QueueClient queue,
                              RoomManager rooms,
                              Broadcaster broadcaster,
                              int initialThreads,
                              boolean autoScale,
                              int minThreads,
                              int maxThreads,
                              int prefetch) throws Exception {
        this.queue = queue;
        this.rooms = rooms;
        this.rooms.setBroadcaster(broadcaster);
        this.initialThreads = initialThreads;
        this.autoScale = autoScale;
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.prefetch = prefetch;
        this.currentThreads = initialThreads;

        // 初始化数据库配置
        this.dbConfig = new DatabaseConfig();
        this.dbConfig.initialize();
        this.messageDao = new MessageDao(dbConfig.getDataSource());

        // 初始化DatabaseWriter
        // 参数：batchSize=1000, flushInterval=500ms, writerThreads=1, bufferCapacity=10000
        this.databaseWriter = new DatabaseWriter(messageDao, 1000, 500, 1, 10000);
        this.databaseWriter.start();

        log.info("Database writer initialized and started");

// 初始化Metrics API Server
        try {
            this.metricsApiServer = new MetricsApiServer(dbConfig.getDataSource(), 9090);
            this.metricsApiServer.start();
        } catch (Exception e) {
            log.error("Failed to start Metrics API Server", e);
            throw new RuntimeException("Metrics API initialization failed", e);
        }
    }

    public void start(List<String> roomIds) throws Exception {
        // Save roomIds for potential restart
        this.roomIds = roomIds;

        consumer = new MessageConsumer(queue, rooms, initialThreads, databaseWriter);
        consumer.start(roomIds);

        // ✅ FORCE DISABLE AutoScale for stability
        if (false) {  // Changed from: if (autoScale)
            scaler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "autoscaler");
                t.setDaemon(true);
                return t;
            });
            scaler.scheduleAtFixedRate(this::maybeScale, 15, 15, TimeUnit.SECONDS);
        }
        log.info("Supervisor started with {} threads for {} rooms (AutoScale: DISABLED)",
                initialThreads, roomIds.size());
    }

    private void maybeScale() {
        try {
            long processed = rooms.messagesProcessed.getAndSet(0);

            // Calculate thread count based on message processing rate
            int threads;
            if (processed == 0) {
                // Keep current thread count or reduce to minimum
                threads = Math.max(minThreads, currentThreads / 2);
            } else {
                threads = Math.max(minThreads,
                        Math.min(maxThreads, (int) Math.ceil(processed / (double) (prefetch * 10))));
            }

            if (threads != currentThreads) {
                log.info("AutoScale: {} -> {} (processed in 15s: {})", currentThreads, threads, processed);

                // Close old consumer
                if (consumer != null) {
                    consumer.close();
                }

                // Create and start new consumer
                consumer = new MessageConsumer(queue, rooms, threads, databaseWriter);
                consumer.start(roomIds);

                currentThreads = threads;
                log.info("AutoScale completed: now running with {} threads", threads);
            }
        } catch (Exception e) {
            log.error("AutoScale failed: {}", e.getMessage(), e);
        }
    }

    public void stop() {
        log.info("Stopping ConsumerSupervisor...");

        try {
            if (scaler != null) {
                scaler.shutdownNow();
            }
        } catch (Exception e) {
            log.warn("Failed to stop scaler: {}", e.getMessage());
        }

        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close consumer: {}", e.getMessage());
        }

        // 关闭数据库写入器
        try {
            if (databaseWriter != null) {
                log.info("Closing database writer...");
                databaseWriter.close();

                // 打印最终统计
                DatabaseWriter.WriterStats stats = databaseWriter.getStats();
                log.info("Final database stats: {}", stats);
            }
        } catch (Exception e) {
            log.error("Failed to close database writer: {}", e.getMessage());
        }

        try {
            if (metricsApiServer != null) {
                metricsApiServer.stop();
            }
        } catch (Exception e) {
            log.error("Failed to stop metrics API server: {}", e.getMessage());
        }

        // 关闭数据库连接池
        try {
            if (dbConfig != null) {
                dbConfig.close();
            }
        } catch (Exception e) {
            log.error("Failed to close database config: {}", e.getMessage());
        }

        log.info("ConsumerSupervisor stopped");
    }
}