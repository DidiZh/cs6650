package chat.consumer.supervisor;

import chat.consumer.broadcast.Broadcaster;
import chat.consumer.manager.RoomManager;
import chat.consumer.queue.QueueClient;
import chat.consumer.worker.MessageConsumer;
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
                              int prefetch) {
        this.queue = queue;
        this.rooms = rooms;
        this.rooms.setBroadcaster(broadcaster);
        this.initialThreads = initialThreads;
        this.autoScale = autoScale;
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.prefetch = prefetch;
        this.currentThreads = initialThreads;
    }

    public void start(List<String> roomIds) throws Exception {
        // Save roomIds for potential restart
        this.roomIds = roomIds;

        consumer = new MessageConsumer(queue, rooms, initialThreads);
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
                consumer = new MessageConsumer(queue, rooms, threads);
                consumer.start(roomIds);

                currentThreads = threads;
                log.info("AutoScale completed: now running with {} threads", threads);
            }
        } catch (Exception e) {
            log.error("AutoScale failed: {}", e.getMessage(), e);
        }
    }

    public void stop() {
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
    }
}