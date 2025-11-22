package chat.consumer.worker;

import chat.consumer.manager.RoomManager;
import chat.consumer.model.ChatMessage;
import chat.consumer.queue.QueueClient;
import chat.consumer.queue.QueueClient.AckContext;
import chat.consumer.util.JsonUtils;
import chat.consumer.persistence.DatabaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class MessageConsumer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    private final QueueClient queue;
    private final RoomManager rooms;
    private final ExecutorService pool;
    private final DatabaseWriter databaseWriter;

    public MessageConsumer(QueueClient queue, RoomManager rooms, int threads, DatabaseWriter databaseWriter) {
        this.queue = queue;
        this.rooms = rooms;
        this.databaseWriter = databaseWriter;
        this.pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "consumer-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start(List<String> roomIds) throws Exception {
        Consumer<AckContext> handler = (ctx) -> pool.execute(() -> handle(ctx));
        queue.consumeRooms(roomIds, handler);
        log.info("MessageConsumer started for rooms {}", roomIds);
    }

    private void handle(AckContext ctx) {
        final long tag = ctx.deliveryTag();
        final String roomId = ctx.roomId();

        try {
            String body = new String(ctx.body(), StandardCharsets.UTF_8);
            ChatMessage msg = JsonUtils.M.readValue(body, ChatMessage.class);

            // 修复：从 routing key 设置 roomId（因为 Server 没有在消息体里包含它）
            if (msg.roomId == null || msg.roomId.isEmpty()) {
                msg.roomId = roomId;
            }

            rooms.deliver(roomId, msg);

            boolean added = databaseWriter.addMessage(msg);
            if (!added) {
                log.warn("Failed to add message to database buffer, buffer might be full");
            }

            queue.ack(ctx, false);
            if (log.isDebugEnabled()) {
                log.debug("ACKed message tag={} room={}", tag, roomId);
            }

        } catch (Exception e) {
            try {
                queue.nack(ctx, true);
            } catch (Exception ex) {
                log.error("Failed to NACK tag={} room={}: {}", tag, roomId, ex.toString());
            }
            log.error("Processing failed for room {} tag={}: {}", roomId, tag, e.toString(), e);
        }
    }

    @Override
    public void close() {
        log.info("Closing MessageConsumer...");
        pool.shutdownNow();

        if (databaseWriter != null) {
            try {
                log.info("Waiting for database writes to complete...");
                databaseWriter.flush();
            } catch (Exception e) {
                log.error("Error flushing database writer", e);
            }
        }
        log.info("MessageConsumer closed");
    }
}
