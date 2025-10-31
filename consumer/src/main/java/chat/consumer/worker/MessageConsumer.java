package chat.consumer.worker;

import chat.consumer.manager.RoomManager;
import chat.consumer.model.ChatMessage;
import chat.consumer.queue.QueueClient;
import chat.consumer.queue.QueueClient.AckContext;
import chat.consumer.util.JsonUtils;
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

    public MessageConsumer(QueueClient queue, RoomManager rooms, int threads) {
        this.queue = queue;
        this.rooms = rooms;
        this.pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "consumer-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /** 使用 AckContext 的新消费入口，保证同通道 ACK/NACK */
    public void start(List<String> roomIds) throws Exception {
        Consumer<AckContext> handler = (ctx) -> pool.execute(() -> handle(ctx));
        queue.consumeRooms(roomIds, handler);
        log.info("MessageConsumer started for rooms {}", roomIds);
    }

    private void handle(AckContext ctx) {
        final long tag = ctx.deliveryTag();
        final String roomId = ctx.roomId();
        try {
            // 解析消息
            String body = new String(ctx.body(), StandardCharsets.UTF_8);
            ChatMessage msg = JsonUtils.M.readValue(body, ChatMessage.class);

            // 业务处理（示例：投递到 RoomManager 或调用 HttpBroadcaster）
            rooms.deliver(roomId, msg);

            // ✅ 成功才 ACK：同一 Channel 上执行
            queue.ack(ctx, false);
            if (log.isDebugEnabled()) {
                log.debug("ACKed message tag={} room={}", tag, roomId);
            }
        } catch (Exception e) {
            // 失败：NACK 并 requeue（或按需丢弃）
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
        pool.shutdownNow();
    }
}
