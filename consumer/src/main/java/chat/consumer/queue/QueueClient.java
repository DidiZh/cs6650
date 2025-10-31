package chat.consumer.queue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public interface QueueClient extends AutoCloseable {

    void connect() throws Exception;

    /** ✅ 新的主要方法：使用 AckContext */
    void consumeRooms(List<String> roomIds, Consumer<AckContext> handler) throws Exception;

    /** ✅ 新的 ACK/NACK 接口 */
    void ack(AckContext ctx, boolean multiple) throws IOException;
    void nack(AckContext ctx, boolean requeue) throws IOException;

    /** ✅ 携带 Channel + deliveryTag 的上下文，保证"同通道 ACK/NACK" */
    final class AckContext {
        private final com.rabbitmq.client.Channel channel;
        private final long deliveryTag;
        private final String roomId;
        private final Envelope envelope;
        private final AMQP.BasicProperties props;
        private final byte[] body;

        public AckContext(com.rabbitmq.client.Channel channel, long deliveryTag, String roomId,
                          Envelope envelope, AMQP.BasicProperties props, byte[] body) {
            this.channel = channel;
            this.deliveryTag = deliveryTag;
            this.roomId = roomId;
            this.envelope = envelope;
            this.props = props;
            this.body = body;
        }

        public com.rabbitmq.client.Channel channel() { return channel; }
        public long deliveryTag() { return deliveryTag; }
        public String roomId() { return roomId; }
        public Envelope envelope() { return envelope; }
        public AMQP.BasicProperties props() { return props; }
        public byte[] body() { return body; }
    }

    /** ✅ 辅助类：只读的 Delivery（如果需要的话） */
    final class Delivery {
        private final Envelope envelope;
        private final AMQP.BasicProperties props;
        private final byte[] body;

        public Delivery(Envelope envelope, AMQP.BasicProperties props, byte[] body) {
            this.envelope = envelope;
            this.props = props;
            this.body = body;
        }

        public Envelope getEnvelope() { return envelope; }
        public AMQP.BasicProperties getProperties() { return props; }
        public byte[] getBody() { return body; }
    }
}