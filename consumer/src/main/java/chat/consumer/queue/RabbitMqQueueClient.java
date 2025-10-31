package chat.consumer.queue;

import chat.consumer.config.ConsumerConfig;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class RabbitMqQueueClient implements QueueClient, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RabbitMqQueueClient.class);

    private final ConsumerConfig cfg;
    private Connection conn;

    private final Map<String, Channel> channelsByQueue = new ConcurrentHashMap<>();

    public RabbitMqQueueClient(ConsumerConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public void connect() throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(cfg.host);
        f.setPort(cfg.port);
        f.setUsername(cfg.username);
        f.setPassword(cfg.password);
        f.setVirtualHost(cfg.vhost);
        f.setAutomaticRecoveryEnabled(true);
        f.setNetworkRecoveryInterval(3000);

        this.conn = f.newConnection("chat-consumer");
        log.info("[MQ] Connected to {}:{} vhost={} user={} (automaticRecovery={})",
                cfg.host, cfg.port, cfg.vhost, cfg.username, f.isAutomaticRecoveryEnabled());
    }

    @Override
    public void consumeRooms(List<String> roomIds, Consumer<QueueClient.AckContext> handler) throws Exception {
        for (String roomId : roomIds) {
            final String queue = cfg.queuePrefix + roomId;
            final Channel ch = conn.createChannel();
            ch.basicQos(cfg.prefetch);

            try {
                ch.queueDeclare(queue, true, false, false, null);
                ch.exchangeDeclare(cfg.exchange, BuiltinExchangeType.TOPIC, true);
                ch.queueBind(queue, cfg.exchange, queue);
            } catch (IOException e) {
                log.warn("[MQ] Topology declare/bind failed for queue {}: {}", queue, e.toString());
            }

            channelsByQueue.put(queue, ch);

            ch.basicConsume(queue, false, new DefaultConsumer(ch) {
                @Override
                public void handleDelivery(String consumerTag,
                                           Envelope env,
                                           AMQP.BasicProperties props,
                                           byte[] body) throws IOException {
                    QueueClient.AckContext ctx = new QueueClient.AckContext(
                            ch, env.getDeliveryTag(), roomId, env, props, body);
                    handler.accept(ctx);
                }
            });

            log.info("Consuming room {} on queue {} (exchange={}, prefetch={})",
                    roomId, queue, cfg.exchange, cfg.prefetch);
        }
    }

    @Override
    public void ack(QueueClient.AckContext ctx, boolean multiple) throws IOException {
        Channel ch = ctx.channel();
        if (ch != null && ch.isOpen()) {
            ch.basicAck(ctx.deliveryTag(), multiple);
            if (log.isDebugEnabled()) {
                log.debug("[ACK] deliveryTag={} multiple={} via {}",
                        ctx.deliveryTag(), multiple, channelName(ch));
            }
        } else {
            log.error("[ACK] Channel closed when ack tag={}, ack failed.", ctx.deliveryTag());
        }
    }

    @Override
    public void nack(QueueClient.AckContext ctx, boolean requeue) throws IOException {
        Channel ch = ctx.channel();
        if (ch != null && ch.isOpen()) {
            ch.basicNack(ctx.deliveryTag(), false, requeue);
            if (log.isDebugEnabled()) {
                log.debug("[NACK] deliveryTag={} requeue={} via {}",
                        ctx.deliveryTag(), requeue, channelName(ch));
            }
        } else {
            log.error("[NACK] Channel closed when nack tag={}, nack failed.", ctx.deliveryTag());
        }
    }

    private String channelName(Channel ch) {
        try {
            return ch.toString();
        } catch (Exception ignored) {
            return "(channel)";
        }
    }

    @Override
    public void close() throws Exception {
        for (Channel ch : channelsByQueue.values()) {
            try { if (ch != null && ch.isOpen()) ch.close(); } catch (Exception ignored) {}
        }
        channelsByQueue.clear();
        if (conn != null && conn.isOpen()) {
            try { conn.close(); } catch (Exception ignored) {}
        }
        log.info("[MQ] Closed all channels and connection");
    }
}