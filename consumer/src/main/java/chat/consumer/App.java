package chat.consumer;

import chat.consumer.broadcast.Broadcaster;
import chat.consumer.broadcast.HttpBroadcaster;
import chat.consumer.config.ConsumerConfig;
import chat.consumer.manager.RoomManager;
import chat.consumer.queue.RabbitMqQueueClient;
import chat.consumer.supervisor.ConsumerSupervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        try {
            // 1) 加载配置
            ConsumerConfig cfg = ConsumerConfig.load();

            // 2) 构造并连接 MQ（不要用 try-with-resources，避免启动完立刻关闭）
            RabbitMqQueueClient mq = new RabbitMqQueueClient(cfg);
            mq.connect();

            // 3) 读取广播目标（ENV 优先，其次 system property，最后给默认）
            List<String> servers = parseList(getenvOrProp("SERVERS", "servers", "http://localhost:8080"));
            String token = getenvOrProp("INTERNAL_TOKEN", "internal.token", "secret");
            String path  = getenvOrProp("BROADCAST_PATH", "internal.broadcastPath", "/internal/broadcast");

            Broadcaster broadcaster = new HttpBroadcaster(servers, path, token);

            // 4) 业务管理器（显式注入 Broadcaster，防止 NPE）
            RoomManager roomManager = new RoomManager();
            roomManager.setBroadcaster(broadcaster);

            // 5) 启动消费者监督器
            ConsumerSupervisor supervisor = new ConsumerSupervisor(
                    mq,
                    roomManager,
                    broadcaster,
                    cfg.consumerThreads,
                    cfg.autoScale,
                    cfg.minThreads,
                    cfg.maxThreads,
                    cfg.prefetch
            );

            log.info("[CONF] mq={}://{}:{} vhost={} user={} exchange={} prefetch={} rooms={} servers={} token(***masked***) path={}",
                    "amqp", cfg.host, cfg.port, cfg.vhost, cfg.username, cfg.exchange, cfg.prefetch,
                    cfg.roomIds, servers, path);

            supervisor.start(cfg.roomIds);

            // 6) 优雅退出：Ctrl-C 时先停 supervisor 再关 MQ
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    log.info("Shutdown signal received. Stopping supervisor & closing MQ...");
                    supervisor.stop();
                } catch (Exception e) {
                    log.warn("Error while stopping supervisor", e);
                }
                try {
                    mq.close();
                } catch (Exception e) {
                    log.warn("Error while closing MQ", e);
                }
                log.info("Shutdown complete.");
            }));

            // 7) 挂住主线程，避免进程直接退出导致 “Closed all channels and connection”
            new CountDownLatch(1).await();

        } catch (Throwable t) {
            LoggerFactory.getLogger(App.class).error("Fatal error in App.main", t);
            System.exit(1);
        }
    }

    private static String getenvOrProp(String envKey, String propKey, String defVal) {
        String env = System.getenv(envKey);
        if (env != null && !env.isBlank()) return env;
        String prop = System.getProperty(propKey);
        if (prop != null && !prop.isBlank()) return prop;
        return defVal;
    }

    private static List<String> parseList(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }
}
