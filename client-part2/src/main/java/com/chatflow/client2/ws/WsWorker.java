package com.chatflow.client2.ws;

import com.chatflow.client2.metrics.Metrics;
import com.chatflow.client2.util.JsonUtil;
import okhttp3.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

public class WsWorker implements Runnable {
    private final String url;
    private final MessageProducer producer;
    private final Metrics metrics;
    private final int maxRetries;
    private final long backoffStartMs;
    private final CountDownLatch doneSignal;
    private final CountDownLatch echoSignal; // 收到回显就 countDown
    private final ConcurrentHashMap<String, Long> inflight;

    public WsWorker(String url,
                    MessageProducer producer,
                    Metrics metrics,
                    int maxRetries,
                    long backoffStartMs,
                    CountDownLatch doneSignal,
                    CountDownLatch echoSignal,
                    ConcurrentHashMap<String, Long> inflight) {
        this.url = url;
        this.producer = producer;
        this.metrics = metrics;
        this.maxRetries = maxRetries;
        this.backoffStartMs = backoffStartMs;
        this.doneSignal = doneSignal;
        this.echoSignal = echoSignal;
        this.inflight = inflight;
    }

    @Override public void run() {
        OkHttpClient client = new OkHttpClient();

        CountDownLatch opened = new CountDownLatch(1);
        final WebSocket[] holder = new WebSocket[1];

        Request req = new Request.Builder().url(url).build();
        WebSocketListener listener = new WebSocketListener() {
            @Override public void onOpen(WebSocket ws, Response resp) {
                holder[0]=ws;
                metrics.connections.incrementAndGet();
                opened.countDown();
            }

            @Override public void onMessage(WebSocket ws, String text) {
                metrics.acks.incrementAndGet();

                // 关联合并：优先用 message 里的 token；没有再用 id（兜底）
                String key = JsonUtil.extractTokenFromMessage(text);
                if (key == null) key = JsonUtil.extractField(text, "id");

                Long start = (key == null) ? null : inflight.remove(key);
                if (start != null) {
                    metrics.recordRtt(System.nanoTime() - start);
                }

                // 统计：按房间、按类型、10s 桶
                Integer room = JsonUtil.extractInt(text, "roomId");
                if (room != null && metrics.perRoomAck.length > room) metrics.perRoomAck[room].increment();
                String type = JsonUtil.extractField(text, "messageType");
                if (type != null) {
                    int idx = switch (type) { case "TEXT" -> 0; case "JOIN" -> 1; case "LEAVE" -> 2; default -> 0; };
                    metrics.perTypeAck[idx].increment();
                }
                Long ts = JsonUtil.extractEpochMillis(text, "serverTimestamp");
                metrics.recordAckBucket(ts != null ? ts : System.currentTimeMillis());

                // 逐条明细（在 MainApp 的 writer 线程里落盘）
                if (start != null) {
                    long latMs = (System.nanoTime() - start) / 1_000_000L;
                    String line = (System.currentTimeMillis()) + "," + (type == null ? "" : type) + "," +
                            latMs + ",OK," + (room == null ? "" : room);
                    JsonUtil.offerDetail(line);
                }

                echoSignal.countDown();
            }

            @Override public void onFailure(WebSocket ws, Throwable t, Response r) {
                // 可选：做一次轻量重连
                try {
                    metrics.reconnects.incrementAndGet();
                    client.newWebSocket(new Request.Builder().url(url).build(), this);
                } catch (Exception ignored) {}
            }
        };

        client.newWebSocket(req, listener);

        try {
            opened.await(5, TimeUnit.SECONDS);
            WebSocket ws = holder[0];
            if (ws == null) return;

            while (true) {
                String msg = producer.nextOrNull();
                if (msg == null) break; // 生产结束

                // 发送前登记 inflight：用 token（或 id 兜底）
                String key = JsonUtil.extractTokenFromMessage(msg);
                if (key == null) key = JsonUtil.extractField(msg, "id");
                if (key != null) inflight.put(key, System.nanoTime());

                boolean ok = false;
                int attempts = 0;
                long backoff = backoffStartMs;
                while (!ok && attempts < maxRetries) {
                    attempts++;
                    if (ws.send(msg)) {
                        metrics.sent.incrementAndGet();
                        ok = true;
                    } else {
                        Thread.sleep(backoff);
                        backoff = Math.min(backoff * 2, 2000);
                    }
                }
                if (!ok) {
                    metrics.fail.incrementAndGet();
                    if (key != null) inflight.remove(key);
                }
            }
            ws.close(1000, "done");
        } catch (Exception ignored) {
            // 失败在发送环节已计入
        } finally {
            client.dispatcher().executorService().shutdown();
            doneSignal.countDown();
        }
    }
}
