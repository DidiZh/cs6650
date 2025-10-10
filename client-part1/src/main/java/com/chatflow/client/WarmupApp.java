package com.chatflow.client;

import okhttp3.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class WarmupApp {
    static final int THREADS = 32;
    static final int MSG_PER_THREAD = 1000;

    public static void main(String[] args) throws Exception {
        String base = args.length > 0 ? args[0] : "ws://localhost:8080/chat";
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        AtomicLong ok = new AtomicLong(), fail = new AtomicLong();

        int total = THREADS * MSG_PER_THREAD;
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) queue.put(MessageFactory.randomJson());
            } catch (InterruptedException ignored) {}
        });
        producer.start();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch done = new CountDownLatch(THREADS);
        long t0 = System.nanoTime();

        for (int i=0;i<THREADS;i++) {
            final String url = base + "/" + (1 + ThreadLocalRandom.current().nextInt(20));
            pool.submit(() -> {
                OkHttpClient client = new OkHttpClient();
                Request req = new Request.Builder().url(url).build();
                CountDownLatch opened = new CountDownLatch(1);
                final WebSocket[] holder = new WebSocket[1];

                client.newWebSocket(req, new WebSocketListener() {
                    @Override public void onOpen(WebSocket ws, Response resp) { holder[0]=ws; opened.countDown(); }
                    @Override public void onMessage(WebSocket ws, String text) { ok.incrementAndGet(); }
                });

                try {
                    opened.await(5, TimeUnit.SECONDS);
                    WebSocket ws = holder[0];
                    for (int k=0;k<MSG_PER_THREAD;k++) {
                        String msg = queue.take();
                        boolean sent=false; int attempts=0; long backoff=100;
                        while(!sent && attempts<5) {
                            attempts++;
                            if (ws.send(msg)) { sent=true; }
                            else { Thread.sleep(backoff); backoff*=2; }
                        }
                        if(!sent) fail.incrementAndGet();
                    }
                    ws.close(1000, "done");
                } catch (Exception e) {
                    fail.addAndGet(MSG_PER_THREAD);
                } finally {
                    client.dispatcher().executorService().shutdown();
                    done.countDown();
                }
            });
        }

        done.await();
        long t1 = System.nanoTime();
        pool.shutdown(); producer.join();
        double sec = (t1 - t0)/1e9;

        System.out.printf("WARMUP OK=%d FAIL=%d TIME=%.2fs TPS=%.1f%n",
                ok.get(), fail.get(), sec, ok.get()/sec);
    }
}
