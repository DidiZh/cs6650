package com.chatflow.client2.app;

import com.chatflow.client2.config.AppConfig;
import com.chatflow.client2.metrics.Metrics;
import com.chatflow.client2.util.CsvWriter;
import com.chatflow.client2.model.MessageFactory;
import com.chatflow.client2.ws.MessageProducer;
import com.chatflow.client2.ws.WsWorker;

import java.nio.file.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MainApp {
    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.fromArgs(args);
        System.out.println("boot base=" + cfg.baseWsUrl() +
                " threads=" + cfg.threads() +
                " total=" + cfg.total() +
                " rooms=" + cfg.rooms());
        System.out.flush();

        // Results directory (parent of client-part2 -> ../results)
        Path resultsDir = Paths.get("../results");
        Files.createDirectories(resultsDir);

        CsvWriter summaryCsv = new CsvWriter(resultsDir.resolve("main_summary.csv"));
        Path detailPath  = resultsDir.resolve("detail.csv");
        Path bucketPath  = resultsDir.resolve("throughput_10s.csv");

        // Detail file header (if missing)
        if (!Files.exists(detailPath)) {
            Files.writeString(detailPath, "epoch_ms,messageType,latency_ms,status,roomId\n",
                    StandardOpenOption.CREATE);
        }

        // Message production
        MessageFactory factory = new MessageFactory();
        int total   = cfg.total();
        int threads = cfg.threads();
        int rooms   = cfg.rooms();

        ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10_000);
        Thread producerThread = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) queue.put(factory.nextJson());
            } catch (InterruptedException ignored) {}
        }, "producer");
        producerThread.start();

        MessageProducer producer = () -> queue.poll(2, TimeUnit.SECONDS);

        // Statistics and synchronization
        Metrics metrics = new Metrics();
        metrics.initPerRoom(rooms);

        CountDownLatch allDone  = new CountDownLatch(threads);
        CountDownLatch echoAll  = new CountDownLatch(total);
        ConcurrentHashMap<String, Long> inflight = new ConcurrentHashMap<>();

        // Detail writer thread (consumes from JsonUtil's detailQueue)
        Thread detailWriter = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String line = com.chatflow.client2.util.JsonUtil.pollDetail(1, TimeUnit.SECONDS);
                    if (line != null) {
                        Files.writeString(detailPath, line + "\n",
                                StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                    }
                }
            } catch (Exception ignored) {}
        }, "detail-writer");
        detailWriter.start();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger roomPicker = new AtomicInteger(0);
        long t0 = System.nanoTime();

        // Monitor thread: print progress every second + sample inflight (for Little's Law)
        ScheduledExecutorService mon = Executors.newSingleThreadScheduledExecutor();
        mon.scheduleAtFixedRate(() -> {
            int infl = inflight.size();
            metrics.sampleInflight(infl); // <<< changed to sample via public method

            System.out.println("progress sent=" + metrics.sent.get()
                    + " acks=" + metrics.acks.get()
                    + " fail=" + metrics.fail.get()
                    + " q=" + queue.size()
                    + " inflight=" + infl);
            System.out.flush();
        }, 1, 1, TimeUnit.SECONDS);

        for (int i = 0; i < threads; i++) {
            int r = 1 + (roomPicker.getAndIncrement() % rooms);
            String url = cfg.baseWsUrl() + "/" + r;
            pool.submit(new WsWorker(
                    url, producer, metrics, cfg.maxRetries(), cfg.backoffStartMs(),
                    allDone, echoAll, inflight
            ));
        }

        allDone.await();                        // All worker threads have finished sending
        echoAll.await(10, TimeUnit.SECONDS);    // Wait for echoes to finish up
        producerThread.join();
        pool.shutdown();
        mon.shutdownNow();
        detailWriter.interrupt();

        long t1 = System.nanoTime();

        // Write 10s bucket CSV
        List<String> bucketLines = metrics.dumpBucketsCsvLines();
        if (bucketLines.size() > 1) {
            Files.write(bucketPath, (String.join("\n", bucketLines) + "\n").getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        // Summary
        var summary = metrics.summarize(t0, t1);
        summaryCsv.append(cfg.baseWsUrl(), threads, total, summary);

        System.out.printf(
                "MAIN sent=%d acks=%d fail=%d time=%.2fs send_tps=%.1f ack_tps=%.1f "
                        + "p50=%.2fms p95=%.2fms p99=%.2fms mean=%.2fms min=%.2fms max=%.2fms "
                        + "conn=%d reconn=%d L(avgInFlight)=%.1f%n",
                summary.sent(), summary.acks(), summary.fail(), summary.seconds(),
                summary.sendTps(), summary.ackTps(),
                summary.p50ms(), summary.p95ms(), summary.p99ms(),
                summary.meanMs(), summary.minMs(), summary.maxMs(),
                summary.connections(), summary.reconnects(), summary.avgInFlight() // <<< name correction
        );
    }
}
