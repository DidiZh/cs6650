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

        // 结果目录（client-part2 的上一级 ../results）
        Path resultsDir = Paths.get("../results");
        Files.createDirectories(resultsDir);

        CsvWriter summaryCsv = new CsvWriter(resultsDir.resolve("main_summary.csv"));
        Path detailPath  = resultsDir.resolve("detail.csv");
        Path bucketPath  = resultsDir.resolve("throughput_10s.csv");

        // Detail 文件表头（若不存在）
        if (!Files.exists(detailPath)) {
            Files.writeString(detailPath, "epoch_ms,messageType,latency_ms,status,roomId\n",
                    StandardOpenOption.CREATE);
        }

        // 消息生产
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

        // 统计与同步
        Metrics metrics = new Metrics();
        metrics.initPerRoom(rooms);

        CountDownLatch allDone  = new CountDownLatch(threads);
        CountDownLatch echoAll  = new CountDownLatch(total);
        ConcurrentHashMap<String, Long> inflight = new ConcurrentHashMap<>();

        // 逐条明细写入线程（从 JsonUtil 的 detailQueue 消费）
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

        // 监控线程：每秒打印进度 + 采样 inflight（用于 Little's Law）
        ScheduledExecutorService mon = Executors.newSingleThreadScheduledExecutor();
        mon.scheduleAtFixedRate(() -> {
            int infl = inflight.size();
            metrics.sampleInflight(infl); // <<< 改为通过公开方法采样

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

        allDone.await();                        // 全部线程发完
        echoAll.await(10, TimeUnit.SECONDS);    // 等回显收尾
        producerThread.join();
        pool.shutdown();
        mon.shutdownNow();
        detailWriter.interrupt();

        long t1 = System.nanoTime();

        // 写 10s 桶 CSV
        List<String> bucketLines = metrics.dumpBucketsCsvLines();
        if (bucketLines.size() > 1) {
            Files.write(bucketPath, (String.join("\n", bucketLines) + "\n").getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        // 汇总
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
                summary.connections(), summary.reconnects(), summary.avgInFlight() // <<< 名字修正
        );
    }
}
