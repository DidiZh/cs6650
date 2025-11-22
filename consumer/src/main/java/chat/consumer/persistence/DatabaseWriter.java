package chat.consumer.persistence;

import chat.consumer.dao.MessageDao;
import chat.consumer.model.ChatMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据库写入服务 - Write-Behind模式
 * 负责批量写入消息到数据库，优化写入性能
 */
public class DatabaseWriter implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DatabaseWriter.class);

    private final MessageDao messageDao;
    private final BlockingQueue<ChatMessage> writeBuffer;
    private final ExecutorService writerPool;
    private final ScheduledExecutorService scheduler;

    // 配置参数
    private final int batchSize;
    private final long flushIntervalMs;
    private final int writerThreads;

    // 统计指标
    private final AtomicLong totalWritten = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong failedWrites = new AtomicLong(0);

    private volatile boolean running = false;

    /**
     * 构造函数
     * @param messageDao 数据访问对象
     * @param batchSize 批量大小（建议：500-1000）
     * @param flushIntervalMs flush间隔（建议：500-1000ms）
     * @param writerThreads 写入线程数（建议：2-4）
     * @param bufferCapacity 缓冲区容量（建议：10000）
     */
    public DatabaseWriter(MessageDao messageDao,
                          int batchSize,
                          long flushIntervalMs,
                          int writerThreads,
                          int bufferCapacity) {
        this.messageDao = messageDao;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.writerThreads = writerThreads;

        // 使用有界队列防止内存溢出
        this.writeBuffer = new LinkedBlockingQueue<>(bufferCapacity);

        // 写入线程池
        this.writerPool = Executors.newFixedThreadPool(writerThreads, r -> {
            Thread t = new Thread(r, "db-writer");
            t.setDaemon(true);
            return t;
        });

        // 定时flush线程
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "db-flusher");
            t.setDaemon(true);
            return t;
        });

        log.info("DatabaseWriter initialized: batchSize={}, flushInterval={}ms, writerThreads={}, bufferCapacity={}",
                batchSize, flushIntervalMs, writerThreads, bufferCapacity);
    }

    /**
     * 启动写入服务
     */
    public void start() {
        if (running) {
            log.warn("DatabaseWriter already running");
            return;
        }

        running = true;

        // 启动写入线程
        for (int i = 0; i < writerThreads; i++) {
            writerPool.submit(this::writerLoop);
        }

        // 启动定时flush
        scheduler.scheduleAtFixedRate(this::periodicFlush,
                flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        log.info("DatabaseWriter started with {} writer threads", writerThreads);
    }

    /**
     * 添加消息到写入缓冲区
     * @param message 消息
     * @return true=成功, false=缓冲区满
     */
    public boolean addMessage(ChatMessage message) {
        if (!running) {
            log.warn("DatabaseWriter not running, message dropped");
            return false;
        }

        boolean added = writeBuffer.offer(message);
        if (!added) {
            log.warn("Write buffer full, message dropped. Current size: {}", writeBuffer.size());
        }
        return added;
    }

    /**
     * 批量添加消息
     */
    public int addMessages(List<ChatMessage> messages) {
        int added = 0;
        for (ChatMessage msg : messages) {
            if (addMessage(msg)) {
                added++;
            }
        }
        return added;
    }

    /**
     * 写入线程主循环
     */
    private void writerLoop() {
        List<ChatMessage> batch = new ArrayList<>(batchSize);

        while (running || !writeBuffer.isEmpty()) {
            try {
                // 阻塞等待第一条消息
                ChatMessage first = writeBuffer.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }

                batch.clear();
                batch.add(first);

                // 批量收集消息（不超过batchSize）
                writeBuffer.drainTo(batch, batchSize - 1);

                // 写入数据库
                if (!batch.isEmpty()) {
                    writeBatch(batch);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Writer thread interrupted");
                break;
            } catch (Exception e) {
                log.error("Error in writer loop", e);
            }
        }

        log.info("Writer thread exiting. Remaining messages: {}", writeBuffer.size());
    }

    /**
     * 定时flush（处理小批量消息）
     */
    private void periodicFlush() {
        if (writeBuffer.isEmpty()) {
            return;
        }

        List<ChatMessage> batch = new ArrayList<>();
        int drained = writeBuffer.drainTo(batch, batchSize);

        if (drained > 0) {
            log.debug("Periodic flush: {} messages", drained);
            writeBatch(batch);
        }
    }

    /**
     * 写入一个批次
     */
    private void writeBatch(List<ChatMessage> batch) {
        if (batch.isEmpty()) {
            return;
        }

        try {
            int written = messageDao.batchInsertMessages(batch);
            if (written > 0) {
                totalWritten.addAndGet(written);
                totalBatches.incrementAndGet();
            } else {
                failedWrites.addAndGet(batch.size());
                log.error("Failed to write batch of {} messages", batch.size());
            }
        } catch (Exception e) {
            failedWrites.addAndGet(batch.size());
            log.error("Exception writing batch of {} messages", batch.size(), e);
        }
    }

    /**
     * 强制flush所有待写入的消息
     */
    public void flush() {
        log.info("Flushing write buffer. Pending messages: {}", writeBuffer.size());

        List<ChatMessage> remaining = new ArrayList<>();
        writeBuffer.drainTo(remaining);

        if (!remaining.isEmpty()) {
            // 分批写入
            for (int i = 0; i < remaining.size(); i += batchSize) {
                int end = Math.min(i + batchSize, remaining.size());
                List<ChatMessage> batch = remaining.subList(i, end);
                writeBatch(batch);
            }
        }

        log.info("Flush complete. Total written: {}", totalWritten.get());
    }

    /**
     * 获取统计信息
     */
    public WriterStats getStats() {
        return new WriterStats(
                totalWritten.get(),
                totalBatches.get(),
                failedWrites.get(),
                writeBuffer.size()
        );
    }

    /**
     * 关闭写入服务
     */
    @Override
    public void close() {
        log.info("Shutting down DatabaseWriter...");
        running = false;

        // 停止定时任务
        scheduler.shutdown();

        // flush所有待写入消息
        flush();

        // 关闭写入线程池
        writerPool.shutdown();
        try {
            if (!writerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                writerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            writerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        WriterStats stats = getStats();
        log.info("DatabaseWriter shutdown complete. Final stats: written={}, batches={}, failed={}",
                stats.totalWritten, stats.totalBatches, stats.failedWrites);
    }

    /**
     * 统计信息类
     */
    public static class WriterStats {
        public final long totalWritten;
        public final long totalBatches;
        public final long failedWrites;
        public final int bufferSize;

        public WriterStats(long totalWritten, long totalBatches, long failedWrites, int bufferSize) {
            this.totalWritten = totalWritten;
            this.totalBatches = totalBatches;
            this.failedWrites = failedWrites;
            this.bufferSize = bufferSize;
        }

        @Override
        public String toString() {
            return String.format("WriterStats{written=%d, batches=%d, failed=%d, buffer=%d}",
                    totalWritten, totalBatches, failedWrites, bufferSize);
        }
    }
}