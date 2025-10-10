package com.chatflow.client2.metrics;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Metrics {

    // 基础计数
    public final AtomicLong sent  = new AtomicLong();
    public final AtomicLong acks  = new AtomicLong();
    public final AtomicLong fail  = new AtomicLong();

    // 连接统计
    public final AtomicLong connections = new AtomicLong();
    public final AtomicLong reconnects  = new AtomicLong();

    // RTT 统计（纳秒）
    private final List<Long> rttsNanos = Collections.synchronizedList(new ArrayList<>());
    private final LongAdder sumRtt = new LongAdder();
    private final AtomicLong minRtt = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxRtt = new AtomicLong(Long.MIN_VALUE);

    // Little's Law 采样：inflight 平均
    private final LongAdder inflightSamples = new LongAdder();
    private final LongAdder inflightSum     = new LongAdder();

    // 10 秒桶吞吐（按 ack 计）
    // key = bucketStartEpochSeconds(10s 对齐), value = 该桶 ack 计数
    public final ConcurrentMap<Long, LongAdder> bucketAck10s = new ConcurrentHashMap<>();

    // 按房间/类型计数（在 MainApp 知道 rooms 数量后初始化）
    public LongAdder[] perRoomAck = new LongAdder[0];
    // 0:TEXT 1:JOIN 2:LEAVE
    public final LongAdder[] perTypeAck = new LongAdder[]{ new LongAdder(), new LongAdder(), new LongAdder() };

    public void initPerRoom(int rooms) {
        perRoomAck = new LongAdder[rooms + 1]; // roomId 从 1 开始
        for (int i = 0; i <= rooms; i++) perRoomAck[i] = new LongAdder();
    }

    /** 外部记录一次 RTT（纳秒） */
    public void recordRtt(long nanos) {
        rttsNanos.add(nanos);
        sumRtt.add(nanos);
        minRtt.getAndUpdate(v -> Math.min(v, nanos));
        maxRtt.getAndUpdate(v -> Math.max(v, nanos));
    }

    /** 外部采样一次当前在飞量，用于平均在飞量 avg_inflight 计算 */
    public void sampleInflight(int inFlight) {
        inflightSamples.increment();
        inflightSum.add(inFlight);
    }

    /** 在 10s 桶里按 ack 计数一次 */
    public void recordAckBucket(long epochMillis) {
        long bucket = (epochMillis / 1000L) / 10L * 10L; // 10s 桶
        bucketAck10s.computeIfAbsent(bucket, k -> new LongAdder()).increment();
    }

    public static record Summary(
            long sent, long acks, long fail,
            double seconds, double sendTps, double ackTps,
            double p50ms, double p95ms, double p99ms, double p999ms,
            double meanMs, double minMs, double maxMs,
            long connections, long reconnects,
            double avgInFlight
    ) {}

    public Summary summarize(long t0, long t1) {
        double seconds = (t1 - t0) / 1_000_000_000.0;
        double sendTps = seconds > 0 ? sent.get() / seconds : 0.0;
        double ackTps  = seconds > 0 ? acks.get() / seconds : 0.0;

        // percentiles
        double p50=0, p95=0, p99=0, p999=0, mean=0, min=0, max=0;
        List<Long> copy;
        synchronized (rttsNanos) { copy = new ArrayList<>(rttsNanos); }
        if (!copy.isEmpty()) {
            copy.sort(Long::compare);
            p50  = toMs(percentile(copy, 0.50));
            p95  = toMs(percentile(copy, 0.95));
            p99  = toMs(percentile(copy, 0.99));
            p999 = toMs(percentile(copy, 0.999));
            mean = toMs(sumRtt.sum() / (double)copy.size());
            min  = toMs(minRtt.get());
            max  = toMs(maxRtt.get());
        }

        double avgInflight = 0.0;
        long s = inflightSamples.sum();
        if (s > 0) avgInflight = inflightSum.sum() / (double) s;

        return new Summary(
                sent.get(), acks.get(), fail.get(),
                seconds, sendTps, ackTps,
                p50, p95, p99, p999,
                mean, min, max,
                connections.get(), reconnects.get(),
                avgInflight
        );
    }

    private static double toMs(double ns) { return ns / 1_000_000.0; }

    private static long percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0L;
        int idx = (int)Math.ceil(p * sorted.size()) - 1;
        idx = Math.max(0, Math.min(idx, sorted.size() - 1));
        return sorted.get(idx);
    }

    // 工具：将 10s 桶输出为 CSV 行（ISO 时间 + count）
    public List<String> dumpBucketsCsvLines() {
        DateTimeFormatter fmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneOffset.UTC);
        List<Long> keys = new ArrayList<>(bucketAck10s.keySet());
        Collections.sort(keys);
        List<String> lines = new ArrayList<>();
        lines.add("bucket_start_iso,count");
        for (Long k : keys) {
            String iso = fmt.format(Instant.ofEpochSecond(k));
            lines.add(iso + "," + bucketAck10s.get(k).sum());
        }
        return lines;
    }
}
