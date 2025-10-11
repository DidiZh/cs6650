package com.chatflow.client2.metrics;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Metrics {

    // Basic counters
    public final AtomicLong sent  = new AtomicLong();
    public final AtomicLong acks  = new AtomicLong();
    public final AtomicLong fail  = new AtomicLong();

    // Connection statistics
    public final AtomicLong connections = new AtomicLong();
    public final AtomicLong reconnects  = new AtomicLong();

    // RTT statistics (nanoseconds)
    private final List<Long> rttsNanos = Collections.synchronizedList(new ArrayList<>());
    private final LongAdder sumRtt = new LongAdder();
    private final AtomicLong minRtt = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxRtt = new AtomicLong(Long.MIN_VALUE);

    // Little's Law sampling: average inflight
    private final LongAdder inflightSamples = new LongAdder();
    private final LongAdder inflightSum     = new LongAdder();

    // 10-second bucket throughput (counted by ack)
    // key = bucketStartEpochSeconds (aligned to 10s), value = ack count in that bucket
    public final ConcurrentMap<Long, LongAdder> bucketAck10s = new ConcurrentHashMap<>();

    // Per-room / per-type counters (initialized after MainApp knows the number of rooms)
    public LongAdder[] perRoomAck = new LongAdder[0];
    // 0:TEXT 1:JOIN 2:LEAVE
    public final LongAdder[] perTypeAck = new LongAdder[]{ new LongAdder(), new LongAdder(), new LongAdder() };

    public void initPerRoom(int rooms) {
        perRoomAck = new LongAdder[rooms + 1]; // roomId starts at 1
        for (int i = 0; i <= rooms; i++) perRoomAck[i] = new LongAdder();
    }

    /** Record one RTT (nanoseconds) from an external caller. */
    public void recordRtt(long nanos) {
        rttsNanos.add(nanos);
        sumRtt.add(nanos);
        minRtt.getAndUpdate(v -> Math.min(v, nanos));
        maxRtt.getAndUpdate(v -> Math.max(v, nanos));
    }

    /** Sample the current inflight count once for computing avg_inflight. */
    public void sampleInflight(int inFlight) {
        inflightSamples.increment();
        inflightSum.add(inFlight);
    }

    /** Count one ack into the 10s bucket corresponding to the given epochMillis. */
    public void recordAckBucket(long epochMillis) {
        long bucket = (epochMillis / 1000L) / 10L * 10L; // 10s bucket
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

        // Percentiles
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

    // Utility: dump 10s buckets as CSV lines (ISO timestamp + count)
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
