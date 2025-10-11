package com.chatflow.client2.util;

import com.chatflow.client2.metrics.Metrics.Summary;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;

public class CsvWriter {
    private final Path file;

    public CsvWriter(Path file) { this.file = file; }

    public void append(String baseUrl, int threads, int total, Summary s) throws IOException {
        boolean needHeader = !Files.exists(file);
        Files.createDirectories(file.getParent());

        try (BufferedWriter w = Files.newBufferedWriter(
                file, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {

            // Unified header: include mean_ms and avg_inflight (and p999_ms for optional later analysis)
            if (needHeader) {
                w.write(String.join(",",
                        "ts",
                        "base_url",
                        "threads",
                        "total_attempt",
                        "total_sent",
                        "total_acked",
                        "total_failed",
                        "duration_s",
                        "send_tps",
                        "ack_tps",
                        "p50_ms",
                        "p95_ms",
                        "p99_ms",
                        "p999_ms",
                        "mean_ms",
                        "avg_inflight"
                ));
                w.newLine();
            }

            // Write data row
            w.write(String.join(",",
                    Instant.now().toString(),
                    baseUrl,
                    String.valueOf(threads),
                    String.valueOf(total),
                    String.valueOf(s.sent()),
                    String.valueOf(s.acks()),
                    String.valueOf(s.fail()),
                    String.format("%.2f", s.seconds()),
                    String.format("%.1f", s.sendTps()),
                    String.format("%.1f", s.ackTps()),
                    String.format("%.2f", s.p50ms()),
                    String.format("%.2f", s.p95ms()),
                    String.format("%.2f", s.p99ms()),
                    String.format("%.2f", s.p999ms()),
                    String.format("%.2f", s.meanMs()),
                    String.format("%.2f", s.avgInFlight())
            ));
            w.newLine();
        }
    }
}
