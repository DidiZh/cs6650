package com.chatflow.client;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

public class MessageFactory {
    static final String[] POOL = {
            "hello","how are you","nice to meet you","lorem ipsum","ping",
            "java","spring","websocket","throughput","latency",
            "p95","p99","warmup","main","metrics",
            "queue","retry","backoff","aws","ec2"
    };

    public static String randomJson() {
        var rnd = ThreadLocalRandom.current();
        int userId = 1 + rnd.nextInt(100000);
        String username = "user" + userId;
        String message = POOL[rnd.nextInt(POOL.length)];
        String ts = Instant.now().toString();
        String type = pickType(rnd.nextDouble());
        return "{\"userId\":"+userId+"," +
                "\"username\":\""+username+"\"," +
                "\"message\":\""+message+"\"," +
                "\"timestamp\":\""+ts+"\"," +
                "\"messageType\":\""+type+"\"}";
    }
    private static String pickType(double p) {
        if (p < 0.90) return "TEXT";
        if (p < 0.95) return "JOIN";
        return "LEAVE";
    }
}
