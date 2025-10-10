package com.chatflow.client2.model;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MessageFactory {
    // 50 条消息池
    private static final String[] POOL = {
            "m0","m1","m2","m3","m4","m5","m6","m7","m8","m9",
            "m10","m11","m12","m13","m14","m15","m16","m17","m18","m19",
            "m20","m21","m22","m23","m24","m25","m26","m27","m28","m29",
            "m30","m31","m32","m33","m34","m35","m36","m37","m38","m39",
            "m40","m41","m42","m43","m44","m45","m46","m47","m48","m49"
    };

    private static String nextType() {
        int r = ThreadLocalRandom.current().nextInt(100);
        if (r < 90) return "TEXT";
        if (r < 95) return "JOIN";
        return "LEAVE";
    }

    public String nextJson() {
        int userId = 1 + ThreadLocalRandom.current().nextInt(100_000);
        String username = "user" + userId;

        String token = UUID.randomUUID().toString().substring(0, 8); // RTT 关联用
        String base  = POOL[ThreadLocalRandom.current().nextInt(POOL.length)];
        String msg   = base + "|" + token;                            // 嵌入 token

        String type = nextType();
        String ts   = Instant.now().toString();
        String id   = UUID.randomUUID().toString();                   // 可有可无

        // 提醒：服务器不会回显 id/clientTimestamp，但我们使用 message 内 token 关联合并
        return "{\"id\":\""+id+"\",\"clientTimestamp\":\""+ts+"\"," +
                "\"userId\":"+userId+"," +
                "\"username\":\""+username+"\"," +
                "\"message\":\""+msg+"\"," +
                "\"timestamp\":\""+ts+"\"," +
                "\"messageType\":\""+type+"\"}";
    }
}
