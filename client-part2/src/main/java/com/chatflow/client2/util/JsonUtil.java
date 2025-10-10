package com.chatflow.client2.util;

import java.util.concurrent.*;
public class JsonUtil {

    // —— 简单 JSON 取值（字符串字段）——
    public static String extractField(String json, String field) {
        String prefix = "\"" + field + "\":\"";
        int i = json.indexOf(prefix);
        if (i < 0) return null;
        int j = json.indexOf("\"", i + prefix.length());
        if (j < 0) return null;
        return json.substring(i + prefix.length(), j);
    }

    public static Integer extractInt(String json, String field) {
        String prefix = "\"" + field + "\":";
        int i = json.indexOf(prefix);
        if (i < 0) return null;
        int j = i + prefix.length();
        // 读到第一个非数字/非负号/非空格/非逗号/非右括号为止
        int k = j;
        while (k < json.length()) {
            char c = json.charAt(k);
            if ((c >= '0' && c <= '9') || c=='-' ) { k++; continue; }
            break;
        }
        try { return Integer.parseInt(json.substring(j, k)); } catch (Exception e) { return null; }
    }

    public static Long extractEpochMillis(String json, String field) {
        // 服务端回的 serverTimestamp 是 ISO 字符串；我们这里只在找不到时用当前时间
        return System.currentTimeMillis();
    }

    // —— 从 message 提取 token：形如 "...|abcdef12" ——
    public static String extractTokenFromMessage(String json) {
        String m = extractField(json, "message");
        if (m == null) return null;
        int k = m.lastIndexOf('|');
        return k >= 0 && k + 1 < m.length() ? m.substring(k + 1) : null;
    }

    // —— 逐条明细：线程安全队列（由 MainApp 的 writer 线程落盘） ——
    private static final BlockingQueue<String> DETAIL = new LinkedBlockingQueue<>();
    public static void offerDetail(String line) { if (line != null) DETAIL.offer(line); }
    public static String pollDetail(long timeout, TimeUnit unit) throws InterruptedException {
        return DETAIL.poll(timeout, unit);
    }
}
