package com.chatflow.client2.util;

import java.util.concurrent.*;

public class JsonUtil {

    // —— Simple JSON value extraction (string field) ——
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
        // Read until the first non-digit / non-minus / non-space / non-comma / non-bracket character
        int k = j;
        while (k < json.length()) {
            char c = json.charAt(k);
            if ((c >= '0' && c <= '9') || c == '-') {
                k++;
                continue;
            }
            break;
        }
        try {
            return Integer.parseInt(json.substring(j, k));
        } catch (Exception e) {
            return null;
        }
    }

    public static Long extractEpochMillis(String json, String field) {
        // The serverTimestamp field returned by the server is an ISO string;
        // here we only return the current time if the field is not found
        return System.currentTimeMillis();
    }

    // —— Extract token from message: e.g., "...|abcdef12" ——
    public static String extractTokenFromMessage(String json) {
        String m = extractField(json, "message");
        if (m == null) return null;
        int k = m.lastIndexOf('|');
        return k >= 0 && k + 1 < m.length() ? m.substring(k + 1) : null;
    }

    // —— Detailed message queue: thread-safe queue (flushed to disk by the writer thread in MainApp) ——
    private static final BlockingQueue<String> DETAIL = new LinkedBlockingQueue<>();

    public static void offerDetail(String line) {
        if (line != null) DETAIL.offer(line);
    }

    public static String pollDetail(long timeout, TimeUnit unit) throws InterruptedException {
        return DETAIL.poll(timeout, unit);
    }
}
