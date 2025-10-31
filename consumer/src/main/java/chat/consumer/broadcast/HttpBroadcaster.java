package chat.consumer.broadcast;

import chat.consumer.model.ChatMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP Broadcaster with round-robin, timeout and per-server retries.
 *
 * Contract (matches Broadcaster interface):
 * - Broadcast the message for the given roomId to ALL target servers.
 * - Return true only if ALL targets acknowledge successfully (2xx).
 * - If any server fails after retries, throw an Exception so the caller can NACK(requeue).
 *
 * Configuration (env vars take precedence over system properties):
 *   HTTP_TIMEOUT_MS (or -Dhttp.timeoutMs) : per-request timeout in milliseconds (default: 1200)
 *   RETRY_MAX      (or -Dretry.max)       : extra retry times per server (default: 2)
 */
public class HttpBroadcaster implements Broadcaster {

    private final List<String> servers;   // e.g. ["http://hostA:8080", "http://hostB:8080"]
    private final String path;            // e.g. "/internal/broadcast"
    private final String token;           // Bearer token for internal auth
    private final int timeoutMs;          // per-request timeout
    private final int retryMax;           // extra retry times per server (total attempts = retryMax + 1)

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    // round-robin index (used when we want a different starting server each call)
    private final AtomicInteger rr = new AtomicInteger(0);

    public HttpBroadcaster(List<String> servers, String path, String token) {
        this.servers = servers;
        this.path = path.startsWith("/") ? path : "/" + path;
        this.token = token;
        this.timeoutMs = readInt("HTTP_TIMEOUT_MS", "http.timeoutMs", 1200);
        this.retryMax  = readInt("RETRY_MAX", "retry.max", 2);
    }

    private int readInt(String envKey, String propKey, int defVal) {
        String env = System.getenv(envKey);
        if (env != null && !env.isBlank()) {
            try { return Integer.parseInt(env.trim()); } catch (Exception ignored) {}
        }
        String prop = System.getProperty(propKey);
        if (prop != null && !prop.isBlank()) {
            try { return Integer.parseInt(prop.trim()); } catch (Exception ignored) {}
        }
        return defVal;
    }

    @Override
    public boolean broadcast(String roomId, ChatMessage msg) throws Exception {
        // Serialize the message as JSON (include roomId if your ChatMessage has it).
        final String json = mapper.writeValueAsString(msg);

        // We must deliver to ALL targets successfully (2xx), otherwise throw.
        // To avoid always starting from servers.get(0), shift the start index by rr.
        final int n = servers.size();
        final int start = Math.floorMod(rr.getAndIncrement(), Math.max(n, 1));

        Exception last = null;

        for (int s = 0; s < n; s++) {
            final String base = servers.get((start + s) % n);

            // Per-server attempts: first try + retryMax extra tries.
            int attempts = retryMax + 1;
            boolean ok = false;

            for (int i = 0; i < attempts; i++) {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(base + path))
                        .timeout(Duration.ofMillis(timeoutMs))
                        .header("Authorization", "Bearer " + token)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();
                try {
                    HttpResponse<Void> res = client.send(req, HttpResponse.BodyHandlers.discarding());
                    int code = res.statusCode();
                    if (code >= 200 && code < 300) {
                        ok = true;
                        break; // success for this server
                    }
                    last = new RuntimeException("non-2xx: " + code + " @ " + base);
                } catch (Exception e) {
                    last = e; // timeout / connection error
                }
            }

            if (!ok) {
                // One server failed after all retries -> fail the whole broadcast
                throw last != null ? last : new RuntimeException("broadcast failed @ " + base);
            }
        }

        // All servers accepted (2xx). Caller may ignore the boolean but it's true by contract.
        return true;
    }
}
