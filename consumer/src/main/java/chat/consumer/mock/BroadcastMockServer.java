package chat.consumer.mock;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

public class BroadcastMockServer {
    private static final String TOKEN = System.getenv().getOrDefault("INTERNAL_TOKEN", "secret");

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/health", ex -> {
            byte[] resp = "OK".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, resp.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(resp); }
        });

        server.createContext("/internal/broadcast", (HttpExchange ex) -> {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { ex.sendResponseHeaders(404, -1); ex.close(); return; }
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            if (!("Bearer " + TOKEN).equals(auth)) { ex.sendResponseHeaders(401, -1); ex.close(); return; }
            try (InputStream is = ex.getRequestBody()) { while (is.read() != -1) {} } // 丢弃请求体
            ex.sendResponseHeaders(204, -1); // 成功返回 204
            ex.close();
        });

        server.setExecutor(Executors.newCachedThreadPool());
        System.out.println("internal API on :" + port);
        server.start();
    }
}
