package com.chatflow.client;

import okhttp3.*;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SingleClient {
    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "ws://localhost:8080/chat/1";
        OkHttpClient client = new OkHttpClient.Builder().build();
        Request req = new Request.Builder().url(url).build();
        CountDownLatch done = new CountDownLatch(1);

        client.newWebSocket(req, new WebSocketListener() {
            @Override public void onOpen(WebSocket ws, Response resp) {
                String msg = """
          {"userId":123,"username":"user123","message":"hello",
           "timestamp":"%s","messageType":"TEXT"}""".formatted(Instant.now().toString());
                ws.send(msg);
            }
            @Override public void onMessage(WebSocket ws, String text) {
                System.out.println("ACK: " + text);
                done.countDown();
            }
            @Override public void onFailure(WebSocket ws, Throwable t, Response r) {
                t.printStackTrace();
                done.countDown();
            }
        });

        done.await(5, TimeUnit.SECONDS);
        client.dispatcher().executorService().shutdown();
    }
}
