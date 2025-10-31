package com.chatflow.server.http;

import com.chatflow.server.ws.RoomRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.Set;

/**
 * 给 Consumer 调用的内部广播接口。
 */
@RestController
@RequestMapping("/internal")
public class InternalBroadcastController {
    private static final Logger log = LoggerFactory.getLogger(InternalBroadcastController.class);

    private final RoomRegistry roomRegistry;

    public InternalBroadcastController(RoomRegistry roomRegistry) {
        this.roomRegistry = roomRegistry;
    }

    @Value("${internal.token}")
    private String token;

    @PostMapping("/broadcast")
    public ResponseEntity<Void> broadcast(
            @RequestHeader(value = "Authorization", required = false) String auth,
            @RequestBody BroadcastRequest req) {

        if (auth == null || !auth.equals("Bearer " + token)) {
            return ResponseEntity.status(401).build();
        }

        Set<WebSocketSession> sessions = roomRegistry.get(String.valueOf(req.getRoomId()));
        int ok = 0;
        for (WebSocketSession ws : sessions) {
            try {
                ws.sendMessage(new TextMessage(req.getMessage().toString()));
                ok++;
            } catch (Exception e) {
                log.warn("[WARN] send fail room={} {}", req.getRoomId(), e.getMessage());
            }
        }
        log.info("[BROADCAST] room={} delivered={}", req.getRoomId(), ok);
        return ResponseEntity.noContent().build(); // 204
    }

    public static class BroadcastRequest {
        private int roomId;
        private Object message;
        public int getRoomId() { return roomId; }
        public void setRoomId(int roomId) { this.roomId = roomId; }
        public Object getMessage() { return message; }
        public void setMessage(Object message) { this.message = message; }
    }
}
