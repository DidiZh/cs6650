package com.chatflow.server.ws;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 管理房间与 WebSocketSession 的映射关系。
 */
@Component
public class RoomRegistry {

    private final Map<String, Set<WebSocketSession>> rooms = new ConcurrentHashMap<>();

    public void add(String roomId, WebSocketSession session) {
        rooms.computeIfAbsent(roomId, k -> new CopyOnWriteArraySet<>()).add(session);
    }

    public void remove(String roomId, WebSocketSession session) {
        Set<WebSocketSession> set = rooms.get(roomId);
        if (set != null) {
            set.remove(session);
            if (set.isEmpty()) rooms.remove(roomId);
        }
    }

    public Set<WebSocketSession> get(String roomId) {
        return rooms.getOrDefault(roomId, Set.of());
    }
}

