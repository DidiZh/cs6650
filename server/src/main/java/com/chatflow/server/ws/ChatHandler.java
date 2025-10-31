package com.chatflow.server.ws;

import com.chatflow.server.mq.MqPublisher;
import com.chatflow.server.model.ChatMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriTemplate;

import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;

/**
 * A2 版 ChatHandler：
 * - 连接建立：根据 /chat/{roomId} 解析 roomId 并登记到 RoomRegistry
 * - 收到客户端文本：解析为 ChatMessage，校验后发布到 MQ（chat.exchange，routingKey=room.{roomId}）
 * - 连接关闭：从 RoomRegistry 移除
 */
@Component
public class ChatHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(ChatHandler.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
    private final UriTemplate template = new UriTemplate("/chat/{roomId}");

    private final RoomRegistry roomRegistry;
    private final MqPublisher publisher;

    public ChatHandler(RoomRegistry roomRegistry, MqPublisher publisher) {
        this.roomRegistry = roomRegistry;
        this.publisher = publisher;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String roomId = extractRoomId(session.getUri());
        if (roomId == null) {
            try { session.close(CloseStatus.BAD_DATA); } catch (Exception ignored) {}
            return;
        }
        roomRegistry.add(roomId, session);
        log.info("[JOIN] room={} total={}", roomId, roomRegistry.get(roomId).size());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        // 1) 解析 JSON -> ChatMessage
        ChatMessage cm;
        try {
            cm = mapper.readValue(message.getPayload(), ChatMessage.class);
        } catch (Exception e) {
            log.warn("[WARN] invalid json: {}", e.getMessage());
            return;
        }

        // 2) Bean 校验
        Set<ConstraintViolation<ChatMessage>> violations = validator.validate(cm);
        if (!violations.isEmpty()) {
            log.warn("[WARN] validation failed: {}", violations);
            return;
        }

        // 3) 时间戳格式校验（ISO-8601）
        try { Instant.parse(cm.timestamp); }
        catch (DateTimeParseException e) {
            log.warn("[WARN] invalid timestamp {}", cm.timestamp);
            return;
        }

        // 4) 解析 roomId 并发布到 MQ
        String roomId = extractRoomId(session.getUri());
        if (roomId == null) return;

        String payloadJson = mapper.writeValueAsString(cm);
        publisher.publish(roomId, payloadJson);
        log.info("[PUBLISH->MQ] room={} userId={} username={}", roomId, cm.userId, cm.username);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String roomId = extractRoomId(session.getUri());
        if (roomId != null) {
            roomRegistry.remove(roomId, session);
            log.info("[LEAVE] room={} remaining={}", roomId, roomRegistry.get(roomId).size());
        }
    }

    private String extractRoomId(URI uri) {
        if (uri == null) return null;
        Map<String, String> vars = template.match(uri.getPath());
        return vars.get("roomId");
    }
}

