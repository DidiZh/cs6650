package com.chatflow.server.ws;

import com.chatflow.server.model.ChatMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;  // ✅ 正确的父类包
import org.springframework.web.util.UriTemplate;

import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;

@Component
public class ChatHandler extends TextWebSocketHandler {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
    private final UriTemplate template = new UriTemplate("/chat/{roomId}");

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        ChatMessage cm;
        try {
            cm = mapper.readValue(message.getPayload(), ChatMessage.class);
        } catch (Exception e) {
            sendError(session, "INVALID_JSON");
            return;
        }

        Set<ConstraintViolation<ChatMessage>> violations = validator.validate(cm);
        if (!violations.isEmpty()) { sendError(session, "VALIDATION_FAILED"); return; }

        try { Instant.parse(cm.timestamp); }
        catch (DateTimeParseException e) { sendError(session, "INVALID_TIMESTAMP"); return; }

        String roomId = extractRoomId(session.getUri());
        if (roomId == null) { sendError(session, "MISSING_ROOM_ID"); return; }

        ObjectNode node = mapper.valueToTree(cm);
        node.put("roomId", roomId);
        node.put("serverTimestamp", Instant.now().toString());
        node.put("status", "OK");
        session.sendMessage(new TextMessage(node.toString()));
    }

    private void sendError(WebSocketSession session, String reason) throws Exception {
        ObjectNode err = mapper.createObjectNode();
        err.put("status","ERROR");
        err.put("reason",reason);
        err.put("serverTimestamp", Instant.now().toString());
        session.sendMessage(new TextMessage(err.toString()));
    }

    private String extractRoomId(URI uri) {
        if (uri == null) return null;
        Map<String,String> vars = template.match(uri.getPath());
        return vars.get("roomId");
    }
}
