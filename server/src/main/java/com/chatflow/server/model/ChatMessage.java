package com.chatflow.server.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties; // ✅ 新增
import jakarta.validation.constraints.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChatMessage {
    @Min(1) @Max(100000)
    public Integer userId;

    @NotNull @Size(min = 3, max = 20)
    @Pattern(regexp = "^[A-Za-z0-9]+$")
    public String username;

    @NotNull @Size(min = 1, max = 500)
    public String message;

    @NotNull
    public String timestamp;

    @NotNull
    public MessageType messageType;
}
