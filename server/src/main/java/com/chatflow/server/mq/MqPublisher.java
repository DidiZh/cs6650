package com.chatflow.server.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把收到的客户端消息发布到 RabbitMQ 的 chat.exchange。
 */
@Component
public class MqPublisher {
    private static final Logger log = LoggerFactory.getLogger(MqPublisher.class);

    @Value("${spring.rabbitmq.host}")
    private String host;

    @Value("${spring.rabbitmq.port}")
    private int port;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;

    @Value("${mq.exchange}")
    private String exchange;

    private Connection connection;
    private Channel channel;

    @PostConstruct
    public void init() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);

        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
        channel.exchangeDeclare(exchange, "topic", true);
        log.info("[BOOT] MQ Publisher connected to {}:{}", host, port);
    }

    @PreDestroy
    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection != null) connection.close();
    }

    public void publish(String roomId, String messageJson) {
        try {
            String routingKey = "room." + roomId;
            byte[] body = messageJson.getBytes();
            channel.basicPublish(exchange, routingKey, null, body);
            log.info("[PUBLISH] room={} bytes={}", roomId, body.length);
        } catch (Exception e) {
            log.error("[ERROR] publish failed", e);
        }
    }
}
