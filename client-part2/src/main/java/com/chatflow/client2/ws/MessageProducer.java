package com.chatflow.client2.ws;

public interface MessageProducer {
    String nextOrNull() throws InterruptedException;
}
