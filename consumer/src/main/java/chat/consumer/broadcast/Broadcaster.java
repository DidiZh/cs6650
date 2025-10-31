package chat.consumer.broadcast;


import chat.consumer.model.ChatMessage;


public interface Broadcaster {
    /**
     * Broadcast a message for the given roomId to all target servers.
     * Return true if ALL targets acknowledged successfully.
     */
    boolean broadcast(String roomId, ChatMessage msg) throws Exception;
}