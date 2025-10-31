package chat.consumer.manager;

import chat.consumer.broadcast.Broadcaster;
import chat.consumer.model.ChatMessage;
import java.util.concurrent.atomic.AtomicLong;

public class RoomManager {
    private Broadcaster broadcaster;
    public final AtomicLong messagesProcessed = new AtomicLong();

    public void setBroadcaster(Broadcaster b){ this.broadcaster = b; }

    public void deliver(String roomId, ChatMessage msg) throws Exception {
        if (broadcaster == null) throw new IllegalStateException("Broadcaster not set");
        broadcaster.broadcast(roomId, msg);   // 通过 HTTP 通知各个 server 自己去广播
        messagesProcessed.incrementAndGet();
    }
}

