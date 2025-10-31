package chat.consumer.config;


import java.io.InputStream; import java.util.*; import java.util.stream.*;


public class ConsumerConfig {
    public final String host; public final int port; public final String username; public final String password; public final String vhost;
    public final String exchange; public final String queuePrefix; public final List<String> roomIds;
    public final int consumerThreads; public final int prefetch; public final boolean autoScale; public final int maxThreads; public final int minThreads;
    // New for HttpBroadcaster
    public final List<String> servers; public final String internalToken; public final String broadcastPath;


    private ConsumerConfig(Properties p){
        this.host = p.getProperty("rabbitmq.host","localhost");
        this.port = Integer.parseInt(p.getProperty("rabbitmq.port","5672"));
        this.username = p.getProperty("rabbitmq.username","guest");
        this.password = p.getProperty("rabbitmq.password","guest");
        this.vhost = p.getProperty("rabbitmq.virtualHost","/");
        this.exchange = p.getProperty("rabbitmq.exchange","chat.exchange");
        this.queuePrefix = p.getProperty("rabbitmq.queuePrefix","room.");
        this.roomIds = Arrays.stream(p.getProperty("rabbitmq.roomIds","1").split(","))
                .map(String::trim).filter(s->!s.isEmpty()).collect(Collectors.toList());
        this.consumerThreads = Integer.parseInt(p.getProperty("consumer.threads","16"));
        this.prefetch = Integer.parseInt(p.getProperty("consumer.prefetch","50"));
        this.autoScale = Boolean.parseBoolean(p.getProperty("consumer.autoScale","true"));
        this.maxThreads = Integer.parseInt(p.getProperty("consumer.maxThreads","64"));
        this.minThreads = Integer.parseInt(p.getProperty("consumer.minThreads","4"));


        this.servers = Arrays.stream(p.getProperty("servers","http://localhost:8080").split(","))
                .map(String::trim).filter(s->!s.isEmpty()).collect(Collectors.toList());
        this.internalToken = p.getProperty("internal.token","CHANGE_ME");
        this.broadcastPath = p.getProperty("internal.broadcastPath","/internal/broadcast");
    }
    public static ConsumerConfig load(){
        try(InputStream in = ConsumerConfig.class.getClassLoader().getResourceAsStream("application.properties")){
            Properties p = new Properties(); p.load(in); return new ConsumerConfig(p);
        } catch(Exception e){ throw new RuntimeException("Failed to load config", e);} }
}


