package com.chatflow.client2.config;

public record AppConfig(
        String baseWsUrl,   // ws://localhost:8080/chat
        int threads,        // number of concurrent threads
        int total,          // total number of messages
        int rooms,          // number of rooms (random/round-robin within 1..rooms)
        int maxRetries,     // maximum retry attempts on send failure
        long backoffStartMs // initial backoff in milliseconds
) {
    public static AppConfig fromArgs(String[] args) {
        String base = args.length > 0 ? args[0] : "ws://localhost:8080/chat";
        int th      = args.length > 1 ? Integer.parseInt(args[1]) : 64;
        int tot     = args.length > 2 ? Integer.parseInt(args[2]) : 500_000;
        int rms     = args.length > 3 ? Integer.parseInt(args[3]) : 20;
        int retries = args.length > 4 ? Integer.parseInt(args[4]) : 5;
        long backoff= args.length > 5 ? Long.parseLong(args[5]) : 100L;
        return new AppConfig(base, th, tot, rms, retries, backoff);
    }
}

