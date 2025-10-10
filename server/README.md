# ChatFlow WebSocket Server (Spring Boot)

## Overview
A simple Spring Boot WebSocket server for the assignment.  
Endpoints:
- WebSocket: `ws://localhost:8080/chat/{roomId}`
- Health check: `GET /health` → `{"status":"UP"}`

## Requirements
- JDK 17 (tested with Amazon Corretto 17)
- Maven 3.9+
- Default HTTP port: 8080

## Build
mvn -q -DskipTests clean package

## Run & Quick Verify
java -jar target/server-0.0.1-SNAPSHOT.jar
curl http://localhost:8080/health   # expected: {"status":"UP"}
