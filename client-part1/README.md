# Client Part 1 — Minimal Client

## Overview
A minimal Java client that opens one WebSocket connection, sends a single message, and prints the server echo.

## Requirements
- JDK 17
- Maven 3.9+

## Build
mvn -q -DskipTests clean package

## Run & Quick Verify
# against local server
java -cp target/client-part1-0.0.1-SNAPSHOT.jar \
com.chatflow.client.SingleClient ws://localhost:8080/chat/1
# expected: a successful echo printed to the console
