package com.kafkafile.cli.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Consumer;

/**
 * Blocking SSE client. Reads events from a server-sent events stream and delivers them
 * to the provided callback. Blocks the calling thread until the stream closes or an error
 * occurs.
 */
public class SseClient {

    private final HttpClient httpClient;
    private final String bearerToken;
    private volatile boolean stopped = false;

    public SseClient(HttpClient httpClient, String bearerToken) {
        this.httpClient = httpClient;
        this.bearerToken = bearerToken;
    }

    /**
     * Opens the SSE stream at {@code url} and calls {@code eventHandler} for every event.
     * This method blocks until the stream ends or {@link #stop()} is called.
     */
    public void connect(String url, Consumer<SseEvent> eventHandler) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Accept", "text/event-stream")
                .header("Cache-Control", "no-cache")
                .GET()
                .build();

        HttpResponse<java.io.InputStream> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            throw new RuntimeException("SSE connect failed: HTTP " + response.statusCode());
        }

        try (var reader = new BufferedReader(new InputStreamReader(response.body()))) {
            String eventType = "message";
            StringBuilder dataBuffer = new StringBuilder();
            String line;

            while (!stopped && (line = reader.readLine()) != null) {
                if (line.startsWith("event:")) {
                    eventType = line.substring(6).trim();
                } else if (line.startsWith("data:")) {
                    if (!dataBuffer.isEmpty()) dataBuffer.append('\n');
                    dataBuffer.append(line.substring(5).trim());
                } else if (line.isEmpty()) {
                    // dispatch event
                    if (!dataBuffer.isEmpty()) {
                        eventHandler.accept(new SseEvent(eventType, dataBuffer.toString()));
                    }
                    eventType = "message";
                    dataBuffer.setLength(0);
                }
            }
        }
    }

    public void stop() {
        stopped = true;
    }
}
