package com.kafkafile.cli.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * HTTP client for the KafkaFile backend REST API.
 */
public class BackendClient {

    private final HttpClient http;
    private final String baseUrl;
    private final String bearerToken;
    private final ObjectMapper mapper = new ObjectMapper();

    public BackendClient(String baseUrl, String bearerToken) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.bearerToken = bearerToken;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public HttpClient getHttpClient() { return http; }

    /**
     * POST /api/v1/transfers — registers a send offer.
     * Returns JsonNode with transferId, window, maxChunkBytes.
     */
    public JsonNode registerOffer(String senderName, String recipientName,
                                   String fileName, long fileSize) throws Exception {
        String body = mapper.writeValueAsString(Map.of(
                "senderName", senderName,
                "recipientName", recipientName,
                "fileName", fileName,
                "fileSize", fileSize));

        HttpResponse<String> resp = http.send(
                buildJsonPost("/api/v1/transfers", body),
                HttpResponse.BodyHandlers.ofString());

        checkStatus(resp, 200);
        return mapper.readTree(resp.body());
    }

    /**
     * POST /api/v1/transfers/{id}/chunks — uploads a binary chunk envelope.
     */
    public void uploadChunk(String transferId, byte[] chunkBytes) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/transfers/" + transferId + "/chunks"))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(chunkBytes))
                .build();

        HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
        if (resp.statusCode() != 204 && resp.statusCode() != 200) {
            throw new RuntimeException("uploadChunk failed: HTTP " + resp.statusCode());
        }
    }

    /**
     * DELETE /api/v1/transfers/{id} — cancels a transfer.
     */
    public void cancel(String transferId) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/transfers/" + transferId))
                .header("Authorization", "Bearer " + bearerToken)
                .DELETE()
                .build();
        http.send(req, HttpResponse.BodyHandlers.discarding());
    }

    /**
     * POST /api/v1/transfers/{id}/accept
     */
    public void acceptTransfer(String transferId, String recipientName) throws Exception {
        String body = mapper.writeValueAsString(Map.of("recipientName", recipientName));
        HttpResponse<String> resp = http.send(
                buildJsonPost("/api/v1/transfers/" + transferId + "/accept", body),
                HttpResponse.BodyHandlers.ofString());
        checkStatus(resp, 200);
    }

    /**
     * GET /api/v1/transfers/{id}/chunks/{seq} — downloads a raw chunk envelope.
     */
    public byte[] downloadChunk(String transferId, int seq) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/transfers/" + transferId + "/chunks/" + seq))
                .header("Authorization", "Bearer " + bearerToken)
                .GET()
                .build();

        HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() != 200) {
            throw new RuntimeException("downloadChunk failed: HTTP " + resp.statusCode());
        }
        return resp.body();
    }

    /**
     * POST /api/v1/transfers/{id}/progress
     */
    public void reportProgress(String transferId, int seq, long lastFsyncedByteOffset) throws Exception {
        String body = mapper.writeValueAsString(Map.of(
                "seq", seq,
                "lastFsyncedByteOffset", lastFsyncedByteOffset));
        HttpResponse<String> resp = http.send(
                buildJsonPost("/api/v1/transfers/" + transferId + "/progress", body),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 204 && resp.statusCode() != 200) {
            throw new RuntimeException("reportProgress failed: HTTP " + resp.statusCode());
        }
    }

    /**
     * POST /api/v1/transfers/{id}/complete
     */
    public void reportComplete(String transferId, String sha256Hex) throws Exception {
        String body = mapper.writeValueAsString(Map.of("sha256", sha256Hex));
        HttpResponse<String> resp = http.send(
                buildJsonPost("/api/v1/transfers/" + transferId + "/complete", body),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 204 && resp.statusCode() != 200) {
            throw new RuntimeException("reportComplete failed: HTTP " + resp.statusCode());
        }
    }

    public String senderEventsUrl(String transferId) {
        return baseUrl + "/api/v1/transfers/" + transferId + "/events";
    }

    public String inboxEventsUrl(String name) {
        return baseUrl + "/api/v1/inbox/events?name=" + name;
    }

    private HttpRequest buildJsonPost(String path, String body) {
        return HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private void checkStatus(HttpResponse<String> resp, int expected) {
        if (resp.statusCode() != expected) {
            throw new RuntimeException("HTTP " + resp.statusCode() + ": " + resp.body());
        }
    }
}
