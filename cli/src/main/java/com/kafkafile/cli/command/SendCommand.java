package com.kafkafile.cli.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkafile.cli.client.BackendClient;
import com.kafkafile.cli.client.SseClient;
import com.kafkafile.cli.client.SseEvent;
import com.kafkafile.cli.config.CliConfig;
import com.kafkafile.cli.config.CliConfigLoader;
import com.kafkafile.cli.transfer.ChunkEnvelope;
import com.kafkafile.cli.transfer.ProgressSidecar;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Command(name = "send", description = "Send a file to a named recipient.")
public class SendCommand implements Runnable {

    @Parameters(index = "0", description = "File to send")
    private File file;

    @Option(names = "--to", required = true, paramLabel = "<name>",
            description = "Recipient's logical name")
    private String to;

    @Option(names = "--name", paramLabel = "<name>",
            description = "Your logical name on this machine (default: from config file)")
    private String nameOverride;

    @Option(names = "--backend-url", paramLabel = "<url>",
            description = "Backend URL (default: from config file, fallback: http://localhost:8080)")
    private String backendUrlOverride;

    @Option(names = "--token", paramLabel = "<token>",
            description = "Bearer token (default: from config file)")
    private String tokenOverride;

    @Option(names = "--chunk-size", paramLabel = "<bytes>",
            description = "Chunk size in bytes (default: from config file, fallback: 1048576)")
    private Integer chunkSizeOverride;

    @Option(names = "--window", paramLabel = "<n>",
            description = "Max in-flight chunks (default: from config file, fallback: 10)")
    private Integer windowOverride;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void run() {
        CliConfig cfg = CliConfigLoader.load();
        if (nameOverride != null) cfg.setName(nameOverride);
        if (backendUrlOverride != null) cfg.setBackendUrl(backendUrlOverride);
        if (tokenOverride != null) cfg.setBearerToken(tokenOverride);
        if (chunkSizeOverride != null) cfg.setChunkSize(chunkSizeOverride);
        if (windowOverride != null) cfg.setWindow(windowOverride);
        cfg.validate();

        if (!file.exists() || !file.isFile()) {
            System.err.println("File not found: " + file);
            System.exit(1);
        }

        BackendClient client = new BackendClient(cfg.getBackendUrl(), cfg.getBearerToken());

        try {
            System.out.printf("Registering offer: '%s' → '%s' (%s)%n",
                    cfg.getName(), to, humanSize(file.length()));

            JsonNode offer = client.registerOffer(cfg.getName(), to, file.getName(), file.length());
            String transferId = offer.get("transferId").asText();
            int serverWindow = offer.has("window") ? offer.get("window").asInt() : cfg.getWindow();
            int maxChunkBytes = offer.has("maxChunkBytes") ? offer.get("maxChunkBytes").asInt() : cfg.getChunkSize();
            int effectiveChunkSize = Math.min(cfg.getChunkSize(), maxChunkBytes);

            System.out.printf("Transfer ID: %s  window=%d  chunk=%s%n",
                    transferId, serverWindow, humanSize(effectiveChunkSize));
            System.out.println("Waiting for receiver to accept...");

            ProgressSidecar sidecar = new ProgressSidecar(file.toPath());

            // --- state shared between SSE thread and upload thread ---
            CountDownLatch accepted = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(1);
            Semaphore windowSemaphore = new Semaphore(serverWindow);
            AtomicInteger ackedCount = new AtomicInteger(0);
            AtomicReference<String> abortReason = new AtomicReference<>();
            boolean[] completed = {false};

            SseClient sse = new SseClient(client.getHttpClient(), cfg.getBearerToken());

            // SSE listener thread
            Thread sseThread = new Thread(() -> {
                try {
                    sse.connect(client.senderEventsUrl(transferId), event -> {
                        handleSseEvent(event, accepted, done, windowSemaphore,
                                ackedCount, abortReason, completed);
                    });
                } catch (Exception e) {
                    abortReason.set("SSE connection lost: " + e.getMessage());
                    accepted.countDown();
                    done.countDown();
                }
            }, "sse-sender");
            sseThread.setDaemon(true);
            sseThread.start();

            accepted.await();

            if (abortReason.get() != null) {
                System.err.println("Transfer aborted: " + abortReason.get());
                System.exit(1);
            }

            System.out.println("Accepted. Sending...");
            sendChunks(client, transferId, file, effectiveChunkSize,
                    serverWindow, windowSemaphore, ackedCount, sidecar);

            done.await();

            if (abortReason.get() != null) {
                System.err.println("Transfer failed: " + abortReason.get());
                sidecar.delete();
                System.exit(1);
            }

            System.out.println("Transfer complete.");
            sidecar.delete();
            sse.stop();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private void handleSseEvent(SseEvent event, CountDownLatch accepted, CountDownLatch done,
                                 Semaphore window, AtomicInteger ackedCount,
                                 AtomicReference<String> abortReason, boolean[] completed) {
        switch (event.type()) {
            case "transferAccepted" -> accepted.countDown();
            case "chunkAck" -> {
                try {
                    int seq = mapper.readTree(event.data()).get("seq").asInt();
                    ackedCount.incrementAndGet();
                    window.release();
                } catch (Exception ignored) {}
            }
            case "completed" -> {
                completed[0] = true;
                accepted.countDown();
                done.countDown();
            }
            case "aborted", "superseded" -> {
                String reason = event.type();
                try {
                    JsonNode node = mapper.readTree(event.data());
                    if (node.has("reason")) reason = node.get("reason").asText();
                } catch (Exception ignored) {}
                abortReason.set(reason);
                accepted.countDown();
                done.countDown();
            }
            case "heartbeat" -> {}
        }
    }

    private void sendChunks(BackendClient client, String transferId, File file,
                             int chunkSize, int window, Semaphore windowSemaphore,
                             AtomicInteger ackedCount, ProgressSidecar sidecar) throws Exception {
        long fileSize = file.length();
        int totalChunks = (int) Math.ceil((double) fileSize / chunkSize);
        UUID uuid = UUID.fromString(transferId);
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");

        long startTime = System.currentTimeMillis();
        long bytesSent = 0;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            for (int seq = 0; seq < totalChunks; seq++) {
                windowSemaphore.acquire();

                long byteOffset = (long) seq * chunkSize;
                int toRead = (int) Math.min(chunkSize, fileSize - byteOffset);
                byte[] payload = new byte[toRead];
                raf.seek(byteOffset);
                raf.readFully(payload);

                sha256.update(payload);
                boolean isEof = (seq == totalChunks - 1);
                byte[] digest = isEof ? sha256.digest() : null;

                byte[] envelope = ChunkEnvelope.encode(uuid, byteOffset, seq, payload, isEof, digest);
                client.uploadChunk(transferId, envelope);

                bytesSent += toRead;
                printProgress(bytesSent, fileSize, startTime);

                sidecar.save(transferId, seq, byteOffset);
            }
        }
        System.out.println();
    }

    private void printProgress(long sent, long total, long startMs) {
        double pct = total > 0 ? (sent * 100.0 / total) : 0;
        long elapsed = System.currentTimeMillis() - startMs;
        double mbps = elapsed > 0 ? (sent / 1_048_576.0) / (elapsed / 1000.0) : 0;
        System.out.printf("\r  %.1f%%  %s / %s  %.2f MB/s     ",
                pct, humanSize(sent), humanSize(total), mbps);
    }

    private static String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1_048_576) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1_073_741_824) return String.format("%.1f MB", bytes / 1_048_576.0);
        return String.format("%.2f GB", bytes / 1_073_741_824.0);
    }
}
