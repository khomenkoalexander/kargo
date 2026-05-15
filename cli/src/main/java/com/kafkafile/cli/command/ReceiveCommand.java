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

import java.io.*;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

@Command(name = "receive", description = "Wait for an incoming file offer and receive it.")
public class ReceiveCommand implements Runnable {

    @Option(names = "--out-dir", paramLabel = "<dir>",
            description = "Directory to save received files (default: from config file, fallback: current directory)")
    private File outDirOverride;

    @Option(names = "--auto",
            description = "Auto-accept all incoming offers without prompting (default: from config file, fallback: false)")
    private boolean autoFlag;

    @Option(names = "--name", paramLabel = "<name>",
            description = "Your logical name on this machine (default: from config file)")
    private String nameOverride;

    @Option(names = "--backend-url", paramLabel = "<url>",
            description = "Backend URL (default: from config file, fallback: http://localhost:8080)")
    private String backendUrlOverride;

    @Option(names = "--token", paramLabel = "<token>",
            description = "Bearer token (default: from config file)")
    private String tokenOverride;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void run() {
        CliConfig cfg = CliConfigLoader.load();
        if (nameOverride != null)    cfg.setName(nameOverride);
        if (backendUrlOverride != null) cfg.setBackendUrl(backendUrlOverride);
        if (tokenOverride != null)   cfg.setBearerToken(tokenOverride);
        if (outDirOverride != null)  cfg.setOutDir(outDirOverride.getPath());
        if (autoFlag)                cfg.setAuto(true);   // --auto on CLI always enables it
        cfg.validate();

        File outDir = new File(cfg.getOutDir());
        if (!outDir.exists() && !outDir.mkdirs()) {
            System.err.println("Cannot create output directory: " + outDir);
            System.exit(1);
        }

        BackendClient client = new BackendClient(cfg.getBackendUrl(), cfg.getBearerToken());
        SseClient sse = new SseClient(client.getHttpClient(), cfg.getBearerToken());

        System.out.printf("Listening for offers as '%s'...%n", cfg.getName());

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<TransferContext> activeTransfer = new AtomicReference<>();
        AtomicReference<String> failReason = new AtomicReference<>();

        Thread sseThread = new Thread(() -> {
            try {
                sse.connect(client.inboxEventsUrl(cfg.getName()), event ->
                        handleEvent(event, cfg, client, activeTransfer, failReason, done));
            } catch (Exception e) {
                failReason.set("Connection lost: " + e.getMessage());
                done.countDown();
            }
        }, "sse-receiver");
        sseThread.setDaemon(true);
        sseThread.start();

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (failReason.get() != null) {
            System.err.println("Transfer failed: " + failReason.get());
            System.exit(1);
        }
        System.out.println("Done.");
    }

    private void handleEvent(SseEvent event, CliConfig cfg, BackendClient client,
                              AtomicReference<TransferContext> activeTransfer,
                              AtomicReference<String> failReason, CountDownLatch done) {
        try {
            JsonNode data = mapper.readTree(event.data());

            switch (event.type()) {
                case "offerArrived" -> {
                    String transferId = data.get("transferId").asText();
                    String senderName = data.get("senderName").asText();
                    String fileName = data.get("fileName").asText();
                    long fileSize = data.get("fileSize").asLong();

                    System.out.printf("%nIncoming: '%s' from '%s' (%s)%n",
                            fileName, senderName, humanSize(fileSize));

                    boolean accept = cfg.isAuto() || promptYesNo("Accept? [y/N] ");
                    if (!accept) {
                        System.out.println("Declined.");
                        return;
                    }

                    client.acceptTransfer(transferId, cfg.getName());
                    Path outFile = Path.of(cfg.getOutDir()).resolve(sanitize(fileName));
                    activeTransfer.set(new TransferContext(transferId, outFile, fileSize));
                    System.out.printf("Receiving → %s%n", outFile);
                }

                case "chunkAvailable" -> {
                    TransferContext ctx = activeTransfer.get();
                    if (ctx == null) return;

                    int seq = data.get("seq").asInt();
                    receiveChunk(client, ctx, seq, failReason, done);
                }

                case "completed" -> {
                    System.out.println("\nTransfer completed successfully.");
                    done.countDown();
                }

                case "aborted" -> {
                    String reason = data.has("reason") ? data.get("reason").asText() : "unknown";
                    failReason.set(reason);
                    done.countDown();
                }

                case "heartbeat" -> {}
            }
        } catch (Exception e) {
            failReason.set("Event handling error: " + e.getMessage());
            done.countDown();
        }
    }

    private void receiveChunk(BackendClient client, TransferContext ctx, int seq,
                               AtomicReference<String> failReason, CountDownLatch done) throws Exception {
        byte[] raw = client.downloadChunk(ctx.transferId(), seq);
        ChunkEnvelope chunk = ChunkEnvelope.decode(raw);

        // Write chunk at correct byte offset
        try (RandomAccessFile raf = new RandomAccessFile(ctx.outFile().toFile(), "rw")) {
            raf.seek(chunk.getByteOffset());
            raf.write(chunk.getPayload());
            raf.getFD().sync();
        }

        ctx.updateProgress(seq, chunk.getByteOffset() + chunk.getPayload().length);

        printProgress(ctx.bytesReceived(), ctx.fileSize(), ctx.startMs());

        if (chunk.isEof()) {
            // verify SHA-256
            String expectedHex = ChunkEnvelope.bytesToHex(chunk.getSha256());
            String actualHex = computeSha256(ctx.outFile());

            boolean ok = expectedHex.equalsIgnoreCase(actualHex);
            client.reportComplete(ctx.transferId(), actualHex);

            if (!ok) {
                failReason.set("SHA-256 mismatch: expected=" + expectedHex + " actual=" + actualHex);
                ctx.outFile().toFile().delete();
                done.countDown();
            }
            // if ok, wait for "completed" event
        } else {
            client.reportProgress(ctx.transferId(), seq, ctx.bytesReceived());
        }
    }

    private String computeSha256(Path file) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (var is = new BufferedInputStream(new FileInputStream(file.toFile()), 65536)) {
            byte[] buf = new byte[65536];
            int n;
            while ((n = is.read(buf)) != -1) md.update(buf, 0, n);
        }
        return ChunkEnvelope.bytesToHex(md.digest());
    }

    private boolean promptYesNo(String prompt) {
        System.out.print(prompt);
        try {
            Scanner sc = new Scanner(System.in);
            String line = sc.nextLine().trim().toLowerCase();
            return line.equals("y") || line.equals("yes");
        } catch (Exception e) {
            return false;
        }
    }

    private void printProgress(long received, long total, long startMs) {
        double pct = total > 0 ? (received * 100.0 / total) : 0;
        long elapsed = System.currentTimeMillis() - startMs;
        double mbps = elapsed > 0 ? (received / 1_048_576.0) / (elapsed / 1000.0) : 0;
        System.out.printf("\r  %.1f%%  %s / %s  %.2f MB/s     ",
                pct, humanSize(received), humanSize(total), mbps);
    }

    private static String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1_048_576) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1_073_741_824) return String.format("%.1f MB", bytes / 1_048_576.0);
        return String.format("%.2f GB", bytes / 1_073_741_824.0);
    }

    private static String sanitize(String name) {
        return name.replaceAll("[\\\\/:*?\"<>|]", "_");
    }

    // --- inner helpers ---

    private static class TransferContext {
        private final String transferId;
        private final Path outFile;
        private final long fileSize;
        private final long startMs = System.currentTimeMillis();
        private volatile long bytesReceived = 0;
        private volatile int lastAckedSeq = -1;

        TransferContext(String transferId, Path outFile, long fileSize) {
            this.transferId = transferId;
            this.outFile = outFile;
            this.fileSize = fileSize;
        }

        void updateProgress(int seq, long bytesReceived) {
            this.lastAckedSeq = seq;
            this.bytesReceived = bytesReceived;
        }

        String transferId() { return transferId; }
        Path outFile() { return outFile; }
        long fileSize() { return fileSize; }
        long startMs() { return startMs; }
        long bytesReceived() { return bytesReceived; }
    }
}
