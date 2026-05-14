package com.kafkafile.app.service;

import com.kafkafile.app.config.KafkaFileProperties;
import com.kafkafile.app.model.Transfer;
import com.kafkafile.app.model.TransferState;
import com.kafkafile.app.sse.SseManager;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class TransferService {

    private static final Logger log = LoggerFactory.getLogger(TransferService.class);

    private final KafkaFileProperties props;
    private final InfoTopicWalService walService;
    private final PartitionSlotManager slotManager;
    private final ChunkRelayService chunkRelayService;
    private final SseManager sseManager;

    private final Map<String, Transfer> transfers = new ConcurrentHashMap<>();
    private volatile boolean ready = false;

    @Autowired
    public TransferService(KafkaFileProperties props,
                           InfoTopicWalService walService,
                           PartitionSlotManager slotManager,
                           ChunkRelayService chunkRelayService,
                           SseManager sseManager) {
        this.props = props;
        this.walService = walService;
        this.slotManager = slotManager;
        this.chunkRelayService = chunkRelayService;
        this.sseManager = sseManager;
    }

    @PostConstruct
    public void init() {
        walService.replayInto(transfers);

        // determine which partitions are in use by non-terminal transfers
        Set<Integer> inUse = new HashSet<>();
        for (Transfer t : transfers.values()) {
            if (!t.getState().isTerminal() && t.getPartitionIndex() >= 0) {
                inUse.add(t.getPartitionIndex());
                // resume consumer for active transfers that were in-progress
                if (t.getState().isActive()) {
                    // resume from last persisted offset; -1 means nothing was consumed yet → seekToEnd
                    chunkRelayService.assignPartition(t.getPartitionIndex(), t.getCurrentOffset());
                }
            }
        }
        slotManager.initialize(props.getKafka().getDataTopicPartitions(), inUse);
        ready = true;
        log.info("TransferService ready. {} transfers in RAM.", transfers.size());
    }

    public boolean isReady() { return ready; }

    public Transfer getTransfer(String transferId) {
        return transfers.get(transferId);
    }

    // --- sender operations ---

    public synchronized Transfer registerOffer(String senderName, String recipientName,
                                               String fileName, long fileSize) {
        // Check for existing offer for this recipient
        Optional<Transfer> existing = transfers.values().stream()
                .filter(t -> recipientName.equals(t.getRecipientName()) && !t.getState().isTerminal())
                .findFirst();

        if (existing.isPresent()) {
            Transfer ex = existing.get();
            if (ex.getState().isActive()) {
                throw new IllegalStateException("transfer in progress");
            }
            // Supersede existing OFFERED transfer
            ex.setState(TransferState.SUPERSEDED);
            walService.writeState(ex);
            sseManager.sendToSender(ex.getTransferId(), "superseded",
                    Map.of("reason", "new sender registered"));
            releaseTransferResources(ex);
            log.info("Superseded offer {} for recipient '{}'", ex.getTransferId(), recipientName);
        }

        Optional<Integer> slot = slotManager.allocate();
        if (slot.isEmpty()) {
            throw new IllegalStateException("no free partition slots — all " +
                    props.getKafka().getDataTopicPartitions() + " concurrent slots in use");
        }

        int partition = slot.get();

        String transferId = UUID.randomUUID().toString();
        Transfer t = new Transfer(transferId, senderName, recipientName, fileName, fileSize);
        t.setPartitionIndex(partition);
        t.setStartOffset(-1);   // -1 = seek-to-end; resolved on consumer thread when accepted
        t.setCurrentOffset(-1);

        transfers.put(transferId, t);
        walService.writeState(t);
        log.info("Offer registered: {} from '{}' to '{}' file='{}'",
                transferId, senderName, recipientName, fileName);

        // notify any receiver already listening on the inbox
        sseManager.sendToInbox(recipientName, "offerArrived",
                Map.of("transferId", transferId,
                        "senderName", senderName,
                        "fileName", fileName,
                        "fileSize", fileSize));

        return t;
    }

    public synchronized void acceptTransfer(String transferId, String recipientName) {
        Transfer t = requireTransfer(transferId);
        if (t.getState() != TransferState.OFFERED) {
            throw new IllegalStateException("Transfer is not in OFFERED state: " + t.getState());
        }
        if (!recipientName.equals(t.getRecipientName())) {
            throw new IllegalArgumentException("Recipient name mismatch");
        }

        t.setState(TransferState.ACCEPTED);
        walService.writeState(t);

        // Wait for the consumer thread to actually seek before notifying the sender.
        // Without this, the sender produces chunks that land in Kafka before the seek,
        // and seekToEnd() skips past them — the receiver never sees them.
        try {
            chunkRelayService.assignPartition(t.getPartitionIndex(), t.getStartOffset())
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Consumer seek timed out for transfer {}: {}", transferId, e.getMessage());
        }

        // signal sender to start producing only after consumer is positioned
        sseManager.sendToSender(transferId, "transferAccepted",
                Map.of("transferId", transferId));
        log.info("Transfer {} accepted by '{}'", transferId, recipientName);
    }

    /** Called by ChunkRelayService when the first chunk arrives from Kafka. */
    public void onFirstChunkReceived(String transferId) {
        Transfer t = transfers.get(transferId);
        if (t != null && (t.getState() == TransferState.ACCEPTED
                || t.getState() == TransferState.PAUSED)) {
            t.setState(TransferState.IN_PROGRESS);
            walService.writeState(t);
        }
    }

    public void handleChunkUploaded(String transferId, byte[] chunkBytes) {
        Transfer t = requireTransfer(transferId);
        if (!t.getState().isActive()) {
            throw new IllegalStateException("Transfer not active: " + t.getState());
        }
        if (chunkBytes.length > props.getTransfer().getMaxChunkBytes() + 69) {
            throw new IllegalArgumentException("Chunk too large");
        }
        chunkRelayService.produce(t.getPartitionIndex(), chunkBytes);
    }

    public synchronized void handleChunkProgress(String transferId, int seq,
                                                  long lastFsyncedByteOffset) {
        Transfer t = requireTransfer(transferId);
        t.setLastAckedSeq(seq);
        t.setLastFsyncedByteOffset(lastFsyncedByteOffset);
        t.removeChunk(seq);

        // periodically persist current offset progress
        if (seq % 50 == 0) {
            walService.writeState(t);
        }

        sseManager.sendToSender(transferId, "chunkAck", Map.of("seq", seq));
    }

    public synchronized void handleTransferComplete(String transferId, String sha256Hex) {
        Transfer t = requireTransfer(transferId);

        boolean match = false;
        if (t.getExpectedSha256() != null && sha256Hex != null) {
            String expected = bytesToHex(t.getExpectedSha256());
            match = expected.equalsIgnoreCase(sha256Hex);
        }

        if (match) {
            t.setState(TransferState.COMPLETED);
            walService.writeState(t);
            sseManager.sendToSender(transferId, "completed", Map.of());
            sseManager.sendToInbox(t.getRecipientName(), "completed",
                    Map.of("transferId", transferId));
            log.info("Transfer {} completed successfully", transferId);
        } else {
            t.setState(TransferState.ABORTED);
            String reason = "sha256 mismatch";
            walService.writeState(t);
            sseManager.sendToSender(transferId, "aborted", Map.of("reason", reason));
            sseManager.sendToInbox(t.getRecipientName(), "aborted",
                    Map.of("transferId", transferId, "reason", reason));
            log.warn("Transfer {} aborted: {}", transferId, reason);
        }

        releaseTransferResources(t);
        scheduleWalTombstone(transferId);
    }

    public synchronized void cancelTransfer(String transferId) {
        Transfer t = requireTransfer(transferId);
        if (t.getState().isTerminal()) return;

        t.setState(TransferState.ABORTED);
        walService.writeState(t);
        sseManager.sendToSender(transferId, "aborted", Map.of("reason", "canceled"));
        sseManager.sendToInbox(t.getRecipientName(), "aborted",
                Map.of("transferId", transferId, "reason", "canceled"));

        releaseTransferResources(t);
        scheduleWalTombstone(transferId);
        log.info("Transfer {} canceled", transferId);
    }

    public synchronized void pauseTransfer(String transferId) {
        Transfer t = transfers.get(transferId);
        if (t == null || !t.getState().isActive()) return;
        t.setState(TransferState.PAUSED);
        walService.writeState(t);
        log.info("Transfer {} paused", transferId);
    }

    /** Called by TtlReaperService for expired transfers. */
    public synchronized void expireTransfer(String transferId, String reason) {
        Transfer t = transfers.get(transferId);
        if (t == null || t.getState().isTerminal()) return;
        t.setState(TransferState.ABORTED);
        walService.writeState(t);
        sseManager.sendToSender(transferId, "aborted", Map.of("reason", reason));
        sseManager.sendToInbox(t.getRecipientName(), "aborted",
                Map.of("transferId", transferId, "reason", reason));
        releaseTransferResources(t);
        scheduleWalTombstone(transferId);
        log.info("Transfer {} expired: {}", transferId, reason);
    }

    public Collection<Transfer> allTransfers() {
        return transfers.values();
    }

    // --- helpers ---

    private Transfer requireTransfer(String transferId) {
        Transfer t = transfers.get(transferId);
        if (t == null) throw new NoSuchElementException("Transfer not found: " + transferId);
        return t;
    }

    private void releaseTransferResources(Transfer t) {
        if (t.getPartitionIndex() >= 0) {
            chunkRelayService.unassignPartition(t.getPartitionIndex());
            slotManager.release(t.getPartitionIndex());
        }
    }

    private void scheduleWalTombstone(String transferId) {
        // Tombstone after a brief delay so receivers can poll the terminal state
        new Thread(() -> {
            try { Thread.sleep(30_000); } catch (InterruptedException ignored) {}
            transfers.remove(transferId);
            walService.tombstone(transferId);
        }, "tombstone-" + transferId.substring(0, 8)).start();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
