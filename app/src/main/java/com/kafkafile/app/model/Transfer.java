package com.kafkafile.app.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)

public class Transfer {

    // WAL-persisted fields
    private String transferId;
    private TransferState state;
    private String recipientName;
    private String senderName;
    private String fileName;
    private long fileSize;
    private int partitionIndex = -1;
    private long startOffset = -1;
    private long currentOffset = -1;
    private Long endOffset;
    private byte[] expectedSha256;
    private Instant createdAt;
    private Instant updatedAt;

    // runtime-only fields (not persisted to WAL)
    @JsonIgnore
    private final Map<Integer, byte[]> chunkBuffer = new ConcurrentHashMap<>();
    @JsonIgnore
    private volatile int lastAckedSeq = -1;
    @JsonIgnore
    private volatile long lastFsyncedByteOffset = 0;
    @JsonIgnore
    private volatile int eofSeq = -1;
    @JsonIgnore
    private volatile Instant lastChunkAt = null;  // null until the first chunk arrives

    public Transfer() {}

    public Transfer(String transferId, String senderName, String recipientName,
                    String fileName, long fileSize) {
        this.transferId = transferId;
        this.senderName = senderName;
        this.recipientName = recipientName;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.state = TransferState.OFFERED;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public void putChunk(int seq, byte[] data) { chunkBuffer.put(seq, data); }
    public byte[] getChunk(int seq) { return chunkBuffer.get(seq); }
    public void removeChunk(int seq) { chunkBuffer.remove(seq); }

    public String getTransferId() { return transferId; }
    public void setTransferId(String v) { this.transferId = v; }

    public TransferState getState() { return state; }
    public void setState(TransferState v) { this.state = v; this.updatedAt = Instant.now(); }

    public String getRecipientName() { return recipientName; }
    public void setRecipientName(String v) { this.recipientName = v; }

    public String getSenderName() { return senderName; }
    public void setSenderName(String v) { this.senderName = v; }

    public String getFileName() { return fileName; }
    public void setFileName(String v) { this.fileName = v; }

    public long getFileSize() { return fileSize; }
    public void setFileSize(long v) { this.fileSize = v; }

    public int getPartitionIndex() { return partitionIndex; }
    public void setPartitionIndex(int v) { this.partitionIndex = v; }

    public long getStartOffset() { return startOffset; }
    public void setStartOffset(long v) { this.startOffset = v; }

    public long getCurrentOffset() { return currentOffset; }
    public void setCurrentOffset(long v) { this.currentOffset = v; }

    public Long getEndOffset() { return endOffset; }
    public void setEndOffset(Long v) { this.endOffset = v; }

    public byte[] getExpectedSha256() { return expectedSha256; }
    public void setExpectedSha256(byte[] v) { this.expectedSha256 = v; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant v) { this.createdAt = v; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant v) { this.updatedAt = v; }

    public int getLastAckedSeq() { return lastAckedSeq; }
    public void setLastAckedSeq(int v) { this.lastAckedSeq = v; }

    public long getLastFsyncedByteOffset() { return lastFsyncedByteOffset; }
    public void setLastFsyncedByteOffset(long v) { this.lastFsyncedByteOffset = v; }

    public int getEofSeq() { return eofSeq; }
    public void setEofSeq(int v) { this.eofSeq = v; }

    /** Called each time a chunk passes through the relay; used by the inactivity reaper. */
    public void touchChunkActivity() { this.lastChunkAt = Instant.now(); }

    /**
     * Returns the last time a chunk was relayed, or {@code null} if no chunk has arrived yet.
     * The reaper falls back to {@link #getUpdatedAt()} when this is null (i.e. transfer was
     * accepted but has never produced a chunk — covers replayed WAL state after a restart).
     */
    public Instant getLastChunkAt() { return lastChunkAt; }
}
