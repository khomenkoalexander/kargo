package com.kafkafile.cli.transfer;

import java.io.*;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Persists transfer progress to a .kfile-progress sidecar file so that a crashed CLI
 * can resume from the last known good position.
 *
 * Sidecar is placed alongside the source or destination file:
 *   /path/to/file.bin  →  /path/to/file.bin.kfile-progress
 */
public class ProgressSidecar {

    private final Path sidecarPath;
    private final Properties props = new Properties();

    public ProgressSidecar(Path filePath) {
        this.sidecarPath = filePath.resolveSibling(filePath.getFileName() + ".kfile-progress");
    }

    public void save(String transferId, int lastAckedSeq, long lastFsyncedByteOffset) {
        props.setProperty("transferId", transferId);
        props.setProperty("lastAckedSeq", String.valueOf(lastAckedSeq));
        props.setProperty("lastFsyncedByteOffset", String.valueOf(lastFsyncedByteOffset));
        try (var os = new FileOutputStream(sidecarPath.toFile())) {
            props.store(os, null);
        } catch (IOException e) {
            System.err.println("Warning: failed to save progress sidecar: " + e.getMessage());
        }
    }

    public boolean exists() { return sidecarPath.toFile().exists(); }

    public String getTransferId() { return props.getProperty("transferId"); }
    public int getLastAckedSeq() { return Integer.parseInt(props.getProperty("lastAckedSeq", "-1")); }
    public long getLastFsyncedByteOffset() {
        return Long.parseLong(props.getProperty("lastFsyncedByteOffset", "0"));
    }

    public void load() throws IOException {
        try (var is = new FileInputStream(sidecarPath.toFile())) {
            props.load(is);
        }
    }

    public void delete() {
        sidecarPath.toFile().delete();
    }

    public Path getSidecarPath() { return sidecarPath; }
}
