package com.kafkafile.app.model;

public enum TransferState {
    OFFERED,
    ACCEPTED,
    IN_PROGRESS,
    PAUSED,
    COMPLETED,
    ABORTED,
    SUPERSEDED;

    public boolean isTerminal() {
        return this == COMPLETED || this == ABORTED || this == SUPERSEDED;
    }

    public boolean isActive() {
        return this == ACCEPTED || this == IN_PROGRESS || this == PAUSED;
    }
}
