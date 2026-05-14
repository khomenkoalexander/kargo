package com.kafkafile.app.service;

import com.kafkafile.app.config.KafkaFileProperties;
import com.kafkafile.app.model.Transfer;
import com.kafkafile.app.model.TransferState;
import com.kafkafile.app.sse.SseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class TtlReaperService {

    private static final Logger log = LoggerFactory.getLogger(TtlReaperService.class);

    @Autowired
    private KafkaFileProperties props;

    @Autowired
    private TransferService transferService;

    @Autowired
    private SseManager sseManager;

    @Scheduled(fixedDelay = 10_000)
    public void reap() {
        if (!transferService.isReady()) return;

        Instant now = Instant.now();

        for (Transfer t : transferService.allTransfers()) {
            if (t.getState().isTerminal()) continue;

            long ageSeconds = now.getEpochSecond() - t.getUpdatedAt().getEpochSecond();

            if (t.getState() == TransferState.OFFERED) {
                if (ageSeconds > props.getTransfer().getTtlOfferSeconds()) {
                    log.info("Expiring stale offer {} (age {}s)", t.getTransferId(), ageSeconds);
                    transferService.expireTransfer(t.getTransferId(), "offer ttl expired");
                }
                continue;
            }

            // Inactivity check: ACCEPTED or IN_PROGRESS with no chunk activity for too long.
            // Uses lastChunkAt if a chunk has ever arrived, otherwise falls back to updatedAt
            // (covers the case where a transfer was accepted but the sender never produced).
            // This fires regardless of SSE connection state — a live-looking SSE connection
            // is no guarantee the sender is actually sending (Tomcat can hold dead connections
            // for up to ~15 s, and the SSE heartbeat window adds another 15 s on top of that).
            if (t.getState() == TransferState.ACCEPTED || t.getState() == TransferState.IN_PROGRESS) {
                Instant lastActivity = t.getLastChunkAt() != null ? t.getLastChunkAt() : t.getUpdatedAt();
                long inactiveSecs = now.getEpochSecond() - lastActivity.getEpochSecond();
                if (inactiveSecs > props.getTransfer().getTtlOrphanSeconds()) {
                    log.info("Aborting inactive transfer {} state={} (no chunk activity for {}s)",
                            t.getTransferId(), t.getState(), inactiveSecs);
                    transferService.expireTransfer(t.getTransferId(), "no chunk activity");
                    continue;
                }
            }

            // Orphan check: non-offered transfer with both SSE connections dead.
            // Covers PAUSED transfers whose clients never came back.
            boolean senderConnected = sseManager.hasSenderEmitter(t.getTransferId());
            boolean receiverConnected = sseManager.hasInboxEmitter(t.getRecipientName());
            if (!senderConnected && !receiverConnected
                    && ageSeconds > props.getTransfer().getTtlOrphanSeconds()) {
                log.info("Aborting orphaned transfer {} state={} (no active connections, age {}s)",
                        t.getTransferId(), t.getState(), ageSeconds);
                transferService.expireTransfer(t.getTransferId(), "no active connections");
                continue;
            }

            if (t.getState().isActive()
                    && ageSeconds > props.getTransfer().getTtlTransferSeconds()) {
                log.info("Expiring stale transfer {} (age {}s)", t.getTransferId(), ageSeconds);
                transferService.expireTransfer(t.getTransferId(), "transfer ttl expired");
            }
        }
    }
}
