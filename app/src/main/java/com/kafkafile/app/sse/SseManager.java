package com.kafkafile.app.sse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkafile.app.config.KafkaFileProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SseManager {

    private static final Logger log = LoggerFactory.getLogger(SseManager.class);

    @Autowired
    private KafkaFileProperties props;

    private final ObjectMapper mapper = new ObjectMapper();

    // sender SSE streams keyed by transferId
    private final Map<String, SseEmitter> senderEmitters = new ConcurrentHashMap<>();
    // receiver inbox SSE streams keyed by recipientName
    private final Map<String, SseEmitter> inboxEmitters = new ConcurrentHashMap<>();

    public SseEmitter createSenderEmitter(String transferId) {
        SseEmitter emitter = new SseEmitter(0L); // no timeout; heartbeats keep it alive
        senderEmitters.put(transferId, emitter);
        emitter.onCompletion(() -> senderEmitters.remove(transferId));
        emitter.onTimeout(() -> senderEmitters.remove(transferId));
        // onError: remove from map and call complete() to formally close the async context,
        // preventing Tomcat from logging further broken-pipe errors for this connection.
        emitter.onError(ex -> {
            senderEmitters.remove(transferId);
            try { emitter.complete(); } catch (Exception ignored) {}
        });
        return emitter;
    }

    public SseEmitter createInboxEmitter(String recipientName) {
        SseEmitter emitter = new SseEmitter(0L);
        inboxEmitters.put(recipientName, emitter);
        emitter.onCompletion(() -> inboxEmitters.remove(recipientName));
        emitter.onTimeout(() -> inboxEmitters.remove(recipientName));
        emitter.onError(ex -> {
            inboxEmitters.remove(recipientName);
            try { emitter.complete(); } catch (Exception ignored) {}
        });
        return emitter;
    }

    public boolean hasSenderEmitter(String transferId) {
        return senderEmitters.containsKey(transferId);
    }

    public boolean hasInboxEmitter(String recipientName) {
        return inboxEmitters.containsKey(recipientName);
    }

    public void sendToSender(String transferId, String eventType, Object data) {
        SseEmitter emitter = senderEmitters.get(transferId);
        if (emitter == null) return;
        if (!send(emitter, eventType, data)) {
            senderEmitters.remove(transferId, emitter);
        }
    }

    public void sendToInbox(String recipientName, String eventType, Object data) {
        SseEmitter emitter = inboxEmitters.get(recipientName);
        if (emitter == null) return;
        if (!send(emitter, eventType, data)) {
            inboxEmitters.remove(recipientName, emitter);
        }
    }

    /**
     * Sends one SSE event. Returns true on success, false if the emitter is dead.
     * Never calls completeWithError() — when a send fails the emitter is already in a
     * terminal state from Tomcat's perspective, and calling completeWithError() on it
     * causes IllegalStateException from AsyncContextImpl.
     */
    private boolean send(SseEmitter emitter, String eventType, Object data) {
        try {
            String json = mapper.writeValueAsString(data);
            emitter.send(SseEmitter.event().name(eventType).data(json));
            return true;
        } catch (Exception e) {
            log.debug("SSE send failed ({}): {}", eventType, e.getMessage());
            return false;
        }
    }

    @Scheduled(fixedDelayString = "${kafkafile.transfer.sse-heartbeat-seconds:15}000")
    public void sendHeartbeats() {
        senderEmitters.forEach((id, emitter) -> {
            if (!send(emitter, "heartbeat", Map.of())) {
                senderEmitters.remove(id, emitter);
            }
        });
        inboxEmitters.forEach((name, emitter) -> {
            if (!send(emitter, "heartbeat", Map.of())) {
                inboxEmitters.remove(name, emitter);
            }
        });
    }
}
