package com.kafkafile.app.controller;

import com.kafkafile.app.model.Transfer;
import com.kafkafile.app.model.TransferState;
import com.kafkafile.app.service.TransferService;
import com.kafkafile.app.sse.SseManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

/**
 * Receiver-side inbox SSE endpoint.
 * On connect, immediately pushes any pending OFFERED transfer for this recipient.
 */
@RestController
@RequestMapping("/api/v1/inbox")
public class InboxController {

    @Autowired
    private TransferService transferService;

    @Autowired
    private SseManager sseManager;

    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter inboxEvents(@RequestParam("name") String name) {
        SseEmitter emitter = sseManager.createInboxEmitter(name);

        // push any already-offered transfer immediately
        transferService.allTransfers().stream()
                .filter(t -> name.equals(t.getRecipientName())
                        && t.getState() == TransferState.OFFERED)
                .findFirst()
                .ifPresent(t -> sseManager.sendToInbox(name, "offerArrived",
                        Map.of("transferId", t.getTransferId(),
                                "senderName", t.getSenderName(),
                                "fileName", t.getFileName(),
                                "fileSize", t.getFileSize())));

        return emitter;
    }
}
