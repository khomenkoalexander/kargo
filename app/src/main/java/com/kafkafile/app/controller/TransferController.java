package com.kafkafile.app.controller;

import com.kafkafile.app.config.KafkaFileProperties;
import com.kafkafile.app.model.Transfer;
import com.kafkafile.app.service.TransferService;
import com.kafkafile.app.sse.SseManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.NoSuchElementException;

@RestController
@RequestMapping("/api/v1")
public class TransferController {

    @Autowired
    private TransferService transferService;

    @Autowired
    private SseManager sseManager;

    @Autowired
    private KafkaFileProperties props;

    @GetMapping("/healthz")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", transferService.isReady() ? "ok" : "initializing"));
    }

    // --- sender endpoints ---

    @PostMapping("/transfers")
    public ResponseEntity<?> registerOffer(@RequestBody RegisterOfferRequest req) {
        if (!transferService.isReady()) {
            return ResponseEntity.status(503).body(Map.of("error", "Backend initializing"));
        }
        try {
            Transfer t = transferService.registerOffer(
                    req.senderName(), req.recipientName(), req.fileName(), req.fileSize());
            return ResponseEntity.ok(Map.of(
                    "transferId", t.getTransferId(),
                    "window", props.getTransfer().getWindow(),
                    "maxChunkBytes", props.getTransfer().getMaxChunkBytes()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping(value = "/transfers/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter senderEvents(@PathVariable("id") String id) {
        return sseManager.createSenderEmitter(id);
    }

    @PostMapping(value = "/transfers/{id}/chunks",
            consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<?> uploadChunk(@PathVariable("id") String id,
                                          @RequestBody byte[] chunkBytes) {
        if (!transferService.isReady()) {
            return ResponseEntity.status(503).body(Map.of("error", "Backend initializing"));
        }
        try {
            transferService.handleChunkUploaded(id, chunkBytes);
            return ResponseEntity.noContent().build();
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        } catch (IllegalStateException | IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/transfers/{id}")
    public ResponseEntity<?> cancel(@PathVariable("id") String id) {
        try {
            transferService.cancelTransfer(id);
            return ResponseEntity.noContent().build();
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        }
    }

    // --- receiver endpoints ---

    @PostMapping("/transfers/{id}/accept")
    public ResponseEntity<?> acceptTransfer(@PathVariable("id") String id,
                                             @RequestBody AcceptRequest req) {
        try {
            transferService.acceptTransfer(id, req.recipientName());
            return ResponseEntity.ok(Map.of("success", true));
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        } catch (IllegalStateException | IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping(value = "/transfers/{id}/chunks/{seq}",
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<byte[]> downloadChunk(@PathVariable("id") String id,
                                                 @PathVariable("seq") int seq) {
        Transfer t = transferService.getTransfer(id);
        if (t == null) return ResponseEntity.notFound().build();

        byte[] chunk = t.getChunk(seq);
        if (chunk == null) return ResponseEntity.status(HttpStatus.NOT_FOUND).build();

        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(chunk);
    }

    @PostMapping("/transfers/{id}/progress")
    public ResponseEntity<?> chunkProgress(@PathVariable("id") String id,
                                            @RequestBody ProgressRequest req) {
        try {
            transferService.handleChunkProgress(id, req.seq(), req.lastFsyncedByteOffset());
            return ResponseEntity.noContent().build();
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/transfers/{id}/complete")
    public ResponseEntity<?> completeTransfer(@PathVariable("id") String id,
                                               @RequestBody CompleteRequest req) {
        try {
            transferService.handleTransferComplete(id, req.sha256());
            return ResponseEntity.noContent().build();
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        }
    }

    // --- request/response records ---

    public record RegisterOfferRequest(String senderName, String recipientName,
                                       String fileName, long fileSize) {}

    public record AcceptRequest(String recipientName) {}

    public record ProgressRequest(int seq, long lastFsyncedByteOffset) {}

    public record CompleteRequest(String sha256) {}
}
