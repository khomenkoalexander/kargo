package com.kafkafile.app.service;

import com.kafkafile.app.config.KafkaFileProperties;
import com.kafkafile.app.model.ChunkEnvelope;
import com.kafkafile.app.model.Transfer;
import com.kafkafile.app.model.TransferState;
import com.kafkafile.app.sse.SseManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Manages Kafka I/O for the xfer.data topic.
 * Producer: called from HTTP threads to relay sender-uploaded chunks to Kafka.
 * Consumer: runs on a dedicated thread, reads chunks back, stores in Transfer buffer,
 * and notifies the receiver via SSE.
 */
@Service
public class ChunkRelayService {

    private static final Logger log = LoggerFactory.getLogger(ChunkRelayService.class);

    private final KafkaFileProperties props;
    private final KafkaProducer<byte[], byte[]> dataProducer;
    private final KafkaConsumer<byte[], byte[]> dataConsumer;
    private final SseManager sseManager;

    @Lazy
    @Autowired
    private TransferService transferService;

    private final BlockingQueue<ConsumerCommand> commandQueue = new LinkedBlockingQueue<>();
    private volatile boolean running = true;
    private Thread consumerThread;

    sealed interface ConsumerCommand {
        record Assign(int partition, long startOffset, CompletableFuture<Void> ready) implements ConsumerCommand {}
        record Unassign(int partition) implements ConsumerCommand {}
        record Stop() implements ConsumerCommand {}
    }

    @Autowired
    public ChunkRelayService(KafkaFileProperties props,
                             KafkaProducer<byte[], byte[]> dataProducer,
                             KafkaConsumer<byte[], byte[]> dataConsumer,
                             SseManager sseManager) {
        this.props = props;
        this.dataProducer = dataProducer;
        this.dataConsumer = dataConsumer;
        this.sseManager = sseManager;
    }

    @PostConstruct
    public void startConsumerThread() {
        consumerThread = new Thread(this::consumerLoop, "chunk-relay-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
        log.info("Chunk relay consumer thread started");
    }

    @PreDestroy
    public void stop() {
        running = false;
        commandQueue.offer(new ConsumerCommand.Stop());
        try {
            consumerThread.join(5000);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        dataProducer.close();
        dataConsumer.close();
    }

    /**
     * Called when a transfer is accepted: begins consuming from the assigned partition.
     * Returns a future that completes once the consumer thread has actually sought to the
     * correct position. Callers must await this before signalling the sender to produce,
     * otherwise chunks land in Kafka before the seek and seekToEnd skips past them.
     * Pass startOffset=-1 to seek to the current end of the partition.
     */
    public CompletableFuture<Void> assignPartition(int partition, long startOffset) {
        CompletableFuture<Void> ready = new CompletableFuture<>();
        commandQueue.offer(new ConsumerCommand.Assign(partition, startOffset, ready));
        return ready;
    }

    /** Called when a transfer terminates: stops consuming from the partition. */
    public void unassignPartition(int partition) {
        commandQueue.offer(new ConsumerCommand.Unassign(partition));
    }

    /**
     * Produces a raw chunk envelope to the data topic on the given partition.
     * Called from HTTP thread when sender uploads a chunk.
     */
    public void produce(int partition, byte[] chunkBytes) {
        String dataTopic = props.getKafka().getDataTopic();
        ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(dataTopic, partition, null, chunkBytes);
        dataProducer.send(rec, (meta, ex) -> {
            if (ex != null) log.error("Failed to produce chunk to partition {}: {}", partition, ex.getMessage());
        });
    }

    // --- consumer thread ---

    private void consumerLoop() {
        Set<TopicPartition> assigned = new HashSet<>();
        String dataTopic = props.getKafka().getDataTopic();

        while (running) {
            // process commands
            ConsumerCommand cmd;
            while ((cmd = commandQueue.poll()) != null) {
                if (cmd instanceof ConsumerCommand.Assign a) {
                    TopicPartition tp = new TopicPartition(dataTopic, a.partition());
                    assigned.add(tp);
                    dataConsumer.assign(assigned);
                    if (a.startOffset() < 0) {
                        dataConsumer.seekToEnd(Collections.singletonList(tp));
                        // seekToEnd is lazy; force position evaluation so the offset is concrete now
                        dataConsumer.position(tp);
                        log.debug("Assigned partition {} at END", a.partition());
                    } else {
                        dataConsumer.seek(tp, a.startOffset());
                        log.debug("Assigned partition {} at offset {}", a.partition(), a.startOffset());
                    }
                    a.ready().complete(null);
                } else if (cmd instanceof ConsumerCommand.Unassign u) {
                    assigned.remove(new TopicPartition(dataTopic, u.partition()));
                    dataConsumer.assign(assigned);
                    log.debug("Unassigned partition {}", u.partition());
                } else if (cmd instanceof ConsumerCommand.Stop) {
                    return;
                }
            }

            if (assigned.isEmpty()) {
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                continue;
            }

            ConsumerRecords<byte[], byte[]> records = dataConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> rec : records) {
                processRecord(rec);
            }
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> rec) {
        try {
            ChunkEnvelope chunk = ChunkEnvelope.decode(rec.value());
            String transferId = chunk.getTransferId().toString();
            Transfer transfer = transferService.getTransfer(transferId);

            if (transfer == null || transfer.getState().isTerminal()) {
                log.debug("Dropping chunk seq={} for unknown/terminal transfer {}", chunk.getSeq(), transferId);
                commitOffset(rec);
                return;
            }

            // record that a chunk just moved through the relay (used by inactivity reaper)
            transfer.touchChunkActivity();

            // store raw bytes in buffer so receiver can download the full envelope
            transfer.putChunk(chunk.getSeq(), rec.value());

            if (chunk.isEof()) {
                transfer.setEofSeq(chunk.getSeq());
                transfer.setExpectedSha256(chunk.getSha256());
            }

            // transition to IN_PROGRESS on first chunk
            if (transfer.getState() == TransferState.ACCEPTED
                    || transfer.getState() == TransferState.PAUSED) {
                transferService.onFirstChunkReceived(transferId);
            }

            commitOffset(rec);

            // notify receiver
            sseManager.sendToInbox(transfer.getRecipientName(), "chunkAvailable",
                    Map.of("transferId", transferId, "seq", chunk.getSeq()));

        } catch (Exception e) {
            log.error("Error processing record from partition {} offset {}: {}",
                    rec.partition(), rec.offset(), e.getMessage());
        }
    }

    private void commitOffset(ConsumerRecord<byte[], byte[]> rec) {
        try {
            dataConsumer.commitSync(Map.of(
                    new TopicPartition(rec.topic(), rec.partition()),
                    new OffsetAndMetadata(rec.offset() + 1)));
        } catch (Exception e) {
            log.warn("Offset commit failed for partition {} offset {}: {}",
                    rec.partition(), rec.offset(), e.getMessage());
        }
    }
}
