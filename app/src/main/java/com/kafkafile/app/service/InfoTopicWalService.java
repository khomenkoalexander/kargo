package com.kafkafile.app.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafkafile.app.config.KafkaFileProperties;
import com.kafkafile.app.model.Transfer;
import com.kafkafile.app.model.TransferState;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * Handles the compacted xfer.info topic: replays records on startup to rebuild RAM state,
 * and appends state-change records during normal operation.
 */
@Service
public class InfoTopicWalService {

    private static final Logger log = LoggerFactory.getLogger(InfoTopicWalService.class);

    private final KafkaFileProperties props;
    private final KafkaProducer<String, String> infoProducer;
    private final KafkaConsumer<String, String> infoConsumer;
    private final ObjectMapper mapper;

    @Autowired
    public InfoTopicWalService(KafkaFileProperties props,
                               KafkaProducer<String, String> infoProducer,
                               KafkaConsumer<String, String> infoConsumer) {
        this.props = props;
        this.infoProducer = infoProducer;
        this.infoConsumer = infoConsumer;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Replays xfer.info from offset 0 into the provided map.
     * Called once at startup by TransferService before serving requests.
     */
    public void replayInto(Map<String, Transfer> transfers) {
        String topic = props.getKafka().getInfoTopic();
        TopicPartition tp = new TopicPartition(topic, 0);
        infoConsumer.assign(Collections.singletonList(tp));
        infoConsumer.seekToBeginning(Collections.singletonList(tp));

        long endOffset = getEndOffset(tp);
        if (endOffset == 0) {
            log.info("xfer.info is empty — starting fresh");
            return;
        }

        log.info("Replaying xfer.info up to offset {}", endOffset);
        long read = 0;
        while (read < endOffset) {
            ConsumerRecords<String, String> records = infoConsumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) break;
            for (ConsumerRecord<String, String> rec : records) {
                read = rec.offset() + 1;
                if (rec.value() == null) {
                    // tombstone
                    transfers.remove(rec.key());
                    continue;
                }
                try {
                    Transfer t = mapper.readValue(rec.value(), Transfer.class);
                    transfers.put(t.getTransferId(), t);
                } catch (Exception e) {
                    log.warn("Failed to parse WAL record key={}: {}", rec.key(), e.getMessage());
                }
            }
        }
        log.info("WAL replay complete: {} transfers loaded", transfers.size());
    }

    /** Writes a state-change record for the given transfer. */
    public void writeState(Transfer t) {
        try {
            String json = mapper.writeValueAsString(new WalEntry(t));
            ProducerRecord<String, String> rec =
                    new ProducerRecord<>(props.getKafka().getInfoTopic(), t.getTransferId(), json);
            infoProducer.send(rec, (meta, ex) -> {
                if (ex != null) log.error("Failed to write WAL for {}: {}", t.getTransferId(), ex.getMessage());
            });
        } catch (Exception e) {
            log.error("WAL serialization error for {}: {}", t.getTransferId(), e.getMessage());
        }
    }

    /** Writes a tombstone for the given transferId (signals deletion to the compacted topic). */
    public void tombstone(String transferId) {
        ProducerRecord<String, String> rec =
                new ProducerRecord<>(props.getKafka().getInfoTopic(), transferId, null);
        infoProducer.send(rec, (meta, ex) -> {
            if (ex != null) log.error("Failed to tombstone {}: {}", transferId, ex.getMessage());
        });
    }

    private long getEndOffset(TopicPartition tp) {
        Map<TopicPartition, Long> ends = infoConsumer.endOffsets(Collections.singletonList(tp));
        return ends.getOrDefault(tp, 0L);
    }

    /** Flat projection of Transfer used for WAL serialization. */
    public static class WalEntry {
        public String transferId;
        public TransferState state;
        public String recipientName;
        public String senderName;
        public String fileName;
        public long fileSize;
        public int partitionIndex;
        public long startOffset;
        public long currentOffset;
        public Long endOffset;
        public Instant createdAt;
        public Instant updatedAt;

        public WalEntry() {}

        public WalEntry(Transfer t) {
            this.transferId = t.getTransferId();
            this.state = t.getState();
            this.recipientName = t.getRecipientName();
            this.senderName = t.getSenderName();
            this.fileName = t.getFileName();
            this.fileSize = t.getFileSize();
            this.partitionIndex = t.getPartitionIndex();
            this.startOffset = t.getStartOffset();
            this.currentOffset = t.getCurrentOffset();
            this.endOffset = t.getEndOffset();
            this.createdAt = t.getCreatedAt();
            this.updatedAt = t.getUpdatedAt();
        }
    }
}
