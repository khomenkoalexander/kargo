package com.kafkafile.app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kafkafile")
public class KafkaFileProperties {

    private Auth auth = new Auth();
    private Kafka kafka = new Kafka();
    private Transfer transfer = new Transfer();

    public Auth getAuth() { return auth; }
    public void setAuth(Auth auth) { this.auth = auth; }

    public Kafka getKafka() { return kafka; }
    public void setKafka(Kafka kafka) { this.kafka = kafka; }

    public Transfer getTransfer() { return transfer; }
    public void setTransfer(Transfer transfer) { this.transfer = transfer; }

    public static class Auth {
        private String bearerToken = "changeme";

        public String getBearerToken() { return bearerToken; }
        public void setBearerToken(String bearerToken) { this.bearerToken = bearerToken; }
    }

    public static class Kafka {
        private String bootstrapServers = "localhost:9092";
        private String infoTopic = "xfer.info";
        private String dataTopic = "xfer.data";
        private int dataTopicPartitions = 6;
        private String consumerGroup = "kafkafile-backend";

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String v) { this.bootstrapServers = v; }

        public String getInfoTopic() { return infoTopic; }
        public void setInfoTopic(String v) { this.infoTopic = v; }

        public String getDataTopic() { return dataTopic; }
        public void setDataTopic(String v) { this.dataTopic = v; }

        public int getDataTopicPartitions() { return dataTopicPartitions; }
        public void setDataTopicPartitions(int v) { this.dataTopicPartitions = v; }

        public String getConsumerGroup() { return consumerGroup; }
        public void setConsumerGroup(String v) { this.consumerGroup = v; }
    }

    public static class Transfer {
        private long ttlOfferSeconds = 1800;
        private long ttlTransferSeconds = 3600;
        private long ttlOrphanSeconds = 60;
        private long sseHeartbeatSeconds = 15;
        private int maxChunkBytes = 4194304;
        private int window = 10;

        public long getTtlOfferSeconds() { return ttlOfferSeconds; }
        public void setTtlOfferSeconds(long v) { this.ttlOfferSeconds = v; }

        public long getTtlTransferSeconds() { return ttlTransferSeconds; }
        public void setTtlTransferSeconds(long v) { this.ttlTransferSeconds = v; }

        public long getTtlOrphanSeconds() { return ttlOrphanSeconds; }
        public void setTtlOrphanSeconds(long v) { this.ttlOrphanSeconds = v; }

        public long getSseHeartbeatSeconds() { return sseHeartbeatSeconds; }
        public void setSseHeartbeatSeconds(long v) { this.sseHeartbeatSeconds = v; }

        public int getMaxChunkBytes() { return maxChunkBytes; }
        public void setMaxChunkBytes(int v) { this.maxChunkBytes = v; }

        public int getWindow() { return window; }
        public void setWindow(int v) { this.window = v; }
    }
}
