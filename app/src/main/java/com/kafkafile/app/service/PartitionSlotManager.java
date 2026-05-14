package com.kafkafile.app.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Manages the pool of data-topic partition slots. Each slot holds at most one active transfer.
 * Slot allocation is initialized after WAL replay in TransferService.
 */
@Component
public class PartitionSlotManager {

    private static final Logger log = LoggerFactory.getLogger(PartitionSlotManager.class);

    private final Set<Integer> freePartitions = new TreeSet<>();

    /** Called after WAL replay to seed the free list. */
    public synchronized void initialize(int totalPartitions, Set<Integer> inUsePartitions) {
        freePartitions.clear();
        for (int i = 0; i < totalPartitions; i++) {
            if (!inUsePartitions.contains(i)) {
                freePartitions.add(i);
            }
        }
        log.info("Partition slot manager ready: {} free, {} in-use out of {}",
                freePartitions.size(), inUsePartitions.size(), totalPartitions);
    }

    public synchronized Optional<Integer> allocate() {
        if (freePartitions.isEmpty()) return Optional.empty();
        Integer p = freePartitions.iterator().next();
        freePartitions.remove(p);
        return Optional.of(p);
    }

    public synchronized void release(int partition) {
        freePartitions.add(partition);
        log.debug("Released partition slot {}", partition);
    }

    public synchronized int freeCount() {
        return freePartitions.size();
    }
}
