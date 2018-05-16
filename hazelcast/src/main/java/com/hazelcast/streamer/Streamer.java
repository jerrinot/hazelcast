package com.hazelcast.streamer;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.function.Consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class Streamer<V> {
    private final IMap<Object, V> map;
    private final int[] sharedPartitionKeys;
    private final int[] allPartitions;
    private final StreamerBackpressure backpressure;

    private Streamer(HazelcastInstance instance, String name) {
        this.map = instance.getMap(name);
        this.sharedPartitionKeys = computeSharedPartitionKeys(instance);
        this.backpressure = new StreamerBackpressure();
        this.allPartitions = getAllPartitions(instance);
    }

    public static <V> Streamer<V> newStreamer(HazelcastInstance instance, String name) {
        return new Streamer<V>(instance, name);
    }

    public ICompletableFuture<Object> send(V value) {
        Object key = randomPartitionKey();
        backpressure.waitForSlot();
        ICompletableFuture f = map.setAsync(key, value);
        backpressure.registerFuture(f);
        return f;
    }

    public ICompletableFuture<Object> send(int partition, V value) {
        Object key = sharedPartitionKeys[partition];
        backpressure.waitForSlot();
        ICompletableFuture f = map.setAsync(key, value);
        backpressure.registerFuture(f);
        return f;
    }

    //todo: make this method async
    public Subscription<V> subscribeSinglePartition(int partitionId, SubscriptionMode mode, StreamConsumer<V> valueCollector, Consumer<Throwable> errorCollector) {
        EventJournalReader<EventJournalMapEvent<Integer, V>> reader = (EventJournalReader<EventJournalMapEvent<Integer, V>>) map;
        return new Subscription<V>(reader, new int[]{partitionId}, mode, valueCollector, errorCollector);
    }

    public Subscription<V> subscribeAllPartitions(SubscriptionMode mode, StreamConsumer<V> valueCollector, Consumer<Throwable> errorCollector) {
        EventJournalReader<EventJournalMapEvent<Integer, V>> reader = (EventJournalReader<EventJournalMapEvent<Integer, V>>) map;
        return new Subscription<V>(reader, allPartitions, mode, valueCollector, errorCollector);
    }

    //todo: timeout is currently ignored
    public List<JournalValue<V>> poll(int partitionId, long offset, int minRecords, int maxRecords, long timeout, TimeUnit timeUnit) {
        EventJournalReader<EventJournalMapEvent<Integer, V>> reader = (EventJournalReader<EventJournalMapEvent<Integer, V>>) map;
        ICompletableFuture<ReadResultSet<EventJournalMapEvent<Integer, V>>> f = reader.readFromEventJournal(offset, minRecords, maxRecords, partitionId, null, null);
        List<JournalValue<V>> results = new ArrayList<JournalValue<V>>();
        try {
            ReadResultSet<EventJournalMapEvent<Integer, V>> rrs = f.get();
            for (int i = 0; i < rrs.size(); i++) {
                EventJournalMapEvent<Integer, V> event = rrs.get(i);
                long sequence = rrs.getSequence(i);
                V value = event.getNewValue();
                results.add(new JournalValue<V>(value, sequence, partitionId));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw ExceptionUtil.rethrow(e);
        }
        return results;
    }


    public void syncBarrier() {
        backpressure.barrier();
    }

    public int getPartitionCount() {
        return sharedPartitionKeys.length;
    }

    private int randomPartitionKey() {
        int randomIndex = ThreadLocalRandomProvider.get().nextInt(sharedPartitionKeys.length);
        return sharedPartitionKeys[randomIndex];
    }

    private int[] getAllPartitions(HazelcastInstance instance) {
        PartitionService partitionService = instance.getPartitionService();
        Set<Partition> partitions = partitionService.getPartitions();

        int[] allPartitions = new int[partitions.size()];
        for (int i = 0; i < partitions.size(); i++) {
            allPartitions[i] = i;
        }
        return allPartitions;
    }

    private int[] computeSharedPartitionKeys(HazelcastInstance instance) {
        PartitionService partitionService = instance.getPartitionService();
        int partitionCount = partitionService.getPartitions().size();
        int[] keys = new int[partitionCount];
        int remainingCount = partitionCount;
        for (int i = 1; remainingCount > 0; i++) {
            int partitionId = partitionService.getPartition(i).getPartitionId();
            if (keys[partitionId] == 0) {
                keys[partitionId] = i;
                remainingCount--;
            }
        }
        return keys;
    }
}
