package com.hazelcast.streamer;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.util.function.Consumer;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface Streamer<T> extends DistributedObject {
    ICompletableFuture<Object> send(T value);
    ICompletableFuture<Object> send(int partition, T value);

    List<JournalValue<T>> poll(int partitionId, long offset, int minRecords, int maxRecords, long timeout, TimeUnit timeUnit);

    Subscription<T> subscribeAllPartitions(SubscriptionMode mode, StreamConsumer<T> valueCollector, Consumer<Throwable> errorCollector);
    Subscription<T> subscribeSinglePartition(int partitionId, SubscriptionMode mode, StreamConsumer<T> valueCollector, Consumer<Throwable> errorCollector);

    void syncBarrier();
    int getPartitionCount();
}
