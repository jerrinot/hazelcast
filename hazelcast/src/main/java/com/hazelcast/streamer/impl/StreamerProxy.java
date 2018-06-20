package com.hazelcast.streamer.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.streamer.JournalValue;
import com.hazelcast.streamer.StreamConsumer;
import com.hazelcast.streamer.Streamer;
import com.hazelcast.streamer.Subscription;
import com.hazelcast.streamer.SubscriptionMode;
import com.hazelcast.streamer.impl.operations.PollOperation;
import com.hazelcast.streamer.impl.operations.SendOperation;
import com.hazelcast.util.function.Consumer;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.streamer.impl.StreamerService.SERVICE_NAME;

public class StreamerProxy<T> extends AbstractDistributedObject<StreamerService> implements Streamer<T> {

    private final String name;
    private final StreamerBackpressure backpressure;
    private final int partitionCount;
    private final OperationService operationService;
    private final int[] allPartitions;

    protected StreamerProxy(String name, NodeEngine nodeEngine, StreamerService service) {
        super(nodeEngine, service);
        this.name = name;
        this.backpressure = new StreamerBackpressure();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.operationService = nodeEngine.getOperationService();
        this.allPartitions = getAllPartitions(partitionCount);
    }

    private static int[] getAllPartitions(int partitionCount) {
        int[] result = new int[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            result[i] = i;
        }
        return result;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public ICompletableFuture<Object> send(T value) {
        int partitionId = randomPartition();
        return send(partitionId, value);
    }

    private int randomPartition() {
        return ThreadLocalRandomProvider.get().nextInt(partitionCount);
    }

    @Override
    public ICompletableFuture<Object> send(int partitionId, T value) {
        Operation op = new SendOperation(value, name);

        backpressure.waitForSlot();
        InternalCompletableFuture<Object> future = operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId)
                .invoke();

        backpressure.registerFuture(future);

        return future;
    }

    @Override
    public Subscription<T> subscribeSinglePartition(int partitionId, SubscriptionMode mode, StreamConsumer<T> valueCollector, Consumer<Throwable> errorCollector) {
        return new Subscription<T>(getNodeEngine(), new int[]{partitionId}, mode, valueCollector, errorCollector, name);
    }

    @Override
    public Subscription<T> subscribeAllPartitions(SubscriptionMode mode, StreamConsumer<T> valueCollector, Consumer<Throwable> errorCollector) {
        return new Subscription<T>(getNodeEngine(), allPartitions, mode, valueCollector, errorCollector, name);
    }

    @Override
    public List<JournalValue<T>> poll(int partitionId, long offset, int minRecords, int maxRecords, long timeout, TimeUnit timeUnit) {
        Operation op = new PollOperation(name, offset, minRecords, maxRecords);
        op.setWaitTimeout(timeUnit.toMillis(timeout));

        //todo: what to do with timeout?
        InternalCompletableFuture<PollResult<T>> future = operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId)
                .invoke();

        return future.join().getResults();
    }

    @Override
    public void syncBarrier() {
        backpressure.barrier();
    }

    @Override
    public int getPartitionCount() {
        return partitionCount;
    }

}
