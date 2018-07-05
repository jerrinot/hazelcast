package com.hazelcast.raft.service.streamer.proxy;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.streamer.operation.PollOp;
import com.hazelcast.raft.service.streamer.operation.SendOp;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.streamer.PollResult;
import com.hazelcast.streamer.StreamConsumer;
import com.hazelcast.streamer.Streamer;
import com.hazelcast.streamer.Subscription;
import com.hazelcast.streamer.SubscriptionMode;
import com.hazelcast.streamer.impl.InternalPollResult;
import com.hazelcast.streamer.impl.StreamerBackpressure;
import com.hazelcast.streamer.impl.StreamerProxy;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.function.Consumer;

import java.util.concurrent.TimeUnit;

public class RaftStreamerProxy<T> implements Streamer<T> {
    private final String name;
    private final RaftGroupId groupId;
    private final RaftInvocationManager raftInvocationManager;
    private final SerializationService serializationService;
    private final StreamerBackpressure backpressure;

    public RaftStreamerProxy(String name, RaftGroupId groupId, RaftInvocationManager raftInvocationManager,
                             SerializationService serializationService) {
        this.name = name;
        this.groupId = groupId;
        this.raftInvocationManager = raftInvocationManager;
        this.serializationService = serializationService;
        this.backpressure = new StreamerBackpressure();
    }

    private RuntimeException notImplemented() {
        throw new UnsupportedOperationException("not implemeted yet");
    }

    @Override
    public ICompletableFuture<Object> send(T value) {
        return send(0, value);
    }

    @Override
    public ICompletableFuture<Object> send(int partition, T value) {
        if (partition != 0) {
            throw new UnsupportedOperationException("raft streamer is not partitioned yet. alwasys use partition 0");
        }
        Data data = serializationService.toData(value);
        backpressure.waitForSlot();
        ICompletableFuture<Object> f = raftInvocationManager.invoke(groupId, new SendOp(name, data));
        backpressure.registerFuture(f);
        return f;
    }

    @Override
    public PollResult<T> poll(int partitionId, long offset, int minRecords, int maxRecords, long timeout, TimeUnit timeUnit) {
        if (minRecords != 0) {
            throw new UnsupportedOperationException("min count is not supported with Raft streamer");
        }
        if (partitionId != 0) {
            throw new UnsupportedOperationException("raft streamer is not partitioned yet. alwasys use partition 0");
        }
        PollOp op = new PollOp(name, offset, maxRecords);
        ICompletableFuture<InternalPollResult> f = raftInvocationManager.invoke(groupId, op);
        InternalPollResult internalPollResult = join(f);

        return StreamerProxy.toPollResult(internalPollResult, 0, serializationService);
    }

    @Override
    public Subscription<T> subscribeAllPartitions(SubscriptionMode mode, StreamConsumer<T> valueCollector, Consumer<Throwable> errorCollector) {
        throw notImplemented();
    }

    @Override
    public Subscription<T> subscribeSinglePartition(int partitionId, SubscriptionMode mode, StreamConsumer<T> valueCollector, Consumer<Throwable> errorCollector) {
        throw notImplemented();
    }

    @Override
    public void syncBarrier() {
        backpressure.barrier();
    }

    @Override
    public int getPartitionCount() {
        throw notImplemented();
    }

    @Override
    public String getPartitionKey() {
        throw notImplemented();
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getServiceName() {
        throw notImplemented();
    }

    @Override
    public void destroy() {
        throw notImplemented();
    }

    private <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

}
