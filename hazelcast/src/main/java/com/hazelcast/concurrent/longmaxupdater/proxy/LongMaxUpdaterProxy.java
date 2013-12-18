package com.hazelcast.concurrent.longmaxupdater.proxy;

import com.hazelcast.concurrent.longmaxupdater.*;
import com.hazelcast.core.ILongMaxUpdater;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ExceptionUtil;

public class LongMaxUpdaterProxy extends AbstractDistributedObject<LongMaxUpdaterService> implements ILongMaxUpdater {

    private int partitionId;
    private String name;

    public LongMaxUpdaterProxy(String name, NodeEngine nodeEngine, LongMaxUpdaterService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    private <E> InternalCompletableFuture<E> asyncInvoke(Operation operation) {
        try {
            return (InternalCompletableFuture<E>)getNodeEngine().getOperationService().invokeOnPartition(LongMaxUpdaterService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public long max() {
        Operation operation = new MaxOperation(name);
        return (Long)  asyncInvoke(operation).getSafely();
    }

    @Override
    public void update(long x) {
        Operation operation = new UpdateOperation(name, x);
        asyncInvoke(operation).getSafely();
    }

    @Override
    public long maxThenReset() {
        Operation operation = new MaxThenResetOperation(name);
        return (Long) asyncInvoke(operation).getSafely();
    }

    @Override
    public void reset() {
        Operation operation = new ResetOperation(name);
        asyncInvoke(operation).getSafely();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return LongMaxUpdaterService.SERVICE_NAME;
    }
}
