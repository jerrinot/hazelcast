package com.hazelcast.streamer.impl;

import com.hazelcast.spi.WaitNotifyKey;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class StreamerWaitNotifyKey implements WaitNotifyKey {

    private final String name;
    private final int partitionId;

    public StreamerWaitNotifyKey(String name, int partitionId) {
        checkNotNull(name);
        this.name = name;
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamerWaitNotifyKey that = (StreamerWaitNotifyKey) o;
        return partitionId == that.partitionId && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + partitionId;
        return result;
    }

    @Override
    public String toString() {
        return "StreamerWaitNotifyKey{"
                + "name=" + name
                + ", partitionId=" + partitionId
                + '}';
    }

    @Override
    public String getServiceName() {
        return StreamerService.SERVICE_NAME;
    }

    @Override
    public String getObjectName() {
        return name;
    }
}