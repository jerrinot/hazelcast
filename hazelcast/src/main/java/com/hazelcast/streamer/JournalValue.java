package com.hazelcast.streamer;

public final class JournalValue<T> {
    private final T value;
    private final long offset;
    private final int partitionId;


    public JournalValue(T value, long offset, int partitionId) {
        this.value = value;
        this.offset = offset;
        this.partitionId = partitionId;
    }

    public T getValue() {
        return value;
    }

    public long getOffset() {
        return offset;
    }

    public int getPartitionId() {
        return partitionId;
    }
}
