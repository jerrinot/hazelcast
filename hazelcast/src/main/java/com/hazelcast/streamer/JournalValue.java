package com.hazelcast.streamer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public final class JournalValue<T> implements DataSerializable {
    private T value;
    private long offset;
    private int partitionId;

    public JournalValue() {

    }

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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeData((Data)value);
        out.writeLong(offset);
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = (T) in.readData();
        offset = in.readLong();
        partitionId = in.readInt();
    }
}
