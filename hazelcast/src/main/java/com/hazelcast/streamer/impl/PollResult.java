package com.hazelcast.streamer.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class PollResult implements DataSerializable, InternalConsumer {

    private List<Data> results;
    private List<Long> offsets;

    private long nextOffset;

    public PollResult() {

    }

    public List<Data> getResults() {
        if (results == null) {
            results = new ArrayList<Data>();
        }
        return results;
    }

    public List<Long> getOffsets() {
        if (offsets == null) {
            offsets = new ArrayList<Long>();
        }
        return offsets;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        List<Data> results = getResults();
        out.writeInt(results.size());
        for (int i = 0; i < results.size(); i++) {
            out.writeData(results.get(i));
        }

        List<Long> offsets = getOffsets();
        for (int i = 0; i < offsets.size(); i++) {
            out.writeLong(offsets.get(i));
        }

        out.writeLong(nextOffset);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        results = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            results.add(data);
        }

        offsets = new ArrayList<Long>(size);
        for (int i = 0; i < size; i++) {
            Long offset = in.readLong();
            offsets.add(offset);
        }

        nextOffset = in.readLong();
    }

    @Override
    public void accept(int partition, long offset, Data value, long nextEntryOffset) {
        getResults().add(value);
        getOffsets().add(offset);
        assert offsets.size() == results.size();
        nextOffset = nextEntryOffset;
    }
}
