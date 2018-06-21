package com.hazelcast.streamer.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.streamer.JournalValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class DummyStore<T> implements DataSerializable {
    private String name;
    private int partitionId;
    private List<Data> values = new ArrayList<Data>();

    public DummyStore(String name, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
    }

    public DummyStore() {

    }

    public void add(Data value) {
        values.add(value);
    }

    public boolean hasEnoughRecordsToRead(long offset, int minRecords) {
        long size = getSize();
        return size - offset >= minRecords;
    }

    public int read(long offset, int maxRecords, PollResult response) {
        int i;
        List<JournalValue<Data>> results = response.getResults();
        long size = getSize();
        for (i = 0; i < maxRecords && i + offset < size; i++) {
            int index = (int) (i + offset);
            Data value = values.get(index);
            JournalValue<Data> journalValue = new JournalValue<Data>(value, index, partitionId);
            results.add(journalValue);
        }
        response.setNextSequence(offset + i);
        return i;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getName() {
        return name;
    }

    private long getSize() {
        return values.size();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(partitionId);
        out.writeInt(values.size());
        for (Data data : values) {
            out.writeData(data);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitionId = in.readInt();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Data value = in.readData();
            values.add(value);
        }
    }
}
