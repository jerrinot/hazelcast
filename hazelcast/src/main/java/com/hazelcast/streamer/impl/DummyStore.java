package com.hazelcast.streamer.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.streamer.JournalValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class DummyStore<T> implements DataSerializable {
    private String name;
    private int partitionId;
    private List<Data> values = new ArrayList<Data>();
    private SerializationService serializationService;

    public DummyStore(String name, int partitionId, SerializationService serializationService) {
        this.name = name;
        this.partitionId = partitionId;
        this.serializationService = serializationService;
    }

    public DummyStore() {

    }

    public void add(Object value) {
        Data data = serializationService.toData(value);
        values.add(data);
    }

    public boolean hasEnoughRecordsToRead(long offset, int minRecords) {
        int size = values.size();
        return size - offset >= minRecords;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getName() {
        return name;
    }

    public int read(long offset, int maxRecords, PollResult response) {
        int i;
        List<JournalValue<Data>> results = response.getResults();
        for (i = 0; i < maxRecords && i + offset < values.size(); i++) {
            int index = (int) (i + offset);
            Data value = values.get(index);
            JournalValue<Data> journalValue = new JournalValue<Data>(value, index, partitionId);
            results.add(journalValue);
        }
        response.setNextSequence(offset + i);
        return i;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(partitionId);
        out.writeObject(values);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitionId = in.readInt();
        values = in.readObject();
    }

    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }
}
