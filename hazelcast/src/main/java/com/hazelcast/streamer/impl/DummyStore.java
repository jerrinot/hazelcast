package com.hazelcast.streamer.impl;

import com.hazelcast.streamer.JournalValue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class DummyStore<T> implements Serializable {
    private final String name;
    private final int partitionId;
    private final List<Object> values = new ArrayList<Object>();

    public DummyStore(String name, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
    }

    public void add(Object value) {
        values.add(value);
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

    public int read(long offset, int maxRecords, PollResult<T> response) {
        int i;
        List<JournalValue<T>> results = response.getResults();
        for (i = 0; i < maxRecords && i + offset < values.size(); i++) {
            int index = (int) (i + offset);
            Object value = values.get(index);
            JournalValue journalValue = new JournalValue(value, index, partitionId);
            results.add(journalValue);
        }
        response.setNextSequence(offset + i);
        return i;
    }
}
