package com.hazelcast.streamer.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.streamer.JournalValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class PollResult<T> implements DataSerializable {

    private List<JournalValue<T>> results;
    private long nextSequence;

    public PollResult() {

    }

    public List<JournalValue<T>> getResults() {
        if (results == null) {
            results = new ArrayList<JournalValue<T>>();
        }
        return results;
    }

    public long getNextSequence() {
        return nextSequence;
    }

    public void setNextSequence(long nextSequence) {
        this.nextSequence = nextSequence;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(results);
        out.writeLong(nextSequence);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        results = in.readObject();
        nextSequence = in.readLong();
    }
}
