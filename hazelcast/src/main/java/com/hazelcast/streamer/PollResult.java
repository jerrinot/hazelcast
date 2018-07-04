package com.hazelcast.streamer;

import java.util.List;

public class PollResult<T> {

    private final long nextOffset;
    private final List<JournalValue<T>> values;

    public PollResult(List<JournalValue<T>> values, long nextOffset) {
        this.values = values;
        this.nextOffset = nextOffset;
    }

    public List<JournalValue<T>> getValues() {
        return values;
    }

    public long getNextOffset() {
        return nextOffset;
    }
}
