package com.hazelcast.streamer.impl;

import com.hazelcast.nio.serialization.Data;

public interface InternalConsumer {
    void accept(int partition, long offset, Data value, long nextEntryOffset);
}
