package com.hazelcast.streamer;

public interface StreamConsumer<T> {
    void accept(int partition, long offset, T value);
}
