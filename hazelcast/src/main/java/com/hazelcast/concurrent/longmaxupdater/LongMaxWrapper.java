package com.hazelcast.concurrent.longmaxupdater;

public class LongMaxWrapper {

    private static final long DEFAULT_INITIAL_VALUE = Long.MIN_VALUE;

    private long value;

    public LongMaxWrapper() {
        reset();
    }

    long max() {
        return value;
    }

    long update(long x) {
        if (x > value) {
            value = x;
        }
        return value;
    }

    long maxThenReset() {
        long max = value;
        reset();
        return max;
    }

    void set(long x) {
        value = x;
    }

    final void reset() {
        value = DEFAULT_INITIAL_VALUE;
    }

}
