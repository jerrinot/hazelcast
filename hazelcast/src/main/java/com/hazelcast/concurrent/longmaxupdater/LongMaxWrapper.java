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

    void update(long x) {
        if (x > value) {
            value = x;
        }
    }

    long maxThenReset() {
        long max = value;
        reset();
        return max;
    }

    final void reset() {
        value = DEFAULT_INITIAL_VALUE;
    }

}
