package com.hazelcast.streamer.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

final class StreamerBackpressure {
    private static final int MAX_INFLIGHT_OPERATION_COUNT = 20000;
    private final AtomicInteger inflightOpCounter = new AtomicInteger();
    private ExecutionCallback callback = new DecrementingCallback();
    private IdleStrategy idleStrategy = new BackoffIdleStrategy(1000, 1000, MICROSECONDS.toNanos(10),
            MICROSECONDS.toNanos(100));


    void waitForSlot() {
        for (long i = 0; !hasSlot(); i++) {
            idleStrategy.idle(i);
        }
    }

    void barrier() {
        for (long i = 0; inflightOpCounter.get() != 0; i++) {
            idleStrategy.idle(i);
        }
    }

    private boolean hasSlot() {
        return inflightOpCounter.get() < MAX_INFLIGHT_OPERATION_COUNT;
    }

    void registerFuture(ICompletableFuture<?> future) {
        inflightOpCounter.incrementAndGet();
        future.andThen(callback);
    }

    private class DecrementingCallback implements ExecutionCallback {
        @Override
        public void onResponse(Object response) {
            inflightOpCounter.decrementAndGet();
        }

        @Override
        public void onFailure(Throwable t) {
            inflightOpCounter.decrementAndGet();
        }
    }
}
