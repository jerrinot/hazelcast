package com.hazelcast.util;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class SamplingLogger {
    private final ILogger delegate;
    private final AtomicLong lastLogged;

    private final AtomicInteger supressed;

    private final long maximumLoggingFrequencyNs;

    public SamplingLogger(ILogger delegate, long maximumLoggingFrequencyMs) {
        this.delegate = delegate;
        this.lastLogged = new AtomicLong();
        this.maximumLoggingFrequencyNs = TimeUnit.MILLISECONDS.toNanos(maximumLoggingFrequencyMs);
        this.supressed = new AtomicInteger();
    }

    public void log(Level level, String message) {
        long currentTime = System.nanoTime();
        long currentLastLogged = lastLogged.get();
        if (currentTime - currentLastLogged > maximumLoggingFrequencyNs) {
            if (lastLogged.compareAndSet(currentLastLogged, currentTime)) {
                int supressedCount = supressed.getAndSet(0);
                message += "\nSupressed Message Count: " + supressedCount;
                delegate.log(level, message);
            } else {
                supressed.incrementAndGet();
            }
        } else {
            supressed.incrementAndGet();
        }
    }
}
