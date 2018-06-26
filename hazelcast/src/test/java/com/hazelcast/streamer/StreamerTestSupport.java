package com.hazelcast.streamer;

import com.hazelcast.config.Config;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableSet;
import static org.junit.Assert.assertEquals;

public abstract class StreamerTestSupport extends HazelcastTestSupport {
    static final int DEFAULT_IN_MEMORY_SIZE_MB = 20;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("/home/jara/tmp"));

    Config createConfig(int partitionCount, String streamerName, int maxMemory) throws IOException {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(partitionCount));
        config.getStreamerConfig(streamerName).setMaxSizeInMemoryMB(maxMemory).setOverflowDir(folder.newFolder().getAbsolutePath());
        return config;
    }

    static void assertSizeEventually(final long expected, final CountingCollector<String> valueCollector) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, valueCollector.getSize());
            }
        });
    }

    static class StoringCollector<T> implements StreamConsumer<T> {
        private Set<T> valueSet = newSetFromMap(new ConcurrentHashMap<T, Boolean>());

        public Set<T> getValues() {
            return unmodifiableSet(valueSet);
        }

        @Override
        public void accept(int partition, long offset, T value) {
            valueSet.add(value);

            int size = valueSet.size();
            if (size % 100000 == 0) {
                System.out.println("Collected " + size + " values so far");
            }
        }
    }

    static class CountingCollector<T> implements StreamConsumer<T> {
        private AtomicLong counter = new AtomicLong();
        private AtomicLong maximumOffset = new AtomicLong();

        @Override
        public void accept(int partition, long offset, T value) {
            long newMaximum;
            for (;;) {
                long currentMaximumOffset = maximumOffset.get();
                newMaximum = Math.max(offset, currentMaximumOffset);
                if (newMaximum == currentMaximumOffset) {
                    break;
                }
                if (maximumOffset.compareAndSet(currentMaximumOffset, newMaximum)) {
                    break;
                }
            }

            long currentSize = counter.incrementAndGet();
            if (currentSize % 100000 == 0) {
                System.out.println("Collected " + currentSize + " values so far, maximum offset: " + newMaximum);
            }
        }

        public long getSize() {
            return counter.get();
        }
    }

}
