package com.hazelcast.streamer;

import com.hazelcast.config.Config;
import com.hazelcast.config.StreamerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.streamer.SubscriptionMode.FROM_OLDEST;
import static com.hazelcast.streamer.Subscription.LOGGING_ERROR_COLLECTOR;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class StreamerSmokeTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int DEFAULT_IN_MEMORY_SIZE_MB = 20;

    @Test
    public void testStreamerInterface_poll() throws Exception {
        String streamerName = randomName();
        HazelcastInstance i1 = createHazelcastInstance(createConfig(271, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        Streamer<String> s = i1.getStreamer(streamerName);

        s.send(0, "foo");
        s.send(1, "bar");

        List<JournalValue<String>> poll = s.poll(0, 0, 1, 100, 1, MINUTES);
        assertEquals(1, poll.size());

        poll = s.poll(1, 0, 2, 100, 10, SECONDS);
        assertEquals(1, poll.size());
    }

    @Test
    public void testDynamicConfiguration() throws Exception {
        int valueCount = 1000;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        //intentionally passing wrong streamerName
        HazelcastInstance i1 = factory.newHazelcastInstance(createConfig(271, "fooName", DEFAULT_IN_MEMORY_SIZE_MB));
        HazelcastInstance i2 = factory.newHazelcastInstance(createConfig(271, "fooName", DEFAULT_IN_MEMORY_SIZE_MB));

        String realName = randomName();
        i1.getConfig().addStreamerConfig(new StreamerConfig()
                .setMaxSizeInMemoryMB(DEFAULT_IN_MEMORY_SIZE_MB)
                .setName(realName)
                .setOverflowDir(folder.newFolder().getAbsolutePath()));

        Streamer<String> streamer = i1.getStreamer(realName);
        for (int i = 0; i < valueCount; i++) {
            streamer.send(UUID.randomUUID().toString());
        }
        final StoringCollector<String> valueCollector = new StoringCollector<String>();
        streamer.subscribeAllPartitions(SubscriptionMode.FROM_OLDEST, valueCollector, LOGGING_ERROR_COLLECTOR);

        assertSizeEventually(valueCount, valueCollector.getValues());
    }

    @Test
    public void testOverflow_with_Migration() throws Exception {
        String streamerName = randomName();
        int valueCount = 10000;
        int partitionCount = 99;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance i1 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, 1));
        HazelcastInstance i2 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, 1));
        Streamer<byte[]> streamer = i1.getStreamer(streamerName);

        for (int i = 0; i < valueCount; i++) {
            streamer.send(new byte[3 * 1024]);
        }

        final StoringCollector<byte[]> valueCollector = new StoringCollector<byte[]>();
        streamer.subscribeAllPartitions(FROM_OLDEST, valueCollector, LOGGING_ERROR_COLLECTOR);

        HazelcastInstance i3 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, 1));
        for (int i = 0; i < valueCount; i++) {
            streamer.send(new byte[3 * 1024]);
        }

        assertSizeEventually(2 * valueCount, valueCollector.getValues());
    }

    @Test
    public void testStoreEvents() throws Exception {
        String streamerName = randomName();
        int keyCount = 100000;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance i1 = factory.newHazelcastInstance(createConfig(271, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        HazelcastInstance i2 = factory.newHazelcastInstance(createConfig(271, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));

        Streamer<String> streamer = i1.getStreamer(streamerName);

        final StoringCollector<String> valueCollector = new StoringCollector<String>();
        streamer.subscribeAllPartitions(FROM_OLDEST, valueCollector, LOGGING_ERROR_COLLECTOR);

        for (int i = 0; i < keyCount; i++) {
            streamer.send(Integer.toString(i));
        }
        HazelcastInstance i3 = factory.newHazelcastInstance(createConfig(271, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        for (int i = keyCount; i < keyCount * 2; i++) {
            streamer.send(Integer.toString(i));
        }

        assertSizeEventually(2 * keyCount, valueCollector.getValues());
    }

    @Test
    public void testMigration_simple() throws Exception {
        String streamerName = randomName();
        int keyCount = 3;
        int partitionCount = 2;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance i1 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        Streamer<String> streamer = i1.getStreamer(streamerName);
        for (int i = 0; i < keyCount; i++) {
            streamer.send(UUID.randomUUID().toString());
        }

        HazelcastInstance i2 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        for (int i = 0; i < keyCount; i++) {
            streamer.send(UUID.randomUUID().toString());
        }

        final StoringCollector<String> valueCollector = new StoringCollector<String>();
        streamer.subscribeAllPartitions(FROM_OLDEST, valueCollector, LOGGING_ERROR_COLLECTOR);

        assertSizeEventually(2 * keyCount, valueCollector.getValues());
    }

    @Test
    public void testMigration() throws Exception {
        String streamerName = randomName();
        int keyCount = 10000;
        int partitionCount = 271;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance i1 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        Streamer<String> streamer = i1.getStreamer(streamerName);
        for (int i = 0; i < keyCount; i++) {
            streamer.send(UUID.randomUUID().toString());
        }

        HazelcastInstance i2 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        for (int i = 0; i < keyCount; i++) {
            streamer.send(UUID.randomUUID().toString());
        }

        HazelcastInstance i3 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        for (int i = 0; i < keyCount; i++) {
            streamer.send(UUID.randomUUID().toString());
        }

        final StoringCollector<String> valueCollector = new StoringCollector<String>();
        streamer.subscribeAllPartitions(FROM_OLDEST, valueCollector, LOGGING_ERROR_COLLECTOR);

        assertSizeEventually(3 * keyCount, valueCollector.getValues());
    }

    @Test
    public void testTimeout() throws Exception {
        String streamerName = randomName();
        int keyCount = 5;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance i1 = factory.newHazelcastInstance(createConfig(271, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));

        Streamer<String> streamer = i1.getStreamer(streamerName);
        for (int i = 0; i < keyCount; i++) {
            streamer.send(0, "bar" + i);
        }

        List<JournalValue<String>> results = streamer.poll(0, 0, 100, 100, 30, SECONDS);
        assertEquals(keyCount, results.size());
    }

    @Test
    public void testSyncBarrierAndBackup() throws Exception {
        String streamerName = randomName();
        int partitionCount = 271;

        int keyCount = 10000;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance i1 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        HazelcastInstance i2 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));

        Streamer<String> streamer = i1.getStreamer(streamerName);
        for (int i = 0; i < keyCount; i++) {
            streamer.send(0, "bar" + i);
            streamer.send(1, "bar" + i);
        }
        streamer.syncBarrier();
        Thread.sleep(1000);

        i1.getLifecycleService().terminate();

        streamer = i2.getStreamer(streamerName);
        List<JournalValue<String>> results = streamer.poll(0, 0, 1, keyCount, 30, SECONDS);
        assertEquals(keyCount, results.size());

        results = streamer.poll(1, 0, 1, keyCount, 30, SECONDS);
        assertEquals(keyCount, results.size());
    }

    @Test
    public void testSubscribtionCancellation() throws Exception {
        String streamerName = randomName();
        final int keyCount = 100000;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance i1 = factory.newHazelcastInstance(createConfig(271, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        HazelcastInstance i2 = factory.newHazelcastInstance(createConfig(271, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));

        Streamer<String> streamer = i1.getStreamer(streamerName);

        final StoringCollector<String> valueCollector = new StoringCollector<String>();
        Subscription<String> subscription = streamer.subscribeAllPartitions(FROM_OLDEST, valueCollector, LOGGING_ERROR_COLLECTOR);

        for (int i = 0; i < keyCount; i++) {
            streamer.send(Integer.toString(i));
        }
        //wait for all existing events to be delivered
        assertSizeEventually(keyCount, valueCollector.getValues());

        subscription.cancel();

        //send a bunch of other events
        for (int i = 0; i < keyCount; i++) {
            streamer.send(Integer.toString(i));
        }

        //assert no new events are being pushed into the consumer - as the subscription is already cancelled
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(keyCount, valueCollector.getValues().size());
            }
        }, 10); //10 seconds


    }

    private static class StoringCollector<T> implements StreamConsumer<T> {
        private Set<T> valueSet = newSetFromMap(new ConcurrentHashMap<T, Boolean>());

        public Set<T> getValues() {
            return unmodifiableSet(valueSet);
        }

        @Override
        public void accept(int partition, long offset, T value) {
            valueSet.add(value);
        }
    }

    private Config createConfig(int partitionCount, String streamerName, int maxMemory) throws IOException {
        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(partitionCount));
        config.getStreamerConfig(streamerName).setMaxSizeInMemoryMB(maxMemory).setOverflowDir(folder.newFolder().getAbsolutePath());
        return config;
    }
}
