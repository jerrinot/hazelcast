package com.hazelcast.streamer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.streamer.Subscription.LOGGING_ERROR_COLLECTOR;
import static com.hazelcast.streamer.SubscriptionMode.FROM_OLDEST;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelTest.class})
public class SlowStreamerTest extends StreamerTestSupport {
    private static final long TEST_TIMEOUT_MS = 2 * 60 * 60 * 1000; //2 hours

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testStoreEvents() throws Exception {
        long startNanos = System.nanoTime();

        String streamerName = randomName();
        long keyCount = 5000000000L;
        int partitionCount = 271;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance i1 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
        HazelcastInstance i2 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));

        Streamer<String> streamer = i1.getStreamer(streamerName);

        final CountingCollector<String> valueCollector = new CountingCollector<String>();
        streamer.subscribeAllPartitions(FROM_OLDEST, valueCollector, LOGGING_ERROR_COLLECTOR);

        for (long i = 0; i < keyCount; i++) {
            streamer.send(Long.toString(i));
            if (i % 1000000 == 0) {
                System.out.println("Send " + i + " values so far");
            }
        }
//        HazelcastInstance i3 = factory.newHazelcastInstance(createConfig(partitionCount, streamerName, DEFAULT_IN_MEMORY_SIZE_MB));
//        for (long i = keyCount; i < keyCount * 2; i++) {
//            streamer.send(Long.toString(i));
//            if (i % 100000 == 0) {
//                System.out.println("Send " + i + " values so far");
//            }
//        }

        assertSizeEventually(keyCount, valueCollector);

        long totalSecond = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
        long txPerSecond = keyCount / totalSecond;
        System.out.println("Duration: " + totalSecond + " seconds. That's " + txPerSecond + " tx/s");
    }
}
