package com.hazelcast.concurrent.longmaxupdater;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILongMaxUpdater;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LongMaxUpdaterTest extends HazelcastTestSupport {

    @Test
    public void testMaxThenReset() {
        final HazelcastInstance instance = createHazelcastInstanceFactory(1).newInstances(new Config())[0];
        ILongMaxUpdater updater = instance.getLongMaxUpdater("updater");

        long maxValue = Long.MIN_VALUE;
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            long value = random.nextLong();
            maxValue = Math.max(maxValue, value);
            updater.update(value);
        }

        long max = updater.maxThenReset();
        assertEquals(maxValue, max);
        assertEquals(Long.MIN_VALUE, updater.max());
    }

    @Test
    public void testMutipleThreadLongMaxUpdater() throws Exception {
        final HazelcastInstance instance = createHazelcastInstanceFactory(3).newInstances(new Config())[0];

        final int noOfThreads = 5;
        final String updaterName = "updater";
        final long[] maxArray = new long[noOfThreads];
        final CountDownLatch countDownLatch = new CountDownLatch(noOfThreads);
        final Random random = new Random();
        for (int i =0; i < noOfThreads; i++) {
            final AtomicInteger threadId = new AtomicInteger(i);
            new Thread() {
                public void run() {
                    ILongMaxUpdater updater = instance.getLongMaxUpdater(updaterName);
                    for (int j = 0; j < 1000; j++) {
                        long value = random.nextLong();
                        updater.update(value);
                        if (value > maxArray[threadId.get()]) {
                            maxArray[threadId.get()] = value;
                        }
                    }
                    countDownLatch.countDown();
                }
            }.start();
        }
        countDownLatch.await(50, TimeUnit.SECONDS);

        long maxFromHZ = instance.getLongMaxUpdater(updaterName).max();
        long maxAsRecorded = findMax(maxArray);
        assertEquals(maxAsRecorded, maxFromHZ);
    }

    private long findMax(long[] maxArray) {
        long max = Long.MIN_VALUE;
        for (long value : maxArray) max = Math.max(max, value);
        return max;
    }

}
