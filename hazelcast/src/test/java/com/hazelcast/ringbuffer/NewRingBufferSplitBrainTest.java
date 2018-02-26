package com.hazelcast.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.SplitBrainTestSupport;

import static org.junit.Assert.assertEquals;

public class NewRingBufferSplitBrainTest extends SplitBrainTestSupport {
    private static final String RING_BUFFER_NAME_A = randomName();
//    private static final String RING_BUFFER_NAME_B = randomName();
//    private static final String RING_BUFFER_NAME_C = randomName();
    private static final int CAPACITY = 100;
    private static final int BUFFER_COUNT = 1;

    private boolean firstIteration = true;

    @Override
    protected int[] brains() {
        return new int[]{2, 3};
    }

    @Override
    protected Config config() {
        Config config = super.config();
        configureRingBuffer(config, RING_BUFFER_NAME_A);
//        configureRingBuffer(config, RING_BUFFER_NAME_B);
//        configureRingBuffer(config, RING_BUFFER_NAME_C);

        return config;
    }

    @Override
    protected int iterations() {
        return 100;
    }

    private void configureRingBuffer(Config config, String name) {
        config.getRingbufferConfig(name + "*")
                .setCapacity(CAPACITY)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setBackupCount(4);
    }

    private void fillRingBuffer(HazelcastInstance instance, String baseName) {
        if (!firstIteration) {
            return;
        }
        for (int a = 0; a < BUFFER_COUNT; a++) {
            String name = baseName + a;
            Ringbuffer<Integer> rb = instance.getRingbuffer(name);

            for (int i = 0; i < CAPACITY; i++) {
                rb.add(i);
            }
        }
    }


    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        fillRingBuffer(instances[0], RING_BUFFER_NAME_A);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) throws Exception {
//        fillRingBuffer(firstBrain[0], RING_BUFFER_NAME_B);
//        fillRingBuffer(secondBrain[0], RING_BUFFER_NAME_C);
        Thread.sleep(1000);
        for (HazelcastInstance hz : firstBrain) {
            waitClusterForSafeState(hz);
        }
        for (HazelcastInstance hz : secondBrain) {
            waitClusterForSafeState(hz);
        }
        Thread.sleep(1000);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        Thread.sleep(1000);
        for (HazelcastInstance hz : instances) {
            waitClusterForSafeState(hz);
        }
        for (HazelcastInstance instance : instances) {
            checkRingBuffer(instance, RING_BUFFER_NAME_A);
//            checkRingBuffer(instance, RING_BUFFER_NAME_B);
//            checkRingBuffer(instance, RING_BUFFER_NAME_C);
        }

        firstIteration = false;
//        clear(instances[0], RING_BUFFER_NAME_A);
//        clear(instances[0], RING_BUFFER_NAME_B);
//        clear(instances[0], RING_BUFFER_NAME_C);
        Thread.sleep(10000);
    }

    private void clear(HazelcastInstance instance, String baseName) {
        for (int i = 0; i < BUFFER_COUNT; i++) {
            String name = baseName + i;
            instance.getRingbuffer(name).destroy();
        }
    }

    private void checkRingBuffer(HazelcastInstance instance, String baseName) {
        for (int a = 0; a < BUFFER_COUNT; a++) {
            String name = baseName + a;
            Ringbuffer<Integer> rb = instance.getRingbuffer(name);

            long size = rb.size();
            assertEquals(CAPACITY, size);

            for (int i = 0; i < CAPACITY; i++) {
                try {
                    int item = rb.readOne(i);
                    assertEquals(i, item);
                } catch (InterruptedException e) {
                    throw new AssertionError();
                }
            }
        }
    }
}
