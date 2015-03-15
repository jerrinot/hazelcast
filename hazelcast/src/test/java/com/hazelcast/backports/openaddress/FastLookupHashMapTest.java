package com.hazelcast.backports.openaddress;

import com.hazelcast.spi.ExecutionService;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FastLookupHashMapTest {

    private LockingOnMutationMap<Integer, String> map;

    @Before
    public void setUp() {
        map = new LockingOnMutationMap<Integer, String>(1);
    }

    @Test
    public void testPut() throws Exception {
        map.put(1, "val");
        assertEquals("val", map.get(1));
    }

    @Test
    public void testPut_replace() throws Exception {
        map.put(1, "foo");
        map.put(1, "bar");

        assertEquals("bar", map.get(1));
    }

    @Test
    public void testRemove() throws Exception {
        map.put(1, "val");
        map.remove(1);
        assertNull(map.get(1));
    }

    @Test
    public void testPut_concurrent() throws Exception {
        int start = 100;
        int size = 512;

        UpdateTask task = new UpdateTask(start, size, map);
        ExecutorService executionService = Executors.newFixedThreadPool(8);

        for (int i = 0; i < 8; i++) {
            executionService.submit(task);
        }

        executionService.shutdown();
        executionService.awaitTermination(1, TimeUnit.MINUTES);

        for (int i = start; i < size + start; i++) {
            String value = map.get(i);
            assertEquals("foo" + i, value);
        }
    }

    private static class UpdateTask implements Runnable {
        private final LockingOnMutationMap<Integer, String> map;
        private final int noOfElements;
        private final int start;

        public UpdateTask(int start, int noOfElements, LockingOnMutationMap<Integer, String> map) {
            this.map = map;
            this.start = start;
            this.noOfElements = noOfElements;
        }

        @Override
        public void run() {
            for (int i = start; i < noOfElements + start; i++) {
                map.put(i, "foo" + i);
            }
        }
    }
 }