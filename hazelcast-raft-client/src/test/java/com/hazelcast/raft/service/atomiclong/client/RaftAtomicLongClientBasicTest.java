package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftAtomicLongConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftAtomicLongClientBasicTest extends HazelcastRaftTestSupport {

    private IAtomicLong atomicLong;
    private String name = "id";
    private int groupSize = 3;

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
        Address[] raftAddresses = createAddresses(groupSize + 2);
        newInstances(raftAddresses, groupSize, 2);

        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        HazelcastInstance client = f.newHazelcastClient();

        atomicLong = RaftAtomicLongProxy.create(client, name);
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    @Test
    public void testSet() {
        atomicLong.set(271);
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testGet() {
        assertEquals(0, atomicLong.get());
    }

    @Test
    public void testDecrementAndGet() {
        assertEquals(-1, atomicLong.decrementAndGet());
        assertEquals(-2, atomicLong.decrementAndGet());
    }

    @Test
    public void testIncrementAndGet() {
        assertEquals(1, atomicLong.incrementAndGet());
        assertEquals(2, atomicLong.incrementAndGet());
    }

    @Test
    public void testGetAndSet() {
        assertEquals(0, atomicLong.getAndSet(271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testAddAndGet() {
        assertEquals(271, atomicLong.addAndGet(271));
    }

    @Test
    public void testGetAndAdd() {
        assertEquals(0, atomicLong.getAndAdd(271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenSuccess() {
        assertTrue(atomicLong.compareAndSet(0, 271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenNotSuccess() {
        assertFalse(atomicLong.compareAndSet(172, 0));
        assertEquals(0, atomicLong.get());
    }

//    @Test
//    public void testAlter() {
//        atomicLong.set(2);
//
//        atomicLong.alter(new MultiplyByTwo());
//
//        assertEquals(4, atomicLong.get());
//    }
//
//    @Test
//    public void testAlterAndGet() {
//        atomicLong.set(2);
//
//        long result = atomicLong.alterAndGet(new MultiplyByTwo());
//
//        assertEquals(4, result);
//    }
//
//    @Test
//    public void testGetAndAlter() {
//        atomicLong.set(2);
//
//        long result = atomicLong.getAndAlter(new MultiplyByTwo());
//
//        assertEquals(2, result);
//        assertEquals(4, atomicLong.get());
//    }
//
//    @Test
//    public void testAlterAsync() throws ExecutionException, InterruptedException {
//        atomicLong.set(2);
//
//        ICompletableFuture<Void> f = atomicLong.alterAsync(new MultiplyByTwo());
//        f.get();
//
//        assertEquals(4, atomicLong.get());
//    }
//
//    @Test
//    public void testAlterAndGetAsync() throws ExecutionException, InterruptedException {
//        atomicLong.set(2);
//
//        ICompletableFuture<Long> f = atomicLong.alterAndGetAsync(new MultiplyByTwo());
//        long result = f.get();
//
//        assertEquals(4, result);
//    }
//
//    @Test
//    public void testGetAndAlterAsync() throws ExecutionException, InterruptedException {
//        atomicLong.set(2);
//
//        ICompletableFuture<Long> f = atomicLong.getAndAlterAsync(new MultiplyByTwo());
//        long result = f.get();
//
//        assertEquals(2, result);
//        assertEquals(4, atomicLong.get());
//    }
//
//    @Test
//    public void testApply() {
//        atomicLong.set(2);
//
//        long result = atomicLong.apply(new MultiplyByTwo());
//
//        assertEquals(4, result);
//        assertEquals(2, atomicLong.get());
//    }
//
//    @Test
//    public void testApplyAsync() throws ExecutionException, InterruptedException {
//        atomicLong.set(2);
//
//        Future<Long> f = atomicLong.applyAsync(new MultiplyByTwo());
//        long result = f.get();
//
//        assertEquals(4, result);
//        assertEquals(2, atomicLong.get());
//    }

    @Override
    protected Config createConfig(Address[] raftAddresses, int metadataGroupSize) {
        Config config = super.createConfig(raftAddresses, metadataGroupSize);

        RaftAtomicLongConfig atomicLongConfig = new RaftAtomicLongConfig(name, new RaftGroupConfig(name, groupSize));
        config.addRaftAtomicLongConfig(atomicLongConfig);
        return config;
    }

    public static class MultiplyByTwo implements IFunction<Long, Long> {

        @Override
        public Long apply(Long input) {
            return input * 2;
        }
    }
}