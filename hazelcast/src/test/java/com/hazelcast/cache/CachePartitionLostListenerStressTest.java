package com.hazelcast.cache;

import com.hazelcast.cache.CachePartitionLostListenerTest.EventCollectingCachePartitionLostListener;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(SlowTest.class)
public class CachePartitionLostListenerStressTest extends AbstractPartitionLostListenerTest {



    @Parameterized.Parameters(name = "numberOfNodesToCrash:{0},withData:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{{1, true}, {1, false}, {2, true}, {2, false}, {3, true}, {3, false}});
    }

    protected int getNodeCount() {
        return 5;
    }

    protected int getCacheEntryCount() {
        return 10000;
    }

    @Parameterized.Parameter(0)
    public int numberOfNodesToCrash;

    @Parameterized.Parameter(1)
    public boolean withData;

    @Test
    public void testCachePartitionLostListener() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        HazelcastInstance instance = survivingInstances.get(0);
        HazelcastServerCachingProvider cachingProvider = createCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        List<EventCollectingCachePartitionLostListener> listeners = registerListeners(cacheManager);

        if (withData) {
            for (int i = 0; i < getNodeCount(); i++) {
                Cache<Integer, Integer> cache = cacheManager.getCache(getIthCacheName(i));
                for (int j = 0; j < getCacheEntryCount(); j++) {
                    cache.put(j, j);
                }
            }
        }

        String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;
        Map<Integer, Integer> survivingPartitions = getMinReplicaIndicesByPartitionId(survivingInstances);

        terminateInstances(terminatingInstances);
        waitAllForSafeState(survivingInstances);

        for (int i = 0; i < getNodeCount(); i++) {
            assertListenerInvocationsEventually(log, i, numberOfNodesToCrash, listeners.get(i), survivingPartitions);
        }

        for (int i = 0; i < getNodeCount(); i++) {
            cacheManager.destroyCache(getIthCacheName(i));
        }
        cacheManager.close();
        cachingProvider.close();
    }

    private List<EventCollectingCachePartitionLostListener> registerListeners(CacheManager cacheManager) {
        CacheConfig<Integer, Integer> config = new CacheConfig<Integer, Integer>();
        List<EventCollectingCachePartitionLostListener> listeners = new ArrayList<EventCollectingCachePartitionLostListener>();
        for (int i = 0; i < getNodeCount(); i++) {
            EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(i);
            listeners.add(listener);
            config.setBackupCount(i);
            Cache<Integer, Integer> cache = cacheManager.createCache(getIthCacheName(i), config);
            ICache iCache = cache.unwrap(ICache.class);
            iCache.addPartitionLostListener(listener);
        }
        return listeners;
    }

    private void assertListenerInvocationsEventually(final String log, final int index, final int numberOfNodesToCrash,
                                                     final EventCollectingCachePartitionLostListener listener,
                                                     final Map<Integer, Integer> survivingPartitions) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                if (index < numberOfNodesToCrash) {
                    assertLostPartitions(log, listener, survivingPartitions);
                } else {
                    String message = log + " listener-" + index + " should not be invoked!";
                    assertTrue(message, listener.getEvents().isEmpty());
                }
            }
        });
    }

    private void assertLostPartitions(String log, EventCollectingCachePartitionLostListener listener,
                                      Map<Integer, Integer> survivingPartitions) {
        List<CachePartitionLostEvent> events = listener.getEvents();
        assertFalse(survivingPartitions.isEmpty());

        for (CachePartitionLostEvent event : events) {
            int failedPartitionId = event.getPartitionId();
            Integer survivingReplicaIndex = survivingPartitions.get(failedPartitionId);
            if (survivingReplicaIndex != null) {
                String message = log + ", PartitionId: " + failedPartitionId
                        + " SurvivingReplicaIndex: " + survivingReplicaIndex + " Event: " + event.toString();
                assertTrue(message, survivingReplicaIndex > listener.getBackupCount());
            }
        }
    }
}
