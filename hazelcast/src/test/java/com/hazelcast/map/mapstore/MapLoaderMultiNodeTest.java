package com.hazelcast.map.mapstore;


import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static com.hazelcast.test.TimeConstants.MINUTE;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapLoaderMultiNodeTest extends HazelcastTestSupport {

    private static final int MAP_STORE_ENTRY_COUNT = 1000;
    private static final int NODE_COUNT = 2;
    private static final int MAX_SIZE_PER_NODE = MAP_STORE_ENTRY_COUNT / 4;

    private AtomicInteger loadedValueCount;
    private TestHazelcastInstanceFactory nodeFactory;

    @Before
    public void setUp() throws Exception {
        loadedValueCount = new AtomicInteger(0);
        nodeFactory = createHazelcastInstanceFactory(NODE_COUNT);
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsNothing_whenLazyAndValueInserted() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, InitialLoadMode.LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);
        map.put(1, 1);

        assertEquals(0, loadedValueCount.get());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsNothing_whenEvictionDisabledAndLazy() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, InitialLoadMode.LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertEquals(0, loadedValueCount.get());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsAll_whenEvictionDisabledAndLazy() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, InitialLoadMode.LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);
        assertEquals(0, loadedValueCount.get());

        assertEquals(MAP_STORE_ENTRY_COUNT, map.size());
        assertEquals(MAP_STORE_ENTRY_COUNT, loadedValueCount.get());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsAll_whenEvictionDisabledAndEager() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);
        assertEquals(MAP_STORE_ENTRY_COUNT, loadedValueCount.get());

        assertEquals(MAP_STORE_ENTRY_COUNT, map.size());
    }

    private IMap<Object, Object> getMap(final String mapName, Config cfg) {
        HazelcastInstance hz = nodeFactory.newInstances(cfg)[0];
        assertClusterSizeEventually(NODE_COUNT, hz);
        IMap<Object, Object> map = hz.getMap(mapName);
        waitClusterForSafeState(hz);
        return map;
    }

    private Config newConfig(String mapName, boolean sizeLimited, MapStoreConfig.InitialLoadMode loadMode) {
        Config cfg = new Config();
        cfg.setGroupConfig(new GroupConfig(getClass().getSimpleName()));
        //cfg.setProperty("hazelcast.partition.count", "5");

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(new CountingMapLoader(MAP_STORE_ENTRY_COUNT, loadedValueCount))
                .setInitialLoadMode(loadMode);

        MapConfig mapConfig = cfg.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        if (sizeLimited) {
            MaxSizeConfig maxSizeConfig = new MaxSizeConfig(MAX_SIZE_PER_NODE, MaxSizeConfig.MaxSizePolicy.PER_NODE);
            mapConfig.setMaxSizeConfig(maxSizeConfig);
            mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        }

        return cfg;
    }

    private static class CountingMapLoader extends SimpleMapLoader {

        private AtomicInteger loadedValueCount;

        CountingMapLoader(int size, AtomicInteger loadedValueCount) {
            super(size, false);
            this.loadedValueCount = loadedValueCount;
        }

        @Override
        public Map loadAll(Collection keys) {
            loadedValueCount.addAndGet(keys.size());
            return super.loadAll(keys);
        }
    }
}
