package com.hazelcast.map.mapstore;


import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

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

    private static final int MAP_STORE_ENTRY_COUNT = 10000;
    private static final int NODE_COUNT = 2;

    private TestHazelcastInstanceFactory nodeFactory;
    private CountingMapLoader mapLoader;

    @Before
    public void setUp() throws Exception {
        nodeFactory = createHazelcastInstanceFactory(NODE_COUNT + 1);
        mapLoader = new CountingMapLoader(MAP_STORE_ENTRY_COUNT);
    }

    @Test(timeout = MINUTE)
    public void testLoadsNothing_whenMapCreated() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, InitialLoadMode.LAZY);

        getMap(mapName, cfg);

        assertEquals(0, mapLoader.getLoadedValueCount());
    }

    @Test(timeout = MINUTE)
    public void testLoadsMap_whenLazyAndValueInserted() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, InitialLoadMode.LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);
        map.put(1, 1);

        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    @Test(timeout = MINUTE)
    public void testLoadsAll_whenMapLazyAndCheckingSize() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, InitialLoadMode.LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertEquals(MAP_STORE_ENTRY_COUNT, map.size());
        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    @Test(timeout = MINUTE)
    public void testLoadsAll_whenMapCreatedInEager() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
        assertEquals(MAP_STORE_ENTRY_COUNT, map.size());
    }

    @Test(timeout = MINUTE)
    public void testDoesNotLoad_whenLoadedAndNodeAdded() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);
        nodeFactory.newHazelcastInstance(cfg);

        assertEquals(1, mapLoader.getLoadAllKeysCount());
        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
        assertEquals(MAP_STORE_ENTRY_COUNT, map.size());
    }

    @Test(timeout = MINUTE)
    public void testLoads_whenLoaderNodeRemoved() throws Exception {
        Config cfg = newConfig("default", false, InitialLoadMode.LAZY);
        HazelcastInstance[] nodes = nodeFactory.newInstances(cfg, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        map.size();
        hz3.shutdown();
        waitAllForSafeState(nodeFactory.getAllHazelcastInstances());

        assertEquals(MAP_STORE_ENTRY_COUNT, map.size());
        assertEquals(1, mapLoader.getLoadAllKeysCount());
        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    @Test(timeout = MINUTE)
    public void testLoadsAll_whenLoaderNodeRemoved() throws Exception {
        Config cfg = newConfig("default", false, InitialLoadMode.LAZY);
        HazelcastInstance[] nodes = nodeFactory.newInstances(cfg, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        map.size();
        hz3.shutdown();
        waitAllForSafeState(nodeFactory.getAllHazelcastInstances());

        map.loadAll(true);

        assertEquals(MAP_STORE_ENTRY_COUNT, map.size());
        assertEquals(2, mapLoader.getLoadAllKeysCount());
        assertEquals(2*MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    @Test(timeout = MINUTE)
    public void testLoadsAll_whenLazyModeAndLoadAll() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, InitialLoadMode.LAZY);

        IMap<Object, Object> map = getMap(mapName, cfg);
        map.loadAll(true);

        assertEquals(1, mapLoader.getLoadAllKeysCount());
        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
        assertEquals(MAP_STORE_ENTRY_COUNT, map.size());
    }

    private IMap<Object, Object> getMap(final String mapName, Config cfg) {
        HazelcastInstance hz = nodeFactory.newInstances(cfg, NODE_COUNT)[0];
        assertClusterSizeEventually(NODE_COUNT, hz);
        IMap<Object, Object> map = hz.getMap(mapName);
        waitClusterForSafeState(hz);
        return map;
    }

    private Config newConfig(String mapName, boolean sizeLimited, MapStoreConfig.InitialLoadMode loadMode) {
        return newConfig(mapName, sizeLimited, loadMode, 1);
    }

    private Config newConfig(String mapName, boolean sizeLimited, MapStoreConfig.InitialLoadMode loadMode, int backups) {
        Config cfg = new Config();
        cfg.setGroupConfig(new GroupConfig(getClass().getSimpleName()));
        //cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "5");

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(mapLoader)
                .setInitialLoadMode(loadMode);

        cfg.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig).setBackupCount(backups);

        return cfg;
    }

}
