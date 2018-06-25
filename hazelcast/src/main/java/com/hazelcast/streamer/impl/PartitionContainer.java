package com.hazelcast.streamer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class PartitionContainer {
    private final ConstructorFunction<String, DummyStore> STORE_CONSTRUCTOR_FUNCTION = new ConstructorFunction<String, DummyStore>() {
        @Override
        public DummyStore createNew(String name) {
            return new DummyStore(name, partitionId, config.getStreamerConfig(name));
        }
    };

    private final ConcurrentMap<String, DummyStore> stores = new ConcurrentHashMap<String, DummyStore>();
    private final int partitionId;
    private Config config;

    public PartitionContainer(int partitionId, Config config) {
        this.partitionId = partitionId;
        this.config = config;
    }

    public DummyStore getOrCreateStore(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(stores, name, STORE_CONSTRUCTOR_FUNCTION);
    }

    public Collection<String> getAllStoreNames() {
        return stores.keySet();
    }

    public void addStore(DummyStore store) {
        stores.put(store.getName(), store);
    }

    public void clear() {
        for (DummyStore store : stores.values()) {
            store.dispose();
        }
        stores.clear();
    }
}
