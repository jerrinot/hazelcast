package com.hazelcast.streamer.impl;

import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;

public final class PartitionContainer {
    private final ConstructorFunction<String, DummyStore> STORE_CONSTRUCTOR_FUNCTION = new ConstructorFunction<String, DummyStore>() {
        @Override
        public DummyStore createNew(String name) {
            return new DummyStore(name, partitionId, serializationService);
        }
    };

    private final ConcurrentHashMap<String, DummyStore> stores = new ConcurrentHashMap<String, DummyStore>();
    private final int partitionId;
    private final SerializationService serializationService;

    public PartitionContainer(int partitionId, SerializationService serializationService) {
        this.partitionId = partitionId;
        this.serializationService = serializationService;
    }

    public DummyStore getOrCreateStore(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(stores, name, STORE_CONSTRUCTOR_FUNCTION);
    }

    public Iterable<String> getAllStoreNames() {
        return stores.keySet();
    }

    public void addStore(DummyStore<?> store) {
        stores.put(store.getName(), store);
    }

    public void clear() {
        stores.clear();
    }
}
