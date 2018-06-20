package com.hazelcast.streamer.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.streamer.impl.PartitionContainer;
import com.hazelcast.streamer.impl.DummyStore;
import com.hazelcast.streamer.impl.PollResult;
import com.hazelcast.streamer.impl.StreamerProxy;
import com.hazelcast.streamer.impl.operations.StreamerMigrationOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class StreamerService implements ManagedService, RemoteService, MigrationAwareService {
    public static final String SERVICE_NAME = "streamer";
    private NodeEngine nodeEngine;
    private PartitionContainer[] partitionContainers;


    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.partitionContainers = createPartitionContainers();
    }

    private PartitionContainer[] createPartitionContainers() {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        PartitionContainer[] containers = new PartitionContainer[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            containers[i] = new PartitionContainer(i);
        }
        return containers;
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new StreamerProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {

    }

    public void addValue(String name, int partitionId, Object value) {
        DummyStore store = getStore(name, partitionId);
        store.add(value);
    }

    private DummyStore getStore(String name, int partitionId) {
        PartitionContainer partitionContainer = partitionContainers[partitionId];
        DummyStore store = partitionContainer.getOrCreateStore(name);
        return store;
    }


    public boolean shouldWait(String name, int partitionId, long offset, int minRecords) {
        DummyStore store = getStore(name, partitionId);
        return !store.hasEnoughRecordsToRead(offset, minRecords);
    }

    public <T> int read(String name, int partitionId, long offset, int maxRecords, PollResult<T> response) {
        DummyStore store = getStore(name, partitionId);
        return store.read(offset, maxRecords, response);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        PartitionContainer container = partitionContainers[partitionId];
        Iterable<String> allStoreNames = container.getAllStoreNames();
        if (!allStoreNames.iterator().hasNext()) {
            return null;
        }
        List<DummyStore<?>> stores = new ArrayList<DummyStore<?>>();
        for (String storeName : allStoreNames) {
            DummyStore<?> store = container.getOrCreateStore(storeName);
            stores.add(store);

        }
        return new StreamerMigrationOperation(stores);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            int thresholdReplicaIndex = event.getNewReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > 1) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            int thresholdReplicaIndex = event.getCurrentReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > 1) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }

    private void clearPartitionReplica(int partitionId) {
        PartitionContainer partitionContainer = partitionContainers[partitionId];
        partitionContainer.clear();
    }

    public void addStores(List<DummyStore<?>> stores) {
        for (DummyStore<?> store : stores) {
            int partitionId = store.getPartitionId();
            PartitionContainer container = partitionContainers[partitionId];
            container.addStore(store);
        }
    }
}
