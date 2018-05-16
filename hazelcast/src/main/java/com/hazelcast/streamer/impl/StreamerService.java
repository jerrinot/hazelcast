package com.hazelcast.streamer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.streamer.impl.operations.StreamerMigrationOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

//todo:
// - implement consumer groups
// - do something about huge dataset migrations. fragmented service?
// - offload storing from memory into disk into non-op thread
// - integrate with Raft
// - what to do with lost partitions?
// - zero-copy polling?
public final class StreamerService implements ManagedService, RemoteService, MigrationAwareService {
    public static final String SERVICE_NAME = "streamer";
    private NodeEngine nodeEngine;
    private PartitionContainer[] partitionContainers;
    private InternalSerializationService serializationService;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.partitionContainers = createPartitionContainers();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
    }

    private PartitionContainer[] createPartitionContainers() {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        PartitionContainer[] containers = new PartitionContainer[partitionCount];
        Config config = nodeEngine.getConfig();
        for (int i = 0; i < partitionCount; i++) {
            containers[i] = new PartitionContainer(i, partitionCount, config);
        }
        return containers;
    }

    @Override
    public void reset() {
        clearEverything();
    }

    @Override
    public void shutdown(boolean terminate) {
        clearEverything();
    }

    private void clearEverything() {
        for (PartitionContainer container : partitionContainers) {
            container.clear();
        }
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new StreamerProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        clearEverything();
    }

    public void addValue(String name, int partitionId, Data data) {
        DummyStore store = getStore(name, partitionId);
        store.add(data);
    }

    private DummyStore getStore(String name, int partitionId) {
        PartitionContainer partitionContainer = partitionContainers[partitionId];
        DummyStore store = partitionContainer.getOrCreateStore(name);
        return store;
    }

    public void read(String name, int partitionId, long offset, InternalConsumer consumer) {
        DummyStore store = getStore(name, partitionId);
        store.read(offset, consumer);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        PartitionContainer container = partitionContainers[partitionId];
        Collection<String> allStoreNames = container.getAllStoreNames();
        if (!allStoreNames.iterator().hasNext()) {
            return null;
        }
        BufferObjectDataOutput bodo = serializationService.createObjectDataOutput();
        try {
            bodo.writeInt(partitionId);
            bodo.writeInt(allStoreNames.size());
            for (String storeName : allStoreNames) {
                DummyStore store = container.getOrCreateStore(storeName);
                bodo.writeUTF(store.getName());
                store.savePayload(bodo);
            }
        } catch (IOException e) {
            throw new IllegalStateException("cannot save streamer store", e);
        }

        return new StreamerMigrationOperation(bodo.toByteArray());
    }

    public void restoreStores(byte[] savedStores) {
        BufferObjectDataInput bodi = serializationService.createObjectDataInput(savedStores);
        try {
            int partitionId = bodi.readInt();
            int storeCount = bodi.readInt();
            PartitionContainer container = partitionContainers[partitionId];

            for (int i = 0; i < storeCount; i++) {
                String storeName = bodi.readUTF();
                DummyStore store = container.getOrCreateStore(storeName);
                store.dispose();
                store.restorePayload(bodi);
            }

        } catch (IOException e) {
            throw new IllegalStateException("cannot restore streamer store", e);
        }
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
}
