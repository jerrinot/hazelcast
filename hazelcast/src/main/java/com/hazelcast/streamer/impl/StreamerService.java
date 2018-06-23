package com.hazelcast.streamer.impl;

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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public final class StreamerService implements ManagedService, RemoteService, MigrationAwareService {
    public static final String SERVICE_NAME = "streamer";
    private NodeEngine nodeEngine;
    private PartitionContainer[] partitionContainers;
    private InternalSerializationService serializationService;

    private static final AtomicInteger COUNTER = new AtomicInteger();

    //todo: move to configuration
    private File baseDir = new File("/tmp/streamers/test-dir-" + System.currentTimeMillis() + "-" + COUNTER.incrementAndGet());

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.partitionContainers = createPartitionContainers();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
    }

    private PartitionContainer[] createPartitionContainers() {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        PartitionContainer[] containers = new PartitionContainer[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            containers[i] = new PartitionContainer(i, baseDir);
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
    public DistributedObject createDistributedObject(String objectName) {
        return new StreamerProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        clearEverything();
    }

    public void addValue(String name, int partitionId, Object value) {
        DummyStore store = getStore(name, partitionId);
        Data data = serializationService.toData(value);
        store.add(data);
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

    public <T> int read(String name, int partitionId, long offset, int maxRecords, PollResult response) {
        DummyStore store = getStore(name, partitionId);
        return store.read(offset, maxRecords, response);
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
