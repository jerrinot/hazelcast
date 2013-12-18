package com.hazelcast.concurrent.longmaxupdater;

import com.hazelcast.concurrent.longmaxupdater.proxy.LongMaxUpdaterProxy;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.*;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class LongMaxUpdaterService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:longMaxUpdaterService";

    private NodeEngine nodeEngine;
    private ConcurrentHashMap<String, LongMaxWrapper> numbers = new ConcurrentHashMap<String, LongMaxWrapper>();

    private final ConstructorFunction<String, LongMaxWrapper> longMaxConstructorFunction = new ConstructorFunction<String, LongMaxWrapper>() {
        public LongMaxWrapper createNew(String key) {
            return new LongMaxWrapper();
        }
    };

    public LongMaxUpdaterService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public LongMaxWrapper getNumber(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(numbers, name, longMaxConstructorFunction);
    }

    @Override
    public void reset() {
        numbers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new LongMaxUpdaterProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        numbers.remove(name);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        Map<String, Long> migrationData = new HashMap<String, Long>();
        for (Map.Entry<String, LongMaxWrapper> entry : numbers.entrySet()) {
            String name = entry.getKey();
            if (getPartitionForKey(name) == partitionId) {
                migrationData.put(name, entry.getValue().max());
            }
        }
        return migrationData.isEmpty() ? null : new ReplicationOperation(migrationData);

    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            removePartitionData(event.getPartitionId());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            removePartitionData(event.getPartitionId());
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        removePartitionData(partitionId);
    }

    private void removePartitionData(int partitionId) {
        Iterator<String> keyIterator = numbers.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (partitionId == getPartitionForKey(key)) {
                keyIterator.remove();
            }
        }
    }

    private int getPartitionForKey(String name) {
        return nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
    }
}
