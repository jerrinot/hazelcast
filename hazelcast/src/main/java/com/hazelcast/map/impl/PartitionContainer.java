/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PartitionContainer {

    private final MapService mapService;

    private final int partitionId;

    private final ConcurrentMap<String, RecordStore> maps = new ConcurrentHashMap<String, RecordStore>(1000);

    private final ConstructorFunction<String, RecordStore> recordStoreConstructor
            = new ConstructorFunction<String, RecordStore>() {
        public RecordStore createNew(String name) {

            MapServiceContext serviceContext = mapService.getMapServiceContext();
            MapContainer mapContainer = serviceContext.getMapContainer(name);
            final PartitioningStrategy strategy = mapContainer.getPartitioningStrategy();
            NodeEngine nodeEngine = serviceContext.getNodeEngine();
            ClusterService cluster = nodeEngine.getClusterService();
            OperationService opService = nodeEngine.getOperationService();
            InternalPartitionService ps = nodeEngine.getPartitionService();
            final SerializationService ss = nodeEngine.getSerializationService();
            ExecutionService execService = nodeEngine.getExecutionService();
            MapConfig mapConfig = mapContainer.getMapConfig();
            MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();

            IFunction<Object, Data> serialize = new IFunction<Object, Data>() {
                @Override
                public Data apply(Object input) {
                    return ss.toData(input, strategy);
                }
            };

            int namePartition = ps.getPartitionId(name);
            boolean isLoader = (namePartition == partitionId) && serviceContext.getOwnedPartitions().contains(namePartition);

            KeyDispatcher dispatcher = new KeyDispatcher(name, opService, ps, serialize, execService, maxSizeConfig);

            DefaultRecordStore recordStore = new DefaultRecordStore(mapContainer, partitionId, isLoader, dispatcher);
            recordStore.startLoading();
            return recordStore;
        }
    };

    /**
     * Flag to check if there is a {@link com.hazelcast.map.impl.operation.ClearExpiredOperation}
     * is running on this partition at this moment or not.
     */
    private volatile boolean hasRunningCleanup;

    private volatile long lastCleanupTime;

    /**
     * Used when sorting partition containers in {@link com.hazelcast.map.impl.eviction.ExpirationManager}
     * A non-volatile copy of lastCleanupTime is used with two reasons.
     * <p/>
     * 1. We need an un-modified field during sorting.
     * 2. Decrease number of volatile reads.
     */
    private long lastCleanupTimeCopy;

    public PartitionContainer(final MapService mapService, final int partitionId) {
        this.mapService = mapService;
        this.partitionId = partitionId;
    }

    public ConcurrentMap<String, RecordStore> getMaps() {
        return maps;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public MapService getMapService() {
        return mapService;
    }

    public RecordStore getRecordStore(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(maps, name, this, recordStoreConstructor);
    }

    public RecordStore getExistingRecordStore(String mapName) {
        return maps.get(mapName);
    }

    void destroyMap(String name) {
        RecordStore recordStore = maps.remove(name);
        if (recordStore != null) {
            recordStore.clearPartition();
        } else {
            clearLockStore(name);
        }
    }

    private void clearLockStore(String name) {
        final NodeEngine nodeEngine = mapService.getMapServiceContext().getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            final DefaultObjectNamespace namespace = new DefaultObjectNamespace(MapService.SERVICE_NAME, name);
            lockService.clearLockStore(partitionId, namespace);
        }
    }

    void clear() {
        for (RecordStore recordStore : maps.values()) {
            recordStore.clearPartition();
        }
        maps.clear();
    }

    public boolean hasRunningCleanup() {
        return hasRunningCleanup;
    }

    public void setHasRunningCleanup(boolean hasRunningCleanup) {
        this.hasRunningCleanup = hasRunningCleanup;
    }

    public long getLastCleanupTime() {
        return lastCleanupTime;
    }

    public void setLastCleanupTime(long lastCleanupTime) {
        this.lastCleanupTime = lastCleanupTime;
    }

    public long getLastCleanupTimeCopy() {
        return lastCleanupTimeCopy;
    }

    public void setLastCleanupTimeCopy(long lastCleanupTimeCopy) {
        this.lastCleanupTimeCopy = lastCleanupTimeCopy;
    }
}
