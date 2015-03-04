package com.hazelcast.map.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.PutFromLoadAllOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ResponseHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for loading keys from configured map store.
 */
class BasicRecordStoreLoader implements RecordStoreLoader {

    private final AtomicBoolean loaded;

    private final ILogger logger;

    private final String name;

    private final MapServiceContext mapServiceContext;

    private final MapDataStore mapDataStore;

    private final RecordStore recordStore;

    private final int partitionId;

    public BasicRecordStoreLoader(RecordStore recordStore) {
        this.recordStore = recordStore;
        final MapContainer mapContainer = recordStore.getMapContainer();
        this.name = mapContainer.getName();
        this.mapServiceContext = mapContainer.getMapServiceContext();
        this.partitionId = recordStore.getPartitionId();
        this.mapDataStore = recordStore.getMapDataStore();
        this.logger = mapServiceContext.getNodeEngine().getLogger(getClass());
        this.loaded = new AtomicBoolean(false);
    }

    @Override
    public Future<?> loadValues(List<Data> keys, boolean replaceExistingValues) {
        final Runnable task = new GivenKeysLoaderTask(keys, replaceExistingValues);
        final String executorName = ExecutionService.MAP_LOAD_ALL_KEYS_EXECUTOR;
        return executeTask(executorName, task);
    }

    private Future<?> executeTask(String executorName, Runnable task) {
        return getExecutionService().submit(executorName, task);
    }

    private ExecutionService getExecutionService() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getExecutionService();
    }

    /**
     * Task for loading given keys.
     * This task is used to make load in an outer thread instead of partition thread.
     */
    private final class GivenKeysLoaderTask implements Runnable {

        private final List<Data> keys;
        private final boolean replaceExistingValues;

        private GivenKeysLoaderTask(List<Data> keys, boolean replaceExistingValues) {
            this.keys = keys;
            this.replaceExistingValues = replaceExistingValues;
        }

        @Override
        public void run() {
            loadValuesInternal(keys, replaceExistingValues);
        }
    }

    private void loadValuesInternal(List<Data> keys, boolean replaceExistingValues) {

        if (!replaceExistingValues) {
            removeExistingKeys(keys);
        }
        removeUnloadableKeys(keys);

        if (keys.isEmpty()) {
            loaded.set(true);
            return;
        }
        doBatchLoad(keys);
    }

    private void doBatchLoad(List<Data> keys) {
        final Queue<List<Data>> batchChunks = createBatchChunks(keys);
        final int size = batchChunks.size();
        final AtomicInteger finishedBatchCounter = new AtomicInteger(size);
        while (!batchChunks.isEmpty()) {
            final List<Data> chunk = batchChunks.poll();
            final List<Data> keyValueSequence = loadAndGet(chunk);
            if (keyValueSequence.isEmpty()) {
                if (finishedBatchCounter.decrementAndGet() == 0) {
                    loaded.set(true);
                }
                continue;
            }
            sendOperation(keyValueSequence, finishedBatchCounter);
        }
    }

    private Queue<List<Data>> createBatchChunks(List<Data> keys) {
        final Queue<List<Data>> chunks = new LinkedList<List<Data>>();
        final int loadBatchSize = getLoadBatchSize();
        int page = 0;
        List<Data> tmpKeys;
        while ((tmpKeys = getBatchChunk(keys, loadBatchSize, page++)) != null) {
            chunks.add(tmpKeys);
        }
        return chunks;
    }

    private List<Data> loadAndGet(List<Data> keys) {
        Map<Object, Object> entries = Collections.emptyMap();
        try {
            entries = mapDataStore.loadAll(keys);
        } catch (Throwable t) {
            logger.warning("Could not load keys from map store", t);
        }
        return getKeyValueSequence(entries);
    }

    private List<Data> getKeyValueSequence(Map<Object, Object> entries) {
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Data> keyValueSequence = new ArrayList<Data>(entries.size());
        for (final Map.Entry<Object, Object> entry : entries.entrySet()) {
            final Object key = entry.getKey();
            final Object value = entry.getValue();
            final Data dataKey = mapServiceContext.toData(key);
            final Data dataValue = mapServiceContext.toData(value);
            keyValueSequence.add(dataKey);
            keyValueSequence.add(dataValue);
        }
        return keyValueSequence;
    }

    /**
     * Used to partition the list to chunks.
     *
     * @param list        to be paged.
     * @param batchSize   batch operation size.
     * @param chunkNumber batch chunk number.
     * @return sub-list of list if any or null.
     */
    private List<Data> getBatchChunk(List<Data> list, int batchSize, int chunkNumber) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        final int start = chunkNumber * batchSize;
        final int end = Math.min(start + batchSize, list.size());
        if (start >= end) {
            return null;
        }
        return list.subList(start, end);
    }

    private void sendOperation(List<Data> keyValueSequence, AtomicInteger finishedBatchCounter) {
        OperationService operationService = mapServiceContext.getNodeEngine().getOperationService();
        final Operation operation = createOperation(keyValueSequence, finishedBatchCounter);
        operationService.executeOperation(operation);
    }

    private Operation createOperation(List<Data> keyValueSequence, final AtomicInteger finishedBatchCounter) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final Operation operation = new PutFromLoadAllOperation(name, keyValueSequence);
        operation.setNodeEngine(nodeEngine);
        operation.setResponseHandler(new ResponseHandler() {
            @Override
            public void sendResponse(Object obj) {
                if (finishedBatchCounter.decrementAndGet() == 0) {
                    loaded.set(true);
                }
            }

            public boolean isLocal() {
                return true;
            }
        });
        operation.setPartitionId(partitionId);
        OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
        operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
        operation.setServiceName(MapService.SERVICE_NAME);
        return operation;
    }

    private void removeExistingKeys(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        final Map<Data, Record> records = recordStore.getRecordMap();
        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data nextKey = iterator.next();
            if (records.containsKey(nextKey)) {
                iterator.remove();
            }
        }
    }

    private void removeUnloadableKeys(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data key = iterator.next();
            if (!mapDataStore.loadable(key)) {
                iterator.remove();
            }
        }
    }

    private int getLoadBatchSize() {
        return mapServiceContext.getNodeEngine().getGroupProperties().MAP_LOAD_CHUNK_SIZE.getInteger();
    }
}
