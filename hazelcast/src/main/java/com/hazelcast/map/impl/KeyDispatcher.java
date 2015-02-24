package com.hazelcast.map.impl;

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.core.IFunction;
import com.hazelcast.map.impl.operation.LoadAllOperation;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.eviction.MaxSizeChecker.getApproximateMaxSize;
import static com.hazelcast.util.IterableUtil.limit;
import static com.hazelcast.util.IterableUtil.map;

import com.sun.xml.internal.ws.Closeable;

public class KeyDispatcher {

    private int maxBatch = 1000; //TODO: property
    static final String executorName = "hz:map:keyDispatcher";

    private String mapName;
    private OperationService opService;
    private InternalPartitionService partitionService;
    private IFunction<Object, Data> toData;
    private ExecutionService execService;
    private MaxSizeConfig maxSizeConfig;

    public KeyDispatcher(String mapName, OperationService opService, InternalPartitionService ps,
            IFunction<Object, Data> serialize, ExecutionService execService, MaxSizeConfig maxSizeConfig) {
        this.mapName = mapName;
        this.opService = opService;
        this.partitionService = ps;
        this.toData = serialize;
        this.execService = execService;
        this.maxSizeConfig = maxSizeConfig;
    }

    public Collection<Future> sendKeys(final Iterable<Object> keys) {

        int maxSizePerNode = getApproximateMaxSize(maxSizeConfig, MaxSizePolicy.PER_NODE); //TODO
        int members = partitionService.getMemberPartitionsMap().size();
        final int maxSize = members * maxSizePerNode;

        Future<Collection<Future>> f = execService.submit(executorName, new Callable<Collection<Future>>() {
            @Override
            public Collection<Future> call() throws Exception {
                return sendKeysInBatches(keys, maxSize);
            }
        });

        try {
            return f.get();
        } catch (Exception e) {
            return Collections.<Future>singleton(f);
        }
    }

    private Collection<Future> sendKeysInBatches(Iterable<Object> allKeys, int maxSize) {
        List<Future> futures = new ArrayList<Future>();
        Map<Integer, List<Data>> batch = new HashMap<Integer, List<Data>>();

        Iterator<Object> keys = allKeys.iterator();
        Iterator<Data> dataKeys = limit(map(keys, toData), maxSize);

        while( dataKeys.hasNext() ) {
            Data dataKey = dataKeys.next();
            int part = partitionService.getPartitionId(dataKey);

            List<Data> partitionKeys = addToBatch(batch, part, dataKey);

            if( partitionKeys.size() >= maxBatch ) {
                futures.addAll( sendBatch(batch) );
                batch = new HashMap<Integer, List<Data>>();
            }
        }

        futures.addAll( sendBatch(batch) );

        if (keys instanceof Closeable) {
            IOUtil.closeResource((Closeable) keys);
        }

        return futures;
    }

    private List<Data> addToBatch(Map<Integer, List<Data>> batch, int part, Data dataKey) {

        List<Data> partitionKeys = batch.get(part);
        if( partitionKeys == null ) {
            partitionKeys = new ArrayList<Data>();
            batch.put(part, partitionKeys);
        }
        partitionKeys.add(dataKey);

        return partitionKeys;
    }

    private List<Future> sendBatch(Map<Integer, List<Data>> batch) {

        List<Future> futures = new ArrayList<Future>();

        for(Entry<Integer, List<Data>> e : batch.entrySet()) {
            int partitionId = e.getKey();
            List<Data> keys = e.getValue();
            LoadAllOperation op = new LoadAllOperation(mapName, keys, true);
            InternalCompletableFuture<Object> fut = opService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
            futures.add(fut);
        }

        return futures;
    }

}
