package com.hazelcast.map.impl;

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IFunction;
import com.hazelcast.map.impl.operation.LoadAllOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;

import java.io.Closeable;
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
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.IterableUtil.limit;
import static com.hazelcast.util.IterableUtil.map;

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

    public Collection<Future> sendKeys(final Iterable<Object> keys, final boolean replaceExistingValues) {

        int maxSizePerNode = getApproximateMaxSize(maxSizeConfig, MaxSizePolicy.PER_NODE); //TODO
        int members = partitionService.getMemberPartitionsMap().size();
        final int maxSize = members * maxSizePerNode;

        Future<Collection<Future>> f = execService.submit(executorName, new Callable<Collection<Future>>() {
            @Override
            public Collection<Future> call() throws Exception {
                return sendKeysInBatches(keys, maxSize, replaceExistingValues);
            }
        });

        execService.asCompletableFuture(f).andThen(new ExecutionCallback<Collection<Future>>() {

            @Override
            public void onResponse(Collection<Future> response) {
                System.err.println("Response: " + response.size());
            }

            @Override
            public void onFailure(Throwable t) {
                t.printStackTrace();
            }
        });

        try {
            return f.get();
        } catch (Exception e) {
            return Collections.<Future>singleton(f);
        }
    }

    private Collection<Future> sendKeysInBatches(Iterable<Object> allKeys, int maxSize, boolean replaceExistingValues) {

        List<Future> futures = new ArrayList<Future>();
        Iterator<Object> keys = allKeys.iterator();
        Iterator<Data> dataKeys = map(keys, toData);
        if( maxSize > 0 )
            dataKeys = limit(dataKeys, maxSize);
        Iterator<Entry<Integer, Data>> partitionsAndKeys = map(dataKeys, toPartition());

        Iterator<Map<Integer, List<Data>>> batches = toBatches(partitionsAndKeys, maxBatch);

        while( batches.hasNext() ) {
            Map<Integer, List<Data>> batch = batches.next();
            futures.addAll( sendBatch(batch, replaceExistingValues) );
        }

        if (keys instanceof Closeable) {
            closeResource((Closeable) keys);
        }

        return futures;
    }

    public static Iterator<Map<Integer, List<Data>>> toBatches(
            final Iterator<Entry<Integer, Data>> entries, final int maxBatch) {

        return new Iterator<Map<Integer, List<Data>>>() {
            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public Map<Integer, List<Data>> next() {
                Map<Integer, List<Data>> batch = new HashMap<Integer, List<Data>>();
                while( entries.hasNext() ) {
                    Entry<Integer, Data> e = entries.next();
                    List<Data> partitionKeys = addToMap(batch, e.getKey(), e.getValue());

                    if( partitionKeys.size() >= maxBatch ) {
                        break;
                    }
                }
                return batch;
            }

            @Override
            public void remove() {
            }

        };
    }

    private IFunction<Data, Entry<Integer, Data>> toPartition() {
        return new IFunction<Data, Entry<Integer,Data>>() {
            @Override
            public Entry<Integer, Data> apply(Data input) {
                Integer partition = partitionService.getPartitionId(input);
                return new MapEntrySimple<Integer, Data> (partition, input);
            }
        };
    }

    static <K, V> List<V> addToMap(Map<K, List<V>> batch, K key, V value) {

        List<V> values = batch.get(key);
        if( values == null ) {
            values = new ArrayList<V>();
            batch.put(key, values);
        }
        values.add(value);

        return values;
    }

    private List<Future> sendBatch(Map<Integer, List<Data>> batch, boolean replaceExistingValues) {

        List<Future> futures = new ArrayList<Future>();

        for(Entry<Integer, List<Data>> e : batch.entrySet()) {
            int partitionId = e.getKey();
            List<Data> keys = e.getValue();
            LoadAllOperation op = new LoadAllOperation(mapName, keys, replaceExistingValues);
            InternalCompletableFuture<Object> fut = opService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
            futures.add(fut);
        }

        return futures;
    }

}
