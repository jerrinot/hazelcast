package com.hazelcast.map.impl;

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IFunction;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.operation.LoadAllOperation;
import com.hazelcast.map.impl.operation.PartitionCheckIfLoadedOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.collection.UnmodifiableIterator;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.eviction.MaxSizeChecker.getApproximateMaxSize;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.IterableUtil.limit;
import static com.hazelcast.util.IterableUtil.map;
import static java.lang.Boolean.TRUE;

public class KeyDispatcher {

    private int maxBatch = 1000; //TODO: take from group property
    static final String executorName = "hz:map:loader";

    private String mapName;
    private OperationService opService;
    private InternalPartitionService partitionService;
    private IFunction<Object, Data> toData;
    private ExecutionService execService;
    private MaxSizeConfig maxSizeConfig;

    private LoadFinishedFuture loadFinished;

    public KeyDispatcher(String mapName, OperationService opService, InternalPartitionService ps,
            IFunction<Object, Data> serialize, ExecutionService execService, MaxSizeConfig maxSizeConfig) {
        this.mapName = mapName;
        this.opService = opService;
        this.partitionService = ps;
        this.toData = serialize;
        this.execService = execService;
        this.maxSizeConfig = maxSizeConfig;
    }

    /**
     * Sends keys too all nodes in batches.
     */
    public Future<?> sendKeys(final MapStoreContext mapStoreContext, final boolean replaceExistingValues) {

        if( loadFinished == null || loadFinished.isDone() ) {
            loadFinished = new LoadFinishedFuture("sendKeys");

            execService.submit(executorName, new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    Iterable<Object> allKeys = mapStoreContext.loadAllKeys();
                    sendKeysInBatches(allKeys, replaceExistingValues, getMaxSize());
                    return null;
                }
            });
        }

        return loadFinished;
    }

    /**
     * Trigger key loading on loader partition
     */
    public Future triggerKeyLoad() {

        if( loadFinished == null || loadFinished.isDone() ) {
            loadFinished = new LoadFinishedFuture("trigger");

            final int partition = partitionService.getPartitionId( toData.apply(mapName) );

            execService.execute(SERVICE_NAME, new Runnable() {
                @Override
                public void run() {
                    Operation op = new PartitionCheckIfLoadedOperation(mapName);
                    System.err.println("Send trigger to: " + partition);
                    opService.<Boolean>invokeOnPartition(SERVICE_NAME, op, partition)
                        .andThen(new ExecutionCallback<Boolean>() {
                            @Override
                            public void onResponse(Boolean loaded) {
                                System.err.println("Trigger response: " + loaded);
                                if( loaded ) {
                                    loadFinished.setResult(loaded);
                                }
                            }
                            @Override
                            public void onFailure(Throwable t) {
                                loadFinished.setResult(t);
                            }
                        });
                }
            });
        }

        return loadFinished;
    }

    public void completeLoading() {
        loadFinished.setResult(TRUE);
    }

    private Collection<Future<Object>> sendKeysInBatches(Iterable<Object> allKeys, boolean replaceExistingValues, int maxSize) {

        List<Future<Object>> futures = new ArrayList<Future<Object>>();

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

        futures.addAll( sendLoadCompleted(partitionService.getPartitionCount(), replaceExistingValues) );

        if (keys instanceof Closeable) {
            closeResource((Closeable) keys);
        }

        return futures;
    }

    public static Iterator<Map<Integer, List<Data>>> toBatches(
            final Iterator<Entry<Integer, Data>> entries, final int maxBatch) {

        return new UnmodifiableIterator<Map<Integer, List<Data>>>() {
            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public Map<Integer, List<Data>> next() {
                return nextBatch(entries, maxBatch);
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

    static <K, V> List<V> addToValueList(Map<K, List<V>> map, K key, V value) {

        List<V> values = map.get(key);
        if( values == null ) {
            values = new ArrayList<V>();
            map.put(key, values);
        }
        values.add(value);

        return values;
    }

    private static Map<Integer, List<Data>> nextBatch(Iterator<Entry<Integer, Data>> entries, int maxBatch) {

        Map<Integer, List<Data>> batch = new HashMap<Integer, List<Data>>();

        while( entries.hasNext() ) {
            Entry<Integer, Data> e = entries.next();
            List<Data> partitionKeys = addToValueList(batch, e.getKey(), e.getValue());

            if( partitionKeys.size() >= maxBatch ) {
                break;
            }
        }

        return batch;
    }

    private List<Future<Object>> sendBatch(Map<Integer, List<Data>> batch, boolean replaceExistingValues) {

        List<Future<Object>> futures = new ArrayList<Future<Object>>();

        for(Entry<Integer, List<Data>> e : batch.entrySet()) {
            int partitionId = e.getKey();
            List<Data> keys = e.getValue();
            LoadAllOperation op = new LoadAllOperation(mapName, keys, replaceExistingValues, false);

            futures.add( opService.invokeOnPartition(SERVICE_NAME, op, partitionId) );
        }

        return futures;
    }

    private List<Future<Object>> sendLoadCompleted(int partitions, boolean replaceExistingValues) {

        List<Future<Object>> futures = new ArrayList<Future<Object>>();
        boolean lastBatch = true;

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            LoadAllOperation op = new LoadAllOperation(mapName, Collections.<Data>emptyList(), replaceExistingValues, lastBatch);
            futures.add( opService.invokeOnPartition(SERVICE_NAME, op, partitionId) );
        }

        return futures;
    }

    private int getMaxSize() {
        int maxSizePerNode = getApproximateMaxSize(maxSizeConfig, MaxSizePolicy.PER_NODE);
        int members = partitionService.getMemberPartitionsMap().size();
        int maxSize = members * maxSizePerNode;
        return maxSize;
    }

    static class LoadFinishedFuture extends AbstractCompletableFuture {

        private String label;

        protected LoadFinishedFuture() {
            super(null, null);
        }

        public LoadFinishedFuture(String label) {
            super(null, null);
            this.label = label;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public Object get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
            if (isDone())
                return getResult();
            throw new UnsupportedOperationException("Future is not done yet");
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + label + "}";
        }
    }
}
