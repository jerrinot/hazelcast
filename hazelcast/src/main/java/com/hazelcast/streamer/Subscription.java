package com.hazelcast.streamer;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.streamer.impl.PollResult;
import com.hazelcast.streamer.impl.StreamerService;
import com.hazelcast.streamer.impl.operations.PollOperation;
import com.hazelcast.util.function.Consumer;

import java.util.List;

public final class Subscription<T> {
    private static final int MIN_BATCH_SIZE = 1;
    private static final int MAX_BATCH_SIZE = 10 * 1000;

    public static final Consumer<Throwable> LOGGING_ERROR_COLLECTOR = new Consumer<Throwable>() {
        @Override
        public void accept(Throwable throwable) {
            System.out.println(throwable);
        }
    };

    private final Callback[] callbacks;


    public Subscription(NodeEngine nodeEngine, int[] partitions, SubscriptionMode mode, final StreamConsumer<T> consumer, Consumer<Throwable> errorCollector, String name) {
        //todo: refactor this mess!
        //todo: what if we get StaleSequenceException? Perhaps we should retry with a new sequence ID during initial registration
        this.callbacks = new Callback[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            int partition = partitions[i];
            OperationService operationService = nodeEngine.getOperationService();
            SerializationService serializationService = nodeEngine.getSerializationService();
            Callback<T> callback = new Callback<T>(consumer, errorCollector, partition, operationService, serializationService, name);
            this.callbacks[i] = callback;

            //todo: implement initial sequence fetch
            long initialOffset = 0;

            Operation pollOperation = new PollOperation(name, initialOffset, MIN_BATCH_SIZE, MAX_BATCH_SIZE);
            InternalCompletableFuture<PollResult> f = operationService.createInvocationBuilder(StreamerService.SERVICE_NAME, pollOperation, partition)
                    .invoke();
            f.andThen(callback);
        }
    }

    public void cancel() {
        for (Callback<?> callback : callbacks) {
            callback.cancel();
        }
    }

    private static class Callback<T> implements ExecutionCallback<PollResult> {
        private final StreamConsumer<T> consumer;
        private final int partition;
        private final Consumer<Throwable> errorCollector;
        private final OperationService operationService;
        private final String name;
        private final SerializationService serializationService;

        private volatile boolean cancelled;

        private Callback(StreamConsumer<T> consumer, Consumer<Throwable> errorCollector, int partition,
                         OperationService operationService, SerializationService serializationService, String name) {
            this.consumer = consumer;
            this.partition = partition;
            this.operationService = operationService;
            this.errorCollector = errorCollector;
            this.name = name;
            this.serializationService = serializationService;
        }

        @Override
        public void onResponse(PollResult response) {
            long nextSequence = response.getNextSequence();
            if (cancelled) {
                return;
            }
            PollOperation operation = new PollOperation(name, nextSequence, MIN_BATCH_SIZE, MAX_BATCH_SIZE);
            InternalCompletableFuture<PollResult> f = operationService.createInvocationBuilder(StreamerService.SERVICE_NAME, operation, partition)
                    .invoke();

            f.andThen(this);

            List<JournalValue<Data>> results = response.getResults();
            int size = results.size();
            for (int i = 0; i < size && !cancelled; i++) {
                JournalValue<Data> binaryValue = results.get(i);
                T deserialized = serializationService.toObject(binaryValue.getValue());
                consumer.accept(partition, binaryValue.getOffset(), deserialized);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (!(t instanceof HazelcastInstanceNotActiveException) && !cancelled) {
                errorCollector.accept(t);
            }
        }

        public void cancel() {
            this.cancelled = true;
        }
    }
}
