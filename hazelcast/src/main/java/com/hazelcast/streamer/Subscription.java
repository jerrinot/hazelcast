package com.hazelcast.streamer;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.function.Consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Subscription<V> {
    private static final int MIN_BATCH_SIZE = 1;
    private static final int MAX_BATCH_SIZE = 10 * 1000;

    private static final int SUBSCRIPTION_REGISTRATION_TIMEOUT_MINUTES = 1;

    public static final Consumer<Throwable> LOGGING_ERROR_COLLECTOR = new Consumer<Throwable>() {
        @Override
        public void accept(Throwable throwable) {
            System.out.println(throwable);
        }
    };

    private final ReadResultSetExecutionCallback[] callbacks;

    public Subscription(EventJournalReader<EventJournalMapEvent<Integer, V>> reader, int[] partitions, SubscriptionMode mode, final StreamConsumer<V> consumer, Consumer<Throwable> errorCollector) {
        //todo: refactor this mess!
        List<Future<EventJournalInitialSubscriberState>> futures = new ArrayList<Future<EventJournalInitialSubscriberState>>();
        for (int i = 0; i < partitions.length; i++) {
            int partitionId = partitions[i];
            ICompletableFuture<EventJournalInitialSubscriberState> f = reader.subscribeToEventJournal(partitionId);
            futures.add(f);
        }
        List<EventJournalInitialSubscriberState> states = new ArrayList<EventJournalInitialSubscriberState>(FutureUtil.returnWithDeadline(futures, SUBSCRIPTION_REGISTRATION_TIMEOUT_MINUTES, TimeUnit.MINUTES));
        //todo: what if we get StaleSequenceException? Perhaps we should retry with a new sequence ID during initial registration
        this.callbacks = new ReadResultSetExecutionCallback[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            int partition = partitions[i];
            ReadResultSetExecutionCallback<V> callback = new ReadResultSetExecutionCallback<V>(consumer, errorCollector, partition, reader);
            this.callbacks[i] = callback;
            EventJournalInitialSubscriberState state = states.get(i);
            long initialSequence = mode == SubscriptionMode.FROM_NEWEST ? state.getNewestSequence() : state.getOldestSequence();
            ICompletableFuture<ReadResultSet<EventJournalMapEvent<Integer, V>>> f = reader.readFromEventJournal(initialSequence, MIN_BATCH_SIZE, MAX_BATCH_SIZE, partition, null, null);
            f.andThen(callback);
        }
    }

    public void cancel() {
        //todo: how to cancel in-flight (blocked?) operation?
        for (ReadResultSetExecutionCallback callback : callbacks) {
            callback.cancel();
        }
    }


    private static class ReadResultSetExecutionCallback<T> implements ExecutionCallback<ReadResultSet<EventJournalMapEvent<Integer, T>>> {
        private final StreamConsumer<T> consumer;
        private final int partition;
        private final EventJournalReader<EventJournalMapEvent<Integer, T>> reader;
        private final Consumer<Throwable> errorCollector;

        private volatile boolean cancelled;

        private ReadResultSetExecutionCallback(StreamConsumer<T> consumer, Consumer<Throwable> errorCollector, int partition, EventJournalReader<EventJournalMapEvent<Integer, T>> reader) {
            this.consumer = consumer;
            this.partition = partition;
            this.reader = reader;
            this.errorCollector = errorCollector;
        }

        @Override
        public void onResponse(ReadResultSet<EventJournalMapEvent<Integer, T>> response) {
            long nextSequence = response.getNextSequenceToReadFrom();
            if (cancelled) {
                return;
            }
            ICompletableFuture<ReadResultSet<EventJournalMapEvent<Integer, T>>> f = reader.readFromEventJournal(nextSequence, MIN_BATCH_SIZE, MAX_BATCH_SIZE, partition, null, null);
            f.andThen(this);

            int size = response.size();
            for (int i = 0; i < size && !cancelled; i++) {
                EventJournalMapEvent<Integer, T> event = response.get(i);
                T value = event.getNewValue();
                long sequence = response.getSequence(i);
                consumer.accept(partition, sequence, value);
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
