package com.hazelcast.internal.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.util.FutureUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.util.Preconditions.checkNotEmpty;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;

public class AggregatedFuture<V> implements ICompletableFuture<Set<V>> {
    private final FutureUtil.ExceptionHandler exceptionHandler;

    private final AtomicInteger remainingResponses;
    private final CountDownLatch completedLatch;
    private final Set<V> results = newSetFromMap(new ConcurrentHashMap<V, Boolean>());
    private volatile RuntimeException exception;
    private final AtomicReference<List<CallbackNode>> callbacks = new AtomicReference<List<CallbackNode>>();

    public AggregatedFuture(Collection<ICompletableFuture<V>> futures, FutureUtil.ExceptionHandler exceptionHandler) {
        checkNotEmpty(futures, "Future collection cannot be empty");
        this.exceptionHandler = exceptionHandler;
        this.remainingResponses = new AtomicInteger(futures.size());
        this.completedLatch = new CountDownLatch(futures.size());
        callbacks.set(new ArrayList<CallbackNode>());

        ExecutionCallback<V> callback = new MyCallbackNode();
        for (ICompletableFuture<V> future : futures) {
            future.andThen(callback);
        }
    }

    @Override
    public void andThen(ExecutionCallback<Set<V>> callback) {
        andThenInternal(callback, null);
    }

    @Override
    public void andThen(final ExecutionCallback<Set<V>> callback, Executor executor) {
        checkNotNull(executor, "Executor cannot be null");
        andThenInternal(callback, null);
    }

    private void andThenInternal(final ExecutionCallback<Set<V>> callback, Executor executor) {
        for (;;) {
            List<CallbackNode> currentCallbacks = callbacks.get();
            if (currentCallbacks == null) {
                // OK, all futures were already completed. this implies:
                // 1. the registered callbacks were already executed -> we need to call this callback on our own
                // 2. result is already set - we can use it
                executeCallback(callback, executor);
                break;
            } else {
                // There is still a list of registered callbacks. Let's try to add the new callback.
                List<CallbackNode> newCallbacks = new ArrayList<CallbackNode>(currentCallbacks.size() + 1);
                newCallbacks.addAll(currentCallbacks);
                CallbackNode callbackNode = new CallbackNode();
                callbackNode.callback = callback;
                callbackNode.executor = executor;
                newCallbacks.add(callbackNode);
                if (callbacks.compareAndSet(currentCallbacks, newCallbacks)) {
                    // we managed to set the new list -> it's no longer our responsibility to execute the callback
                    // -> we are done
                    break;
                }
                // We failed to add a new callback to the callback list. it can mean 2 different things:
                // 1. someone else added another callback
                // 2. all futures are completed and callbacks registered previously were already completed
                // in both cases it's safe to re-run the whole registration procedure again
            }
        }
    }

    private void executeCallback(final ExecutionCallback<Set<V>> callback, Executor executor) {
        if (executor == null) {
            if (exception == null) {
                callback.onResponse(results);
            } else {
                callback.onFailure(exception);
            }
        } else {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    if (exception == null) {
                        callback.onResponse(results);
                    } else {
                        callback.onFailure(exception);
                    }
                }
            });
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("canceling is not implemented");
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return remainingResponses.get() == 0;
    }

    @Override
    public Set<V> get() throws InterruptedException, ExecutionException {
        completedLatch.await();
        return throwExceptionOrReturnResult();
    }

    @Override
    public Set<V> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!completedLatch.await(timeout, unit)) {
            throw new TimeoutException();
        }
        return throwExceptionOrReturnResult();
    }

    private Set<V> throwExceptionOrReturnResult() {
        if (exception == null) {
            return results;
        }
        throw exception;
    }

    private class MyCallbackNode implements ExecutionCallback<V> {
        @Override
        public void onResponse(V response) {
            results.add(response);
            onOneComplete();
        }

        @Override
        public void onFailure(Throwable t) {
            try {
                exceptionHandler.handleException(t);
            } catch (RuntimeException e) {
                exception = e;
            }
            onOneComplete();
        }

        private void onOneComplete() {
            if (remainingResponses.decrementAndGet() == 0) {
                onAllCompleted();
            }
            completedLatch.countDown();
        }

        private void onAllCompleted() {
            List<CallbackNode> callbacksSnapshot = callbacks.getAndSet(null);
            for (CallbackNode callbackNode : callbacksSnapshot) {
                Executor executor = callbackNode.executor;
                ExecutionCallback<Set<V>> callback = callbackNode.callback;
                executeCallback(callback, executor);
            }
        }
    }

    private class CallbackNode {
        private ExecutionCallback<Set<V>> callback;
        private Executor executor;
    }
}
