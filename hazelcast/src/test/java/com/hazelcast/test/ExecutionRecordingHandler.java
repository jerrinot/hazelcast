package com.hazelcast.test;

import com.hazelcast.util.ExceptionUtil;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

class ExecutionRecordingHandler<T> implements InvocationHandler {
    private static final ThreadLocal<ExecutionRecordingHandler> lastHandler = new ThreadLocal<ExecutionRecordingHandler>();

    private T delegate;
    private Method method;
    private Object[] args;

    ExecutionRecordingHandler(T delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (isReady()) {
            throw new IllegalStateException("There is already a call recorded!");
        }

        this.method = method;
        this.args = args;
        lastHandler.set(this);
        return get();
    }

    public static Object get() {
        ExecutionRecordingHandler lastHandler = ExecutionRecordingHandler.lastHandler.get();

        try {
            return lastHandler.method.invoke(lastHandler.delegate, lastHandler.args);
        } catch (IllegalAccessException e) {
            throw ExceptionUtil.rethrow(e);
        } catch (InvocationTargetException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static void reset() {
        ExecutionRecordingHandler.lastHandler.set(null);
    }

    public static boolean isReady() {
        return ExecutionRecordingHandler.lastHandler.get() != null;
    }

}
