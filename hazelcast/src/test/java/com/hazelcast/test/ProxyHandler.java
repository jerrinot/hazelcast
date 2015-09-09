package com.hazelcast.test;

import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.function.Supplier;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

class ProxyHandler<T> implements InvocationHandler {
    private static final ThreadLocal<ProxyHandler> lastHandler = new ThreadLocal<ProxyHandler>();

    private T delegate;
    private Method method;
    private Object[] args;

    ProxyHandler(T delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        this.method = method;
        this.args = args;
        lastHandler.set(this);
        return get();
    }

    public static Object get() {
        ProxyHandler lastHandler = ProxyHandler.lastHandler.get();

        try {
            return lastHandler.method.invoke(lastHandler.delegate, lastHandler.args);
        } catch (IllegalAccessException e) {
            throw ExceptionUtil.rethrow(e);
        } catch (InvocationTargetException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

}
