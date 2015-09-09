package com.hazelcast.test;

import java.lang.reflect.Proxy;

public class ProxyFactory {

    public static <T> T on(T o) {
        Class<?> clazz = o.getClass();
        ClassLoader cl = clazz.getClassLoader();
        Class<?>[] interfaces = clazz.getInterfaces();
        T p = (T) Proxy.newProxyInstance(cl, interfaces, new ProxyHandler<T>(o));
        return p;
    }


}
