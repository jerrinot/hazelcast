package com.hazelcast.test;

import java.lang.reflect.Proxy;

public class AssertEventuallySupport {

    public static <T> T on(T o) {
        Class<?> clazz = o.getClass();
        ClassLoader cl = clazz.getClassLoader();
        Class<?>[] interfaces = clazz.getInterfaces();
        //known issue: it works only if the class implements an interface.
        //this is just an implementation detail as we could use a different proxying mechanism.
        T p = (T) Proxy.newProxyInstance(cl, interfaces, new ExecutionRecordingHandler<T>(o));
        return p;
    }


}
