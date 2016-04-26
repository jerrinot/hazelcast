package com.hazelcast.util;


import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public class MutexProvider {
    private Map<Object, Mutex> mutexMap = new HashMap<Object, Mutex>();
    private Object mainMutex = new Object();

    public synchronized Mutex getMutex(Object mutexKey) {
        Mutex mutex;
        synchronized (mainMutex) {
            mutex = mutexMap.get(mutexKey);
            if (mutex == null) {
                mutex = new Mutex();
                mutexMap.put(mutexKey, mutex);
            }
            mutex.referenceCount++;
        }
        return mutex;
    }

    public class Mutex implements Closeable {
        private int referenceCount;

        @Override
        public void close() {
            synchronized (mainMutex) {
                referenceCount--;
                if (referenceCount == 0) {
                    mutexMap.remove(this);
                }
            }
        }
    }
}


