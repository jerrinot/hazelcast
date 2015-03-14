package com.hazelcast.backports.queues;

import com.hazelcast.nio.UnsafeHelper;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class WatchDog extends Thread {
    private volatile Object[] object = new Object[0];
    private static final CopyOnWriteArrayList<Alarm> cowal = new CopyOnWriteArrayList();

    public void add(Alarm alarm) {
        cowal.add(alarm);
    }

    @Override
    public void run() {
        for (;;) {
            int size = cowal.size();
            for (int i = 0; i < size; i++) {
                Alarm alarm = cowal.get(i);
                alarm.wakeIfNotEmpty();
            }
        }
    }
}
