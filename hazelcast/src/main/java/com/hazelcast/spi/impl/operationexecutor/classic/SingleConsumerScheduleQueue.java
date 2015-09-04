package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.util.FastQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class SingleConsumerScheduleQueue implements ScheduleQueue {
    static final Object TRIGGER_TASK = new Object() {
        public String toString() {
            return "triggerTask";
        }
    };

    private final FastQueue normalQueue;
    private final ConcurrentLinkedQueue priorityQueue;

    private Object[] normalQueueCache = new Object[0];
    private int normalQueueCachePosition;

    public SingleConsumerScheduleQueue(Thread consumer) {
        this.normalQueue = new FastQueue<Object>(consumer);
        this.priorityQueue = new ConcurrentLinkedQueue();
    }


    @Override
    public void add(Object task) {
        checkNotNull(task, "task can't be null");

        normalQueue.add(task);
    }

    @Override
    public void addUrgent(Object task) {
        checkNotNull(task, "task can't be null");

        priorityQueue.add(task);
        normalQueue.add(TRIGGER_TASK);
    }

    @Override
    public int normalSize() {
        return normalQueue.size();
    }

    @Override
    public int prioritySize() {
        return priorityQueue.size();
    }

    @Override
    public int size() {
        return normalQueue.size() + priorityQueue.size();
    }

    @Override
    public Object take() throws InterruptedException {
        ConcurrentLinkedQueue priorityQueue = this.priorityQueue;
        for (; ; ) {
            Object priorityItem = priorityQueue.poll();
            if (priorityItem != null) {
                return priorityItem;
            }

            if (normalQueueCachePosition == normalQueueCache.length) {
                resetCache();
            }

            Object normalItem = normalQueueCache[normalQueueCachePosition];
            if (normalItem == null) {
                resetCache();
                continue;
            }
            normalQueueCachePosition++;

            if (normalItem == TRIGGER_TASK) {
                continue;
            }
            if (normalItem == null) {
                System.out.println("null");
            }

            return normalItem;
        }
    }

    private void resetCache() throws InterruptedException {
        normalQueueCache = normalQueue.takeAll();
        normalQueueCachePosition = 0;
    }
}
