package com.hazelcast.backports.queues;

import java.util.Queue;
import java.util.concurrent.locks.LockSupport;

class Alarm {
    private Queue queue;
    private Thread thread;
    private volatile boolean sleeping;

    Alarm(Queue queue, Thread thread) {
        this.queue = queue;
        this.thread = thread;
    }

    void sleep() {
        sleeping = true;
        LockSupport.park();
    }

    boolean wakeIfNotEmpty() {
        if (!sleeping) {
            return false;
        }

        if (!queue.isEmpty()) {
            LockSupport.unpark(thread);
            return true;
        }
        return false;
    }

}
