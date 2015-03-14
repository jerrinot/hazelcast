package com.hazelcast.backports.queues;

import org.jctools.queues.MpscArrayQueue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueFactory {
    private static WatchDog watchDog;

    public static BlockingQueue mpscBlocking() {
        return mpscBlocking(10000);
    }

    public static BlockingQueue mpscBlocking(int capacity) {
        if (watchDog == null) {
            watchDog = new WatchDog();
            watchDog.setDaemon(true);
            watchDog.start();
        }

        return new MyBlocking(new MpscArrayQueue(capacity));
    }

    private static class MyBlocking implements BlockingQueue {
        private Queue delegate;
        private Alarm alarm;

        public MyBlocking(Queue delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean add(Object o) {
            return delegate.add(o);
        }

        @Override
        public boolean offer(Object o) {
            return delegate.offer(o);
        }

        @Override
        public Object remove() {
            return delegate.remove();
        }

        @Override
        public Object poll() {
            return delegate.poll();
        }

        @Override
        public Object element() {
            return delegate.element();
        }

        @Override
        public Object peek() {
            return delegate.peek();
        }

        @Override
        public void put(Object o) throws InterruptedException {
            notSupported();
        }

        private RuntimeException notSupported() {
            throw new UnsupportedOperationException("not implemented yet");
        }

        @Override
        public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException {
            throw notSupported();
        }

        @Override
        public Object take() throws InterruptedException {
            Object o;
            do {
                o = delegate.poll();
                if (o == null) {
                    if (alarm == null) {
                        alarm = new Alarm(delegate, Thread.currentThread());
                        watchDog.add(alarm);
                    }
                    alarm.sleep();
                }
            } while (o == null);
            return o;
        }

        @Override
        public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
            throw notSupported();
        }

        @Override
        public int remainingCapacity() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean remove(Object o) {
            return delegate.remove(o);
        }

        @Override
        public boolean addAll(Collection c) {
            return delegate.addAll(c);
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public boolean retainAll(Collection c) {
            return delegate.retainAll(c);
        }

        @Override
        public boolean removeAll(Collection c) {
            return delegate.removeAll(c);
        }

        @Override
        public boolean containsAll(Collection c) {
            return delegate.containsAll(c);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return delegate.contains(o);
        }

        @Override
        public Iterator iterator() {
            return delegate.iterator();
        }

        @Override
        public Object[] toArray() {
            return delegate.toArray();
        }

        @Override
        public Object[] toArray(Object[] a) {
            return delegate.toArray(a);
        }

        @Override
        public int drainTo(Collection c) {
            throw notSupported();
        }

        @Override
        public int drainTo(Collection c, int maxElements) {
            throw notSupported();
        }
    }
}
