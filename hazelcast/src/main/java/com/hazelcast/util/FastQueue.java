package com.hazelcast.util;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Single consumer, multi producer variable length queue implementation.
 * <p/>
 * The fast queue is a blocking implementation.
 *
 * @param <E>
 */
public final class FastQueue<E> {
    private final static Node BLOCKED = new Node();

    private final Thread owningThread;
    private final AtomicReference<Node> head = new AtomicReference<Node>();
    private Object[] drain;

    public FastQueue(Thread owningThread) {
        if (owningThread == null) {
            throw new IllegalArgumentException("owningThread can't be null");
        }
        this.owningThread = owningThread;
        this.drain = new Object[512];
    }

    public void clear() {
        head.set(null);
    }

    public void add(E value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null");
        }

        AtomicReference<Node> head = this.head;
        Node newHead = new Node();
        newHead.value = value;

        for (; ; ) {
            Node oldHead = head.get();
            if (oldHead == null || oldHead == BLOCKED) {
                newHead.next = null;
                newHead.size = 1;
            } else {
                newHead.next = oldHead;
                newHead.size = oldHead.size + 1;
            }

            if (!head.compareAndSet(oldHead, newHead)) {
                continue;
            }

            if (oldHead == BLOCKED) {
                LockSupport.unpark(owningThread);
            }

            return;
        }
    }

    public Object[] takeAll() throws InterruptedException {
        AtomicReference<Node> head = this.head;
        for (; ; ) {
            Node currentHead = head.get();

            if (currentHead == null) {
                // there is nothing to be take, so lets block.
                if (!head.compareAndSet(null, BLOCKED)) {
                    continue;
                }
                LockSupport.park();

                if (owningThread.isInterrupted()) {
                    head.compareAndSet(BLOCKED, null);
                    throw new InterruptedException();
                }

            } else {
                if (!head.compareAndSet(currentHead, null)) {
                    continue;
                }

                return copyToDrain(currentHead);
            }
        }
    }

    public Object[] pollAll() {
        AtomicReference<Node> head = this.head;
        for (; ; ) {
            Node headNode = head.get();
            if (headNode == null) {
                return null;
            }

            if (head.compareAndSet(headNode, null)) {
                return copyToDrain(headNode);
            }
        }
    }

    private Object[] copyToDrain(Node head) {
        int size = head.size;

        Object[] drain = this.drain;
        if (size > drain.length) {
            drain = new Object[head.size * 2];
            this.drain = drain;
        }

        for (int index = size - 1; index >= 0; index--) {
            drain[index] = head.value;
            head = head.next;
        }
        return drain;
    }

    public int size() {
        Node h = head.get();
        return h == null ? 0 : h.size;
    }

    public boolean isEmpty() {
        return head.get() == null;
    }

    private static final class Node<E> {
        Node next;
        E value;
        int size;
    }
}