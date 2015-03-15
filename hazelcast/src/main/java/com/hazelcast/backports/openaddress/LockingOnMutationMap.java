package com.hazelcast.backports.openaddress;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.util.QuickMath.modPowerOfTwo;
import static com.hazelcast.util.QuickMath.nextPowerOfTwo;
import static com.hazelcast.util.ValidationUtil.checkNotNull;

public class LockingOnMutationMap<K, V> {
    private static final int DEFAULT_INITIAL_CAPACITY = 100;
    private static final float DEFAULT_LOAD_FACTOR = 0.7f;

    private final Lock mutationLock;
    private final float loadFactor;
    private final int initialCapacity;

    private int threshold;
    private int hashSeed = 0;

    private volatile Object[] array;
    private volatile int size;

    public LockingOnMutationMap() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public LockingOnMutationMap(int initialCapacity) {
        this.initialCapacity = nextPowerOfTwo(initialCapacity);
        int size = 2 * this.initialCapacity;
        this.array = new Object[size];
        this.mutationLock = new ReentrantLock();
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        this.threshold = (int) (loadFactor * this.initialCapacity);
    }

    public void put(K key, V value) {
        checkNotNull(key, "Key cannot be null");
        checkNotNull(value, "Value cannot be null");
        mutationLock.lock();
        try {
            Object[] currentArray = array;
            int capacity = currentArray.length / 2;

            if (size == threshold) {
                capacity = 2 * capacity;
                currentArray = resize(currentArray, capacity);
            }
            putInternal(key, value, currentArray);
            size++;
        } finally {
            mutationLock.unlock();
        }
    }

    public void remove(K key) {
        mutationLock.lock();
        try {
            Object[] currentArray = array;
            int capacity = currentArray.length / 2;

            int hash = hash(key);
            int initialSlot = modPowerOfTwo(hash, capacity) * 2;
            int slot = initialSlot;

            do {
                K keyFromArray = (K) currentArray[initialSlot];
                if (keyFromArray == null) {
                    return;
                } else if (keyFromArray.equals(key)) {
                    currentArray[slot] = null;
                    size++;
                    return;
                }
            } while (slot != initialSlot);
        } finally {
            mutationLock.unlock();
        }
    }

    public V get(K key) {
        checkNotNull(key, "Key cannot be null");

        Object[] currentArray = array;
        int capacity = currentArray.length / 2;

        int hash = hash(key);
        int initialSlot = modPowerOfTwo(hash, capacity) * 2;
        int slot = initialSlot;

        do {
            K keyFromArray = (K) currentArray[initialSlot];
            if (keyFromArray == null) {
                return null;
            } else if (keyFromArray.equals(key)) {
                V value = (V) currentArray[slot + 1];
                return value;
            }
            hash++;
            slot = modPowerOfTwo(hash, capacity) * 2;
        } while (slot != initialSlot);
        return null;
    }

    private Object[] resize(Object[] orig, int newCapacity) {
        Object[] newArray = new Object[newCapacity * 2];
        for (int i = 0; i < orig.length; i += 2) {
            transferEntry(orig, newArray, i);
        }
        threshold = (int) (loadFactor * newCapacity);
        array = newArray;
        return newArray;
    }

    private void transferEntry(Object[] orig, Object[] newArray, int i) {
        Object key = orig[i];
        if (key != null) {
            V value = (V) orig[i + 1];
            putInternal(key, value, newArray);
        }
    }

    private void putInternal(Object key, V value, Object[] target) {
        int hash = hash(key);
        int capacity = target.length / 2;

        int initialSlot = modPowerOfTwo(hash, capacity) * 2;
        int slot = initialSlot;

        do {
            K currentKey = (K) target[slot];
            if (currentKey == null || currentKey.equals(key)) {
                target[slot] = key;
                target[slot + 1] = value;
                return;
            }
            hash++;
            slot = modPowerOfTwo(hash, capacity) * 2;
        } while (slot != initialSlot);
        throw new IllegalStateException("Capacity Exceeded.");
    }

    private int hash(Object k) {
        int h = 0;
        h ^= k.hashCode();
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    public int size() {
        return size;
    }

    public Collection<V> values() {
        Object[] currentArray = array;

        List<V> result = new ArrayList<V>(size);
        for (int i = 0; i < currentArray.length; i += 2) {
            K key = (K) currentArray[i];
            if (key != null) {
                V value = (V) currentArray[i + 1];
                result.add(value);
            }
        }
        return result;
    }

    public void clear() {
        mutationLock.lock();
        try {
            int length = initialCapacity * 2;
            array = new Object[length];
            size = 0;
            threshold = (int) (initialCapacity * loadFactor);
        } finally {
            mutationLock.unlock();
        }
    }

    public void remove(K key, V value) {
        checkNotNull(key, "Key cannot be null");
        checkNotNull(value, "Value cannot be null");
        mutationLock.lock();
        try {
            Object[] currentArray = array;

            int hash = hash(key);
            int capacity = currentArray.length / 2;

            int initialSlot = modPowerOfTwo(hash, capacity) * 2;
            int slot = initialSlot;

            do {
                K currentKey = (K) currentArray[slot];
                if (currentKey == null) {
                    return;
                } else if (currentKey.equals(key)) {
                    V currentValue = (V) currentArray[slot + 1];
                    if (value.equals(currentValue)) {
                        currentArray[slot] = null;
                    }
                    return;
                }
                hash++;
                slot = modPowerOfTwo(hash, capacity) * 2;
            } while (slot != initialSlot);
        } finally {
            mutationLock.unlock();
        }
    }
}
