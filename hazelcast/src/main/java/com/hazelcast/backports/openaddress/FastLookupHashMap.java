package com.hazelcast.backports.openaddress;

import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.QuickMath;
import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.util.QuickMath.modPowerOfTwo;
import static com.hazelcast.util.QuickMath.nextPowerOfTwo;
import static com.hazelcast.util.ValidationUtil.checkNotNull;
import static com.hazelcast.util.ValidationUtil.shouldBePositive;

public final class FastLookupHashMap<K, V> {

    private static final int DEFAULT_CAPACITY = 100;
    private static final Object TOMBSTONE = new Object();

    private static final Unsafe unsafe;
    private static final int base;
    private static final int shift;

    private volatile Object[] array;

    static {
        int scale;
        try {
            unsafe = UnsafeHelper.UNSAFE;
            base = unsafe.arrayBaseOffset(Object[].class);
            scale = unsafe.arrayIndexScale(Object[].class);
        } catch (Exception e) {
            throw new Error(e);
        }
        if (!QuickMath.isPowerOfTwo(scale))
            throw new Error("data type scale not a power of two");
        shift = 31 - Integer.numberOfLeadingZeros(scale);
    }

    public FastLookupHashMap(int capacity) {
        shouldBePositive(capacity, "capacity");

        int size = nextPowerOfTwo(capacity * 2);
        array = new Object[size];
    }

    public FastLookupHashMap() {
        this(DEFAULT_CAPACITY);
    }

    public void put(K key, V value) {
        checkNotNull(key, "Key cannot be null");
        checkNotNull(value, "Value cannot be null");

        int capacity = array.length / 2;

        int initialHashCode = key.hashCode();
        int hashCode = initialHashCode;

        int initialKeySlot = modPowerOfTwo(hashCode, capacity) * 2;
        int keySlot = initialKeySlot;
        do {
            long keyOffset = byteOffset(keySlot);
            if (unsafe.compareAndSwapObject(array, keyOffset, null, key)) {
                long valueOffset = nextOffset(keyOffset);
                unsafe.putObjectVolatile(array, valueOffset, value);
                return;
            }

            Object keyFromArray = unsafe.getObjectVolatile(array, keyOffset);
            if (keyFromArray.equals(key) || keyFromArray.equals(TOMBSTONE)) {
                if (unsafe.compareAndSwapObject(array, keyOffset, keyFromArray, key)) {
                    long valueOffset = nextOffset(keyOffset);
                    unsafe.putObjectVolatile(array, valueOffset, value);
                    return;
                }
            }

            hashCode++;
            keySlot = modPowerOfTwo(hashCode, capacity) * 2;
        } while (keySlot != initialKeySlot);
        throw new IllegalStateException("Full capacity");
    }

    public V get(K key) {
        checkNotNull(key, "Key cannot be null");

        int capacity = array.length / 2;

        int initialHashCode = key.hashCode();
        int hashCode = initialHashCode;

        int initialKeySlot = modPowerOfTwo(hashCode, capacity) << 1;
        int keySlot = initialKeySlot;


        do {
            long keyOffset = byteOffset(keySlot);
            Object keyFromArray = unsafe.getObjectVolatile(array, keyOffset);
            if (keyFromArray == null) {
                return null;
            } else if (keyFromArray.equals(key)) {
                long valueOffset = nextOffset(keyOffset);
                V value = (V) unsafe.getObjectVolatile(array, valueOffset);
                if (value != null) {
                    return value;
                }
            }
            hashCode++;
            keySlot = modPowerOfTwo(hashCode, capacity) * 2;
        } while (keySlot != initialKeySlot);
        return null;
    }

    public void remove(K key) {
        checkNotNull(key, "Key cannot be null");

        int capacity = array.length >> 1;

        int initialHashCode = key.hashCode();
        int hashCode = initialHashCode;

        int initialKeySlot = modPowerOfTwo(hashCode, capacity) << 1;
        int keySlot = initialKeySlot;

        do {
            long keyOffset = byteOffset(keySlot);
            Object keyFromArray = unsafe.getObjectVolatile(array, keyOffset);
            if (keyFromArray == null) {
                return;
            } else if (keyFromArray.equals(key)) {
                if (unsafe.compareAndSwapObject(array, keyOffset, keyFromArray, TOMBSTONE)) {
                    return;
                }
            }
            hashCode++;
            keySlot = modPowerOfTwo(hashCode, capacity) * 2;
        } while (keySlot != initialKeySlot);

    }

    public int size() {
        return Integer.MAX_VALUE;
    }

    public Collection<V> values() {
        List<V> result = new ArrayList<V>();
        int length = array.length;

        for (int i = 0; i < length; i += 2) {
            long keyOffset = byteOffset(i);
            Object key = unsafe.getObjectVolatile(array, keyOffset);
            if (key != null && !key.equals(TOMBSTONE)) {
                long valueOffset = nextOffset(keyOffset);
                V value = (V) unsafe.getObjectVolatile(array, valueOffset);
                result.add(value);
            }
        }
        return result;
    }

    public void clear() {
        int length = array.length;

        for (int i = 0; i < length; i += 2) {
            unsafe.putObjectVolatile(array, byteOffset(i), TOMBSTONE);
        }
    }

    private long nextOffset(long keyOffset) {
        return keyOffset + (1 << shift);
    }

    private static long byteOffset(int i) {
        return ((long) i << shift) + base;

    }
}
