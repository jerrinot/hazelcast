package com.hazelcast.util.collection;

import java.util.BitSet;

public final class BitSetUtils {

    private BitSetUtils() {

    }

    public static boolean hasAtLeastOneBitSet(BitSet bitSet, Iterable<Integer> indexes) {
        for (Integer index : indexes) {
            if (bitSet.get(index)) {
                return true;
            }
        }
        return false;
    }

    public static void setBits(BitSet bitSet, Iterable<Integer> indexes) {
        for (Integer index : indexes) {
            bitSet.set(index);
        }
    }


}
