/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.util.collection;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Provides fast {@link Set} implementation for cases where items are known to not
 * contain duplicates.
 *
 * It doesn't not call equals/hash methods on initial data insertion hence it avoids
 * performance penalty in the case these methods are expensive. It also means it does
 * not detect duplicates - it's a responsibility of a caller to make sure no duplicated
 * entries are inserted.
 *
 * Once the initial load is done then caller should indicate it
 * by calling {@link #close()}. It switches operational mode and if any item is
 * inserted afterward then the Set will inflate - it copies its content into
 * internal HashSet. This means duplicate detection is enabled, but obviously
 * it ruins the initial performance gain. We are making a bet the Set will not be
 * modified once it's closed. The Set will also inflate on {@link #contains(Object)}
 * and {@link #containsAll(Collection)} calls to enable fast look-ups.
 *
 * It's intended to be use in cases where a contract mandates us to return Set,
 * but we know our data contains not duplicates. It performs the best in cases
 * biased towards sequential iterations.
 *
 *
 * @param <T>
 */
public class InflatableSet<T> extends AbstractSet<T> implements Set<T> {
    private enum State {
        //Set is still open for initial load. It's caller's responsibility to make
        //sure no duplicates are added to this Set in this state
        INITIAL_LOAD,

        //Only array-backed representation exists
        COMPACT,

        //both array-backed & hashset-backed representation exist.
        //this is needed as we are creating HashSet on contains()
        //but we don't want invalidate existing iterators.
        HYBRID,

        //only hashset based representation exists
        INFLATED
    }

    private final List<T> compactList;
    private Set<T> inflatedSet;
    private State state = State.INITIAL_LOAD;

    public InflatableSet(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Initial Capacity must be a positive number. Current capacity: "
                    + initialCapacity);
        }
        compactList = new ArrayList<T>(initialCapacity);
    }


    @Override
    public int size() {
        if (state == State.INFLATED) {
            return inflatedSet.size();
        } else {
            return compactList.size();
        }
    }

    @Override
    public boolean isEmpty() {
        if (state == State.INFLATED) {
            return inflatedSet.isEmpty();
        } else {
            return compactList.isEmpty();
        }
    }

    @Override
    public boolean contains(Object o) {
        if (state == State.INITIAL_LOAD) {
            return compactList.contains(o);
        }
        if (state == State.COMPACT) {
            toHybridState();
        }
        return inflatedSet.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        if (state == State.INFLATED) {
            return inflatedSet.iterator();
        }
        return new HybridIterator();
    }

    @Override
    public boolean add(T t) {
        if (state == State.INITIAL_LOAD) {
            return compactList.add(t);
        }
        toInflatedState();
        return inflatedSet.add(t);
    }

    @Override
    public boolean remove(Object o) {
        switch (state) {
            case COMPACT:
                return compactList.remove(o);
            case HYBRID:
                compactList.remove(o);
                return inflatedSet.remove(o);
            case INFLATED:
                return inflatedSet.remove(o);
            case INITIAL_LOAD:
                return compactList.remove(o);
            default:
                throw new IllegalStateException("Unknown state " + state);
        }
    }

    @Override
    public void clear() {
        switch (state) {
            case INITIAL_LOAD:
                compactList.clear();
                break;
            case COMPACT:
                compactList.clear();
                break;
            case HYBRID:
                inflatedSet.clear();
                compactList.clear();
                break;
            case INFLATED:
                inflatedSet.clear();
                break;
            default:
                throw new IllegalStateException("Unknown State: " + state);
        }
    }

    public void close() {
        if (state != State.INITIAL_LOAD) {
            throw new IllegalStateException("InflatableSet can be only closed during InitialLoad. Current state: "
                    + state);
        }
        state = State.COMPACT;
    }

    private void inflateIfNeeded() {
        if (inflatedSet == null) {
            inflatedSet = new HashSet<T>(compactList);
        }
    }

    private void toHybridState() {
        if (state == State.HYBRID) {
            return;
        }

        state = State.HYBRID;
        inflateIfNeeded();
    }

    private void toInflatedState() {
        if (state == State.INFLATED) {
            return;
        }

        state = State.INFLATED;
        inflateIfNeeded();
        invalidateIterators();
    }

    private void invalidateIterators() {
        if (compactList.size() == 0) {
            compactList.clear();
        } else {
            compactList.remove(0);
        }
    }

    private class HybridIterator implements Iterator<T> {
        private Iterator<T> innerIterator;
        private T currentValue;

        public HybridIterator() {
            innerIterator = compactList.iterator();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public T next() {
            currentValue = innerIterator.next();
            return currentValue;
        }

        @Override
        public void remove() {
            innerIterator.remove();
            if (inflatedSet != null) {
                inflatedSet.remove(currentValue);
            }
        }
    }
}
