package com.hazelcast.query.impl;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class SmartIterator<T> implements Iterator<T> {

    private final Iterator<Set<T>> outerIterator;
    private Iterator<T> innerIterator;

    public SmartIterator(Map<Comparable, Set<T>> map) {
        outerIterator = map.values().iterator();
        if (outerIterator.hasNext()) {
            innerIterator = outerIterator.next().iterator();
        } else {
            innerIterator = Collections.EMPTY_SET.iterator();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean hasNext() {
        if (!advance()) {
            return false;
        }
        return true;
    }

    @Override
    public T next() {
        if (!advance()) {
            throw  new NoSuchElementException();
        }
        return innerIterator.next();
    }

    private boolean advance() {
        while (!innerIterator.hasNext()) {
            if (!outerIterator.hasNext()) {
                return false;
            }
            innerIterator = outerIterator.next().iterator();
        }
        return true;
    }
}
