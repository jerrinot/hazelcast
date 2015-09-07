package com.hazelcast.query.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class FalsePredicate implements DataSerializable, Predicate, IndexAwarePredicate {
    /**
     * An instance of the FalsePredicate.
     */
    public static final FalsePredicate INSTANCE = new FalsePredicate();

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return false;
    }

    @Override
    public String toString() {
        return "FalsePredicate{}";
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return Collections.emptySet();
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return true;
    }
}
