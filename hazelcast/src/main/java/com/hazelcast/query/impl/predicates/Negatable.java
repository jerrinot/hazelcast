package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;

public interface Negatable {
    Predicate negate();
}
