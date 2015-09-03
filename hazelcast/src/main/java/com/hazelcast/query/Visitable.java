package com.hazelcast.query;

import com.hazelcast.query.impl.predicates.Visitor;

public interface Visitable {
    void accept(Visitor visitor);
}
