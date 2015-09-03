package com.hazelcast.query.impl.predicates;

public interface Visitor {
    void visit(AndPredicate andPredicate);

    void visit(OrPredicate orPredicate);
}
