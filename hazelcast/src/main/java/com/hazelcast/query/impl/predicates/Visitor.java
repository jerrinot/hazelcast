package com.hazelcast.query.impl.predicates;


public interface Visitor {
    void visit(AndPredicate predicate);

    void visit(OrPredicate predicate);

    void visit(NotPredicate predicate);

}
