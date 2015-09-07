package com.hazelcast.query.impl.predicates;


import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SqlPredicate;

public interface Visitor {
    Predicate visit(AndPredicate predicate);

    Predicate visit(OrPredicate predicate);

    Predicate visit(NotPredicate predicate);

    Predicate visit(PredicateBuilder predicateBuilder);

    Predicate visit(SqlPredicate sqlPredicate);

    Predicate visit(LikePredicate likePredicate);
}
