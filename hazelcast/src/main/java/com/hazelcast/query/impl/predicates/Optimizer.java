package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;

public class Optimizer {

    public <K, V> Predicate<K, V> optimize(Predicate<K, V> predicate) {
        if (predicate instanceof AndPredicate) {
            AndPredicate andPredicate = (AndPredicate) predicate;
            return optimizeInternal(andPredicate);
        } else if (predicate instanceof OrPredicate) {
            OrPredicate orPredicate = (OrPredicate) predicate;
            return optimizeInternal(orPredicate);
        }
        return predicate;
    }

    private Predicate optimizeInternal(AndPredicate predicate) {
        AndPredicate andPredicate = (AndPredicate) predicate;
        Predicate[] nestedPredicates = andPredicate.predicates;
        for (Predicate nestedPredicate : nestedPredicates) {

        }

        return predicate;
    }

    private Predicate optimizeInternal(OrPredicate predicate) {
        OrPredicate orPredicate = (OrPredicate) predicate;
        Predicate[] nestedPredicates = orPredicate.predicates;

        return predicate;
    }

}
