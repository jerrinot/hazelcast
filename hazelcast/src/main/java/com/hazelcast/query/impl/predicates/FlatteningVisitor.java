package com.hazelcast.query.impl.predicates;


import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SqlPredicate;

import java.util.ArrayList;

public class FlatteningVisitor implements Visitor {

    @Override
    public Predicate visit(AndPredicate andPredicate) {
        Predicate[] predicates = andPredicate.predicates;
        ArrayList<Predicate> toBeAdded = null;
        for (int i = 0; i < predicates.length; i++) {
            Predicate predicate = predicates[i];
            if (predicate instanceof AndPredicate) {
                Predicate[] subPredicates = ((AndPredicate) predicate).predicates;
                if (subPredicates != null && subPredicates.length != 0) {
                    predicates[i] = subPredicates[0];
                    for (int j = 1; j < subPredicates.length; j++) {
                        if (toBeAdded == null) {
                            toBeAdded = new ArrayList<Predicate>();
                        }
                        toBeAdded.add(subPredicates[j]);
                    }
                }
            }
        }
        if (toBeAdded != null && toBeAdded.size() != 0) {
            int newSize = predicates.length + toBeAdded.size();
            Predicate[] newPredicates = new Predicate[newSize];
            System.arraycopy(predicates, 0, newPredicates, 0, predicates.length);
            for (int i = predicates.length; i < newSize; i++) {
                newPredicates[i] = toBeAdded.get(i - predicates.length);
            }
            andPredicate.predicates = newPredicates;
        }
        return andPredicate;
    }

    @Override
    public Predicate visit(OrPredicate orPredicate) {
        Predicate[] predicates = orPredicate.predicates;
        ArrayList<Predicate> toBeAdded = null;
        for (int i = 0; i < predicates.length; i++) {
            Predicate predicate = predicates[i];
            if (predicate instanceof OrPredicate) {
                Predicate[] subPredicates = ((OrPredicate) predicate).predicates;
                if (subPredicates != null && subPredicates.length != 0) {
                    predicates[i] = subPredicates[0];
                    for (int j = 1; j < subPredicates.length; j++) {
                        if (toBeAdded == null) {
                            toBeAdded = new ArrayList<Predicate>();
                        }
                        toBeAdded.add(subPredicates[j]);
                    }
                }
            } else if (predicate instanceof PredicateBuilder){
                predicates[i] = ((PredicateBuilder) predicate).getPredicate();
            }
        }
        if (toBeAdded != null && toBeAdded.size() != 0) {
            int newSize = predicates.length + toBeAdded.size();
            Predicate[] newPredicates = new Predicate[newSize];
            System.arraycopy(predicates, 0, newPredicates, 0, predicates.length);
            for (int i = predicates.length; i < newSize; i++) {
                newPredicates[i] = toBeAdded.get(i - predicates.length);
            }
            orPredicate.predicates = newPredicates;
        }
        return orPredicate;
    }

    @Override
    public Predicate visit(NotPredicate predicate) {
        Predicate inner = predicate.predicate;
        if (inner instanceof PredicateBuilder) {
            predicate.predicate = ((PredicateBuilder) inner).getPredicate();
        }
        return predicate;
    }

    @Override
    public Predicate visit(PredicateBuilder predicateBuilder) {
        return predicateBuilder.getPredicate();
    }

    @Override
    public Predicate visit(SqlPredicate sqlPredicate) {
        return sqlPredicate;
    }

    @Override
    public Predicate visit(LikePredicate likePredicate) {
        return likePredicate;
    }
}
