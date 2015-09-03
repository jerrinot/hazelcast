package com.hazelcast.query.impl.predicates;


import com.hazelcast.query.Predicate;

import java.util.ArrayList;

public class FlatteningVisitor implements Visitor {

    @Override
    public void visit(AndPredicate andPredicate) {
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
    }

    @Override
    public void visit(OrPredicate orPredicate) {
        Predicate[] predicates = orPredicate.predicates;

    }
}
