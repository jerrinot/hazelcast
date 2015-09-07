package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SqlPredicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrToInVisitor implements Visitor {
    @Override
    public Predicate visit(AndPredicate predicate) {
        return predicate;
    }

    @Override
    public Predicate visit(OrPredicate predicate) {
        Predicate[] innerPredicates = predicate.predicates;
        if (innerPredicates == null || innerPredicates.length < 5) {
            return predicate;
        }

        Map<String, List<Integer>> candidates = findCandidates(innerPredicates);
        if (candidates == null) {
            return predicate;
        }
        int toBeRemoved = 0;
        for (Map.Entry<String, List<Integer>> candidate : candidates.entrySet()) {
            List<Integer> positions = candidate.getValue();
            if (positions.size() < 5) {
                continue;
            }

            Comparable[] values = new Comparable[positions.size()];
            for (int i = 0; i < positions.size(); i++) {
                int position = positions.get(i);
                EqualPredicate equalPredicate = ((EqualPredicate)innerPredicates[position]);
                values[i] = equalPredicate.value;
                innerPredicates[position] = null;
                toBeRemoved++;
            }
            String attribute = candidate.getKey();
            InPredicate inPredicate = new InPredicate(attribute, values);
            innerPredicates[positions.get(0)] = inPredicate;
            toBeRemoved--;
        }
        if (toBeRemoved > 0) {
            int removed = 0;
            int newSize = innerPredicates.length - toBeRemoved;
            Predicate[] newPredicates = new Predicate[newSize];
            for (int i = 0; i < innerPredicates.length; i++) {
                Predicate p = innerPredicates[i];
                if (p != null) {
                    newPredicates[i - removed] = p;
                } else {
                    removed++;
                }
            }
            predicate.predicates = newPredicates;
        }
        return predicate;
    }

    private Map<String, List<Integer>> findCandidates(Predicate[] innerPredicates) {
        Map<String, List<Integer>> candidates = null;
        for (int i = 0; i < innerPredicates.length; i++) {
            Predicate p = innerPredicates[i];
            if (p.getClass().equals(EqualPredicate.class)) {
                EqualPredicate equalPredicate = (EqualPredicate) p;
                String attribute = equalPredicate.attribute;
                if (candidates == null) {
                    candidates = new HashMap<String, List<Integer>>();
                }
                List<Integer> equalPredicates = candidates.get(attribute);
                if (equalPredicates == null) {
                    equalPredicates = new ArrayList<Integer>();
                    candidates.put(attribute, equalPredicates);
                }
                equalPredicates.add(i);
            }
        }
        return candidates;
    }

    @Override
    public Predicate visit(NotPredicate predicate) {
        return predicate;
    }

    @Override
    public Predicate visit(PredicateBuilder predicateBuilder) {
        return predicateBuilder;
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
