package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BetweenVisitor implements Visitor {

    @Override
    public void visit(AndPredicate andPredicate) {
        Predicate[] originalPredicates = andPredicate.predicates;
        Map<String, List<GreaterLessPredicate>> candidates = findCandidates(originalPredicates);

        if (candidates == null) {
            return;
        }

        int toBeRemoved = 0;
        for (Map.Entry<String, List<GreaterLessPredicate>> entry : candidates.entrySet()) {
            List<GreaterLessPredicate> predicates = entry.getValue();
            if (predicates.size() == 1) {
                continue;
            }
            String attributeName = entry.getKey();

            GreaterLessPredicate mostLeft = null;
            GreaterLessPredicate mostRight = null;

            for (GreaterLessPredicate predicate : predicates) {
                if (!predicate.equal) {
                    continue;
                }
                if (!predicate.less) {
                    if (mostLeft == null) {
                        mostLeft = predicate;
                    } else {
                        if (predicate.value.compareTo(mostLeft.value) < 0) {
                            mostLeft = predicate;
                        }
                    }
                } else {
                    if (mostRight == null) {
                        mostRight = predicate;
                    } else {
                        if (predicate.value.compareTo(mostRight.value) > 0) {
                            mostRight = predicate;
                        }
                    }
                }
            }
            if (predicates.size() != 2) {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            if (mostLeft == null || mostRight == null) {
                continue;
            }

            BetweenPredicate rewritten = new BetweenPredicate(attributeName, mostLeft.value, mostRight.value);
            for (int i = 0; i < originalPredicates.length; i++) {
                if (originalPredicates[i] == mostLeft) {
                    originalPredicates[i] = rewritten;
                } else if (originalPredicates[i] == mostRight) {
                    originalPredicates[i] = null;
                    toBeRemoved++;
                }
            }
        }

        int newSize = originalPredicates.length - toBeRemoved;
        Predicate[] newPredicates = new Predicate[newSize];
        int skipped = 0;
        for (int i = 0; i < originalPredicates.length; i++) {
            Predicate predicate = originalPredicates[i];
            if (predicate == null) {
                skipped++;
            } else {
                newPredicates[i - skipped] = predicate;
            }
        }
        andPredicate.predicates = newPredicates;
    }


    private Map<String, List<GreaterLessPredicate>> findCandidates(Predicate[] originalPredicates) {
        Map<String, List<GreaterLessPredicate>> candidates = null;
        for (Predicate predicate : originalPredicates) {
            if (!(predicate instanceof GreaterLessPredicate)) {
                continue;
            }
            GreaterLessPredicate greaterLessPredicate = (GreaterLessPredicate) predicate;
            if (candidates == null) {
                candidates = new HashMap<String, List<GreaterLessPredicate>>();
            }
            List<GreaterLessPredicate> greaterLessPredicates = candidates.get(greaterLessPredicate.attribute);
            if (greaterLessPredicates == null) {
                greaterLessPredicates = new ArrayList<GreaterLessPredicate>();
                candidates.put(greaterLessPredicate.attribute, greaterLessPredicates);
            }
            greaterLessPredicates.add(greaterLessPredicate);
        }
        return candidates;
    }

    @Override
    public void visit(OrPredicate orPredicate) {

    }

    private static class ValueComparator implements Comparator<GreaterLessPredicate> {
        private static final ValueComparator INSTANCE = new ValueComparator();

        @Override
        public int compare(GreaterLessPredicate o1, GreaterLessPredicate o2) {
            return o1.value.compareTo(o2.value);
        }
    }
}
