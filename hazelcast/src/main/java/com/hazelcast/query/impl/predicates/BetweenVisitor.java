package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.impl.FalsePredicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BetweenVisitor implements Visitor {

    @Override
    public Predicate visit(AndPredicate andPredicate) {
        Predicate[] originalPredicates = andPredicate.predicates;
        Map<String, List<GreaterLessPredicate>> candidates = findCandidates(originalPredicates);

        if (candidates == null) {
            return andPredicate;
        }

        int toBeRemoved = 0;
        for (Map.Entry<String, List<GreaterLessPredicate>> entry : candidates.entrySet()) {
            List<GreaterLessPredicate> predicates = entry.getValue();
            if (predicates.size() == 1) {
                continue;
            }
            GreaterLessPredicate mostLeft = null;
            GreaterLessPredicate mostRight = null;
            for (GreaterLessPredicate predicate : predicates) {
                if (!predicate.equal) {
                    continue;
                }
                if (predicate.less) {
                    if (mostRight == null || predicate.value.compareTo(mostRight.value) < 0) {
                        mostRight = predicate;
                    }
                } else {
                    if (mostLeft == null || predicate.value.compareTo(mostLeft.value) > 0) {
                        mostLeft = predicate;
                    }
                }
            }

            if (mostLeft == null || mostRight == null) {
                continue;
            }

            if (replaceWithFalsePredicateIfPossible(andPredicate, mostLeft, mostRight)) {
                return andPredicate;
            }

            String attributeName = entry.getKey();
            toBeRemoved = rewriteAttribute(attributeName, mostLeft, mostRight, originalPredicates, toBeRemoved);
        }
        andPredicate.predicates = removeEliminatedPredicates(originalPredicates, toBeRemoved);
        return andPredicate;
    }

    private Predicate[] removeEliminatedPredicates(Predicate[] originalPredicates, int toBeRemoved) {
        if (toBeRemoved == 0) {
            return originalPredicates;
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
        return newPredicates;
    }

    private boolean replaceWithFalsePredicateIfPossible(AndPredicate andPredicate, GreaterLessPredicate mostLeft, GreaterLessPredicate mostRight) {
        if (mostLeft.value.compareTo(mostRight.value) > 0) {
            Predicate[] newPredicates = new Predicate[1];
            newPredicates[0] = FalsePredicate.INSTANCE;
            andPredicate.predicates = newPredicates;
            return true;
        }
        return false;
    }

    private int rewriteAttribute(String attributeName, GreaterLessPredicate mostLeft, GreaterLessPredicate mostRight, Predicate[] originalPredicates, int toBeRemovedCount) {
        BetweenPredicate rewritten = new BetweenPredicate(attributeName, mostLeft.value, mostRight.value);
        for (int i = 0; i < originalPredicates.length; i++) {
            Predicate currentPredicate = originalPredicates[i];
            if (currentPredicate == mostLeft) {
                originalPredicates[i] = rewritten;
            } else if (currentPredicate == mostRight) {
                originalPredicates[i] = null;
                toBeRemovedCount++;
            } else if (canBeEliminated(currentPredicate, mostLeft, mostRight)) {
                originalPredicates[i] = null;
                toBeRemovedCount++;
            }
        }
        return toBeRemovedCount;
    }

    private boolean canBeEliminated(Predicate currentPredicate, GreaterLessPredicate mostLeft, GreaterLessPredicate mostRight) {
        if (!(currentPredicate instanceof GreaterLessPredicate)) {
            return false;
        }
        GreaterLessPredicate glp = (GreaterLessPredicate) currentPredicate;
        if (glp.less) {
            return glp.value.compareTo(mostRight.value) > 0;
        } else {
            return glp.value.compareTo(mostLeft.value) < 0;
        }
    }


    private Map<String, List<GreaterLessPredicate>> findCandidates(Predicate[] originalPredicates) {
        Map<String, List<GreaterLessPredicate>> candidates = null;
        for (Predicate predicate : originalPredicates) {
            if (!(predicate instanceof GreaterLessPredicate)) {
                continue;
            }
            GreaterLessPredicate greaterLessPredicate = (GreaterLessPredicate) predicate;
            if (!(greaterLessPredicate.equal)) {
                continue;
            }
            candidates = addIntoCandidates(greaterLessPredicate, candidates);
        }
        return candidates;
    }

    private Map<String, List<GreaterLessPredicate>> addIntoCandidates(GreaterLessPredicate predicate, Map<String, List<GreaterLessPredicate>> currentCandidates) {
        if (currentCandidates == null) {
            currentCandidates = new HashMap<String, List<GreaterLessPredicate>>();
        }
        String attribute = predicate.attribute;
        List<GreaterLessPredicate> greaterLessPredicates = currentCandidates.get(attribute);
        if (greaterLessPredicates == null) {
            greaterLessPredicates = new ArrayList<GreaterLessPredicate>();
            currentCandidates.put(attribute, greaterLessPredicates);
        }
        greaterLessPredicates.add(predicate);
        return currentCandidates;
    }

    @Override
    public Predicate visit(OrPredicate orPredicate) {
        return orPredicate;
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
