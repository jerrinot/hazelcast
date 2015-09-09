package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SqlPredicate;

public class NotEliminatingVisitor implements Visitor {
    @Override
    public Predicate visit(AndPredicate predicate) {
        return predicate;
    }

    @Override
    public Predicate visit(OrPredicate predicate) {
        return predicate;
    }

    @Override
    public Predicate visit(NotPredicate predicate) {
        Predicate innerPredicate = predicate.predicate;
        if (innerPredicate instanceof OrPredicate) {
            Predicate[] predicates = ((OrPredicate) innerPredicate).predicates;
            Predicate[] newPredicates = invertPredicates(predicates);
            return new AndPredicate(newPredicates);
        } else if (innerPredicate instanceof AndPredicate) {
            Predicate[] predicates = ((AndPredicate) innerPredicate).predicates;
            Predicate[] newPredicates = invertPredicates(predicates);
            return new OrPredicate(newPredicates);
        } else if (innerPredicate instanceof GreaterLessPredicate) {
            GreaterLessPredicate greaterLessPredicate = (GreaterLessPredicate) innerPredicate;
            greaterLessPredicate.equal = !greaterLessPredicate.equal;
            greaterLessPredicate.less = !greaterLessPredicate.less;
            return greaterLessPredicate;
        }
        return predicate;
    }

    private Predicate[] invertPredicates(Predicate[] predicates) {
        int size = predicates.length;
        Predicate[] newPredicates = new Predicate[size];
        for (int i = 0; i < size; i++) {
            Predicate original = predicates[i];
            Predicate invertedPredicate = invertPredicate(original);
            newPredicates[i] = invertedPredicate;
        }
        return newPredicates;
    }

    private Predicate invertPredicate(Predicate original) {
        if (original instanceof Negatable) {
            return ((Negatable) original).negate();
        } else {
            return new NotPredicate(original);
        }
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
        return null;
    }
}
