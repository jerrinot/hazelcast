package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.junit.Before;
import org.junit.Test;

public class BetweenVisitorTest {

    private BetweenVisitor visitor;

    @Before
    public void setUp() {
        visitor = new BetweenVisitor();
    }

    @Test
    public void testRewrite() {
        Predicate leftMost = Predicates.greaterEqual("foo", 20);
        Predicate rightMost = Predicates.lessEqual("foo", 15);
        Predicate extra1 = Predicates.lessThan("foo", 15);
        Predicate extra2 = Predicates.greaterEqual("foo", 5);

        AndPredicate and = (AndPredicate) Predicates.and(leftMost, rightMost, extra1, extra2);

        System.out.println("Before: " + and);
        and.accept(visitor);
        System.out.println("After: " + and);
    }


}
