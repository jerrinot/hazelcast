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
        Predicate leftMost = Predicates.greaterEqual("foo", 10);
        Predicate rightMost = Predicates.lessEqual("foo", 15);

        AndPredicate and = (AndPredicate) Predicates.and(leftMost, rightMost);

        System.out.println("Before: " + and);
        and.accept(visitor);
        System.out.println("After: " + and);
    }


}
