package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.junit.Before;
import org.junit.Test;

public class FlatteningVisitorTest {

    private FlatteningVisitor visitor;

    @Before
    public void setUp() {
        visitor = new FlatteningVisitor();
    }

    @Test
    public void testFlattening() {
        Predicate leftMostPredicate = Predicates.greaterEqual("A", 1);
        Predicate equalPredicate2 = Predicates.equal("B", 2);
        Predicate equalPredicate3 = Predicates.equal("C", 3);
        Predicate equalPredicate4 = Predicates.equal("D", 4);
        Predicate rightMostPredicate = Predicates.lessEqual("A", 5);

        Predicate innerPredicate1 = Predicates.and(leftMostPredicate, equalPredicate2);
        Predicate innerPredicate2 = Predicates.or(equalPredicate3, equalPredicate4);
        Predicate innerPredicate3 = Predicates.and(innerPredicate2, rightMostPredicate);


        AndPredicate outerPredicate = (AndPredicate) Predicates.and(innerPredicate1, innerPredicate3);

        System.out.println("Before: ");
        System.out.println(outerPredicate);

        outerPredicate.accept(visitor);
        outerPredicate.accept(new BetweenVisitor());

        System.out.println("After: ");
        System.out.println(outerPredicate);
    }

}
