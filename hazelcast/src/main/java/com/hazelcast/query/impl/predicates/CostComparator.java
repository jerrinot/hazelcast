package com.hazelcast.query.impl.predicates;

import java.util.Comparator;

public final class CostComparator implements Comparator<Object> {
    public static final Comparator<Object> INSTANCE = new CostComparator();

    private CostComparator() {

    }


    @Override
    public int compare(Object p1, Object p2) {
        long p1Cost = (p1 instanceof Accountable) ? ((Accountable) p1).getCost() : Accountable.NORMAL_COST;
        long p2Cost = (p2 instanceof Accountable) ? ((Accountable) p2).getCost() : Accountable.NORMAL_COST;
        return (p1Cost < p2Cost) ? -1 : ((p1Cost == p2Cost) ? 0 : 1);
    }
}
