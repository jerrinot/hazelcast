package com.hazelcast.query.impl.predicates;

public interface Accountable {
    long LOST_COST = 1;
    long NORMAL_COST = 10;
    long HIGH_COST = 100;

    long getCost();
}
