package com.hazelcast.spring.jet;

import java.util.function.Function;

public class SpringManagedFunction implements Function<Integer, Long> {

    @Override
    public Long apply(Integer i) {
        return (long)-i;
    }
}
