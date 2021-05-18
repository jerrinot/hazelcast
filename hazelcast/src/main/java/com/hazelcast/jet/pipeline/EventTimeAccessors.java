package com.hazelcast.jet.pipeline;

import com.hazelcast.function.FunctionEx;

@FunctionalInterface
public interface EventTimeAccessors<T, R> extends FunctionEx<T, R> {
    R applyWithEventTimeEx(T t, long eventTime) throws Exception;

    static <T, R> FunctionEx<? super T, ? extends R> function(EventTimeAccessors<? super T, ? extends R> fun) {
        return fun;
    }

    @Override
    default R applyEx(T t) throws Exception {
        throw new UnsupportedOperationException("not implemented");
    }
}
