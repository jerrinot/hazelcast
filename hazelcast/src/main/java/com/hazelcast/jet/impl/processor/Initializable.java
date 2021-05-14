package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.Processor;

import javax.annotation.Nonnull;

public interface Initializable {
    void init(@Nonnull Processor.Context context);
}
