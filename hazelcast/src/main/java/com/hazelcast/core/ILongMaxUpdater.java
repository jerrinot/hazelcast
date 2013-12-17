package com.hazelcast.core;

public interface ILongMaxUpdater extends DistributedObject{

    long max();

    void update(long x);

    long maxThenReset();

}
