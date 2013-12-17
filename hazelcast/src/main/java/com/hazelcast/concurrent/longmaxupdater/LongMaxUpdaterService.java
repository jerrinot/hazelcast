package com.hazelcast.concurrent.longmaxupdater;

import com.hazelcast.concurrent.longmaxupdater.proxy.LongMaxUpdaterProxy;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class LongMaxUpdaterService implements ManagedService, RemoteService{

    public static final String SERVICE_NAME = "hz:impl:longMaxUpdaterService";

    private NodeEngine nodeEngine;
    private ConcurrentHashMap<String, LongMaxWrapper> numbers = new ConcurrentHashMap<String, LongMaxWrapper>();

    private final ConstructorFunction<String, LongMaxWrapper> longMaxConstructorFunction = new ConstructorFunction<String, LongMaxWrapper>() {
        public LongMaxWrapper createNew(String key) {
            return new LongMaxWrapper();
        }
    };

    public LongMaxUpdaterService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public LongMaxWrapper getNumber(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(numbers, name, longMaxConstructorFunction);
    }

    @Override
    public void reset() {
        numbers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new LongMaxUpdaterProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        numbers.remove(name);
    }
}
