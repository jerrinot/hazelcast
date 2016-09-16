package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.nio.serialization.Data;

public abstract class AbstractBackupCacheOperation extends AbstractCacheOperation {

    protected abstract void runInternal() throws Exception;
    protected abstract void afterRunInternal() throws Exception;

    AbstractBackupCacheOperation() {
    }

    AbstractBackupCacheOperation(String name, Data key) {
        super(name, key);
    }

    @Override
    public final void beforeRun() throws Exception {
        try {
            super.beforeRun();
        } catch (CacheNotExistsException e) {
            cache = null;
            getLogger().finest("Error while getting a cache", e);
        }
    }

    @Override
    public final void run() throws Exception {
        if (cache != null) {
            runInternal();
        }
    }

    @Override
    public final void afterRun() throws Exception {
        if (cache != null) {
            afterRunInternal();
        }
    }
}
