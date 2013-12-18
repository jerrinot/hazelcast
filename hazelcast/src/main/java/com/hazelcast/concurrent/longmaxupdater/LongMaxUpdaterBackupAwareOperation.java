package com.hazelcast.concurrent.longmaxupdater;

import com.hazelcast.spi.BackupAwareOperation;

public abstract class LongMaxUpdaterBackupAwareOperation extends LongMaxUpdaterBaseOperation implements BackupAwareOperation{

    protected boolean shouldBackup = true;

    public LongMaxUpdaterBackupAwareOperation() {
    }

    public LongMaxUpdaterBackupAwareOperation(String name) {
        super(name);
    }

    public boolean shouldBackup() {
        return shouldBackup;
    }

    public int getSyncBackupCount() {
        return 1;
    }

    public int getAsyncBackupCount() {
        return 0;
    }
}
