package com.hazelcast.concurrent.longmaxupdater;

import com.hazelcast.spi.Operation;

public class ResetOperation extends LongMaxUpdaterBackupAwareOperation {

    public ResetOperation() {
    }

    public ResetOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        LongMaxWrapper number = getNumber();
        number.reset();
    }

    @Override
    public Operation getBackupOperation() {
        return new ResetBackupOperation(name);
    }

}
