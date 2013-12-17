package com.hazelcast.concurrent.longmaxupdater;

public class MaxThenResetOperation extends LongMaxUpdaterBaseOperation {

    public MaxThenResetOperation() {

    }

    public MaxThenResetOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        LongMaxWrapper number = getNumber();
        returnValue = number.maxThenReset();
    }
}
