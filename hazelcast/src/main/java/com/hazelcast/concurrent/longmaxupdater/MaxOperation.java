package com.hazelcast.concurrent.longmaxupdater;

public class MaxOperation extends LongMaxUpdaterBaseOperation {
    public MaxOperation() {

    }

    public MaxOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        returnValue = getNumber().max();
    }

}
