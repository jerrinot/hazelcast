package com.hazelcast.concurrent.longmaxupdater;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

abstract class LongMaxUpdaterBaseOperation extends Operation implements PartitionAwareOperation {
    protected String name;
    protected long returnValue;

    public LongMaxUpdaterBaseOperation() {

    }

    public LongMaxUpdaterBaseOperation(String name) {
        this.name = name;
    }

    @Override
    public void beforeRun() throws Exception {

    }

    @Override
    public void afterRun() throws Exception {

    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    protected LongMaxWrapper getNumber() {
        LongMaxUpdaterService service = getService();
        return service.getNumber(name);
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }
}
