package com.hazelcast.concurrent.longmaxupdater;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class UpdateOperation extends LongMaxUpdaterBaseOperation {
    private long x;

    public UpdateOperation() {

    }

    public UpdateOperation(String name, long x) {
        super(name);
        this.x = x;
    }


    @Override
    public void run() throws Exception {
        getNumber().update(x);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(x);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        x = in.readLong();
    }

}
