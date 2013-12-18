package com.hazelcast.concurrent.longmaxupdater;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class UpdateBackupOperation extends LongMaxUpdaterBaseOperation {

    private long x;

    public UpdateBackupOperation() {
    }

    public UpdateBackupOperation(String name, long x) {
        super(name);
        this.x = x;
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

    @Override
    public void run() throws Exception {
        getNumber().set(x);
    }
}
