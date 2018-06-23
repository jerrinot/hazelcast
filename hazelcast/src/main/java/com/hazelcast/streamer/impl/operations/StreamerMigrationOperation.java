package com.hazelcast.streamer.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.streamer.impl.StreamerService;

import java.io.IOException;

public class StreamerMigrationOperation extends Operation implements DataSerializable {

    private byte[] stores;

    public StreamerMigrationOperation(byte[] stores) {
        this.stores = stores;
    }

    public StreamerMigrationOperation() {

    }

    @Override
    public void run() throws Exception {
        StreamerService service = getService();
        service.restoreStores(stores);
    }

    @Override
    public String getServiceName() {
        return StreamerService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeByteArray(stores);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        stores = in.readByteArray();
    }
}
