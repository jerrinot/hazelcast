package com.hazelcast.streamer.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.streamer.impl.StreamerService;
import com.hazelcast.streamer.impl.DummyStore;

import java.io.IOException;
import java.util.List;

public class StreamerMigrationOperation extends Operation implements DataSerializable {

    private List<DummyStore<?>> stores;

    public StreamerMigrationOperation() {

    }

    public StreamerMigrationOperation(List<DummyStore<?>> stores) {
        this.stores = stores;
    }

    @Override
    public void run() throws Exception {
        StreamerService service = getService();
        service.addStores(stores);
    }

    @Override
    public String getServiceName() {
        return StreamerService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(stores);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        stores = in.readObject();
    }
}
