package com.hazelcast.streamer.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.streamer.impl.StreamerService;

import java.io.IOException;

public class BackupSendOperation extends Operation {

    private Data value;
    private String name;

    public BackupSendOperation() {

    }

    public BackupSendOperation(Data value, String name) {
        this.value = value;
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return StreamerService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        StreamerService service = getService();
        service.addValue(name, getPartitionId(), value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readData();
        name = in.readUTF();
    }
}