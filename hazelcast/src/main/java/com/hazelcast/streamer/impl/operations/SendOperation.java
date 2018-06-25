package com.hazelcast.streamer.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.streamer.impl.StreamerService;
import com.hazelcast.streamer.impl.StreamerWaitNotifyKey;

import java.io.IOException;

//todo: implement IDS
public class SendOperation extends Operation implements DataSerializable, Notifier, BackupAwareOperation {
    private Data value;
    private String name;

    public SendOperation() {

    }

    public SendOperation(Data value, String name) {
        this.value = value;
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        StreamerService service = getService();
        service.addValue(name, getPartitionId(), value);
    }

    @Override
    public String getServiceName() {
        return StreamerService.SERVICE_NAME;
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

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new StreamerWaitNotifyKey(name, getPartitionId());
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return 1;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new BackupSendOperation(value, name);
    }
}
