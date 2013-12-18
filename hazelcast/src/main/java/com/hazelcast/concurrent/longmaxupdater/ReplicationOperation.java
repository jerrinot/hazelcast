package com.hazelcast.concurrent.longmaxupdater;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReplicationOperation extends AbstractOperation {
    private Map<String, Long> migrationData;

    public ReplicationOperation() {

    }

    public ReplicationOperation(Map<String, Long> migrationData) {
        this.migrationData = migrationData;
    }


    @Override
    public void run() throws Exception {
        LongMaxUpdaterService service = getService();
        for (Map.Entry<String, Long> entry : migrationData.entrySet()) {
            String name = entry.getKey();
            Long value = entry.getValue();
            service.getNumber(name).update(value);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        migrationData = new HashMap<String, Long>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            Long value = in.readLong();
            migrationData.put(name, value);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, Long> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }
}
