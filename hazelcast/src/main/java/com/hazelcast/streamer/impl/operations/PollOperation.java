package com.hazelcast.streamer.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.streamer.impl.StreamerService;
import com.hazelcast.streamer.impl.PollResult;
import com.hazelcast.streamer.impl.StreamerWaitNotifyKey;

import java.io.IOException;

public class PollOperation<T> extends Operation implements DataSerializable, BlockingOperation {

    private String name;
    private long offset;
    private int minRecords;
    private int maxRecords;

    private PollResult<T> response;

    public PollOperation() {

    }

    public PollOperation(String name, long offset, int minRecords, int maxRecords) {
        this.name = name;
        this.offset = offset;
        this.minRecords = minRecords;
        this.maxRecords = maxRecords;
    }

    @Override
    public PollResult<T> getResponse() {
        return response;
    }

    @Override
    public String getServiceName() {
        return StreamerService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeLong(offset);
        out.writeInt(minRecords);
        out.writeInt(maxRecords);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        offset = in.readLong();
        minRecords = in.readInt();
        maxRecords = in.readInt();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new StreamerWaitNotifyKey(name, getPartitionId());
    }

    @Override
    public void run() throws Exception {
        //already done in shouldWait
    }

    @Override
    public boolean shouldWait() {
        if (response == null) {
            response = new PollResult<T>();
        }

        StreamerService service = getService();
        int read = service.read(name, getPartitionId(), offset, maxRecords, response);
        minRecords -= read;
        maxRecords -= read;
        offset += read;

        return minRecords > 0;
    }

    @Override
    public void onWaitExpire() {
        getOperationResponseHandler().sendResponse(this, response);
    }
}
