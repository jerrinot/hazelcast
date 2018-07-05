package com.hazelcast.raft.service.streamer.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.streamer.impl.DummyStore;
import com.hazelcast.streamer.impl.InternalPollResult;

import java.io.IOException;

public class PollOp extends AbstractStreamerOp {
    private long offset;
    private int maxRecords;

    public PollOp(String name, long offset, int maxRecords) {
        super(name);
        this.offset = offset;
        this.maxRecords = maxRecords;
    }

    public PollOp() {

    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        DummyStore store = getStore(groupId);
        final InternalPollResult internalPollResult = new InternalPollResult(maxRecords, offset);
        store.read(offset, internalPollResult);
        return internalPollResult;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(offset);
        out.writeInt(maxRecords);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        offset = in.readLong();
        maxRecords = in.readInt();
    }
}
