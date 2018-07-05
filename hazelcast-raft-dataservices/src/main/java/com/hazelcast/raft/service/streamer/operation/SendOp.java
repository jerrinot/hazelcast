package com.hazelcast.raft.service.streamer.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.streamer.RaftStreamerService;
import com.hazelcast.streamer.impl.DummyStore;

import java.io.IOException;

public class SendOp extends AbstractStreamerOp {
    private Data data;

    public SendOp(String name, Data data) {
        super(name);
        this.data = data;
    }

    public SendOp() {
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) throws Exception {
        DummyStore store = getStore(groupId);
        store.add(data);
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeData(data);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        data = in.readData();
    }
}
