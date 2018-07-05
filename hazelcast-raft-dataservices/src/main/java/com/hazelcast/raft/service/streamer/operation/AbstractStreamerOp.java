package com.hazelcast.raft.service.streamer.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.streamer.RaftStreamerService;
import com.hazelcast.streamer.impl.DummyStore;

import java.io.IOException;

public abstract class AbstractStreamerOp extends RaftOp  {
    private String name;

    public AbstractStreamerOp(String name) {
        this.name = name;
    }

    public AbstractStreamerOp() {

    }

    @Override
    protected String getServiceName() {
        return RaftStreamerService.SERVICE_NAME;
    }

    DummyStore getStore(RaftGroupId groupId) {
        RaftStreamerService service = getService();
        return service.getStore(groupId, name);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }
}
