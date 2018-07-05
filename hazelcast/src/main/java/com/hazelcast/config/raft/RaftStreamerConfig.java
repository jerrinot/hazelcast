package com.hazelcast.config.raft;

public class RaftStreamerConfig extends AbstractRaftObjectConfig {
    public RaftStreamerConfig() {
        super();
    }

    public RaftStreamerConfig(String name, String raftGroupRef) {
        super(name, raftGroupRef);
    }
}
