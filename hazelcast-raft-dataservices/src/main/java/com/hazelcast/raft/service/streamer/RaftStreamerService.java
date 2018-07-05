package com.hazelcast.raft.service.streamer;

import com.hazelcast.config.StreamerConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftStreamerConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftGroupLifecycleAwareService;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongSnapshot;
import com.hazelcast.raft.service.spi.RaftRemoteService;
import com.hazelcast.raft.service.streamer.proxy.RaftStreamerProxy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.streamer.Streamer;
import com.hazelcast.streamer.impl.DummyStore;
import com.hazelcast.util.ExceptionUtil;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class RaftStreamerService implements ManagedService, RaftRemoteService, RaftGroupLifecycleAwareService, SnapshotAwareService<Void> {
    public static final String SERVICE_NAME = "hz:raft:streamerService";
    private final NodeEngine nodeEngine;
    private RaftService raftService;
    private final Map<Tuple2<RaftGroupId, String>, DummyStore> stores = new ConcurrentHashMap<Tuple2<RaftGroupId, String>, DummyStore>();


    public RaftStreamerService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }


    @Override
    public Void takeSnapshot(RaftGroupId groupId, long commitIndex) {
        return null;
//        throw new UnsupportedOperationException("snapshots not implemeted yet");
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, Void snapshot) {
//        throw new UnsupportedOperationException("snapshots not implemeted yet");
    }

    @Override
    public void onGroupDestroy(RaftGroupId groupId) {

    }

    @Override
    public Streamer createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = createRaftGroup(name).get();
            return new RaftStreamerProxy(name, groupId, raftService.getInvocationManager(), nodeEngine.getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String name) {
        String raftGroupRef = getRaftGroupRef(name);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        return invocationManager.createRaftGroup(raftGroupRef);
    }

    private String getRaftGroupRef(String name) {
        RaftStreamerConfig config = getConfig(name);
        return config != null ? config.getRaftGroupRef() : RaftGroupConfig.DEFAULT_GROUP;
    }

    private RaftStreamerConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftStreamerConfig(name);
    }

    @Override
    public boolean destroyRaftObject(RaftGroupId groupId, String objectName) {
        return false;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    public DummyStore getStore(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Tuple2<RaftGroupId, String> key = Tuple2.of(groupId, name);
//        if (destroyedLongs.contains(key)) {
//            throw new DistributedObjectDestroyedException("AtomicLong[" + name + "] is already destroyed!");
//        }
        DummyStore store = stores.get(key);
        if (store == null) {
//            atomicLong = new DummyStore(groupId, groupId.name());
            StreamerConfig config = new StreamerConfig().setOverflowDir("/tmp/raft");
            store = new DummyStore(name, 0, 1, config);
            stores.put(key, store);
        }
        return store;
    }
}
