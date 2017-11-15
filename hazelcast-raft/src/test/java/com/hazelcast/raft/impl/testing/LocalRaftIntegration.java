package com.hazelcast.raft.impl.testing;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.impl.executionservice.impl.DelegatingTaskScheduler;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.version.MemberVersion;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LocalRaftIntegration implements RaftIntegration {

    private final RaftEndpoint localEndpoint;
    private final SnapshotAwareService service;
    private final StripedExecutor stripedExecutor;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ConcurrentMap<RaftEndpoint, RaftNode> nodes = new ConcurrentHashMap<RaftEndpoint, RaftNode>();
    private final SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private final LoggingServiceImpl loggingService;

    private final Set<EndpointDropEntry> endpointDropRules = Collections.newSetFromMap(new ConcurrentHashMap<EndpointDropEntry, Boolean>());
    private final Set<Class> dropAllRules = Collections.newSetFromMap(new ConcurrentHashMap<Class, Boolean>());

    public LocalRaftIntegration(RaftEndpoint localEndpoint, SnapshotAwareService service) {
        this.localEndpoint = localEndpoint;
        this.service = service;
        this.loggingService = new LoggingServiceImpl("dev", "log4j2", BuildInfoProvider.getBuildInfo());
        loggingService.setThisMember(getThisMember(localEndpoint));
        this.stripedExecutor = new StripedExecutor(getLogger("executor"), "executor", 1, Integer.MAX_VALUE);
    }

    private MemberImpl getThisMember(RaftEndpoint localEndpoint) {
        return new MemberImpl(localEndpoint.getAddress(), MemberVersion.of(Versions.CURRENT_CLUSTER_VERSION.toString()), true, localEndpoint.getUid());
    }

    public void discoverNode(RaftNode node) {
        assertNotEquals(localEndpoint, node.getLocalEndpoint());
        RaftNode old = nodes.putIfAbsent(node.getLocalEndpoint(), node);
        assertThat(old, anyOf(nullValue(), sameInstance(node)));
    }

    public boolean removeNode(RaftNode node) {
        assertNotEquals(localEndpoint, node.getLocalEndpoint());
        return nodes.remove(node.getLocalEndpoint(), node);
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    @Override
    public TaskScheduler getTaskScheduler() {
        return new DelegatingTaskScheduler(scheduledExecutor, executor);
    }

    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return loggingService.getLogger(clazz);
    }

    @Override
    public boolean isJoined() {
        return true;
    }

    @Override
    public boolean isReachable(RaftEndpoint endpoint) {
        return localEndpoint.equals(endpoint) || nodes.containsKey(endpoint);
    }

    @Override
    public boolean send(VoteRequest request, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNode node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handleVoteRequest(request);
        return true;
    }

    @Override
    public boolean send(VoteResponse response, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNode node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handleVoteResponse(response);
        return true;
    }

    @Override
    public boolean send(AppendRequest request, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNode node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        request = serializationService.toObject(serializationService.toData(request));
        node.handleAppendRequest(request);
        return true;
    }

    @Override
    public boolean send(AppendSuccessResponse response, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNode node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handleAppendResponse(response);
        return true;
    }

    @Override
    public boolean send(AppendFailureResponse response, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNode node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handleAppendResponse(response);
        return true;
    }

    @Override
    public boolean send(InstallSnapshot request, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNode node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handleInstallSnapshot(request);
        return true;
    }

    private boolean shouldDrop(Object message, RaftEndpoint target) {
        return dropAllRules.contains(message.getClass())
                || endpointDropRules.contains(new EndpointDropEntry(message.getClass(), target));
    }

    @Override
    public Object runOperation(RaftOperation operation, int commitIndex) {
        if (operation == null) {
            return null;
        }
        operation.setService(service);
        operation.setCommitIndex(commitIndex);
        try {
            operation.beforeRun();
            operation.run();
            operation.afterRun();
            return operation.getResponse();
        } catch (Throwable t) {
            return t;
        }
    }

    @Override
    public long getLeaderElectionTimeoutInMillis() {
        return 2000;
    }

    @Override
    public long getHeartbeatPeriodInMillis() {
        return 5000;
    }

    @Override
    public int getAppendRequestMaxEntryCount() {
        return 20;
    }

    @Override
    public int getCommitIndexAdvanceCountToSnapshot() {
        return 50;
    }

    @Override
    public int getUncommittedEntryCountToRejectNewAppends() {
        return 10000;
    }

    void dropMessagesToEndpoint(RaftEndpoint endpoint, Class messageType) {
        endpointDropRules.add(new EndpointDropEntry(messageType, endpoint));
    }

    void allowMessagesToEndpoint(RaftEndpoint endpoint, Class messageType) {
        endpointDropRules.remove(new EndpointDropEntry(messageType, endpoint));
    }

    void allowAllMessagesToEndpoint(RaftEndpoint endpoint) {
        Iterator<EndpointDropEntry> iter = endpointDropRules.iterator();
        while (iter.hasNext()) {
            EndpointDropEntry entry = iter.next();
            if (endpoint.equals(entry.endpoint)) {
                iter.remove();
            }
        }
    }

    void dropMessagesToAll(Class messageType) {
        dropAllRules.add(messageType);
    }

    void allowMessagesToAll(Class messageType) {
        dropAllRules.remove(messageType);
    }

    void resetAllDropRules() {
        dropAllRules.clear();
        endpointDropRules.clear();
    }

    public <T extends SnapshotAwareService> T getService() {
        return (T) service;
    }

    void shutdown() {
        stripedExecutor.shutdown();
        scheduledExecutor.shutdown();
        executor.shutdown();
    }

    StripedExecutor getStripedExecutor() {
        return stripedExecutor;
    }

    boolean isAlive() {
        return stripedExecutor.isLive();
    }

    private static class EndpointDropEntry {
        final Class messageType;
        final RaftEndpoint endpoint;

        private EndpointDropEntry(Class messageType, RaftEndpoint endpoint) {
            this.messageType = messageType;
            this.endpoint = endpoint;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EndpointDropEntry)) return false;

            EndpointDropEntry that = (EndpointDropEntry) o;
            return messageType.equals(that.messageType) && endpoint.equals(that.endpoint);
        }

        @Override
        public int hashCode() {
            int result = messageType.hashCode();
            result = 31 * result + endpoint.hashCode();
            return result;
        }
    }
}