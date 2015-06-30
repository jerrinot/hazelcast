/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.partition.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.SelfResponseProviderReplicationOperation;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class ReplicaSyncRequest extends Operation implements PartitionAwareOperation, MigrationCycleOperation {

    public ReplicaSyncRequest() {
    }

    public ReplicaSyncRequest(int partitionId, int replicaIndex) {
        setPartitionId(partitionId);
        setReplicaIndex(replicaIndex);
    }

    @Override
    public void beforeRun() throws Exception {
        int syncReplicaIndex = getReplicaIndex();
        if (syncReplicaIndex < 1 || syncReplicaIndex > InternalPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index should be in range [1-"
                    + InternalPartition.MAX_BACKUP_COUNT + "]");
        }
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        long[] replicaVersions = partitionService.getPartitionReplicaVersions(partitionId);

        if (!partitionService.isMigrationActive()) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Migration is paused! Cannot run replica sync -> " + toString());
            }
            sendRetryResponse();
            return;
        }

        if (!preCheckReplicaSync(nodeEngine, partitionId, replicaIndex)) {
            return;
        }

        try {
            List<Operation> tasks = createReplicationOperations();
            if (tasks.isEmpty()) {
                logNoReplicaDataFound(partitionId, replicaIndex);
                sendEmptyResponse(replicaVersions);
            } else {
                List<SelfResponseProviderReplicationOperation> selfResponseProviderTasks =
                        new ArrayList<SelfResponseProviderReplicationOperation>();
                Iterator<Operation> taskIt = tasks.iterator();
                while (taskIt.hasNext()) {
                    Operation task = taskIt.next();
                    if (task instanceof SelfResponseProviderReplicationOperation) {
                        selfResponseProviderTasks.add((SelfResponseProviderReplicationOperation) task);
                        taskIt.remove();
                    }
                }
                sendResponse(tasks, replicaVersions, null);
                for (SelfResponseProviderReplicationOperation selfResponseProviderTask : selfResponseProviderTasks) {
                    List<Operation> taskList = ((List) Arrays.asList(selfResponseProviderTask));
                    sendResponse(taskList,
                                 replicaVersions,
                                 selfResponseProviderTask.createReplicaSyncResponse(taskList, replicaVersions));
                }
            }
        } finally {
            partitionService.releaseReplicaSyncPermit();
        }
    }

    private boolean preCheckReplicaSync(NodeEngineImpl nodeEngine, int partitionId, int replicaIndex) throws IOException {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        InternalPartitionImpl partition = partitionService.getPartition(partitionId);
        Address owner = partition.getOwnerOrNull();
        long[] replicaVersions = partitionService.getPartitionReplicaVersions(partitionId);
        long currentVersion = replicaVersions[replicaIndex - 1];

        ILogger logger = getLogger();
        if (!nodeEngine.getThisAddress().equals(owner)) {
            if (logger.isFinestEnabled()) {
                logger.finest("Wrong target! " + toString() + " cannot be processed! Target should be: " + owner);
            }
            sendRetryResponse();
            return false;
        }

        if (currentVersion == 0) {
            if (logger.isFinestEnabled()) {
                logger.finest("Current replicaVersion=0, sending empty response for partitionId="
                        + getPartitionId() + ", replicaIndex=" + getReplicaIndex() + ", replicaVersions="
                        + Arrays.toString(replicaVersions));
            }
            sendEmptyResponse(replicaVersions);
            return false;
        }

        if (!partitionService.tryToAcquireReplicaSyncPermit()) {
            if (logger.isFinestEnabled()) {
                logger.finest(
                        "Max parallel replication process limit exceeded! Could not run replica sync -> " + toString());
            }
            sendRetryResponse();
            return false;
        }
        return true;
    }

    private void sendRetryResponse() {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        ReplicaSyncRetryResponse response = new ReplicaSyncRetryResponse();
        response.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        Address target = getCallerAddress();
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(response, target);
    }

    private List<Operation> createReplicationOperations() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);
        PartitionReplicationEvent event = new PartitionReplicationEvent(getPartitionId(), getReplicaIndex());
        List<Operation> tasks = new LinkedList<Operation>();
        for (ServiceInfo serviceInfo : services) {
            MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
            Operation op = service.prepareReplicationOperation(event);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                tasks.add(op);
            }
        }
        return tasks;
    }

    private void sendEmptyResponse(long[] replicaVersions) throws IOException {
        sendResponse(null, replicaVersions, null);
    }

    private void sendResponse(List<Operation> data,
                              long[] replicaVersions,
                              ReplicaSyncResponse response) throws IOException {
        NodeEngine nodeEngine = getNodeEngine();

        ReplicaSyncResponse syncResponse = createResponse(data, replicaVersions, response);
        Address target = getCallerAddress();
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Sending sync response to -> " + target + " for partitionId="
                    + getPartitionId() + ", replicaIndex=" + getReplicaIndex());
        }
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(syncResponse, target);
    }

    private ReplicaSyncResponse createResponse(List<Operation> data,
                                               long[] replicaVersions,
                                               ReplicaSyncResponse syncResponse) throws IOException {
        int partitionId = getPartitionId();
        if (syncResponse == null) {
            syncResponse = new ReplicaSyncResponse(data, replicaVersions);
        }
        syncResponse.setPartitionId(partitionId).setReplicaIndex(getReplicaIndex());
        return syncResponse;
    }

    private void logNoReplicaDataFound(int partitionId, int replicaIndex) {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        ILogger logger = nodeEngine.getLogger(getClass());

        if (logger.isFinestEnabled()) {
            logger.finest("No replica data is found for partitionId=" + partitionId + ", replicaIndex=" + replicaIndex);
        }
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{partitionId=" + getPartitionId() + ", replicaIndex=" + getReplicaIndex() + '}';
    }
}
