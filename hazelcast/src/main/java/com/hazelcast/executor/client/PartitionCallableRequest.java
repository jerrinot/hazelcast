/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor.client;

import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.executor.CallableTaskOperation;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.executor.ExecutorPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * This class is used for sending the task to a particular partition
 */
public class PartitionCallableRequest extends PartitionClientRequest {

    private String name;
    private UUID uuid;
    private Callable callable;
    private int partitionId;

    public PartitionCallableRequest() {
    }

    public PartitionCallableRequest(String name, UUID uuid, Callable callable, int partitionId) {
        this.name = name;
        this.uuid = uuid;
        this.callable = callable;
        this.partitionId = partitionId;
    }

    @Override
    protected Operation prepareOperation() {
        SecurityContext securityContext = getClientEngine().getSecurityContext();
        if (securityContext != null) {
            callable = securityContext.createSecureCallable(getEndpoint().getSubject(), callable);
        }
        return new CallableTaskOperation(name, uuid, callable);
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ExecutorPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ExecutorPortableHook.PARTITION_CALLABLE_REQUEST;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeLong("u-l", uuid.getLeastSignificantBits());
        writer.writeLong("u-m", uuid.getMostSignificantBits());
        writer.writeInt("p", partitionId);
        ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        rawDataOutput.writeObject(callable);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        long leastSig = reader.readLong("u-l");
        long mostSig = reader.readLong("u-m");
        uuid = new UUID(mostSig, leastSig);
        partitionId = reader.readInt("p");
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        callable = rawDataInput.readObject();
    }
}
