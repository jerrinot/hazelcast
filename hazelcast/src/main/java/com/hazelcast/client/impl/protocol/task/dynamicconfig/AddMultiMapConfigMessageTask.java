/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MapSizeCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.function.Supplier;

import java.security.Permission;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.internal.dynamicconfig.ConfigurationService.SERVICE_NAME;

public class AddMultiMapConfigMessageTask extends AbstractMultiTargetMessageTask<DynamicConfigAddMultiMapConfigCodec.RequestParameters> {

    public AddMultiMapConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Permission getRequiredPermission() {
        // todo proper security for client-side config updates
        return null;
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setName(parameters.name);
        multiMapConfig.setValueCollectionType(parameters.collectionType);
        multiMapConfig.setAsyncBackupCount(parameters.asyncBackupCount);
        multiMapConfig.setBackupCount(parameters.backupCount);
        multiMapConfig.setBinary(parameters.isBinary);
        multiMapConfig.setStatisticsEnabled(parameters.statisticsEnabled);
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            for (EntryListenerConfigHolder configHolder : parameters.listenerConfigs) {
                EntryListenerConfig entryListenerConfig = configHolder.asEntryListenerConfig(serializationService);
                multiMapConfig.addEntryListenerConfig(entryListenerConfig);
            }
        }
        return new AddDynamicConfigOperationSupplier(multiMapConfig);
    }

    @Override
    public Collection<Member> getTargets() {
        return nodeEngine.getClusterService().getMembers();
    }

    @Override
    protected Object reduce(Map map) throws Throwable {
        for (Object result : map.values()) {
            if (result instanceof Throwable) {
                throw (Throwable) result;
            }
        }
        return true;
    }

    @Override
    protected DynamicConfigAddMultiMapConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddMultiMapConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        // todo no need for a response, help!
        return MapSizeCodec.encodeResponse(10);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }


}
