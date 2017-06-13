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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamicConfigReplicationOperation extends AbstractDynamicConfigOperation {

    private IdentifiedDataSerializable[] configs;

    public DynamicConfigReplicationOperation(Map<?, ? extends IdentifiedDataSerializable>...configs) {
        List<IdentifiedDataSerializable> allConfigs = new ArrayList<IdentifiedDataSerializable>();
        for (Map<?, ? extends IdentifiedDataSerializable> config : configs) {
            allConfigs.addAll(config.values());
        }
        this.configs = allConfigs.toArray(new IdentifiedDataSerializable[0]);
    }

    public DynamicConfigReplicationOperation() {

    }

    @Override
    public void run() throws Exception {
        ClusterWideConfigurationService service = getService();
        for (IdentifiedDataSerializable multiMapConfig : configs) {
            service.registerLocally(multiMapConfig);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(configs.length);
        for (IdentifiedDataSerializable config: configs) {
            out.writeObject(config);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        configs = new IdentifiedDataSerializable[size];
        for (int i = 0; i < size; i++) {
            configs[i] = in.readObject();
        }
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.REPLICATE_CONFIGURATIONS_OP;
    }
}
