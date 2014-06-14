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

package com.hazelcast.cluster.client;

import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.cluster.MemberAttributeOperationType.PUT;

public class MemberAttributeChange implements DataSerializable {

    private UUID uuid;
    private MemberAttributeOperationType operationType;
    private String key;
    private Object value;

    public MemberAttributeChange() {
    }

    public MemberAttributeChange(UUID uuid, MemberAttributeOperationType operationType, String key, Object value) {
        this.uuid = uuid;
        this.operationType = operationType;
        this.key = key;
        this.value = value;
    }

    public UUID getUuid() {
        return uuid;
    }

    public MemberAttributeOperationType getOperationType() {
        return operationType;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(uuid.getLeastSignificantBits());
        out.writeLong(uuid.getMostSignificantBits());
        out.writeUTF(key);
        out.writeByte(operationType.getId());
        if (operationType == PUT) {
            IOUtil.writeAttributeValue(value, out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        long leastSig = in.readLong();
        long mostSig = in.readLong();
        uuid = new UUID(mostSig, leastSig);
        key = in.readUTF();
        operationType = MemberAttributeOperationType.getValue(in.readByte());
        if (operationType == PUT) {
            value = IOUtil.readAttributeValue(in);
        }
    }
}
