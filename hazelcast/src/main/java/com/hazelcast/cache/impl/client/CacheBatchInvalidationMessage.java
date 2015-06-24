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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CacheBatchInvalidationMessage extends CacheInvalidationMessage {

    private List<String> sourceUuids;
    private List<Data> keys;

    public CacheBatchInvalidationMessage() {

    }

    public CacheBatchInvalidationMessage(String name) {
        super(name);
        this.sourceUuids = new ArrayList<String>();
        this.keys = new ArrayList<Data>();
    }

    public CacheBatchInvalidationMessage(String name, int expectedMessageCount) {
        super(name);
        this.sourceUuids = new ArrayList<String>(expectedMessageCount);
        this.keys = new ArrayList<Data>(expectedMessageCount);
    }

    public CacheBatchInvalidationMessage(String name, List<String> sourceUuids, List<Data> keys) {
        super(name);
        this.sourceUuids = sourceUuids;
        this.keys = keys;
    }

    public CacheBatchInvalidationMessage addInvalidationMessage(String sourceUuid, Data key) {
        sourceUuids.add(sourceUuid);
        keys.add(key);
        return this;
    }

    public List<String> getSourceUuids() {
        return sourceUuids;
    }

    public List<Data> getKeys() {
        return keys;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.BATCH_INVALIDATION_MESSAGE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        boolean hasInvalidationMessages = keys != null;
        out.writeBoolean(hasInvalidationMessages);
        if (hasInvalidationMessages) {
            out.writeInt(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                out.writeUTF(sourceUuids.get(i));
                out.writeData(keys.get(i));
            }
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        ObjectDataInput in = reader.getRawDataInput();
        if (in.readBoolean()) {
            int size = in.readInt();
            sourceUuids = new ArrayList<String>(size);
            keys = new ArrayList<Data>(size);
            for (int i = 0; i < size; i++) {
                sourceUuids.add(in.readUTF());
                keys.add(in.readData());
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheBatchInvalidationMessage{");
        sb.append("name='").append(name).append('\'');
        sb.append(", sourceUuids=").append(sourceUuids);
        sb.append(", keys=").append(keys);
        sb.append('}');
        return sb.toString();
    }

}
